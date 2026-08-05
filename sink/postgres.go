package sink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// PostgresSink writes records to a Postgres database in batches.
// It implements the Sink interface for use in weibo pipelines.
//
// Configure a PostgresSink with functional options via NewPostgresSink:
//
//	sink := sink.NewPostgresSink(
//	    sink.PostgresDSN("postgres://user:pass@localhost:5432/dbname?sslmode=disable"),
//	    sink.PostgresMapper(func(r types.Record) (string, []string, []any) {
//	        o := r.Parsed.(*Order)
//	        return "orders",
//	            []string{"order_id", "customer", "amount"},
//	            []any{o.OrderID, o.Customer, o.Amount}
//	    }),
//	    sink.PostgresBatchSize(500),
//	)
//
// Records are accumulated and inserted in batches using multi-value INSERT
// statements for efficiency. On context cancellation, the sink drains
// remaining records for up to 5 seconds before flushing the final batch.
type PostgresSink struct {
	cfg  postgresSinkConfig
	pool *pgxpool.Pool
}

// NewPostgresSink creates a Sink that writes to Postgres.
// DSN and a RecordMapper are required; if missing, NewPostgresSink panics.
//
// The connection pool is created immediately. If the database is unreachable,
// NewPostgresSink panics to fail fast at construction time.
func NewPostgresSink(opts ...PostgresSinkOption) *PostgresSink {
	cfg := postgresSinkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if cfg.dsn == "" {
		panic("weibo/sink: PostgresSink requires PostgresDSN(...)")
	}
	if cfg.mapper == nil {
		panic("weibo/sink: PostgresSink requires PostgresMapper(...)")
	}
	if err := cfg.validateWriteMode(); err != nil {
		panic(fmt.Sprintf("weibo/sink: %v", err))
	}

	pool, err := pgxpool.New(context.Background(), cfg.dsn)
	if err != nil {
		panic(fmt.Sprintf("weibo/sink: postgres connection failed: %v", err))
	}

	return &PostgresSink{cfg: cfg, pool: pool}
}

// pendingRow holds a mapped record waiting to be batch-inserted.
type pendingRow struct {
	table   string
	columns []string
	values  []any
	record  types.Record
}

// Write reads records from the input channel and writes them to Postgres
// in batches. On context cancellation, the sink drains remaining records
// for up to shutdownTimeout before flushing.
func (p *PostgresSink) Write(ctx context.Context, in <-chan types.Record) error {
	defer p.pool.Close()

	bw := &batchWriter[pendingRow]{
		batchSize:     p.cfg.batchSize,
		flushInterval: p.cfg.flushInterval,
		// Synchronous: each insert applies backpressure, and rows within
		// a batch are grouped per table, so overlapping flushes could
		// interleave writes to the same table.
		async: false,
		convert: func(r types.Record) (pendingRow, bool) {
			row := p.mapRecord(r)
			if row == nil {
				return pendingRow{}, false // mapper declined this record
			}
			return *row, true
		},
		flush: p.insertBatch,
	}
	return bw.run(ctx, in)
}

// mapRecord runs the user-provided mapper on a record.
// Returns nil if the mapper returns an empty table or mismatched columns/values.
func (p *PostgresSink) mapRecord(r types.Record) *pendingRow {
	table, columns, values := p.cfg.mapper(r)
	if table == "" || len(columns) == 0 || len(columns) != len(values) {
		return nil
	}
	return &pendingRow{table: table, columns: columns, values: values, record: r}
}

// insertBatch groups rows by table+columns and inserts each group with
// a single multi-value INSERT statement. Failed batches are retried up
// to cfg.maxRetries times with exponential backoff.
func (p *PostgresSink) insertBatch(ctx context.Context, rows []pendingRow) error {
	// Group rows by table + column signature.
	type groupKey struct {
		table   string
		columns string
	}
	groups := make(map[groupKey][]pendingRow)
	order := []groupKey{}

	for _, row := range rows {
		key := groupKey{table: row.table, columns: strings.Join(row.columns, ",")}
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], row)
	}

	for _, key := range order {
		groupRows := groups[key]
		if err := p.insertGroupWithRetry(ctx, key.table, groupRows[0].columns, groupRows); err != nil {
			return fmt.Errorf("postgres insert into %s: %w", key.table, err)
		}
	}

	return nil
}

// insertGroupWithRetry inserts a group of rows with the same table+columns
// using a single multi-value INSERT, retrying on failure. After all retries
// are exhausted, the failure policy is applied to each row.
func (p *PostgresSink) insertGroupWithRetry(ctx context.Context, table string, columns []string, rows []pendingRow) error {
	query := buildPostgresWriteQuery(postgresWriteQuery{
		Table:           table,
		Columns:         columns,
		RowCount:        len(rows),
		Mode:            p.cfg.mode,
		ConflictColumns: p.cfg.conflictCols,
		UpdateColumns:   p.cfg.updateCols,
	})

	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		args = append(args, row.values...)
	}

	var lastErr error
	for attempt := 0; attempt <= p.cfg.maxRetries; attempt++ {
		_, err := p.pool.Exec(ctx, query, args...)
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt < p.cfg.maxRetries {
			backoff := time.Duration(1<<attempt) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	// All retries exhausted — apply failure policy per row.
	for _, row := range rows {
		if ferr := applyFailurePolicy(ctx, p.cfg.failurePolicy, p.cfg.dlq, row.record); ferr != nil {
			return fmt.Errorf("postgres insert into %s: %w (failure policy: %w)", table, lastErr, ferr)
		}
	}
	return nil
}

type postgresWriteQuery struct {
	Table           string
	Columns         []string
	RowCount        int
	Mode            PostgresWriteMode
	ConflictColumns []string
	UpdateColumns   []string
}

// BuildPostgresWriteQuery constructs the SQL used by PostgresSink.
// It is exported for tests and SQL inspection; callers should still use
// NewPostgresSink for actual writes.
func BuildPostgresWriteQuery(table string, columns []string, rowCount int, mode PostgresWriteMode, conflictColumns, updateColumns []string) string {
	return buildPostgresWriteQuery(postgresWriteQuery{
		Table:           table,
		Columns:         columns,
		RowCount:        rowCount,
		Mode:            mode,
		ConflictColumns: conflictColumns,
		UpdateColumns:   updateColumns,
	})
}

// buildPostgresWriteQuery constructs a multi-value INSERT/UPSERT statement.
// Example: INSERT INTO "orders" ("order_id","amount") VALUES ($1,$2)
// ON CONFLICT ("order_id") DO UPDATE SET "amount"=EXCLUDED."amount"
func buildPostgresWriteQuery(q postgresWriteQuery) string {
	if q.Mode == "" {
		q.Mode = PostgresInsert
	}
	quotedCols := make([]string, len(q.Columns))
	for i, c := range q.Columns {
		quotedCols[i] = fmt.Sprintf(`"%s"`, c)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, `INSERT INTO %s (%s) VALUES `, quotePostgresTable(q.Table), strings.Join(quotedCols, ","))

	placeholder := 1
	for i := 0; i < q.RowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for j := range q.Columns {
			if j > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "$%d", placeholder)
			placeholder++
		}
		sb.WriteString(")")
	}

	if q.Mode == PostgresUpsert {
		sb.WriteString(" ON CONFLICT (")
		for i, col := range q.ConflictColumns {
			if i > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, `"%s"`, col)
		}
		sb.WriteString(")")

		updates := q.UpdateColumns
		if len(updates) == 0 {
			conflicts := map[string]bool{}
			for _, col := range q.ConflictColumns {
				conflicts[col] = true
			}
			for _, col := range q.Columns {
				if !conflicts[col] {
					updates = append(updates, col)
				}
			}
		}
		if len(updates) == 0 {
			sb.WriteString(" DO NOTHING")
		} else {
			sb.WriteString(" DO UPDATE SET ")
			for i, col := range updates {
				if i > 0 {
					sb.WriteString(",")
				}
				fmt.Fprintf(&sb, `"%s"=EXCLUDED."%s"`, col, col)
			}
		}
	}

	return sb.String()
}

func quotePostgresTable(table string) string {
	parts := strings.Split(table, ".")
	for i, part := range parts {
		parts[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(part, `"`, `""`))
	}
	return strings.Join(parts, ".")
}

// Compile-time check.
var _ Sink = (*PostgresSink)(nil)

// Describe returns metadata about this Postgres sink for the dashboard.
func (p *PostgresSink) Describe() SinkInfo {
	props := map[string]string{
		"batch_size":     fmt.Sprintf("%d", p.cfg.batchSize),
		"flush_interval": p.cfg.flushInterval.String(),
		"max_retries":    fmt.Sprintf("%d", p.cfg.maxRetries),
	}

	return SinkInfo{
		Type:  "Postgres",
		Props: props,
	}
}
