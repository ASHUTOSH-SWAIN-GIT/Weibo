package sink

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	wlog "github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/log"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/observability/trace"
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
// Construction is side-effect free: the connection pool is opened when Write
// starts. Use NewPostgresSinkE when you want errors instead of panics.
func NewPostgresSink(opts ...PostgresSinkOption) *PostgresSink {
	s, err := NewPostgresSinkE(opts...)
	if err != nil {
		panic(fmt.Sprintf("weibo/sink: %v", err))
	}
	return s
}

// NewPostgresSinkE creates a Postgres sink without panicking. It validates the
// static configuration but does not open a network connection.
func NewPostgresSinkE(opts ...PostgresSinkOption) (*PostgresSink, error) {
	cfg := postgresSinkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if cfg.dsn == "" {
		return nil, fmt.Errorf("PostgresSink requires PostgresDSN(...)")
	}
	if cfg.mapper == nil {
		return nil, fmt.Errorf("PostgresSink requires PostgresMapper(...)")
	}
	if err := cfg.validateWriteMode(); err != nil {
		return nil, err
	}

	return &PostgresSink{cfg: cfg}, nil
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
	if err := p.Open(ctx); err != nil {
		return err
	}
	defer p.Close()

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

// Open initializes the Postgres connection pool. It is called automatically by
// Write, and is exposed so runtime code can perform an explicit connectivity
// check when desired. Open is idempotent.
func (p *PostgresSink) Open(ctx context.Context) error {
	if p.pool != nil {
		return nil
	}
	pool, err := pgxpool.New(ctx, p.cfg.dsn)
	if err != nil {
		return fmt.Errorf("postgres connection pool: %w", err)
	}
	p.pool = pool
	return nil
}

// Check opens the pool if needed and verifies the database is reachable. It is
// intentionally opt-in so compile/validation remains side-effect free; callers
// should pass a context with a bounded timeout.
func (p *PostgresSink) Check(ctx context.Context) error {
	if err := p.Open(ctx); err != nil {
		return err
	}
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres connectivity check: %w", err)
	}
	return nil
}

// Close releases the Postgres pool if it has been opened.
func (p *PostgresSink) Close() {
	if p.pool == nil {
		return
	}
	p.pool.Close()
	p.pool = nil
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

// log returns the sink logger, defaulting to discard so unconfigured
// sinks stay silent as before.
func (p *PostgresSink) log() *slog.Logger {
	if p.cfg.logger != nil {
		return p.cfg.logger
	}
	return wlog.Discard()
}

// tracing returns the sink tracer, defaulting to no-op.
func (p *PostgresSink) tracing() trace.Tracer {
	if p.cfg.tracer != nil {
		return p.cfg.tracer
	}
	return trace.Noop()
}

// insertBatch groups rows by table+columns and inserts each group with
// a single multi-value INSERT statement. Failed batches are retried up
// to cfg.maxRetries times with exponential backoff.
func (p *PostgresSink) insertBatch(ctx context.Context, rows []pendingRow) error {
	ctx, span := p.tracing().Start(ctx, "sink.postgres.flush", trace.Int("rows", len(rows)))
	defer span.End()
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
			span.RecordError(err)
			p.log().Warn("postgres batch failed, applying failure policy",
				"table", key.table, "rows", len(groupRows), "error", err)
			return fmt.Errorf("postgres insert into %s: %w", key.table, err)
		}
	}

	return nil
}

// insertGroupWithRetry inserts a group of rows with the same table+columns
// using a single multi-value INSERT, retrying on failure. After all retries
// are exhausted, the failure policy is applied to each row.
func (p *PostgresSink) insertGroupWithRetry(ctx context.Context, table string, columns []string, rows []pendingRow) error {
	if p.cfg.mode == PostgresUpsert {
		rows = dedupeByConflictKey(rows, columns, p.cfg.conflictCols)
	}
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

// dedupeByConflictKey keeps only the last row per conflict key. Postgres
// rejects a single INSERT ... ON CONFLICT DO UPDATE that touches the same key
// twice ("cannot affect row a second time"), so duplicates within one batch
// are collapsed to the latest value, matching upsert semantics.
func dedupeByConflictKey(rows []pendingRow, columns, conflictCols []string) []pendingRow {
	idx := make([]int, 0, len(conflictCols))
	for _, cc := range conflictCols {
		for i, c := range columns {
			if c == cc {
				idx = append(idx, i)
				break
			}
		}
	}
	if len(idx) != len(conflictCols) {
		return rows
	}
	pos := make(map[string]int, len(rows))
	out := make([]pendingRow, 0, len(rows))
	for _, r := range rows {
		parts := make([]any, len(idx))
		for i, j := range idx {
			parts[i] = r.values[j]
		}
		key := fmt.Sprintf("%#v", parts)
		if at, ok := pos[key]; ok {
			out[at] = r
			continue
		}
		pos[key] = len(out)
		out = append(out, r)
	}
	return out
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
