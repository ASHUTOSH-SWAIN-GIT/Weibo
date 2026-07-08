package sink

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"mailer/types"
)

// PostgresSink writes records to a Postgres database in batches.
// It implements the Sink interface for use in mailer pipelines.
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
		panic("mailer/sink: PostgresSink requires PostgresDSN(...)")
	}
	if cfg.mapper == nil {
		panic("mailer/sink: PostgresSink requires PostgresMapper(...)")
	}

	pool, err := pgxpool.New(context.Background(), cfg.dsn)
	if err != nil {
		panic(fmt.Sprintf("mailer/sink: postgres connection failed: %v", err))
	}

	return &PostgresSink{cfg: cfg, pool: pool}
}

// pendingRow holds a mapped record waiting to be batch-inserted.
type pendingRow struct {
	table   string
	columns []string
	values  []any
}

// Write reads records from the input channel and writes them to Postgres
// in batches. On context cancellation, the sink drains remaining records
// for up to shutdownTimeout before flushing.
func (p *PostgresSink) Write(ctx context.Context, in <-chan types.Record) error {
	defer p.pool.Close()

	const shutdownTimeout = 5 * time.Second

	var (
		batch []pendingRow
		mu    sync.Mutex
	)

	// flushBatch inserts all pending rows. Rows in a single batch are
	// grouped by table+columns and inserted with multi-value INSERTs.
	flushBatch := func(flushCtx context.Context) error {
		mu.Lock()
		if len(batch) == 0 {
			mu.Unlock()
			return nil
		}
		toWrite := batch
		batch = nil
		mu.Unlock()

		return p.insertBatch(flushCtx, toWrite)
	}

	// drain reads remaining records from the channel until it closes or
	// the timeout expires, mapping and accumulating them into the batch.
	drain := func() {
		deadline := time.NewTimer(shutdownTimeout)
		defer deadline.Stop()
		for {
			select {
			case record, ok := <-in:
				if !ok {
					return
				}
				row := p.mapRecord(record)
				if row == nil {
					continue
				}
				mu.Lock()
				batch = append(batch, *row)
				mu.Unlock()
			case <-deadline.C:
				return
			}
		}
	}

	// Periodic flush timer.
	ticker := time.NewTicker(p.cfg.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			drain()
			err := flushBatch(shutdownCtx)
			cancel()
			return err

		case record, ok := <-in:
			if !ok {
				return flushBatch(ctx)
			}

			row := p.mapRecord(record)
			if row == nil {
				continue
			}

			mu.Lock()
			batch = append(batch, *row)
			full := len(batch) >= p.cfg.batchSize
			mu.Unlock()

			if full {
				if err := flushBatch(ctx); err != nil {
					return err
				}
			}

		case <-ticker.C:
			if err := flushBatch(ctx); err != nil {
				return err
			}
		}
	}
}

// mapRecord runs the user-provided mapper on a record.
// Returns nil if the mapper returns an empty table or mismatched columns/values.
func (p *PostgresSink) mapRecord(r types.Record) *pendingRow {
	table, columns, values := p.cfg.mapper(r)
	if table == "" || len(columns) == 0 || len(columns) != len(values) {
		return nil
	}
	return &pendingRow{table: table, columns: columns, values: values}
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
// using a single multi-value INSERT, retrying on failure.
func (p *PostgresSink) insertGroupWithRetry(ctx context.Context, table string, columns []string, rows []pendingRow) error {
	query := buildInsertQuery(table, columns, len(rows))

	// Flatten all values into a single slice for the query.
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
	return lastErr
}

// buildInsertQuery constructs a multi-value INSERT statement.
// Example: INSERT INTO "orders" ("order_id","customer","amount") VALUES ($1,$2,$3),($4,$5,$6)
func buildInsertQuery(table string, columns []string, rowCount int) string {
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = fmt.Sprintf(`"%s"`, c)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, `INSERT INTO "%s" (%s) VALUES `, table, strings.Join(quotedCols, ","))

	placeholder := 1
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for j := range columns {
			if j > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "$%d", placeholder)
			placeholder++
		}
		sb.WriteString(")")
	}

	return sb.String()
}

// Compile-time check.
var _ Sink = (*PostgresSink)(nil)
