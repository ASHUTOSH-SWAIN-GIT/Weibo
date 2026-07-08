package sink

import (
	"time"

	"mailer/types"
)

// RecordMapper extracts the table name, column names, and values from a Record.
// The returned columns and values must be the same length and in the same order.
//
// This is the core mapping function for the Postgres sink — it gives the
// user full control over how each Record maps to a database row.
//
// Example:
//
//	mapper := func(r types.Record) (string, []string, []any) {
//	    o := r.Parsed.(*Order)
//	    return "orders",
//	        []string{"order_id", "customer", "amount"},
//	        []any{o.OrderID, o.Customer, o.Amount}
//	}
type RecordMapper func(r types.Record) (table string, columns []string, values []any)

// postgresSinkConfig holds the resolved configuration for a PostgresSink.
type postgresSinkConfig struct {
	dsn           string
	mapper        RecordMapper
	batchSize     int
	flushInterval time.Duration
	maxRetries    int
}

// PostgresSinkOption configures a PostgresSink. Pass one or more to
// NewPostgresSink. DSN and a RecordMapper are required.
type PostgresSinkOption func(*postgresSinkConfig)

// PostgresDSN sets the Postgres connection string.
// Required. Example: "postgres://user:pass@localhost:5432/dbname?sslmode=disable".
func PostgresDSN(dsn string) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.dsn = dsn }
}

// PostgresMapper sets the function that maps each Record to a table,
// column list, and value list. Required.
func PostgresMapper(m RecordMapper) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.mapper = m }
}

// PostgresBatchSize sets the max records per INSERT batch (default 100).
// Larger batches reduce round-trips but increase memory usage.
func PostgresBatchSize(n int) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.batchSize = n }
}

// PostgresFlushInterval sets the max wait before flushing a partial batch
// (default 5s). Bounds latency even when the batch is not full.
func PostgresFlushInterval(d time.Duration) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.flushInterval = d }
}

// PostgresMaxRetries sets the number of times a failed batch insert is
// retried before giving up (default 3).
func PostgresMaxRetries(n int) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.maxRetries = n }
}

// applyDefaults fills in zero-value config fields with sensible defaults.
func (c *postgresSinkConfig) applyDefaults() {
	if c.batchSize == 0 {
		c.batchSize = 100
	}
	if c.flushInterval == 0 {
		c.flushInterval = 5 * time.Second
	}
	if c.maxRetries == 0 {
		c.maxRetries = 3
	}
}
