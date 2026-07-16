package sink

import (
	"fmt"
	"regexp"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

var postgresIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// PostgresWriteMode selects how rows are written.
type PostgresWriteMode string

const (
	PostgresInsert PostgresWriteMode = "insert"
	PostgresUpsert PostgresWriteMode = "upsert"
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
	mode          PostgresWriteMode
	conflictCols  []string
	updateCols    []string

	failurePolicy FailurePolicy
	dlq           DLQ
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

// PostgresMode sets whether writes use plain INSERT or INSERT ... ON
// CONFLICT DO UPDATE. Default is PostgresInsert.
func PostgresMode(mode PostgresWriteMode) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.mode = mode }
}

// PostgresConflictColumns sets the ON CONFLICT target for upserts.
func PostgresConflictColumns(cols ...string) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.conflictCols = append([]string(nil), cols...) }
}

// PostgresUpdateColumns sets the columns updated from EXCLUDED values
// on conflict. If omitted in upsert mode, all non-conflict inserted
// columns are updated.
func PostgresUpdateColumns(cols ...string) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.updateCols = append([]string(nil), cols...) }
}

// PostgresFailurePolicy sets what happens when a row fails after all retries.
// Default is FailurePolicyDrop (row is silently discarded).
func PostgresFailurePolicy(p FailurePolicy) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.failurePolicy = p }
}

// PostgresDLQ sets the dead-letter-queue for failed records.
// Only used when PostgresFailurePolicy is set to FailurePolicyDLQ.
func PostgresDLQ(dlq DLQ) PostgresSinkOption {
	return func(c *postgresSinkConfig) { c.dlq = dlq }
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
	if c.mode == "" {
		c.mode = PostgresInsert
	}
}

func (c postgresSinkConfig) validateWriteMode() error {
	switch c.mode {
	case PostgresInsert:
		if len(c.conflictCols) > 0 || len(c.updateCols) > 0 {
			return fmt.Errorf("postgres conflict/update columns require upsert mode")
		}
		return nil
	case PostgresUpsert:
		if len(c.conflictCols) == 0 {
			return fmt.Errorf("postgres upsert requires at least one conflict column")
		}
		for _, col := range c.conflictCols {
			if !postgresIdentRe.MatchString(col) {
				return fmt.Errorf("unsafe postgres conflict column %q", col)
			}
		}
		for _, col := range c.updateCols {
			if !postgresIdentRe.MatchString(col) {
				return fmt.Errorf("unsafe postgres update column %q", col)
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported postgres write mode %q", c.mode)
	}
}
