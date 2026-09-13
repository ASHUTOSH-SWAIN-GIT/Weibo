package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// ---------------------------------------------------------------------------
// Offline half: runs on every PR, no database needed.
// ---------------------------------------------------------------------------

// Misconfiguration fails fast at construction; an unreachable database
// fails fast at Check/Write instead of hanging the pipeline.
func TestTier_PostgresValidation(t *testing.T) {
	if _, err := sink.NewPostgresSinkE(); err == nil {
		t.Error("expected error with no DSN or mapper")
	}
	if _, err := sink.NewPostgresSinkE(
		sink.PostgresDSN("postgres://example/db"),
		sink.PostgresMapper(func(types.Record) (string, []string, []any) {
			return "", nil, nil
		}),
		sink.PostgresMode(sink.PostgresUpsert),
	); err == nil || !strings.Contains(err.Error(), "upsert requires") {
		t.Errorf("expected upsert validation error, got %v", err)
	}

	newDead := func(policy sink.FailurePolicy) *sink.PostgresSink {
		s, err := sink.NewPostgresSinkE(
			sink.PostgresDSN("postgres://user:pass@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"),
			sink.PostgresMapper(func(types.Record) (string, []string, []any) {
				return "t", []string{"id"}, []any{"1"}
			}),
			// Note: MaxRetries(1), not 0 — zero means "unset" and
			// falls back to the default of 3 (1+2+4s backoff).
			sink.PostgresMaxRetries(1),
			sink.PostgresFailurePolicy(policy),
		)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dead := newDead(sink.FailurePolicyFail)
	if err := dead.Check(ctx); err == nil {
		t.Error("Check against a dead database returned nil error")
	}
	if err := dead.Write(ctx, closedRecords(`{"id":"x"}`)); err == nil {
		t.Error("Write with FailurePolicyFail against a dead database returned nil error")
	}
	dead.Close()
	// Disconnect tolerance is by design: the default Drop policy keeps the
	// pipeline alive (rows shed) instead of failing the run.
	dropping := newDead(sink.FailurePolicyDrop)
	if err := dropping.Write(ctx, closedRecords(`{"id":"x"}`)); err != nil {
		t.Errorf("Write with FailurePolicyDrop against a dead database: %v", err)
	}
	dropping.Close()
}

func closedRecords(values ...string) <-chan types.Record {
	ch := make(chan types.Record, len(values))
	for _, v := range values {
		ch <- types.Record{Value: []byte(v)}
	}
	close(ch)
	return ch
}

// ---------------------------------------------------------------------------
// Live half: needs POSTGRES_DSN (merge/nightly with a Postgres service).
// ---------------------------------------------------------------------------

func livePostgres(t *testing.T) (context.Context, context.CancelFunc, *pgx.Conn) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		cancel()
		t.Skipf("postgres unreachable: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return ctx, cancel, conn
}

func liveTable(t *testing.T, ctx context.Context, conn *pgx.Conn) string {
	t.Helper()
	table := fmt.Sprintf("weibo_tier_%d", time.Now().UnixNano()%1000000000)
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id TEXT PRIMARY KEY, v BIGINT NOT NULL)`, table))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	})
	return table
}

func kvMapper(table string) sink.RecordMapper {
	return func(r types.Record) (string, []string, []any) {
		var m map[string]any
		if err := json.Unmarshal(r.Value, &m); err != nil {
			return "", nil, nil
		}
		id, _ := m["id"].(string)
		var v int64
		switch n := m["v"].(type) {
		case float64:
			v = int64(n)
		}
		return table, []string{"id", "v"}, []any{id, v}
	}
}

func writeLive(t *testing.T, ctx context.Context, s *sink.PostgresSink, values ...string) {
	t.Helper()
	ch := make(chan types.Record, len(values))
	for _, v := range values {
		ch <- types.Record{Value: []byte(v)}
	}
	close(ch)
	done := make(chan error, 1)
	go func() { done <- s.Write(ctx, ch) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Write did not return")
	}
}

func countLive(t *testing.T, ctx context.Context, conn *pgx.Conn, table string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// Batched inserts land exactly once (retry tier: multi-flush write of 50
// rows across several batches).
func TestLive_PostgresRetryInsert(t *testing.T) {
	ctx, _, conn := livePostgres(t)
	table := liveTable(t, ctx, conn)
	s, err := sink.NewPostgresSinkE(
		sink.PostgresDSN(os.Getenv("POSTGRES_DSN")),
		sink.PostgresMapper(kvMapper(table)),
		sink.PostgresBatchSize(10), sink.PostgresFlushInterval(200*time.Millisecond),
		sink.PostgresMaxRetries(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	values := make([]string, 50)
	for i := range values {
		values[i] = fmt.Sprintf(`{"id":"k%d","v":%d}`, i, i)
	}
	writeLive(t, ctx, s, values...)
	if n := countLive(t, ctx, conn, table); n != 50 {
		t.Fatalf("rows = %d, want 50", n)
	}
}

// The same key written twice converges to one row with the latest value.
func TestLive_PostgresUpsert(t *testing.T) {
	ctx, _, conn := livePostgres(t)
	table := liveTable(t, ctx, conn)
	s, err := sink.NewPostgresSinkE(
		sink.PostgresDSN(os.Getenv("POSTGRES_DSN")),
		sink.PostgresMapper(kvMapper(table)),
		sink.PostgresMode(sink.PostgresUpsert),
		sink.PostgresConflictColumns("id"),
		sink.PostgresBatchSize(10), sink.PostgresFlushInterval(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	writeLive(t, ctx, s, `{"id":"dup","v":1}`, `{"id":"dup","v":2}`)
	if n := countLive(t, ctx, conn, table); n != 1 {
		t.Fatalf("rows = %d, want 1 after upsert", n)
	}
	var v int64
	if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT v FROM %s WHERE id='dup'`, table)).Scan(&v); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if v != 2 {
		t.Fatalf("upserted value = %d, want 2", v)
	}
}

// Cancelling mid-write stops the sink promptly, the pool can be reopened,
// and buffered records still flush on graceful shutdown.
func TestLive_PostgresDisconnectAndShutdownFlush(t *testing.T) {
	ctx, _, conn := livePostgres(t)
	table := liveTable(t, ctx, conn)
	dsn := os.Getenv("POSTGRES_DSN")

	// Disconnect tier: cancel while blocked on a full channel feed.
	s, err := sink.NewPostgresSinkE(
		sink.PostgresDSN(dsn), sink.PostgresMapper(kvMapper(table)),
		sink.PostgresBatchSize(1000), sink.PostgresFlushInterval(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	feed := make(chan types.Record) // never closed by us; ctx ends the write
	cancelCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Write(cancelCtx, feed) }()
	time.Sleep(300 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("cancelled Write returned nil error")
		}
	case <-time.After(15 * time.Second):
		t.Error("cancelled Write hung instead of stopping")
	}

	// Shutdown-flush tier: records fed before cancellation are persisted.
	s2, err := sink.NewPostgresSinkE(
		sink.PostgresDSN(dsn), sink.PostgresMapper(kvMapper(table)),
		sink.PostgresBatchSize(1000), sink.PostgresFlushInterval(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	feed2 := make(chan types.Record, 4)
	feed2 <- types.Record{Value: []byte(`{"id":"f1","v":1}`)}
	feed2 <- types.Record{Value: []byte(`{"id":"f2","v":2}`)}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer flushCancel()
	flushed := make(chan error, 1)
	go func() { flushed <- s2.Write(flushCtx, feed2) }()
	time.Sleep(300 * time.Millisecond)
	flushCancel() // shutdown: drain, then flush the partial batch
	select {
	case err := <-flushed:
		if err == nil {
			t.Error("shutdown Write returned nil error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown Write hung")
	}
	if n := countLive(t, ctx, conn, table); n != 2 {
		t.Fatalf("flushed rows = %d, want 2", n)
	}
}
