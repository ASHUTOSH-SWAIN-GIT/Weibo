package sink_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// The point of these implementations is that one instance can absorb
// failures from a sink, a source deserializer and a Process operator.
// These assertions fail at compile time if any of the three interfaces
// drifts apart from the others.
var (
	_ sink.DLQ            = (*sink.FileDLQ)(nil)
	_ source.RecordSink   = (*sink.FileDLQ)(nil)
	_ operator.RecordSink = (*sink.FileDLQ)(nil)

	_ sink.DLQ            = (*sink.KafkaDLQ)(nil)
	_ source.RecordSink   = (*sink.KafkaDLQ)(nil)
	_ operator.RecordSink = (*sink.KafkaDLQ)(nil)
)

// readEnvelopes parses the JSON-lines a FileDLQ wrote.
func readEnvelopes(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read DLQ file: %v", err)
	}
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("DLQ line is not valid JSON (%v): %s", err, line)
		}
		out = append(out, m)
	}
	return out
}

// TestFileDLQ_WritesReplayableEnvelope verifies the envelope carries what
// a replay needs — partition and offset above all — and that a JSON value
// is embedded as JSON rather than Go's default base64 for []byte, which
// would make the file unreadable.
func TestFileDLQ_WritesReplayableEnvelope(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dead", "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path)
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}
	defer dlq.Close()

	rec := types.Record{
		Key:       []byte("user-7"),
		Value:     []byte(`{"amount":42}`),
		Timestamp: time.Unix(1700000000, 0),
		Offset:    99,
		Partition: 3,
		Headers:   map[string][]byte{"source": []byte("orders")},
	}
	if err := dlq.Write(context.Background(), rec); err != nil {
		t.Fatalf("Write: %v", err)
	}

	envs := readEnvelopes(t, path)
	if len(envs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envs))
	}
	e := envs[0]

	if e["key"] != "user-7" {
		t.Errorf("key = %v", e["key"])
	}
	if got, ok := e["offset"].(float64); !ok || int64(got) != 99 {
		t.Errorf("offset = %v, want 99", e["offset"])
	}
	if got, ok := e["partition"].(float64); !ok || int(got) != 3 {
		t.Errorf("partition = %v, want 3", e["partition"])
	}
	val, ok := e["value"].(map[string]any)
	if !ok {
		t.Fatalf("value was not embedded as JSON: %#v", e["value"])
	}
	if amt, ok := val["amount"].(float64); !ok || amt != 42 {
		t.Errorf("value.amount = %v, want 42", val["amount"])
	}
	if e["dlq_time"] == "" || e["dlq_time"] == nil {
		t.Error("dlq_time missing")
	}
	hdrs, ok := e["headers"].(map[string]any)
	if !ok || hdrs["source"] != "orders" {
		t.Errorf("headers = %#v", e["headers"])
	}
}

// TestFileDLQ_NonJSONValueStaysReadable verifies a non-JSON payload is
// written as a JSON string, keeping the line parseable and the content
// legible instead of base64.
func TestFileDLQ_NonJSONValueStaysReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path)
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}
	defer dlq.Close()

	if err := dlq.Write(context.Background(), types.Record{Value: []byte("not json at all")}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	envs := readEnvelopes(t, path)
	if len(envs) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(envs))
	}
	if got, ok := envs[0]["value"].(string); !ok || got != "not json at all" {
		t.Errorf("value = %#v, want the plain string", envs[0]["value"])
	}
}

// TestFileDLQ_AppendsAcrossRecords verifies records accumulate rather
// than overwrite — a DLQ that keeps only the last failure is useless.
func TestFileDLQ_AppendsAcrossRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path)
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}
	defer dlq.Close()

	for i := range 5 {
		rec := types.Record{Value: []byte(`{"i":` + string(rune('0'+i)) + `}`)}
		if err := dlq.Write(context.Background(), rec); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if envs := readEnvelopes(t, path); len(envs) != 5 {
		t.Errorf("expected 5 envelopes, got %d", len(envs))
	}
}

// TestFileDLQ_ConcurrentWrites verifies the mutex actually protects the
// file: one DLQ instance is shared across sinks, sources and operators,
// and the Kafka sink flushes from several goroutines at once.
func TestFileDLQ_ConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path, sink.FileDLQNoSync())
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}
	defer dlq.Close()

	const writers, each = 8, 25
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range each {
				rec := types.Record{Value: []byte(`{"w":1}`), Offset: int64(w*each + i)}
				if err := dlq.Write(context.Background(), rec); err != nil {
					t.Errorf("concurrent write: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Every line must still be individually parseable — interleaved
	// writes would produce corrupt JSON.
	if envs := readEnvelopes(t, path); len(envs) != writers*each {
		t.Errorf("expected %d envelopes, got %d", writers*each, len(envs))
	}
}

// TestFileDLQ_CloseSemantics verifies Close is idempotent and that a
// write afterwards reports an error rather than silently vanishing.
func TestFileDLQ_CloseSemantics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path)
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}

	if err := dlq.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := dlq.Close(); err != nil {
		t.Errorf("second Close should be a no-op, got %v", err)
	}
	if err := dlq.Write(context.Background(), types.Record{Value: []byte("x")}); err == nil {
		t.Error("expected an error writing after Close")
	}
}

// TestFileDLQ_UnwritablePathReturnsError verifies construction fails
// loudly instead of yielding a DLQ that silently drops everything.
func TestFileDLQ_UnwritablePathReturnsError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "notadir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := sink.NewFileDLQ(filepath.Join(blocker, "letters.jsonl")); err == nil {
		t.Error("expected an error when the parent path is a file")
	}
}

// TestFileDLQ_ReceivesFailedSinkRecords wires the DLQ into a real failure
// path: an HTTP sink whose endpoint rejects everything. This is the
// end-to-end contract the DLQ options across the codebase promise.
func TestFileDLQ_ReceivesFailedSinkRecords(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest) // permanent: straight to the DLQ
	}))
	defer ts.Close()

	path := filepath.Join(t.TempDir(), "letters.jsonl")
	dlq, err := sink.NewFileDLQ(path)
	if err != nil {
		t.Fatalf("NewFileDLQ: %v", err)
	}
	defer dlq.Close()

	h := sink.NewHTTPSink(
		sink.HTTPURL(ts.URL),
		sink.HTTPBatchSize(2),
		sink.HTTPMaxRetries(0),
		sink.HTTPFailurePolicy(sink.FailurePolicyDLQ),
		sink.HTTPDLQ(dlq),
	)

	in := make(chan types.Record, 2)
	in <- types.Record{Value: []byte(`{"i":1}`), Offset: 1}
	in <- types.Record{Value: []byte(`{"i":2}`), Offset: 2}
	close(in)
	if err := h.Write(context.Background(), in); err != nil {
		t.Fatalf("sink Write: %v", err)
	}

	envs := readEnvelopes(t, path)
	if len(envs) != 2 {
		t.Fatalf("expected both rejected records in the DLQ, got %d", len(envs))
	}
	for i, e := range envs {
		if got, ok := e["offset"].(float64); !ok || int64(got) != int64(i+1) {
			t.Errorf("envelope %d offset = %v", i, e["offset"])
		}
	}
}

// TestKafkaDLQ_RequiresBrokersAndTopic covers the fail-fast construction
// contract shared with the Kafka sink. (Delivery itself needs a broker
// and is exercised by the integration suite.)
func TestKafkaDLQ_RequiresBrokersAndTopic(t *testing.T) {
	tests := []struct {
		name string
		opts []sink.KafkaDLQOption
	}{
		{name: "no brokers", opts: []sink.KafkaDLQOption{sink.KafkaDLQTopic("dead")}},
		{name: "no topic", opts: []sink.KafkaDLQOption{sink.KafkaDLQBrokers("localhost:9092")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic")
				}
			}()
			sink.NewKafkaDLQ(tt.opts...)
		})
	}
}

// TestKafkaDLQ_Accessors verifies a valid construction wires through.
func TestKafkaDLQ_Accessors(t *testing.T) {
	d := sink.NewKafkaDLQ(
		sink.KafkaDLQBrokers("localhost:9092"),
		sink.KafkaDLQTopic("orders.dead"),
	)
	defer d.Close()

	if d.Topic() != "orders.dead" {
		t.Errorf("topic = %q", d.Topic())
	}
}
