package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// capturedRequest is one request the test server received.
type capturedRequest struct {
	body        string
	contentType string
	method      string
	headers     http.Header
}

// testServer records incoming requests and replies with codes from a
// scripted sequence (the last code repeats once exhausted).
type testServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []capturedRequest
	codes    []int
	headers  map[string]string // extra response headers
}

func newTestServer(codes ...int) *testServer {
	if len(codes) == 0 {
		codes = []int{http.StatusOK}
	}
	ts := &testServer{codes: codes}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		ts.mu.Lock()
		n := len(ts.requests)
		ts.requests = append(ts.requests, capturedRequest{
			body:        string(body),
			contentType: r.Header.Get("Content-Type"),
			method:      r.Method,
			headers:     r.Header.Clone(),
		})
		code := ts.codes[min(n, len(ts.codes)-1)]
		extra := ts.headers
		ts.mu.Unlock()

		for k, v := range extra {
			w.Header().Set(k, v)
		}
		w.WriteHeader(code)
	}))
	return ts
}

func (ts *testServer) captured() []capturedRequest {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]capturedRequest(nil), ts.requests...)
}

// recordingDLQ captures records routed to the dead-letter queue.
type recordingDLQ struct {
	mu      sync.Mutex
	records []types.Record
}

func (d *recordingDLQ) Write(_ context.Context, r types.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.records = append(d.records, r)
	return nil
}

func (d *recordingDLQ) snapshot() []types.Record {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]types.Record(nil), d.records...)
}

// writeRecords feeds values through a sink and returns Write's error.
func writeRecords(t *testing.T, h *HTTPSink, values ...string) error {
	t.Helper()
	in := make(chan types.Record, len(values))
	for _, v := range values {
		in <- types.Record{Key: []byte("k"), Value: []byte(v)}
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- h.Write(context.Background(), in) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("HTTP sink Write did not return")
		return nil
	}
}

// TestHTTPSink_PostsNDJSONBatch covers the default path: one request per
// batch, one serialized record per line.
func TestHTTPSink_PostsNDJSONBatch(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(HTTPURL(ts.URL), HTTPBatchSize(10))
	if err := writeRecords(t, h, `{"a":1}`, `{"a":2}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	if got[0].method != http.MethodPost {
		t.Errorf("method = %s, want POST", got[0].method)
	}
	if got[0].contentType != "application/x-ndjson" {
		t.Errorf("content-type = %q, want application/x-ndjson", got[0].contentType)
	}
	if got[0].body != "{\"a\":1}\n{\"a\":2}\n" {
		t.Errorf("body = %q", got[0].body)
	}
}

// TestHTTPSink_JSONArrayFormat covers the array body, including the
// non-JSON payload case that must stay parseable.
func TestHTTPSink_JSONArrayFormat(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(HTTPURL(ts.URL), HTTPBatchSize(10), HTTPFormat(HTTPBodyJSONArray))
	if err := writeRecords(t, h, `{"a":1}`, `plain text`); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	if got[0].contentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", got[0].contentType)
	}
	var parsed []any
	if err := json.Unmarshal([]byte(got[0].body), &parsed); err != nil {
		t.Fatalf("body is not valid JSON (%v): %s", err, got[0].body)
	}
	if len(parsed) != 2 {
		t.Fatalf("expected 2 elements, got %d: %s", len(parsed), got[0].body)
	}
	if s, ok := parsed[1].(string); !ok || s != "plain text" {
		t.Errorf("non-JSON payload = %#v, want the string \"plain text\"", parsed[1])
	}
}

// TestHTTPSink_BatchSizeSplitsRequests verifies the batch threshold
// actually bounds request size.
func TestHTTPSink_BatchSizeSplitsRequests(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(HTTPURL(ts.URL), HTTPBatchSize(2))
	if err := writeRecords(t, h, "1", "2", "3", "4", "5"); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 3 {
		t.Fatalf("expected 3 requests (2+2+1), got %d", len(got))
	}
	if lines := strings.Count(got[2].body, "\n"); lines != 1 {
		t.Errorf("final request had %d lines, want 1", lines)
	}
}

// TestHTTPSink_RetriesServerError verifies 5xx is retried and that a
// later success ends the retry loop.
func TestHTTPSink_RetriesServerError(t *testing.T) {
	ts := newTestServer(http.StatusInternalServerError, http.StatusOK)
	defer ts.Close()

	dlq := &recordingDLQ{}
	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(1), HTTPMaxRetries(1),
		HTTPFailurePolicy(FailurePolicyDLQ), HTTPDLQ(dlq),
	)
	if err := writeRecords(t, h, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}

	if n := len(ts.captured()); n != 2 {
		t.Errorf("expected 2 attempts (fail then succeed), got %d", n)
	}
	if n := len(dlq.snapshot()); n != 0 {
		t.Errorf("record went to DLQ despite eventual success: %d", n)
	}
}

// TestHTTPSink_DoesNotRetryClientError pins the classification: a 400 is
// the sink's own fault and will fail identically forever, so it must go
// straight to the failure policy instead of burning retries.
func TestHTTPSink_DoesNotRetryClientError(t *testing.T) {
	ts := newTestServer(http.StatusBadRequest)
	defer ts.Close()

	dlq := &recordingDLQ{}
	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(1), HTTPMaxRetries(5),
		HTTPFailurePolicy(FailurePolicyDLQ), HTTPDLQ(dlq),
	)
	if err := writeRecords(t, h, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}

	if n := len(ts.captured()); n != 1 {
		t.Errorf("expected exactly 1 attempt for a 4xx, got %d", n)
	}
	if n := len(dlq.snapshot()); n != 1 {
		t.Errorf("expected the record in the DLQ, got %d", n)
	}
}

// TestHTTPSink_ExhaustedRetriesGoToDLQ verifies undeliverable records
// reach the dead-letter queue rather than vanishing.
func TestHTTPSink_ExhaustedRetriesGoToDLQ(t *testing.T) {
	ts := newTestServer(http.StatusServiceUnavailable)
	defer ts.Close()

	dlq := &recordingDLQ{}
	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(2), HTTPMaxRetries(0),
		HTTPFailurePolicy(FailurePolicyDLQ), HTTPDLQ(dlq),
	)
	if err := writeRecords(t, h, "a", "b"); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := dlq.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected both records in the DLQ, got %d", len(got))
	}
}

// TestHTTPSink_FailurePolicyFailStopsPipeline verifies FailurePolicyFail
// surfaces an error to the caller instead of dropping data.
func TestHTTPSink_FailurePolicyFailStopsPipeline(t *testing.T) {
	ts := newTestServer(http.StatusServiceUnavailable)
	defer ts.Close()

	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(1), HTTPMaxRetries(0),
		HTTPFailurePolicy(FailurePolicyFail),
	)
	if err := writeRecords(t, h, "a"); err == nil {
		t.Fatal("expected an error under FailurePolicyFail")
	}
}

// TestHTTPSink_SendsHeaders verifies custom headers and bearer auth
// reach the endpoint.
func TestHTTPSink_SendsHeaders(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(1),
		HTTPBearerToken("s3cret"),
		HTTPHeader("X-Tenant", "acme"),
		HTTPMethod(http.MethodPut),
	)
	if err := writeRecords(t, h, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	if v := got[0].headers.Get("Authorization"); v != "Bearer s3cret" {
		t.Errorf("Authorization = %q", v)
	}
	if v := got[0].headers.Get("X-Tenant"); v != "acme" {
		t.Errorf("X-Tenant = %q", v)
	}
	if got[0].method != http.MethodPut {
		t.Errorf("method = %s, want PUT", got[0].method)
	}
}

// TestHTTPSink_CustomEncoder covers the escape hatch used for formats
// needing per-record framing, e.g. Elasticsearch _bulk.
func TestHTTPSink_CustomEncoder(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(10),
		HTTPEncoderFunc(func(batch []types.Record) ([]byte, string, error) {
			var sb strings.Builder
			for _, r := range batch {
				sb.WriteString(`{"index":{}}` + "\n")
				sb.Write(r.Value)
				sb.WriteString("\n")
			}
			return []byte(sb.String()), "application/x-ndjson", nil
		}),
	)
	if err := writeRecords(t, h, `{"a":1}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	if got[0].body != "{\"index\":{}}\n{\"a\":1}\n" {
		t.Errorf("body = %q", got[0].body)
	}
}

// TestHTTPSink_SerializeErrorGoesToDLQ pins the behavior that a record
// which cannot be serialized is dead-lettered, never shipped raw — the
// destination must not receive data the serializer rejected.
func TestHTTPSink_SerializeErrorGoesToDLQ(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	dlq := &recordingDLQ{}
	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(2),
		HTTPSerialize(SerializerFunc(func(r types.Record) ([]byte, error) {
			if string(r.Value) == "bad" {
				return nil, errors.New("cannot serialize")
			}
			return r.Value, nil
		})),
		HTTPFailurePolicy(FailurePolicyDLQ), HTTPDLQ(dlq),
	)
	if err := writeRecords(t, h, "bad", "good"); err != nil {
		t.Fatalf("write: %v", err)
	}

	got := ts.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	if strings.Contains(got[0].body, "bad") {
		t.Errorf("unserializable record was sent anyway: %q", got[0].body)
	}
	if body := got[0].body; body != "good\n" {
		t.Errorf("body = %q, want %q", body, "good\n")
	}
	if d := dlq.snapshot(); len(d) != 1 || string(d[0].Value) != "bad" {
		t.Errorf("DLQ = %v, want the unserializable record", d)
	}
}

// TestHTTPSink_AllRecordsUnserializableSendsNothing verifies no empty
// request is made when the whole batch was dead-lettered.
func TestHTTPSink_AllRecordsUnserializableSendsNothing(t *testing.T) {
	ts := newTestServer()
	defer ts.Close()

	h := NewHTTPSink(
		HTTPURL(ts.URL), HTTPBatchSize(1),
		HTTPSerialize(SerializerFunc(func(types.Record) ([]byte, error) {
			return nil, errors.New("nope")
		})),
	)
	if err := writeRecords(t, h, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if n := len(ts.captured()); n != 0 {
		t.Errorf("expected no request, got %d", n)
	}
}

func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want time.Duration
		zero bool
	}{
		{name: "empty", in: "", zero: true},
		{name: "seconds", in: "5", want: 5 * time.Second},
		{name: "zero seconds", in: "0", zero: true},
		{name: "negative", in: "-3", zero: true},
		{name: "garbage", in: "soon", zero: true},
		{name: "past date", in: time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat), zero: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseRetryAfter(tt.in)
			if tt.zero {
				if got != 0 {
					t.Errorf("got %v, want 0", got)
				}
				return
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	t.Run("future date", func(t *testing.T) {
		in := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
		if got := parseRetryAfter(in); got <= 0 || got > 31*time.Second {
			t.Errorf("got %v, want a positive duration under ~30s", got)
		}
	})
}

// TestHTTPSink_DescribeRedactsCredentials pins that Describe, which feeds
// the dashboard, never exposes a token — neither the auth header nor
// credentials embedded in the URL.
func TestHTTPSink_DescribeRedactsCredentials(t *testing.T) {
	h := NewHTTPSink(
		HTTPURL("https://user:pass@example.com/ingest?api_key=s3cret"),
		HTTPBearerToken("tok3n"),
	)
	info := h.Describe()

	blob := fmt.Sprintf("%v", info.Props)
	for _, secret := range []string{"pass", "s3cret", "tok3n"} {
		if strings.Contains(blob, secret) {
			t.Errorf("Describe leaked %q: %s", secret, blob)
		}
	}
	if info.Type != "HTTP" {
		t.Errorf("type = %q, want HTTP", info.Type)
	}
	if !strings.Contains(info.Props["url"], "example.com/ingest") {
		t.Errorf("url lost its identifying part: %q", info.Props["url"])
	}
}

// TestHTTPSink_RequiresURL verifies the fail-fast contract shared with
// the other sinks.
func TestHTTPSink_RequiresURL(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic when HTTPURL is missing")
		}
	}()
	NewHTTPSink(HTTPBatchSize(1))
}
