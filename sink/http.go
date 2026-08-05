package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// maxErrorBodyBytes caps how much of a failed response body is read into
// the error message — enough to identify the problem, bounded so a
// misbehaving endpoint can't blow up memory or logs.
const maxErrorBodyBytes = 512

// HTTPSink posts batches of records to an HTTP endpoint.
//
// It is the general-purpose egress: anything that accepts batched JSON
// over HTTP works without a dedicated connector — internal APIs,
// webhooks, and the HTTP ingest paths of systems like Elasticsearch and
// ClickHouse (via HTTPEncoderFunc for formats that need per-record
// framing).
//
// Delivery semantics are at-least-once: a request that times out or
// fails mid-flight is retried, so an endpoint that processed the batch
// before failing to respond will see it twice. Make the endpoint
// idempotent when duplicates matter.
//
// Failures are classified before any retry:
//   - 2xx                     — success
//   - 429, 5xx, network error — retryable, with backoff; Retry-After is
//     honored when present
//   - other 4xx               — permanent (bad request, bad auth); retrying
//     a malformed body just wastes attempts, so
//     the failure policy applies immediately
type HTTPSink struct {
	cfg    httpSinkConfig
	client *http.Client
}

// NewHTTPSink creates a Sink that posts records to an HTTP endpoint.
// HTTPURL is required; if missing, NewHTTPSink panics.
func NewHTTPSink(opts ...HTTPSinkOption) *HTTPSink {
	cfg := httpSinkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if cfg.url == "" {
		panic("weibo/sink: HTTPSink requires HTTPURL(...)")
	}
	if _, err := url.Parse(cfg.url); err != nil {
		panic(fmt.Sprintf("weibo/sink: HTTPSink invalid URL: %v", err))
	}

	return &HTTPSink{cfg: cfg, client: cfg.client}
}

// Write reads records from the input channel and posts them in batches.
// On context cancellation it drains remaining records briefly and sends a
// final request so in-flight records are not lost.
func (h *HTTPSink) Write(ctx context.Context, in <-chan types.Record) error {
	bw := &batchWriter[types.Record]{
		batchSize:     h.cfg.batchSize,
		flushInterval: h.cfg.flushInterval,
		// Synchronous: one request at a time keeps ordering intact and
		// applies backpressure, which is what an endpoint under load
		// needs. Concurrency here would mostly serve to overwhelm it.
		async:   false,
		convert: func(r types.Record) (types.Record, bool) { return r, true },
		flush:   h.flushBatch,
	}
	return bw.run(ctx, in)
}

// flushBatch encodes one batch and delivers it, applying the failure
// policy to records that could not be sent.
func (h *HTTPSink) flushBatch(ctx context.Context, batch []types.Record) error {
	body, contentType, encoded, err := h.encode(ctx, batch)
	if err != nil {
		return err
	}
	if len(encoded) == 0 {
		return nil // every record was dropped by the failure policy
	}

	if err := h.sendWithRetry(ctx, body, contentType); err != nil {
		for _, r := range encoded {
			if ferr := applyFailurePolicy(ctx, h.cfg.failurePolicy, h.cfg.dlq, r); ferr != nil {
				return fmt.Errorf("http sink: %w (failure policy: %w)", err, ferr)
			}
		}
	}
	return nil
}

// encode builds the request body, returning it alongside the records it
// actually covers. A record whose serialization fails goes through the
// failure policy and is excluded rather than being sent raw — silently
// shipping unserializable data would corrupt the destination.
func (h *HTTPSink) encode(ctx context.Context, batch []types.Record) (body []byte, contentType string, encoded []types.Record, err error) {
	if h.cfg.encoder != nil {
		body, contentType, err = h.cfg.encoder(batch)
		if err != nil {
			return nil, "", nil, fmt.Errorf("http sink: encode batch: %w", err)
		}
		return body, contentType, batch, nil
	}

	payloads := make([][]byte, 0, len(batch))
	encoded = make([]types.Record, 0, len(batch))
	for _, r := range batch {
		b, serErr := h.cfg.serializer.Serialize(r)
		if serErr != nil {
			if ferr := applyFailurePolicy(ctx, h.cfg.failurePolicy, h.cfg.dlq, r); ferr != nil {
				return nil, "", nil, fmt.Errorf("http sink: serialize: %w (failure policy: %w)", serErr, ferr)
			}
			continue
		}
		payloads = append(payloads, b)
		encoded = append(encoded, r)
	}
	if len(encoded) == 0 {
		return nil, "", nil, nil
	}

	if h.cfg.format == HTTPBodyJSONArray {
		body, err = encodeJSONArray(payloads)
		if err != nil {
			return nil, "", nil, fmt.Errorf("http sink: encode batch: %w", err)
		}
		return body, "application/json", encoded, nil
	}
	return encodeNDJSON(payloads), "application/x-ndjson", encoded, nil
}

// encodeNDJSON joins payloads one per line. Embedded newlines would
// break the framing, so they are escaped by re-encoding such a payload
// as a JSON string.
func encodeNDJSON(payloads [][]byte) []byte {
	var buf bytes.Buffer
	for _, p := range payloads {
		if bytes.ContainsAny(p, "\n\r") {
			if quoted, err := json.Marshal(string(p)); err == nil {
				p = quoted
			}
		}
		buf.Write(p)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// encodeJSONArray wraps payloads in a JSON array. A payload that is not
// valid JSON is embedded as a JSON string so the body stays parseable.
func encodeJSONArray(payloads [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, p := range payloads {
		if i > 0 {
			buf.WriteByte(',')
		}
		if json.Valid(p) {
			buf.Write(p)
			continue
		}
		quoted, err := json.Marshal(string(p))
		if err != nil {
			return nil, err
		}
		buf.Write(quoted)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// sendWithRetry delivers the body, retrying retryable failures up to
// maxRetries times. Returns nil once the endpoint accepts the batch.
func (h *HTTPSink) sendWithRetry(ctx context.Context, body []byte, contentType string) error {
	var lastErr error
	for attempt := 0; attempt <= h.cfg.maxRetries; attempt++ {
		retryable, retryAfter, err := h.send(ctx, body, contentType)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retryable {
			return err // permanent: further attempts cannot help
		}
		if attempt == h.cfg.maxRetries {
			break
		}

		backoff := time.Duration(1<<uint(attempt)) * time.Second
		if retryAfter > 0 {
			backoff = retryAfter // the server told us when to come back
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return lastErr
}

// send performs one attempt. It reports whether the failure is worth
// retrying and, for 429/503, how long the server asked us to wait.
func (h *HTTPSink) send(ctx context.Context, body []byte, contentType string) (retryable bool, retryAfter time.Duration, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, h.cfg.timeout)
	defer cancel()

	req, err := h.cfg.newRequest(reqCtx, body, contentType)
	if err != nil {
		return false, 0, fmt.Errorf("http sink: build request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		// Transport-level failure (connection refused, timeout, reset):
		// the endpoint may well be back on the next attempt.
		return true, 0, fmt.Errorf("http sink: %s %s: %w", h.cfg.method, h.cfg.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Drain so the connection can be reused by keep-alive.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
		return false, 0, nil
	}

	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
	statusErr := fmt.Errorf("http sink: %s %s: status %d: %s",
		h.cfg.method, h.cfg.url, resp.StatusCode, bytes.TrimSpace(snippet))

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return true, parseRetryAfter(resp.Header.Get("Retry-After")), statusErr
	}
	return false, 0, statusErr
}

// parseRetryAfter reads a Retry-After header in either supported form —
// delay in seconds, or an HTTP date. Returns 0 when absent or unusable,
// leaving the caller on its normal backoff.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// bytesReader is a helper so each retry attempt gets a fresh reader over
// the same body.
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

// Describe implements Describable for the dashboard. Credentials are
// never included: headers are omitted entirely and the URL is stripped
// of userinfo and query string, either of which can carry a key.
func (h *HTTPSink) Describe() SinkInfo {
	return SinkInfo{
		Type: "HTTP",
		Props: map[string]string{
			"url":            redactURL(h.cfg.url),
			"method":         h.cfg.method,
			"format":         h.cfg.format.Display(),
			"batch_size":     strconv.Itoa(h.cfg.batchSize),
			"flush_interval": h.cfg.flushInterval.String(),
			"timeout":        h.cfg.timeout.String(),
			"max_retries":    strconv.Itoa(h.cfg.maxRetries),
		},
	}
}

// redactURL removes credentials from a URL for display.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable url)"
	}
	u.User = nil
	if u.RawQuery != "" {
		u.RawQuery = "(redacted)"
	}
	return u.String()
}
