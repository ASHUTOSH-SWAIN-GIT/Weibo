package sink

import (
	"context"
	"net/http"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// HTTPBodyFormat selects how a batch of records becomes a request body.
type HTTPBodyFormat int

const (
	// HTTPBodyNDJSON writes one serialized record per line
	// (Content-Type: application/x-ndjson). The default: it streams
	// naturally, needs no enclosing structure, and is what most bulk
	// ingest endpoints expect.
	HTTPBodyNDJSON HTTPBodyFormat = iota

	// HTTPBodyJSONArray wraps the batch in a JSON array
	// (Content-Type: application/json). Serialized records that are not
	// valid JSON are embedded as JSON strings so the body stays valid.
	HTTPBodyJSONArray
)

// Display returns a human-readable format name for the dashboard.
func (f HTTPBodyFormat) Display() string {
	if f == HTTPBodyJSONArray {
		return "json-array"
	}
	return "ndjson"
}

// HTTPEncoder builds a request body from a batch of records. It returns
// the body bytes and the Content-Type to send with them.
//
// Use it when neither built-in format fits — e.g. Elasticsearch's _bulk
// API, which interleaves an action line before each document.
type HTTPEncoder func(batch []types.Record) (body []byte, contentType string, err error)

// httpSinkConfig holds the resolved configuration for an HTTPSink.
type httpSinkConfig struct {
	url     string
	method  string
	headers map[string]string

	format     HTTPBodyFormat
	encoder    HTTPEncoder
	serializer Serializer

	batchSize     int
	flushInterval time.Duration
	timeout       time.Duration
	maxRetries    int

	client *http.Client

	failurePolicy FailurePolicy
	dlq           DLQ
}

// HTTPSinkOption configures an HTTPSink. Pass one or more to
// NewHTTPSink. HTTPURL is required.
type HTTPSinkOption func(*httpSinkConfig)

// HTTPURL sets the endpoint records are posted to. Required.
func HTTPURL(url string) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.url = url }
}

// HTTPMethod overrides the request method. Defaults to POST; PUT and
// PATCH are the other useful choices.
func HTTPMethod(method string) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.method = method }
}

// HTTPHeader adds a request header. Call it once per header.
// Content-Type is set by the body format and should not be set here.
func HTTPHeader(key, value string) HTTPSinkOption {
	return func(c *httpSinkConfig) {
		if c.headers == nil {
			c.headers = make(map[string]string)
		}
		c.headers[key] = value
	}
}

// HTTPBearerToken sets an Authorization: Bearer header. The token is
// never included in Describe output.
func HTTPBearerToken(token string) HTTPSinkOption {
	return func(c *httpSinkConfig) {
		if c.headers == nil {
			c.headers = make(map[string]string)
		}
		c.headers["Authorization"] = "Bearer " + token
	}
}

// HTTPFormat selects the built-in body format. Ignored when
// HTTPEncoderFunc is set.
func HTTPFormat(f HTTPBodyFormat) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.format = f }
}

// HTTPEncoderFunc supplies a custom body encoder, overriding HTTPFormat.
func HTTPEncoderFunc(fn HTTPEncoder) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.encoder = fn }
}

// HTTPSerialize sets how each record becomes bytes within the body.
// Defaults to JSONSerializer (Record.Parsed if set, else Record.Value).
func HTTPSerialize(s Serializer) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.serializer = s }
}

// HTTPBatchSize sets how many records are sent per request.
func HTTPBatchSize(n int) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.batchSize = n }
}

// HTTPFlushInterval bounds how long a partial batch waits before being
// sent. Zero disables periodic flushing.
func HTTPFlushInterval(d time.Duration) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.flushInterval = d }
}

// HTTPTimeout bounds a single request attempt (retries get a fresh
// timeout each).
func HTTPTimeout(d time.Duration) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.timeout = d }
}

// HTTPMaxRetries sets how many times a retryable failure is retried
// before the failure policy applies.
func HTTPMaxRetries(n int) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.maxRetries = n }
}

// HTTPClient injects a custom *http.Client — for proxies, custom TLS,
// or connection tuning.
func HTTPClient(client *http.Client) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.client = client }
}

// HTTPFailurePolicy sets what happens to records that could not be
// delivered after all retries.
func HTTPFailurePolicy(p FailurePolicy) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.failurePolicy = p }
}

// HTTPDLQ sets the dead-letter-queue for undeliverable records.
// Used with FailurePolicyDLQ.
func HTTPDLQ(dlq DLQ) HTTPSinkOption {
	return func(c *httpSinkConfig) { c.dlq = dlq }
}

func (c *httpSinkConfig) applyDefaults() {
	if c.method == "" {
		c.method = http.MethodPost
	}
	if c.batchSize <= 0 {
		c.batchSize = defaultBatchSize
	}
	// Unlike the Kafka sink, which relies on the client's own batch
	// timeout, nothing else would ever push a partial batch out of an
	// HTTP sink — so it flushes on an interval by default.
	if c.flushInterval == 0 {
		c.flushInterval = time.Second
	}
	if c.timeout <= 0 {
		c.timeout = 30 * time.Second
	}
	if c.maxRetries < 0 {
		c.maxRetries = 0
	}
	if c.serializer == nil {
		c.serializer = NewJSONSerializer()
	}
	if c.client == nil {
		c.client = &http.Client{}
	}
}

// newRequest builds a single attempt's request.
func (c *httpSinkConfig) newRequest(ctx context.Context, body []byte, contentType string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, c.method, c.url, bytesReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}
