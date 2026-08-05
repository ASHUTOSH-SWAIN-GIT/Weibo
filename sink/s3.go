package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Sink uploads batches of records to S3 (or any S3-compatible store)
// as newline-delimited JSON objects.
//
// It is the data-lake landing path: records accumulate into large
// objects laid out under Hive-style time partitions, which is what query
// engines — Athena, Trino, DuckDB, Spark — expect to prune on.
//
// Sizing matters more here than for the other sinks. Object storage
// bills per request, and a lake of tiny files is slow to query, so the
// defaults favour large batches (5000 records) with a one-minute
// interval bounding staleness when throughput is low.
//
// Delivery is at-least-once. Every flush writes a uniquely-keyed object,
// so a retry after a partly-successful upload creates a second object
// rather than corrupting the first — but records replayed after a
// restart appear in both the pre-crash and post-restart objects.
// Deduplicate on a record identity downstream if that matters.
type S3Sink struct {
	cfg    s3SinkConfig
	client S3API
	seq    atomic.Int64
}

// NewS3Sink creates a Sink that uploads records to S3. S3Bucket is
// required; if missing, NewS3Sink panics.
//
// Unless a client is injected with S3Client, one is built here from the
// ambient AWS configuration, and construction panics if that fails — so
// a misconfigured pipeline dies at startup rather than at the first
// flush, minutes in.
func NewS3Sink(opts ...S3SinkOption) *S3Sink {
	cfg := s3SinkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if cfg.bucket == "" {
		panic("weibo/sink: S3Sink requires S3Bucket(...)")
	}

	client := cfg.client
	if client == nil {
		client = buildS3Client(cfg)
	}
	return &S3Sink{cfg: cfg, client: client}
}

// buildS3Client assembles an S3 client from the sink's configuration,
// falling back to the default AWS credential chain when no static
// credentials were supplied.
func buildS3Client(cfg s3SinkConfig) *s3.Client {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRetryMaxAttempts(cfg.maxAttempts),
	}
	if cfg.region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.region))
	}
	if cfg.accessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.accessKey, cfg.secretKey, cfg.sessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		panic(fmt.Sprintf("weibo/sink: S3Sink: load AWS config: %v", err))
	}

	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.endpoint != "" {
			o.BaseEndpoint = &cfg.endpoint
		}
		if cfg.pathStyle {
			o.UsePathStyle = true
		}
	})
}

// Write reads records from the input channel and uploads them in
// batches. On context cancellation it drains briefly and uploads a final
// object so buffered records are not lost.
func (s *S3Sink) Write(ctx context.Context, in <-chan types.Record) error {
	bw := &batchWriter[types.Record]{
		batchSize:     s.cfg.batchSize,
		flushInterval: s.cfg.flushInterval,
		// Synchronous: uploads are large and the memory for the next
		// batch is only reclaimed once the current one is done.
		// Overlapping them would multiply peak footprint.
		async:   false,
		convert: func(r types.Record) (types.Record, bool) { return r, true },
		flush:   s.flushBatch,
	}
	return bw.run(ctx, in)
}

// flushBatch serializes one batch into an object and uploads it,
// applying the failure policy to records that could not be written.
func (s *S3Sink) flushBatch(ctx context.Context, batch []types.Record) error {
	payloads := make([][]byte, 0, len(batch))
	encoded := make([]types.Record, 0, len(batch))
	for _, r := range batch {
		b, err := s.cfg.serializer.Serialize(r)
		if err != nil {
			// Never upload a record the serializer rejected: a lake
			// object holding malformed lines breaks every consumer that
			// reads it later.
			if ferr := applyFailurePolicy(ctx, s.cfg.failurePolicy, s.cfg.dlq, r); ferr != nil {
				return fmt.Errorf("s3 sink: serialize: %w (failure policy: %w)", err, ferr)
			}
			continue
		}
		payloads = append(payloads, b)
		encoded = append(encoded, r)
	}
	if len(encoded) == 0 {
		return nil
	}

	body := encodeNDJSON(payloads)
	if s.cfg.gzip {
		compressed, err := gzipBytes(body)
		if err != nil {
			return fmt.Errorf("s3 sink: gzip: %w", err)
		}
		body = compressed
	}

	key := s.objectKey(time.Now())
	if err := s.put(ctx, key, body); err != nil {
		for _, r := range encoded {
			if ferr := applyFailurePolicy(ctx, s.cfg.failurePolicy, s.cfg.dlq, r); ferr != nil {
				return fmt.Errorf("s3 sink: %w (failure policy: %w)", err, ferr)
			}
		}
	}
	return nil
}

// put uploads one object. SDK-level retries are configured through
// S3MaxAttempts, so a failure here is already final.
func (s *S3Sink) put(ctx context.Context, key string, body []byte) error {
	input := &s3.PutObjectInput{
		Bucket:      &s.cfg.bucket,
		Key:         &key,
		Body:        bytes.NewReader(body),
		ContentType: strPtr("application/x-ndjson"),
	}
	if s.cfg.gzip {
		input.ContentEncoding = strPtr("gzip")
	}

	if _, err := s.client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("s3 sink: put s3://%s/%s: %w", s.cfg.bucket, key, err)
	}
	return nil
}

// objectKey builds the key for the next object.
func (s *S3Sink) objectKey(now time.Time) string {
	seq := s.seq.Add(1)
	if s.cfg.keyFunc != nil {
		return s.cfg.keyFunc(now, seq)
	}

	utc := now.UTC()
	var sb strings.Builder
	if p := strings.Trim(s.cfg.prefix, "/"); p != "" {
		sb.WriteString(p)
		sb.WriteByte('/')
	}
	// Hive-style partitions: query engines prune whole directories from
	// these, which is the difference between scanning an hour and
	// scanning the bucket.
	sb.WriteString("date=")
	sb.WriteString(utc.Format("2006-01-02"))
	sb.WriteString("/hour=")
	sb.WriteString(utc.Format("15"))
	sb.WriteString("/part-")
	sb.WriteString(utc.Format("20060102T150405"))
	sb.WriteByte('-')
	sb.WriteString(strconv.FormatInt(seq, 10))
	sb.WriteByte('-')
	// A random suffix keeps keys unique across parallel sink instances
	// and across restarts, where the per-instance sequence resets. S3
	// PUT overwrites silently, so a collision would destroy a batch.
	sb.WriteString(randomSuffix())
	sb.WriteString(".jsonl")
	if s.cfg.gzip {
		sb.WriteString(".gz")
	}
	return sb.String()
}

// randomSuffix returns 8 hex characters of randomness.
func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice; fall back to the clock
		// rather than aborting an upload over it.
		return strconv.FormatInt(time.Now().UnixNano()&0xffffffff, 16)
	}
	return hex.EncodeToString(b[:])
}

// gzipBytes compresses b.
func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func strPtr(s string) *string { return &s }

// Describe implements Describable for the dashboard. Credentials are
// never included.
func (s *S3Sink) Describe() SinkInfo {
	props := map[string]string{
		"bucket":         s.cfg.bucket,
		"prefix":         s.cfg.prefix,
		"batch_size":     strconv.Itoa(s.cfg.batchSize),
		"flush_interval": s.cfg.flushInterval.String(),
		"gzip":           strconv.FormatBool(s.cfg.gzip),
		"max_attempts":   strconv.Itoa(s.cfg.maxAttempts),
	}
	if s.cfg.region != "" {
		props["region"] = s.cfg.region
	}
	if s.cfg.endpoint != "" {
		props["endpoint"] = s.cfg.endpoint
	}
	return SinkInfo{Type: "S3", Props: props}
}
