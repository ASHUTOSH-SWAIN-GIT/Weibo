package sink

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of the S3 client the sink uses. Depending on the
// operation rather than the concrete client keeps the sink testable
// without a bucket, and leaves room for S3-compatible services.
type S3API interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3KeyFunc builds the object key for one uploaded batch. seq counts
// uploads within this sink instance, starting at 1.
//
// Keys must be unique: S3 PUT overwrites silently, so a colliding key
// destroys the earlier batch.
type S3KeyFunc func(now time.Time, seq int64) string

// s3SinkConfig holds the resolved configuration for an S3Sink.
type s3SinkConfig struct {
	bucket string
	region string
	prefix string

	endpoint  string
	pathStyle bool

	accessKey    string
	secretKey    string
	sessionToken string

	serializer Serializer
	keyFunc    S3KeyFunc
	gzip       bool

	batchSize     int
	flushInterval time.Duration
	maxAttempts   int

	client S3API

	failurePolicy FailurePolicy
	dlq           DLQ
}

// S3SinkOption configures an S3Sink. Pass one or more to NewS3Sink.
// S3Bucket is required.
type S3SinkOption func(*s3SinkConfig)

// S3Bucket sets the destination bucket. Required.
func S3Bucket(bucket string) S3SinkOption {
	return func(c *s3SinkConfig) { c.bucket = bucket }
}

// S3Region sets the AWS region. Falls back to the ambient AWS
// configuration (env, shared config, instance role) when unset.
func S3Region(region string) S3SinkOption {
	return func(c *s3SinkConfig) { c.region = region }
}

// S3Prefix sets a key prefix — the "directory" objects land under.
func S3Prefix(prefix string) S3SinkOption {
	return func(c *s3SinkConfig) { c.prefix = prefix }
}

// S3Endpoint overrides the service endpoint, for S3-compatible storage
// such as MinIO, Cloudflare R2 or Backblaze B2. Most of those also need
// S3PathStyle.
func S3Endpoint(endpoint string) S3SinkOption {
	return func(c *s3SinkConfig) { c.endpoint = endpoint }
}

// S3PathStyle addresses buckets as endpoint/bucket/key instead of
// bucket.endpoint/key. Required by most S3-compatible services.
func S3PathStyle() S3SinkOption {
	return func(c *s3SinkConfig) { c.pathStyle = true }
}

// S3StaticCredentials sets explicit credentials. Leave unset to use the
// default AWS chain (env vars, shared config, IRSA, instance role),
// which is preferable in production — nothing to rotate in code.
// sessionToken may be empty for long-lived credentials.
func S3StaticCredentials(accessKey, secretKey, sessionToken string) S3SinkOption {
	return func(c *s3SinkConfig) {
		c.accessKey, c.secretKey, c.sessionToken = accessKey, secretKey, sessionToken
	}
}

// S3Serialize sets how each record becomes a line in the object.
// Defaults to JSONSerializer.
func S3Serialize(s Serializer) S3SinkOption {
	return func(c *s3SinkConfig) { c.serializer = s }
}

// S3KeyNaming overrides object key generation. The default lays out
// Hive-style time partitions; override it when a query engine expects a
// different layout.
func S3KeyNaming(fn S3KeyFunc) S3SinkOption {
	return func(c *s3SinkConfig) { c.keyFunc = fn }
}

// S3Gzip compresses each object with gzip and appends .gz to the
// default key. Worth it for JSON, which compresses well.
func S3Gzip() S3SinkOption {
	return func(c *s3SinkConfig) { c.gzip = true }
}

// S3BatchSize sets how many records accumulate into one object.
// Object storage rewards large objects — the default is deliberately
// far higher than the other sinks'.
func S3BatchSize(n int) S3SinkOption {
	return func(c *s3SinkConfig) { c.batchSize = n }
}

// S3FlushInterval bounds how long a partial batch waits before being
// uploaded. Zero disables periodic flushing.
func S3FlushInterval(d time.Duration) S3SinkOption {
	return func(c *s3SinkConfig) { c.flushInterval = d }
}

// S3MaxAttempts caps how many times the AWS SDK retries a failed
// upload before the failure policy applies.
func S3MaxAttempts(n int) S3SinkOption {
	return func(c *s3SinkConfig) { c.maxAttempts = n }
}

// S3Client injects a pre-built client, bypassing the sink's own
// construction. Useful for custom AWS configuration and for tests.
func S3Client(api S3API) S3SinkOption {
	return func(c *s3SinkConfig) { c.client = api }
}

// S3FailurePolicy sets what happens to records in a batch that could not
// be uploaded.
func S3FailurePolicy(p FailurePolicy) S3SinkOption {
	return func(c *s3SinkConfig) { c.failurePolicy = p }
}

// S3DLQ sets the dead-letter-queue for records that could not be
// uploaded. Used with FailurePolicyDLQ.
func S3DLQ(dlq DLQ) S3SinkOption {
	return func(c *s3SinkConfig) { c.dlq = dlq }
}

func (c *s3SinkConfig) applyDefaults() {
	if c.batchSize <= 0 {
		// Object storage bills per request and query engines suffer on
		// many small files, so batches are much larger here than for the
		// row-at-a-time sinks.
		c.batchSize = 5000
	}
	if c.flushInterval == 0 {
		// Bounds how stale the newest partition can be when throughput
		// is too low to fill a batch.
		c.flushInterval = time.Minute
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = 3
	}
	if c.serializer == nil {
		c.serializer = NewJSONSerializer()
	}
}
