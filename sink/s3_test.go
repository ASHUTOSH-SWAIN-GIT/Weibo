package sink

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// putCall records one upload the fake client received.
type putCall struct {
	bucket          string
	key             string
	body            []byte
	contentType     string
	contentEncoding string
}

// fakeS3 implements S3API in memory.
type fakeS3 struct {
	mu    sync.Mutex
	calls []putCall
	err   error // returned by every PutObject when set
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, _ := io.ReadAll(in.Body)

	f.mu.Lock()
	defer f.mu.Unlock()
	call := putCall{key: deref(in.Key), body: body, bucket: deref(in.Bucket)}
	call.contentType = deref(in.ContentType)
	call.contentEncoding = deref(in.ContentEncoding)
	f.calls = append(f.calls, call)

	if f.err != nil {
		return nil, f.err
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) snapshot() []putCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]putCall(nil), f.calls...)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// writeToS3 feeds values through a sink and returns Write's error.
func writeToS3(t *testing.T, s *S3Sink, values ...string) error {
	t.Helper()
	in := make(chan types.Record, len(values))
	for i, v := range values {
		in <- types.Record{Key: []byte("k"), Value: []byte(v), Offset: int64(i)}
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- s.Write(context.Background(), in) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("S3 sink Write did not return")
		return nil
	}
}

// TestS3Sink_UploadsNDJSONObject covers the default path: one object per
// batch, one serialized record per line.
func TestS3Sink_UploadsNDJSONObject(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(S3Bucket("lake"), S3Client(fake), S3BatchSize(10))

	if err := writeToS3(t, s, `{"a":1}`, `{"a":2}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(calls))
	}
	if calls[0].bucket != "lake" {
		t.Errorf("bucket = %q", calls[0].bucket)
	}
	if string(calls[0].body) != "{\"a\":1}\n{\"a\":2}\n" {
		t.Errorf("body = %q", calls[0].body)
	}
	if calls[0].contentType != "application/x-ndjson" {
		t.Errorf("content-type = %q", calls[0].contentType)
	}
	if calls[0].contentEncoding != "" {
		t.Errorf("unexpected content-encoding %q without gzip", calls[0].contentEncoding)
	}
}

// TestS3Sink_KeyLayout verifies the Hive-style partition layout that
// query engines prune on, plus the prefix.
func TestS3Sink_KeyLayout(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(S3Bucket("lake"), S3Client(fake), S3BatchSize(1), S3Prefix("/events/orders/"))

	if err := writeToS3(t, s, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(calls))
	}
	key := calls[0].key
	want := regexp.MustCompile(`^events/orders/date=\d{4}-\d{2}-\d{2}/hour=\d{2}/part-\d{8}T\d{6}-1-[0-9a-f]{8}\.jsonl$`)
	if !want.MatchString(key) {
		t.Errorf("key = %q, does not match expected layout %s", key, want)
	}
	// The prefix's surrounding slashes must not produce empty segments.
	if strings.Contains(key, "//") {
		t.Errorf("key has an empty path segment: %q", key)
	}
}

// TestS3Sink_KeysAreUnique guards the property that makes at-least-once
// safe here: S3 PUT overwrites silently, so two batches sharing a key
// would destroy data.
func TestS3Sink_KeysAreUnique(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(S3Bucket("lake"), S3Client(fake), S3BatchSize(1))

	if err := writeToS3(t, s, "a", "b", "c", "d"); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 4 {
		t.Fatalf("expected 4 uploads, got %d", len(calls))
	}
	seen := make(map[string]bool, len(calls))
	for _, c := range calls {
		if seen[c.key] {
			t.Errorf("duplicate object key would overwrite data: %q", c.key)
		}
		seen[c.key] = true
	}
}

// TestS3Sink_Gzip verifies the object is really gzip and round-trips,
// and that the encoding is declared so readers decompress it.
func TestS3Sink_Gzip(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(S3Bucket("lake"), S3Client(fake), S3BatchSize(10), S3Gzip())

	if err := writeToS3(t, s, `{"a":1}`); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(calls))
	}
	if calls[0].contentEncoding != "gzip" {
		t.Errorf("content-encoding = %q, want gzip", calls[0].contentEncoding)
	}
	if !strings.HasSuffix(calls[0].key, ".jsonl.gz") {
		t.Errorf("key = %q, want a .jsonl.gz suffix", calls[0].key)
	}

	zr, err := gzip.NewReader(bytes.NewReader(calls[0].body))
	if err != nil {
		t.Fatalf("body is not gzip: %v", err)
	}
	defer zr.Close()
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(got) != "{\"a\":1}\n" {
		t.Errorf("decompressed = %q", got)
	}
}

// TestS3Sink_BatchSizeSplitsObjects verifies large batches are what get
// uploaded — the whole point of the sizing defaults here.
func TestS3Sink_BatchSizeSplitsObjects(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(S3Bucket("lake"), S3Client(fake), S3BatchSize(2))

	if err := writeToS3(t, s, "1", "2", "3", "4", "5"); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 3 {
		t.Fatalf("expected 3 objects (2+2+1), got %d", len(calls))
	}
	if n := bytes.Count(calls[2].body, []byte("\n")); n != 1 {
		t.Errorf("final object had %d lines, want 1", n)
	}
}

// TestS3Sink_CustomKeyNaming covers the override used when a query
// engine expects a different layout.
func TestS3Sink_CustomKeyNaming(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(fake), S3BatchSize(1),
		S3KeyNaming(func(_ time.Time, seq int64) string {
			return fmt.Sprintf("custom/obj-%d.ndjson", seq)
		}),
	)

	if err := writeToS3(t, s, "a", "b"); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected 2 uploads, got %d", len(calls))
	}
	if calls[0].key != "custom/obj-1.ndjson" || calls[1].key != "custom/obj-2.ndjson" {
		t.Errorf("keys = %q, %q", calls[0].key, calls[1].key)
	}
}

// TestS3Sink_UploadFailureGoesToDLQ verifies a failed upload sends every
// record in the batch to the dead-letter queue rather than losing them.
func TestS3Sink_UploadFailureGoesToDLQ(t *testing.T) {
	fake := &fakeS3{err: errors.New("access denied")}
	dlq := &recordingDLQ{}
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(fake), S3BatchSize(3),
		S3FailurePolicy(FailurePolicyDLQ), S3DLQ(dlq),
	)

	if err := writeToS3(t, s, "a", "b", "c"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := dlq.snapshot(); len(got) != 3 {
		t.Errorf("expected all 3 records in the DLQ, got %d", len(got))
	}
}

// TestS3Sink_UploadFailureCanFailPipeline verifies FailurePolicyFail
// surfaces the error instead of dropping the batch.
func TestS3Sink_UploadFailureCanFailPipeline(t *testing.T) {
	fake := &fakeS3{err: errors.New("access denied")}
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(fake), S3BatchSize(1),
		S3FailurePolicy(FailurePolicyFail),
	)

	if err := writeToS3(t, s, "a"); err == nil {
		t.Fatal("expected an error under FailurePolicyFail")
	}
}

// TestS3Sink_SerializeErrorExcludedFromObject pins that a record the
// serializer rejected never reaches the object — a lake file containing
// malformed lines breaks every consumer that reads it later.
func TestS3Sink_SerializeErrorExcludedFromObject(t *testing.T) {
	fake := &fakeS3{}
	dlq := &recordingDLQ{}
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(fake), S3BatchSize(2),
		S3Serialize(SerializerFunc(func(r types.Record) ([]byte, error) {
			if string(r.Value) == "bad" {
				return nil, errors.New("cannot serialize")
			}
			return r.Value, nil
		})),
		S3FailurePolicy(FailurePolicyDLQ), S3DLQ(dlq),
	)

	if err := writeToS3(t, s, "bad", "good"); err != nil {
		t.Fatalf("write: %v", err)
	}

	calls := fake.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(calls))
	}
	if string(calls[0].body) != "good\n" {
		t.Errorf("object body = %q, want only the serializable record", calls[0].body)
	}
	if d := dlq.snapshot(); len(d) != 1 || string(d[0].Value) != "bad" {
		t.Errorf("DLQ = %v, want the unserializable record", d)
	}
}

// TestS3Sink_AllRecordsUnserializableUploadsNothing verifies no empty
// object is created when the whole batch was dead-lettered.
func TestS3Sink_AllRecordsUnserializableUploadsNothing(t *testing.T) {
	fake := &fakeS3{}
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(fake), S3BatchSize(1),
		S3Serialize(SerializerFunc(func(types.Record) ([]byte, error) {
			return nil, errors.New("nope")
		})),
	)

	if err := writeToS3(t, s, "a"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if n := len(fake.snapshot()); n != 0 {
		t.Errorf("expected no upload, got %d", n)
	}
}

// TestS3Sink_RequiresBucket covers the fail-fast construction contract.
func TestS3Sink_RequiresBucket(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic when S3Bucket is missing")
		}
	}()
	NewS3Sink(S3Client(&fakeS3{}))
}

// TestS3Sink_DescribeOmitsCredentials pins that Describe, which feeds the
// dashboard, never exposes static credentials.
func TestS3Sink_DescribeOmitsCredentials(t *testing.T) {
	s := NewS3Sink(
		S3Bucket("lake"), S3Client(&fakeS3{}),
		S3Region("us-east-1"),
		S3StaticCredentials("AKIAEXAMPLE", "sup3rs3cret", "tok3n"),
	)
	info := s.Describe()

	blob := fmt.Sprintf("%v", info.Props)
	for _, secret := range []string{"AKIAEXAMPLE", "sup3rs3cret", "tok3n"} {
		if strings.Contains(blob, secret) {
			t.Errorf("Describe leaked %q: %s", secret, blob)
		}
	}
	if info.Type != "S3" {
		t.Errorf("type = %q, want S3", info.Type)
	}
	if info.Props["bucket"] != "lake" {
		t.Errorf("bucket = %q", info.Props["bucket"])
	}
}
