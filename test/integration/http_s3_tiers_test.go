package integration_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sdk"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// ---------------------------------------------------------------------------
// Offline half: runs on every PR.
// ---------------------------------------------------------------------------

// scriptedServer replays HTTP status codes (last repeats) and records every
// request body it receives.
type scriptedServer struct {
	*httptest.Server
	mu     sync.Mutex
	codes  []int
	bodies []string
	count  int
}

func newScriptedServer(codes ...int) *scriptedServer {
	s := &scriptedServer{codes: codes}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.bodies = append(s.bodies, string(body))
		code := s.codes[min(s.count, len(s.codes)-1)]
		s.count++
		s.mu.Unlock()
		w.WriteHeader(code)
	}))
	return s
}

func writeHTTPSink(t *testing.T, h *sink.HTTPSink, values ...string) error {
	t.Helper()
	in := make(chan types.Record, len(values))
	for _, v := range values {
		in <- types.Record{Value: []byte(v)}
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

// Retry tier: a 500 then a 200 delivers exactly once — the retried request
// carries the identical body (idempotent retry).
func TestTier_HTTPRetryIdempotent(t *testing.T) {
	ts := newScriptedServer(http.StatusInternalServerError, http.StatusOK)
	defer ts.Close()

	h := sink.NewHTTPSink(
		sink.HTTPURL(ts.URL), sink.HTTPBatchSize(10), sink.HTTPMaxRetries(3),
	)
	if err := writeHTTPSink(t, h, `{"id":"a"}`, `{"id":"b"}`); err != nil {
		t.Fatalf("Write: %v", err)
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.count != 2 {
		t.Fatalf("attempts = %d, want 2 (fail then succeed)", ts.count)
	}
	if ts.bodies[0] != ts.bodies[1] {
		t.Fatalf("retry body changed:\nfirst:  %q\nsecond: %q", ts.bodies[0], ts.bodies[1])
	}
}

// A 400 is permanent: one attempt, then the failure policy applies.
func TestTier_HTTPNoRetryOnClientError(t *testing.T) {
	ts := newScriptedServer(http.StatusBadRequest)
	defer ts.Close()

	dlq := &chanDLQ{ch: make(chan types.Record, 8)}
	h := sink.NewHTTPSink(
		sink.HTTPURL(ts.URL), sink.HTTPBatchSize(1), sink.HTTPMaxRetries(5),
		sink.HTTPFailurePolicy(sink.FailurePolicyDLQ), sink.HTTPDLQ(dlq),
	)
	if err := writeHTTPSink(t, h, `{"id":"bad"}`); err != nil {
		t.Fatalf("Write: %v", err)
	}
	ts.mu.Lock()
	attempts := ts.count
	ts.mu.Unlock()
	if attempts != 1 {
		t.Errorf("attempts = %d, want exactly 1 for a 4xx", attempts)
	}
	select {
	case <-dlq.ch:
	default:
		t.Error("permanently failed record did not reach the DLQ")
	}
}

type chanDLQ struct{ ch chan types.Record }

func (d *chanDLQ) Write(_ context.Context, r types.Record) error {
	d.ch <- r
	return nil
}

// Savepoint round trip through the file blobstore: promote → restore into
// a fresh storage with state and offsets intact (cross-node/job portable).
func TestTier_SavepointRoundTrip(t *testing.T) {
	src := checkpoint.NewFileStorage(t.TempDir())
	seedTierCheckpoint(t, src, "cp-1", "worker-0", "sst-bytes")
	bs := checkpoint.NewFileBlobstore(t.TempDir())

	promoted, err := checkpoint.CreateSavepoint(src, bs, "before-upgrade")
	if err != nil {
		t.Fatalf("CreateSavepoint: %v", err)
	}
	if promoted != "cp-1" {
		t.Errorf("promoted id = %q, want cp-1", promoted)
	}
	labels, err := checkpoint.ListSavepoints(bs)
	if err != nil || len(labels) != 1 || labels[0] != "before-upgrade" {
		t.Fatalf("ListSavepoints = %v, %v", labels, err)
	}

	dst := checkpoint.NewFileStorage(t.TempDir())
	id, err := checkpoint.RestoreSavepoint(dst, bs, "before-upgrade")
	if err != nil {
		t.Fatalf("RestoreSavepoint: %v", err)
	}
	if id != "cp-1" {
		t.Errorf("restored id = %q, want cp-1", id)
	}
	data, err := dst.LoadLatestCompleted()
	if err != nil || data == nil || string(data.Source["offset"]) != "42" {
		t.Fatalf("restored checkpoint wrong: %+v err=%v", data, err)
	}
	got, err := os.ReadFile(filepath.Join(dst.StateDir("cp-1"), "worker-0", "000001.sst"))
	if err != nil || string(got) != "sst-bytes" {
		t.Fatalf("restored state wrong: %q err=%v", got, err)
	}
}

func seedTierCheckpoint(t *testing.T, fs *checkpoint.FileStorage, id, owner, stateContent string) {
	t.Helper()
	ownerDir := filepath.Join(fs.StateDir(id), owner)
	if err := os.MkdirAll(ownerDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownerDir, "000001.sst"), []byte(stateContent), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := fs.Save(&checkpoint.CheckpointData{
		ID:        id,
		Operators: map[string][]byte{owner: []byte(`{"state_ref":"` + owner + `"}`)},
		Source:    map[string][]byte{"offset": []byte("42")},
		Status:    checkpoint.StatusCompleted,
		StateDirs: map[string]string{owner: owner},
	}); err != nil {
		t.Fatal(err)
	}
}

// S3 savepoint configuration resolves from the documented environment
// without touching the network.
func TestTier_S3SavepointConfigFromEnv(t *testing.T) {
	t.Setenv("SAVEPOINT_S3_BUCKET", "weibo-savepoints")
	t.Setenv("SAVEPOINT_S3_PREFIX", "prod")
	t.Setenv("SAVEPOINT_S3_ENDPOINT", "https://minio.example.com")
	t.Setenv("SAVEPOINT_S3_PATH_STYLE", "true")
	t.Setenv("AWS_ACCESS_KEY_ID", "akid")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "shh")
	opts := sdk.SavepointS3FromEnv(os.Getenv)
	if opts.Bucket != "weibo-savepoints" || opts.Prefix != "prod" {
		t.Errorf("bucket/prefix = %q/%q", opts.Bucket, opts.Prefix)
	}
	if opts.Endpoint != "https://minio.example.com" || !opts.PathStyle {
		t.Errorf("endpoint/pathstyle = %q/%v", opts.Endpoint, opts.PathStyle)
	}
	if opts.AccessKey != "akid" || opts.SecretKey != "shh" {
		t.Error("credentials not resolved from AWS_* env")
	}
	if _, err := checkpoint.NewS3Blobstore(checkpoint.S3BlobBucket("")); err == nil {
		t.Error("expected error for an empty S3 bucket")
	}
}

// ---------------------------------------------------------------------------
// Live half: needs SAVEPOINT_S3_BUCKET (+ endpoint/creds for MinIO).
// ---------------------------------------------------------------------------

// The same savepoint archive round-trips through S3-compatible storage.
func TestLive_S3SavepointRoundTrip(t *testing.T) {
	bucket := os.Getenv("SAVEPOINT_S3_BUCKET")
	if bucket == "" {
		t.Skip("SAVEPOINT_S3_BUCKET is not set")
	}
	var opts []checkpoint.S3BlobstoreOption
	opts = append(opts, checkpoint.S3BlobBucket(bucket))
	if p := os.Getenv("SAVEPOINT_S3_PREFIX"); p != "" {
		opts = append(opts, checkpoint.S3BlobPrefix("tier-"+p))
	} else {
		opts = append(opts, checkpoint.S3BlobPrefix("weibo-tier"))
	}
	if e := os.Getenv("SAVEPOINT_S3_ENDPOINT"); e != "" {
		opts = append(opts, checkpoint.S3BlobEndpoint(e))
	}
	if strings.EqualFold(os.Getenv("SAVEPOINT_S3_PATH_STYLE"), "true") {
		opts = append(opts, checkpoint.S3BlobPathStyle())
	}
	if ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"); ak != "" {
		opts = append(opts, checkpoint.S3BlobStaticCredentials(ak, sk, os.Getenv("AWS_SESSION_TOKEN")))
	}
	bs, err := checkpoint.NewS3Blobstore(opts...)
	if err != nil {
		t.Fatalf("NewS3Blobstore: %v", err)
	}

	src := checkpoint.NewFileStorage(t.TempDir())
	seedTierCheckpoint(t, src, "cp-s3", "worker-0", "s3-state-bytes")
	label := "tier-roundtrip"
	if _, err := checkpoint.CreateSavepoint(src, bs, label); err != nil {
		t.Skipf("S3 savepoint not reachable: %v", err)
	}
	t.Cleanup(func() { _ = bs.Delete(checkpoint.SavepointKey(label)) })

	dst := checkpoint.NewFileStorage(t.TempDir())
	id, err := checkpoint.RestoreSavepoint(dst, bs, label)
	if err != nil {
		t.Fatalf("RestoreSavepoint from S3: %v", err)
	}
	if id != "cp-s3" {
		t.Fatalf("restored id = %q, want cp-s3", id)
	}
	got, err := os.ReadFile(filepath.Join(dst.StateDir("cp-s3"), "worker-0", "000001.sst"))
	if err != nil || string(got) != "s3-state-bytes" {
		t.Fatalf("restored S3 state wrong: %q err=%v", got, err)
	}
}
