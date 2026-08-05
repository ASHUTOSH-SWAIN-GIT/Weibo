package sink_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	weibo "github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// TestHTTPSink_EndToEndPipeline runs the HTTP sink as the tail of a real
// pipeline — source, operator, checkpoint barriers and all — rather than
// driving Write directly. It pins two things at once: records survive the
// full path, and the engine's control records (barriers, watermarks)
// never reach the endpoint as data.
func TestHTTPSink_EndToEndPipeline(t *testing.T) {
	var (
		mu    sync.Mutex
		lines []string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if l != "" {
				lines = append(lines, l)
			}
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	const n = 50
	records := make([]types.Record, n)
	for i := range records {
		records[i] = types.Record{
			Key:       []byte("k"),
			Value:     []byte(`{"i":` + strconv.Itoa(i) + `}`),
			Timestamp: time.Unix(int64(i), 0),
			Offset:    int64(i),
		}
	}

	env := weibo.NewEnv().
		WithCheckpointing(5*time.Millisecond, checkpoint.NewFileStorage(t.TempDir()))
	env.FromSource(source.NewSliceSource(records)).
		Map(func(r types.Record) types.Record { return r }, "passthrough").
		ToSink(sink.NewHTTPSink(
			sink.HTTPURL(ts.URL),
			sink.HTTPBatchSize(10),
			sink.HTTPFlushInterval(5*time.Millisecond),
		))

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(lines) != n {
		t.Errorf("endpoint received %d lines, want %d", len(lines), n)
	}
	for _, l := range lines {
		// A leaked barrier or watermark serializes to an empty or
		// null-ish payload rather than one of our records.
		if !strings.HasPrefix(l, `{"i":`) {
			t.Errorf("non-record payload reached the endpoint: %q", l)
		}
	}
}
