package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

const historySDKManifest = `kind: sdk
name: orders-sdk
image: my-registry/orders-sdk:v1
`

func historyController(t *testing.T, fake *backend.Fake) (*Controller, *store.SQLite) {
	t.Helper()
	st, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := New(Options{
		Store: st, Backend: fake, Image: "img", StopTimeout: time.Second,
		Restart: lifecycle.DefaultRestartPolicy(),
	})
	return c, st
}

func TestPromSample(t *testing.T) {
	name, v, ok := promSample(`weibo_edge_queue_size{edge="edge-0"} 42`)
	if !ok || name != "weibo_edge_queue_size" || v != 42 {
		t.Fatalf("got %q %d %v", name, v, ok)
	}
	if _, _, ok := promSample(`# HELP weibo_edge_queue_size x`); ok {
		t.Fatal("comments must not parse")
	}
	if _, _, ok := promSample(`garbage`); ok {
		t.Fatal("garbage must not parse")
	}
	if _, _, ok := promSample(`weibo_stage_errors_total 3.0`); !ok {
		t.Fatal("bare (label-free) series must parse")
	}
}

func TestSumLag(t *testing.T) {
	src := []any{
		map[string]any{"topic": "orders", "partition": float64(0), "lag": float64(7)},
		map[string]any{"topic": "orders", "partition": float64(1), "lag": float64(5)},
	}
	if got := sumLag(src); got != 12 {
		t.Fatalf("lag=%d, want 12", got)
	}
	if got := sumLag(nil); got != 0 {
		t.Fatalf("nil lag=%d, want 0", got)
	}
	if got := sumLag("kafka"); got != 0 {
		t.Fatalf("wrong-shape lag=%d, want 0", got)
	}
}

func TestRecorderSamplesLiveJobs(t *testing.T) {
	fake := backend.NewFake()
	c, _ := historyController(t, fake)
	job, err := c.Submit(context.Background(), []byte(historySDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	r := newHistoryRecorder(c, c.history)
	calls := 0
	r.fetch = func(ctx context.Context, addr string) (Sample, error) {
		calls++
		return Sample{At: time.Now().UTC(), Phase: "running", RecordsOut: int64(calls * 100)}, nil
	}
	r.sampleOnce(context.Background())

	series := c.history.Series(job.ID, 0)
	if len(series) != 1 || series[0].RecordsOut != 100 {
		t.Fatalf("series=%+v", series)
	}
}

func TestRecorderSkipsFailedScrapes(t *testing.T) {
	fake := backend.NewFake()
	c, _ := historyController(t, fake)
	job, err := c.Submit(context.Background(), []byte(historySDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	r := newHistoryRecorder(c, c.history)
	r.fetch = func(ctx context.Context, addr string) (Sample, error) {
		return Sample{}, errors.New("agent down")
	}
	r.sampleOnce(context.Background())

	if series := c.history.Series(job.ID, 0); len(series) != 0 {
		t.Fatalf("failed scrape must leave no sample, got %+v", series)
	}
}
