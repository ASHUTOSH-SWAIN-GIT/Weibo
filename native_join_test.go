package weibo_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/checkpoint"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestNativeJoinSources_ExecutesTwoInputJoin(t *testing.T) {
	ts := time.Unix(100, 0).UTC()
	orders := source.NewSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"order_id":"o1","amount":42}`), Timestamp: ts},
		{Key: []byte("o2"), Value: []byte(`{"order_id":"o2","amount":10}`), Timestamp: ts},
	})
	payments := source.NewSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"order_id":"o1","status":"paid"}`), Timestamp: ts.Add(500 * time.Millisecond)},
		{Key: []byte("o3"), Value: []byte(`{"order_id":"o3","status":"paid"}`), Timestamp: ts},
	})
	cap := &captureSink{}

	env := weibo.NewEnv()
	env.JoinSourcesWithin("orders", orders, "payments", payments, time.Second, nil, "orders-payments").
		ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute native join: %v", err)
	}
	if len(cap.vals) != 1 {
		t.Fatalf("joined records: got %d values %q, want 1", len(cap.vals), cap.vals)
	}

	var got struct {
		Left  map[string]any `json:"left"`
		Right map[string]any `json:"right"`
	}
	if err := json.Unmarshal(cap.vals[0], &got); err != nil {
		t.Fatalf("join output is not JSON: %v; value=%q", err, cap.vals[0])
	}
	if got.Left["order_id"] != "o1" || got.Right["status"] != "paid" {
		t.Fatalf("unexpected join payload: %+v", got)
	}
}

func TestNativeJoinSources_PlanGraphShowsTwoParents(t *testing.T) {
	ts := time.Unix(100, 0).UTC()
	orders := source.NewSliceSource([]types.Record{{Key: []byte("o1"), Value: []byte(`{"id":"o1"}`), Timestamp: ts}})
	payments := source.NewSliceSource([]types.Record{{Key: []byte("o1"), Value: []byte(`{"id":"o1"}`), Timestamp: ts}})

	env := weibo.NewEnv()
	env.JoinSourcesWithin("orders", orders, "payments", payments, time.Second, nil).
		ToSink(&captureSink{})
	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute native join: %v", err)
	}

	plan := env.PlanJSON()
	for _, want := range []string{
		`"name": "source-orders"`,
		`"name": "source-payments"`,
		`"name": "join-0"`,
		`"from": "source-orders"`,
		`"from": "source-payments"`,
		`"to": "join-0"`,
	} {
		if !strings.Contains(plan, want) {
			t.Fatalf("plan missing %s:\n%s", want, plan)
		}
	}
}

func TestNativeJoinSources_CheckpointsAndRestoresBothSourceOffsets(t *testing.T) {
	ts := time.Unix(100, 0).UTC()
	left1 := newCheckpointSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"o1"}`), Timestamp: ts},
	})
	right1 := newCheckpointSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"o1"}`), Timestamp: ts},
	})
	storage := checkpoint.NewFileStorage(t.TempDir())

	env1 := weibo.NewEnv().WithCheckpointing(time.Hour, storage)
	env1.JoinSourcesWithin("left", left1, "right", right1, time.Second, nil).
		ToSink(&captureSink{})
	if err := env1.Execute(context.Background()); err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	cp, err := storage.Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if cp == nil || len(cp.Source["join:left"]) == 0 || len(cp.Source["join:right"]) == 0 {
		t.Fatalf("checkpoint did not store both join source offsets: %+v", cp)
	}

	left2 := newCheckpointSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"duplicate-left"}`), Timestamp: ts},
		{Key: []byte("o2"), Value: []byte(`{"id":"new-left"}`), Timestamp: ts},
	})
	right2 := newCheckpointSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"duplicate-right"}`), Timestamp: ts},
		{Key: []byte("o2"), Value: []byte(`{"id":"new-right"}`), Timestamp: ts},
	})
	cap := &captureSink{}
	env2 := weibo.NewEnv().WithCheckpointing(time.Hour, storage)
	env2.JoinSourcesWithin("left", left2, "right", right2, time.Second, nil).
		ToSink(cap)
	if err := env2.Execute(context.Background()); err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if left2.start() != 1 || right2.start() != 1 {
		t.Fatalf("sources did not restore offsets: left=%d right=%d", left2.start(), right2.start())
	}
	if len(cap.vals) != 1 || !strings.Contains(string(cap.vals[0]), "new-left") || !strings.Contains(string(cap.vals[0]), "new-right") {
		t.Fatalf("restored run joined wrong records: %q", cap.vals)
	}
}

type checkpointSliceSource struct {
	mu      sync.Mutex
	records []types.Record
	pos     int
	started int
}

func newCheckpointSliceSource(records []types.Record) *checkpointSliceSource {
	return &checkpointSliceSource{records: records}
}

func (s *checkpointSliceSource) Run(ctx context.Context, out chan<- types.Record) error {
	s.mu.Lock()
	s.started = s.pos
	s.mu.Unlock()
	for {
		s.mu.Lock()
		if s.pos >= len(s.records) {
			s.mu.Unlock()
			return nil
		}
		r := s.records[s.pos]
		s.pos++
		r.Offset = int64(s.pos - 1)
		s.mu.Unlock()
		select {
		case out <- r:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *checkpointSliceSource) CheckpointOffset() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(map[string]int64{"0": int64(s.pos)})
}

func (s *checkpointSliceSource) RestoreOffset(data []byte) error {
	var offsets map[string]int64
	if err := json.Unmarshal(data, &offsets); err != nil {
		return err
	}
	off, ok := offsets["0"]
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pos = int(off)
	return nil
}

func (s *checkpointSliceSource) start() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}
