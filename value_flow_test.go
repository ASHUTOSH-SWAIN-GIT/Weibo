package weibo_test

import (
	"context"
	"sync"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/operators"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/record"
)

// captureSink records the (copied) Value of every record it receives.
type captureSink struct {
	mu   sync.Mutex
	vals [][]byte
}

func (s *captureSink) Write(ctx context.Context, in <-chan types.Record) error {
	for r := range in {
		v := make([]byte, len(r.Value))
		copy(v, r.Value)
		s.mu.Lock()
		s.vals = append(s.vals, v)
		s.mu.Unlock()
	}
	return nil
}

// Does a record's JSON Value survive source → declarative filter → sink?
// This mirrors the declarative YAML path (raw bytes, no deserializer).
func TestValueSurvivesFilterToSink(t *testing.T) {
	src := source.NewGeneratorSource([]types.Record{
		{Value: []byte(`{"status":"completed","order_id":"o1","amount":10}`)},
		{Value: []byte(`{"status":"pending","order_id":"o2","amount":5}`)},
		{Value: []byte(`{"status":"completed","order_id":"o3","amount":7}`)},
	})
	cap := &captureSink{}
	f := operators.BuildFilter(operators.FilterConfig{Field: "status", Operator: "equals", Value: "completed"})

	env := weibo.NewEnv().FromSource(src).Filter(f, "completed").ToSink(cap)
	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if len(cap.vals) != 2 {
		t.Fatalf("expected 2 completed records at sink, got %d: %q", len(cap.vals), cap.vals)
	}
	for i, v := range cap.vals {
		jr, err := record.DecodeJSON(types.Record{Value: v})
		oid, ok := record.GetField(jr, "order_id")
		t.Logf("sink record %d: value=%q -> order_id=%v (present=%v, err=%v)", i, v, oid, ok, err)
		if !ok {
			t.Errorf("record %d lost its value between filter and sink: %q", i, v)
		}
	}
}
