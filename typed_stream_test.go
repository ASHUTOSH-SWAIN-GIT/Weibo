package weibo_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type typedOrder struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Amount int    `json:"amount"`
}

type typedReceipt struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func TestTypedStream_FilterMapAndUntypedSink(t *testing.T) {
	src := source.NewSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"o1","status":"paid","amount":10}`), Source: "orders", Partition: 3, Offset: 7},
		{Key: []byte("o2"), Value: []byte(`{"id":"o2","status":"pending","amount":20}`), Source: "orders", Partition: 3, Offset: 8},
	})
	cap := &recordCaptureSink{}
	env := weibo.NewEnv()

	weibo.FromTypedSource[typedOrder](env, src).
		Filter(func(o typedOrder) bool { return o.Status == "paid" }, "paid-only").
		Map(func(o typedOrder) typedOrder {
			o.Amount *= 2
			return o
		}, "double").
		ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(cap.records) != 1 {
		t.Fatalf("records: got %d, want 1", len(cap.records))
	}
	var got typedOrder
	if err := json.Unmarshal(cap.records[0].Value, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got.ID != "o1" || got.Amount != 20 {
		t.Fatalf("output = %+v, want doubled paid order", got)
	}
	if cap.records[0].Source != "orders" || cap.records[0].Partition != 3 || cap.records[0].Offset != 7 {
		t.Fatalf("metadata was not preserved: %+v", cap.records[0])
	}
}

func TestTypedStream_TypedSourceFlatMapAndTypedSink(t *testing.T) {
	env := weibo.NewEnv()
	var mu sync.Mutex
	var got []weibo.TypedRecord[typedReceipt]

	weibo.FlatMapTyped(weibo.FromTypedValues(env, []typedOrder{
		{ID: "o1", Status: "paid", Amount: 10},
		{ID: "o2", Status: "paid", Amount: 20},
	}), func(o typedOrder) []typedReceipt {
		return []typedReceipt{
			{ID: o.ID + "-a", Total: o.Amount},
			{ID: o.ID + "-b", Total: o.Amount * 2},
		}
	}).ToTypedSink(weibo.TypedSinkFunc[typedReceipt](func(_ context.Context, r weibo.TypedRecord[typedReceipt]) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, r)
		return nil
	}))

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("typed sink records: got %d, want 4: %+v", len(got), got)
	}
	if got[0].Value != (typedReceipt{ID: "o1-a", Total: 10}) || got[3].Value != (typedReceipt{ID: "o2-b", Total: 40}) {
		t.Fatalf("unexpected typed sink values: %+v", got)
	}
}

func TestTypedStream_KeyByPreservesTypedPayload(t *testing.T) {
	env := weibo.NewEnv()
	cap := &recordCaptureSink{}

	weibo.FromTypedValues(env, []typedOrder{
		{ID: "o1", Status: "paid", Amount: 10},
	}).
		KeyBy(func(o typedOrder) []byte { return []byte(o.ID) }).
		ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(cap.records) != 1 {
		t.Fatalf("records: got %d, want 1", len(cap.records))
	}
	if string(cap.records[0].Key) != "o1" {
		t.Fatalf("key = %q, want o1", cap.records[0].Key)
	}
	var got typedOrder
	if err := json.Unmarshal(cap.records[0].Value, &got); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if got.ID != "o1" || got.Amount != 10 {
		t.Fatalf("typed payload changed: %+v", got)
	}
}

func TestTypedStream_MapTypedChangesPayloadType(t *testing.T) {
	src := source.NewSliceSource([]types.Record{
		{Key: []byte("o1"), Value: []byte(`{"id":"o1","status":"paid","amount":10}`)},
	})
	cap := &recordCaptureSink{}
	env := weibo.NewEnv()

	weibo.MapTyped(weibo.FromTypedSource[typedOrder](env, src), func(o typedOrder) typedReceipt {
		return typedReceipt{ID: o.ID, Total: o.Amount}
	}).ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	var got typedReceipt
	if err := json.Unmarshal(cap.records[0].Value, &got); err != nil {
		t.Fatalf("unmarshal receipt: %v", err)
	}
	if got != (typedReceipt{ID: "o1", Total: 10}) {
		t.Fatalf("receipt = %+v", got)
	}
}

func TestTypedStream_DecodeFailureFailsPipeline(t *testing.T) {
	env := weibo.NewEnv()
	src := source.NewSliceSource([]types.Record{{Key: []byte("bad"), Value: []byte(`not-json`)}})
	weibo.FromTypedSource[typedOrder](env, src).
		Map(func(o typedOrder) typedOrder { return o }).
		ToSink(&recordCaptureSink{})

	err := env.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode JSON") {
		t.Fatalf("Execute error = %v, want decode JSON failure", err)
	}
}

type recordCaptureSink struct {
	records []types.Record
}

func (s *recordCaptureSink) Write(ctx context.Context, in <-chan types.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-in:
			if !ok {
				return nil
			}
			s.records = append(s.records, r)
		}
	}
}
