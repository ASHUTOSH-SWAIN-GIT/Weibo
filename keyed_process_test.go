package weibo_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/operator"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/source"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestProcessKeyed_StateAndEventTimeTimer(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	src := source.NewSliceSource([]types.Record{
		{Key: []byte("a"), Value: []byte(`{"id":"a"}`), Timestamp: base},
		{Key: []byte("a"), Value: []byte(`{"id":"a"}`), Timestamp: base.Add(time.Second)},
		types.NewWatermark(base.Add(6 * time.Second)),
	})
	cap := &recordCaptureSink{}

	env := weibo.NewEnv()
	env.FromSource(src).
		KeyBy(func(r types.Record) []byte { return r.Key }).WithPartitions(2).
		ProcessKeyed(func(ctx *operator.KeyedContext, r types.Record) ([]types.Record, error) {
			vs := ctx.ValueState("count")
			var n uint64
			if raw := vs.Get(); len(raw) == 8 {
				n = binary.BigEndian.Uint64(raw)
			}
			n++
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], n)
			vs.Set(buf[:])
			ctx.RegisterEventTimeTimer(r.Timestamp.Add(5 * time.Second))
			return nil, nil
		}, func(ctx *operator.KeyedContext, ts time.Time) ([]types.Record, error) {
			vs := ctx.ValueState("count")
			n := binary.BigEndian.Uint64(vs.Get())
			value, _ := json.Marshal(map[string]any{"key": ctx.KeyString(), "count": n, "timer": ts.Unix()})
			return []types.Record{{Key: ctx.Key(), Value: value, Timestamp: ts}}, nil
		}, "stateful").
		ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(cap.records) != 2 {
		t.Fatalf("timer outputs: got %d records, want 2: %+v", len(cap.records), cap.records)
	}
	for _, r := range cap.records {
		var got map[string]any
		if err := json.Unmarshal(r.Value, &got); err != nil {
			t.Fatalf("timer output not JSON: %v", err)
		}
		if got["key"] != "a" || got["count"].(float64) != 2 {
			t.Fatalf("unexpected timer output: %s", r.Value)
		}
	}
}

func TestTypedProcessKeyed_StatefulTypedOutput(t *testing.T) {
	base := time.Unix(100, 0).UTC()
	src := source.NewSliceSource([]types.Record{
		{Value: []byte(`{"id":"a","status":"paid","amount":10}`), Timestamp: base},
		types.NewWatermark(base.Add(time.Second)),
	})
	cap := &recordCaptureSink{}

	env := weibo.NewEnv()
	weibo.FromTypedSource[typedOrder](env, src).
		KeyBy(func(o typedOrder) []byte { return []byte(o.ID) }).
		ProcessKeyed(func(ctx *operator.KeyedContext, o typedOrder) ([]typedOrder, error) {
			if err := ctx.ValueState("last").SetJSON(o); err != nil {
				return nil, err
			}
			ctx.RegisterEventTimeTimer(base.Add(time.Second))
			return nil, nil
		}, func(ctx *operator.KeyedContext, _ time.Time) ([]typedOrder, error) {
			var o typedOrder
			if err := ctx.ValueState("last").GetJSON(&o); err != nil {
				return nil, err
			}
			o.Amount++
			return []typedOrder{o}, nil
		}).
		ToSink(cap)

	if err := env.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(cap.records) != 1 {
		t.Fatalf("records: got %d, want 1", len(cap.records))
	}
	var got typedOrder
	if err := json.Unmarshal(cap.records[0].Value, &got); err != nil {
		t.Fatalf("unmarshal typed output: %v", err)
	}
	if got.ID != "a" || got.Amount != 11 {
		t.Fatalf("typed timer output = %+v", got)
	}
}
