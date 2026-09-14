package operator

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestKeyedProcessOperator_StateSnapshotRestore(t *testing.T) {
	op := KeyedProcess(func(ctx *KeyedContext, r types.Record) ([]types.Record, error) {
		vs := ctx.ValueState("count")
		var n uint64
		if raw := vs.Get(); len(raw) == 8 {
			n = binary.BigEndian.Uint64(raw)
		}
		n++
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], n)
		vs.Set(buf[:])
		ctx.RegisterEventTimeTimer(r.Timestamp.Add(time.Second))
		return nil, nil
	}, nil)

	in := make(chan types.Record, 1)
	out := make(chan types.Record, 1)
	in <- types.Record{Key: []byte("k"), Timestamp: time.Unix(10, 0)}
	close(in)
	op.Process(in, out)

	snap, err := op.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := KeyedProcess(nil, nil)
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	vs := restored.backend.ValueState("count")
	vs.SetKey("k")
	if got := binary.BigEndian.Uint64(vs.Get()); got != 1 {
		t.Fatalf("restored count = %d, want 1", got)
	}
	timers := restored.backend.ValueState(keyedProcessTimersNS)
	timers.SetKey("k")
	if len(unmarshalTimers(timers.Get())) != 1 {
		t.Fatalf("timer was not restored")
	}
}
