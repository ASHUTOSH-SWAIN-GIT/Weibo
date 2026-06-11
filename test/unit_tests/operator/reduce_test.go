package operator_test

import (
	"encoding/binary"
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
)

func TestReduce_BasicAggregation(t *testing.T) {
	sumFn := func(accum []byte, curr types.Record) []byte {
		prev := 0
		if accum != nil {
			prev = int(binary.BigEndian.Uint64(accum))
		}
		next := prev + len(curr.Value)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(next))
		return buf
	}

	op := operator.Reduce(sumFn)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k1"), Value: []byte("hello")}
	in <- types.Record{Key: []byte("k1"), Value: []byte("world!")}

	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if got := binary.BigEndian.Uint64(results[0].Value); got != 5 {
		t.Errorf("first result: got %d, want 5", got)
	}
	if got := binary.BigEndian.Uint64(results[1].Value); got != 11 {
		t.Errorf("second result: got %d, want 11", got)
	}
}

func TestReduce_PerKeyState(t *testing.T) {
	countFn := func(accum []byte, curr types.Record) []byte {
		prev := 0
		if accum != nil {
			prev = int(binary.BigEndian.Uint64(accum))
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(prev+1))
		return buf
	}

	op := operator.Reduce(countFn)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("alice"), Value: []byte("a")}
	in <- types.Record{Key: []byte("bob"), Value: []byte("b")}
	in <- types.Record{Key: []byte("alice"), Value: []byte("c")}

	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	aliceResults := []uint64{}
	bobResults := []uint64{}
	for _, r := range results {
		val := binary.BigEndian.Uint64(r.Value)
		switch string(r.Key) {
		case "alice":
			aliceResults = append(aliceResults, val)
		case "bob":
			bobResults = append(bobResults, val)
		}
	}

	if len(aliceResults) != 2 {
		t.Fatalf("alice: expected 2 results, got %d", len(aliceResults))
	}
	if aliceResults[0] != 1 {
		t.Errorf("alice first: got %d, want 1", aliceResults[0])
	}
	if aliceResults[1] != 2 {
		t.Errorf("alice second: got %d, want 2", aliceResults[1])
	}

	if len(bobResults) != 1 {
		t.Fatalf("bob: expected 1 result, got %d", len(bobResults))
	}
	if bobResults[0] != 1 {
		t.Errorf("bob first: got %d, want 1", bobResults[0])
	}
}

func TestReduce_PassesThroughWatermarks(t *testing.T) {
	op := operator.Reduce(func(accum []byte, curr types.Record) []byte { return curr.Value })

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	wm := types.NewWatermark(time.Unix(100, 0))
	in <- wm
	close(in)

	var gotWM bool
	for r := range out {
		if r.IsWatermark {
			gotWM = true
			if !r.Timestamp.Equal(time.Unix(100, 0)) {
				t.Errorf("watermark timestamp: got %v, want %v", r.Timestamp, time.Unix(100, 0))
			}
		}
	}
	if !gotWM {
		t.Error("expected watermark to pass through Reduce")
	}
}

func TestReduce_WithWindowHeaders(t *testing.T) {
	countFn := func(accum []byte, curr types.Record) []byte {
		prev := 0
		if accum != nil {
			prev = int(binary.BigEndian.Uint64(accum))
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(prev+1))
		return buf
	}

	op := operator.Reduce(countFn)

	in := make(chan types.Record, 10)
	out := make(chan types.Record, 10)

	go op.Process(in, out)

	ws1 := "2026-01-01T00:00:00Z"
	we1 := "2026-01-01T00:05:00Z"
	ws2 := "2026-01-01T00:05:00Z"
	we2 := "2026-01-01T00:10:00Z"

	in <- types.Record{
		Key:   []byte("k1"),
		Value: []byte("a"),
		Headers: map[string][]byte{
			"window_start": []byte(ws1),
			"window_end":   []byte(we1),
		},
	}
	in <- types.Record{
		Key:   []byte("k1"),
		Value: []byte("b"),
		Headers: map[string][]byte{
			"window_start": []byte(ws1),
			"window_end":   []byte(we1),
		},
	}
	in <- types.Record{
		Key:   []byte("k1"),
		Value: []byte("c"),
		Headers: map[string][]byte{
			"window_start": []byte(ws2),
			"window_end":   []byte(we2),
		},
	}

	close(in)

	var window1Count, window2Count uint64
	var window1Results, window2Results int
	for r := range out {
		ws := string(r.Headers["window_start"])
		val := binary.BigEndian.Uint64(r.Value)
		if ws == ws1 {
			window1Results++
			window1Count = val
		} else if ws == ws2 {
			window2Results++
			window2Count = val
		}
	}

	if window1Results != 2 {
		t.Errorf("window1: expected 2 results, got %d", window1Results)
	}
	if window1Count != 2 {
		t.Errorf("window1 final count: got %d, want 2", window1Count)
	}
	if window2Results != 1 {
		t.Errorf("window2: expected 1 result, got %d", window2Results)
	}
	if window2Count != 1 {
		t.Errorf("window2 final count: got %d, want 1", window2Count)
	}
}

func TestReduce_FirstRecordGetsNilAccum(t *testing.T) {
	var firstAccum []byte
	op := operator.Reduce(func(accum []byte, curr types.Record) []byte {
		if firstAccum == nil && accum != nil {
			t.Error("expected nil accum on first call")
		}
		if accum == nil {
			firstAccum = []byte("was-nil")
		}
		return curr.Value
	})

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	in <- types.Record{Key: []byte("k"), Value: []byte("first")}
	close(in)

	for range out {
	}
}

func TestStateKey_WithWindowHeaders(t *testing.T) {
	r := types.Record{
		Key: []byte("mykey"),
		Headers: map[string][]byte{
			"window_start": []byte("2026-01-01T00:00:00Z"),
			"window_end":   []byte("2026-01-01T00:05:00Z"),
		},
	}
	key := operator.StateKey(r)
	want := "mykey/2026-01-01T00:00:00Z/2026-01-01T00:05:00Z"
	if key != want {
		t.Errorf("stateKey with window: got %q, want %q", key, want)
	}
}

func TestStateKey_WithoutWindowHeaders(t *testing.T) {
	r := types.Record{Key: []byte("mykey")}
	key := operator.StateKey(r)
	if key != "mykey" {
		t.Errorf("stateKey without window: got %q, want %q", key, "mykey")
	}
}
