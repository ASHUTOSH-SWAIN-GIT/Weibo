package operator_test

import (
	"strings"
	"testing"
	"time"

	"mailer/operator"
	"mailer/types"
)

func TestMapOperator_BasicTransform(t *testing.T) {
	op := operator.Map(func(r types.Record) types.Record {
		r.Value = []byte(strings.ToUpper(string(r.Value)))
		return r
	})

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	in <- types.Record{Value: []byte("hello")}
	in <- types.Record{Value: []byte("world")}
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Value) != "HELLO" {
		t.Errorf("first: got %q, want %q", results[0].Value, "HELLO")
	}
	if string(results[1].Value) != "WORLD" {
		t.Errorf("second: got %q, want %q", results[1].Value, "WORLD")
	}
}

func TestMapOperator_PassesThroughWatermarks(t *testing.T) {
	op := operator.Map(func(r types.Record) types.Record { return r })

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	wm := types.NewWatermark(time.Unix(42, 0))
	in <- wm
	close(in)

	r := <-out
	if !r.IsWatermark {
		t.Error("expected watermark to pass through")
	}
	if !r.Timestamp.Equal(time.Unix(42, 0)) {
		t.Errorf("watermark: got %v, want %v", r.Timestamp, time.Unix(42, 0))
	}
}

func TestFilterOperator_KeepsMatching(t *testing.T) {
	op := operator.Filter(func(r types.Record) bool {
		return len(r.Value) > 3
	})

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	in <- types.Record{Value: []byte("hi")}
	in <- types.Record{Value: []byte("hello")}
	in <- types.Record{Value: []byte("hey")}
	in <- types.Record{Value: []byte("world!")}
	close(in)

	var count int
	for range out {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 records, got %d", count)
	}
}

func TestFilterOperator_PassesThroughWatermarks(t *testing.T) {
	op := operator.Filter(func(r types.Record) bool { return false })

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	wm := types.NewWatermark(time.Unix(10, 0))
	in <- wm
	close(in)

	r := <-out
	if !r.IsWatermark {
		t.Error("watermark should pass through Filter even if predicate is false")
	}
}

func TestFlatMapOperator_OneToMany(t *testing.T) {
	op := operator.FlatMap(func(r types.Record) []types.Record {
		return []types.Record{
			{Value: []byte(string(r.Value) + "-1")},
			{Value: []byte(string(r.Value) + "-2")},
		}
	})

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	in <- types.Record{Value: []byte("x")}
	close(in)

	var results []types.Record
	for r := range out {
		results = append(results, r)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Value) != "x-1" {
		t.Errorf("first: got %q", results[0].Value)
	}
	if string(results[1].Value) != "x-2" {
		t.Errorf("second: got %q", results[1].Value)
	}
}

func TestFlatMapOperator_EmptySliceFiltersOut(t *testing.T) {
	op := operator.FlatMap(func(r types.Record) []types.Record {
		if string(r.Value) == "drop" {
			return nil
		}
		return []types.Record{r}
	})

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	in <- types.Record{Value: []byte("keep")}
	in <- types.Record{Value: []byte("drop")}
	in <- types.Record{Value: []byte("also")}
	close(in)

	var count int
	for range out {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 records after filtering, got %d", count)
	}
}

func TestFlatMapOperator_PassesThroughWatermarks(t *testing.T) {
	op := operator.FlatMap(func(r types.Record) []types.Record { return []types.Record{r} })

	in := make(chan types.Record, 5)
	out := make(chan types.Record, 5)

	go op.Process(in, out)

	wm := types.NewWatermark(time.Unix(7, 0))
	in <- wm
	close(in)

	r := <-out
	if !r.IsWatermark {
		t.Error("watermark should pass through FlatMap")
	}
}
