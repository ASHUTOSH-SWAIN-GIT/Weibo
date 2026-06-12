package source_test

import (
	"context"
	"testing"
	"time"

	"mailer/source"
	"mailer/types"
)

func TestSliceSource_Empty(t *testing.T) {
	src := source.NewSliceSource(nil)
	ch := make(chan types.Record, 1)

	err := src.Run(context.Background(), ch)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestSliceSource_EmitsAllRecords(t *testing.T) {
	records := []types.Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	src := source.NewSliceSource(records)
	ch := make(chan types.Record, len(records))

	go func() {
		if err := src.Run(context.Background(), ch); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	var got []types.Record
	for i := 0; i < len(records); i++ {
		select {
		case r := <-ch:
			got = append(got, r)
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for record %d", i)
		}
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
	for i, r := range got {
		if string(r.Key) != string(records[i].Key) {
			t.Errorf("record %d: key got %q, want %q", i, r.Key, records[i].Key)
		}
		if string(r.Value) != string(records[i].Value) {
			t.Errorf("record %d: value got %q, want %q", i, r.Value, records[i].Value)
		}
	}
}

func TestSliceSource_RespectsContextCancellation(t *testing.T) {
	records := []types.Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	src := source.NewSliceSource(records)
	ch := make(chan types.Record, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := src.Run(ctx, ch)
	if err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("expected context error, got: %v", err)
	}
}

func TestSliceSource_ImplementsSource(t *testing.T) {
	var _ source.Source = source.NewSliceSource(nil)
}
