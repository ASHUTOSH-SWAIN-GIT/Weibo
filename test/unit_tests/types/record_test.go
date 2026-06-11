package types_test

import (
	"testing"
	"time"

	"mailer/types"
)

func TestNewRecord(t *testing.T) {
	r := types.NewRecord([]byte("key1"), []byte("val1"))
	if string(r.Key) != "key1" {
		t.Errorf("Key: got %q, want %q", r.Key, "key1")
	}
	if string(r.Value) != "val1" {
		t.Errorf("Value: got %q, want %q", r.Value, "val1")
	}
	if r.IsWatermark {
		t.Error("IsWatermark should be false for data records")
	}
	if r.Timestamp.IsZero() {
		t.Error("Timestamp should be set to current time")
	}
}

func TestNewWatermark(t *testing.T) {
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := types.NewWatermark(ts)
	if !r.IsWatermark {
		t.Error("IsWatermark should be true")
	}
	if !r.Timestamp.Equal(ts) {
		t.Errorf("Timestamp: got %v, want %v", r.Timestamp, ts)
	}
	if r.Key != nil || r.Value != nil {
		t.Error("watermark should have nil Key and Value")
	}
}

func TestWithTimestamp(t *testing.T) {
	r := types.NewRecord([]byte("k"), []byte("v"))
	ts := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	r2 := r.WithTimestamp(ts)
	if !r2.Timestamp.Equal(ts) {
		t.Errorf("WithTimestamp: got %v, want %v", r2.Timestamp, ts)
	}
	if r.Timestamp.Equal(ts) {
		t.Error("original record should not be modified")
	}
}

func TestWithOffset(t *testing.T) {
	r := types.NewRecord([]byte("k"), []byte("v"))
	r2 := r.WithOffset(42)
	if r2.Offset != 42 {
		t.Errorf("WithOffset: got %d, want 42", r2.Offset)
	}
	if r.Offset != 0 {
		t.Error("original record should not be modified")
	}
}

func TestWithHeader(t *testing.T) {
	r := types.NewRecord([]byte("k"), []byte("v"))
	r2 := r.WithHeader("trace-id", []byte("abc"))
	if string(r2.Headers["trace-id"]) != "abc" {
		t.Errorf("WithHeader: got %q, want %q", r2.Headers["trace-id"], "abc")
	}
	if r.Headers != nil {
		t.Error("original record Headers should be nil")
	}
}

func TestWithHeader_PreservesExisting(t *testing.T) {
	r := types.Record{
		Key:   []byte("k"),
		Value: []byte("v"),
		Headers: map[string][]byte{
			"existing": []byte("val"),
		},
	}
	r2 := r.WithHeader("new", []byte("val2"))
	if string(r2.Headers["existing"]) != "val" {
		t.Errorf("existing header lost: got %q", r2.Headers["existing"])
	}
	if string(r2.Headers["new"]) != "val2" {
		t.Errorf("new header: got %q, want %q", r2.Headers["new"], "val2")
	}
}
