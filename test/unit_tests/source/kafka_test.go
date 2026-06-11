package source_test

import (
	"encoding/binary"
	"testing"
	"time"

	"mailer/source"

	"github.com/segmentio/kafka-go"
)

func TestKafkaSource_ImplementsSource(t *testing.T) {
	var _ source.Source = (*source.KafkaSource)(nil)
}

func TestKafkaToRecord(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	msg := kafka.Message{
		Key:     []byte("key-1"),
		Value:   []byte("value-1"),
		Time:    ts,
		Offset:  42,
		Headers: []kafka.Header{{Key: "trace-id", Value: []byte("abc123")}},
	}

	r := source.KafkaToRecord(msg)

	if string(r.Key) != "key-1" {
		t.Errorf("Key: got %q, want %q", r.Key, "key-1")
	}
	if string(r.Value) != "value-1" {
		t.Errorf("Value: got %q, want %q", r.Value, "value-1")
	}
	if !r.Timestamp.Equal(ts) {
		t.Errorf("Timestamp: got %v, want %v", r.Timestamp, ts)
	}
	if r.Offset != 42 {
		t.Errorf("Offset: got %d, want 42", r.Offset)
	}
	if string(r.Headers["trace-id"]) != "abc123" {
		t.Errorf("Headers[trace-id]: got %q, want %q", r.Headers["trace-id"], "abc123")
	}
	if r.IsWatermark {
		t.Error("IsWatermark should be false for a regular message")
	}
}

func TestKafkaToRecord_NoHeaders(t *testing.T) {
	r := source.KafkaToRecord(kafka.Message{})
	if r.Headers == nil {
		t.Error("Headers should be non-nil empty map, got nil")
	}
}

func TestBinaryEncoding_RoundTrip(t *testing.T) {
	const want = uint64(0xDEADBEEFCAFEBABE)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, want)

	got := binary.BigEndian.Uint64(buf)
	if got != want {
		t.Errorf("round trip: got %x, want %x", got, want)
	}
}
