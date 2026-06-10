package source

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// TestKafkaSource_ImplementsSource ensures KafkaSource satisfies the Source interface
// at compile time. If Run's signature changes incompatibly, this won't compile.
func TestKafkaSource_ImplementsSource(t *testing.T) {
	var _ Source = (*KafkaSource)(nil)
}

// TestKafkaToRecord verifies the message-to-record mapping.
func TestKafkaToRecord(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	msg := kafka.Message{
		Key:     []byte("key-1"),
		Value:   []byte("value-1"),
		Time:    ts,
		Offset:  42,
		Headers: []kafka.Header{{Key: "trace-id", Value: []byte("abc123")}},
	}

	r := kafkaToRecord(msg)

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

// TestKafkaToRecord_NoHeaders ensures headers map is always allocated (not nil)
// so downstream code can safely write to it.
func TestKafkaToRecord_NoHeaders(t *testing.T) {
	r := kafkaToRecord(kafka.Message{})
	if r.Headers == nil {
		t.Error("Headers should be non-nil empty map, got nil")
	}
}

// TestBinaryEncoding_RoundTrip is a sanity check that the binary encoding pattern
// used in examples (binary.BigEndian uint64 in Value) round-trips correctly.
func TestBinaryEncoding_RoundTrip(t *testing.T) {
	const want = uint64(0xDEADBEEFCAFEBABE)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, want)

	got := binary.BigEndian.Uint64(buf)
	if got != want {
		t.Errorf("round trip: got %x, want %x", got, want)
	}
}
