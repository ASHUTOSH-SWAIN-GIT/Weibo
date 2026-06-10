package sink

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"mailer/types"
)

// TestKafkaSink_ImplementsSink ensures KafkaSink satisfies the Sink interface
// at compile time. If Write's signature changes incompatibly, this won't compile.
func TestKafkaSink_ImplementsSink(t *testing.T) {
	var _ Sink = (*KafkaSink)(nil)
}

// TestRecordToKafka verifies the record-to-message mapping.
func TestRecordToKafka(t *testing.T) {
	ts := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	r := types.Record{
		Key:       []byte("customer-42"),
		Value:     []byte("payload"),
		Timestamp: ts,
		Offset:    99,
		Headers: map[string][]byte{
			"window_start": []byte("2026-06-10T12:00:00Z"),
		},
	}

	msg := recordToKafka(r)

	if string(msg.Key) != "customer-42" {
		t.Errorf("Key: got %q, want %q", msg.Key, "customer-42")
	}
	if string(msg.Value) != "payload" {
		t.Errorf("Value: got %q, want %q", msg.Value, "payload")
	}
	if !msg.Time.Equal(ts) {
		t.Errorf("Time: got %v, want %v", msg.Time, ts)
	}

	if len(msg.Headers) != 1 {
		t.Fatalf("expected 1 header, got %d", len(msg.Headers))
	}
	if msg.Headers[0].Key != "window_start" {
		t.Errorf("header key: got %q, want %q", msg.Headers[0].Key, "window_start")
	}
}

// TestRecordToKafka_ZeroTimestamp ensures zero timestamps don't break the producer.
func TestRecordToKafka_ZeroTimestamp(t *testing.T) {
	r := types.Record{Key: []byte("k"), Value: []byte("v")}
	msg := recordToKafka(r)
	if !msg.Time.IsZero() {
		t.Errorf("expected zero time, got %v", msg.Time)
	}
}

// TestRecordToKafka_EmptyHeaders ensures no header allocation when input is empty.
func TestRecordToKafka_EmptyHeaders(t *testing.T) {
	r := types.Record{Key: []byte("k"), Value: []byte("v")}
	msg := recordToKafka(r)
	if len(msg.Headers) != 0 {
		t.Errorf("expected no headers, got %d", len(msg.Headers))
	}
}

// Compile-time check: kafka-go types are used in this package.
var _ = kafka.Message{}
