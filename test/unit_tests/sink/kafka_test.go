package sink_test

import (
	"testing"
	"time"

	"mailer/sink"
	"mailer/types"
)

func TestKafkaSink_ImplementsSink(t *testing.T) {
	var _ sink.Sink = (*sink.KafkaSink)(nil)
}

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

	msg := sink.RecordToKafka(r)

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

func TestRecordToKafka_ZeroTimestamp(t *testing.T) {
	r := types.Record{Key: []byte("k"), Value: []byte("v")}
	msg := sink.RecordToKafka(r)
	if !msg.Time.IsZero() {
		t.Errorf("expected zero time, got %v", msg.Time)
	}
}

func TestRecordToKafka_EmptyHeaders(t *testing.T) {
	r := types.Record{Key: []byte("k"), Value: []byte("v")}
	msg := sink.RecordToKafka(r)
	if len(msg.Headers) != 0 {
		t.Errorf("expected no headers, got %d", len(msg.Headers))
	}
}
