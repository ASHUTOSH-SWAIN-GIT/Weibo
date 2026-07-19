package source

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

// KafkaToRecord must own its bytes: kafka-go reuses the message Value/Key
// buffers for later messages, so a record that merely aliases them gets
// corrupted once it sits in a channel while the reader advances. That is
// what silently turned Kafka→sink field mappings into NULLs.
func TestKafkaToRecord_OwnsValueAndKey(t *testing.T) {
	valBuf := []byte(`{"order_id":"o1","amount":10}`)
	keyBuf := []byte("customer-1")
	r := KafkaToRecord(kafka.Message{Value: valBuf, Key: keyBuf})

	// Simulate kafka-go reusing the same backing arrays for the next
	// message before the record is consumed downstream.
	for i := range valBuf {
		valBuf[i] = 'X'
	}
	for i := range keyBuf {
		keyBuf[i] = 'Y'
	}

	if string(r.Value) != `{"order_id":"o1","amount":10}` {
		t.Errorf("record Value corrupted by buffer reuse: %q", r.Value)
	}
	if string(r.Key) != "customer-1" {
		t.Errorf("record Key corrupted by buffer reuse: %q", r.Key)
	}
}
