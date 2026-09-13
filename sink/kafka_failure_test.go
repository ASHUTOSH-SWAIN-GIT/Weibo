package sink

import (
	"errors"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestKafkaSinkRecordToKafkaReturnsSerializerError(t *testing.T) {
	want := errors.New("cannot encode")
	k := &KafkaSink{cfg: kafkaSinkConfig{
		serializer: SerializerFunc(func(types.Record) ([]byte, error) { return nil, want }),
	}}

	msg, err := k.recordToKafka(types.Record{Value: []byte("raw-must-not-be-published")})
	if !errors.Is(err, want) {
		t.Fatalf("recordToKafka error = %v, want %v", err, want)
	}
	if msg.Value != nil {
		t.Fatalf("recordToKafka value = %q, want no fallback raw payload", msg.Value)
	}
}

func TestNewKafkaSinkEReportsConfigErrors(t *testing.T) {
	if _, err := NewKafkaSinkE(KafkaSinkTopic("orders")); err == nil {
		t.Fatal("expected missing brokers error")
	}
	if _, err := NewKafkaSinkE(KafkaSinkBrokers("localhost:9092")); err == nil {
		t.Fatal("expected missing topic error")
	}
	if _, err := NewKafkaSinkE(
		KafkaSinkBrokers("localhost:9092"),
		KafkaSinkTopic("orders"),
		KafkaSinkSASL(auth.SASLConfig{Mechanism: "kerberos"}),
	); err == nil || !strings.Contains(err.Error(), "unsupported SASL mechanism") {
		t.Fatalf("expected SASL validation error, got %v", err)
	}
	if k, err := NewKafkaSinkE(KafkaSinkBrokers("localhost:9092"), KafkaSinkTopic("orders")); err != nil || k == nil {
		t.Fatalf("valid Kafka sink: k=%v err=%v", k, err)
	}
}
