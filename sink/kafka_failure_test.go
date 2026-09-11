package sink

import (
	"errors"
	"testing"

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
