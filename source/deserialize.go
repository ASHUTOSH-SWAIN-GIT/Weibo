package source

import "encoding/json"

// Deserializer converts raw Kafka bytes into a typed payload.
// Implementations are plugged into a KafkaSource via the KafkaDeserialize option.
//
// If Deserialize returns an error, the record is dropped from the stream.
// The returned value is stored in Record.Parsed; Record.Value keeps the raw bytes.
type Deserializer interface {
	Deserialize(data []byte, headers map[string][]byte) (any, error)
}

// DeserializerFunc lets a plain function satisfy the Deserializer interface.
type DeserializerFunc func(data []byte, headers map[string][]byte) (any, error)

func (f DeserializerFunc) Deserialize(data []byte, headers map[string][]byte) (any, error) {
	return f(data, headers)
}

// JSONDeserializer unmarshals each message's Value into a fresh T and
// returns a *T as the Parsed payload.
//
// Usage:
//
//	deser := source.NewJSONDeserializer[Order]()
//	src := source.NewKafkaSource(
//	    source.KafkaBrokers("localhost:9092"),
//	    source.KafkaTopic("orders"),
//	    source.KafkaDeserialize(deser),
//	)
type JSONDeserializer[T any] struct{}

// NewJSONDeserializer creates a JSON deserializer for type T.
func NewJSONDeserializer[T any]() *JSONDeserializer[T] {
	return &JSONDeserializer[T]{}
}

func (d *JSONDeserializer[T]) Deserialize(data []byte, _ map[string][]byte) (any, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}
