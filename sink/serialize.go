package sink

import (
	"encoding/json"

	"mailer/types"
)

// Serializer converts a Record into bytes for writing to an external system.
// Implementations are plugged into a KafkaSink via the KafkaSinkSerialize option.
//
// If Record.Parsed is non-nil, serializers typically serialize the typed
// payload. Otherwise they fall back to Record.Value (the raw bytes).
// The returned bytes become the Kafka message value.
type Serializer interface {
	Serialize(r types.Record) ([]byte, error)
}

// SerializerFunc lets a plain function satisfy the Serializer interface.
type SerializerFunc func(r types.Record) ([]byte, error)

func (f SerializerFunc) Serialize(r types.Record) ([]byte, error) {
	return f(r)
}

// JSONSerializer marshals Record.Parsed (if set) or Record.Value to JSON.
// When Parsed is nil and Value is already JSON bytes, this effectively
// passes them through (re-encoded, which is a no-op for valid JSON).
type JSONSerializer struct{}

// NewJSONSerializer creates a JSON serializer.
func NewJSONSerializer() *JSONSerializer { return &JSONSerializer{} }

func (s *JSONSerializer) Serialize(r types.Record) ([]byte, error) {
	if r.Parsed != nil {
		return json.Marshal(r.Parsed)
	}
	return r.Value, nil
}
