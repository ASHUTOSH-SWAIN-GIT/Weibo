package source

// Deserializer converts raw Kafka bytes into a typed payload.
// Implementations are plugged into a KafkaSource via the KafkaDeserialize option.
//
// If Deserialize returns an error, the record is dropped from the stream.
// The returned value is stored in Record.Parsed; Record.Value keeps the raw bytes.
type Deserializer interface {
	Deserialize(data []byte, headers map[string][]byte) (any, error)
}
