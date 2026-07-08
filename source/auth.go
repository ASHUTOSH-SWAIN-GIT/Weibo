package source

import "github.com/segmentio/kafka-go"

// SASLMechanism identifies a Kafka SASL authentication mechanism.
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// SASLConfig holds Kafka SASL authentication settings.
// Used with the KafkaSASL option.
type SASLConfig struct {
	Mechanism SASLMechanism
	Username  string
	Password  string
}

// TLSConfig holds Kafka TLS settings.
// Used with the KafkaTLS option.
type TLSConfig struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
}

// buildDialer constructs a kafka-go Dialer with SASL and/or TLS applied.
// Returns nil if neither is configured (caller should use the default dialer).
//
// Full implementation lands in Phase 2.
func buildDialer(sasl *SASLConfig, tls *TLSConfig) *kafka.Dialer {
	dialer := &kafka.Dialer{}
	_ = sasl
	_ = tls
	return dialer
}
