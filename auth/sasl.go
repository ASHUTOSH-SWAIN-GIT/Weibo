package auth

import (
	"fmt"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// BuildSASLMechanism maps a mailer SASLConfig to a kafka-go SASL mechanism.
// Returns an error if the mechanism is unsupported.
func BuildSASLMechanism(cfg SASLConfig) (sasl.Mechanism, error) {
	switch cfg.Mechanism {
	case SASLPlain:
		return plain.Mechanism{
			Username: cfg.Username,
			Password: cfg.Password,
		}, nil

	case SASLScramSHA256:
		m, err := scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("build SCRAM-SHA-256 mechanism: %w", err)
		}
		return m, nil

	case SASLScramSHA512:
		m, err := scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("build SCRAM-SHA-512 mechanism: %w", err)
		}
		return m, nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.Mechanism)
	}
}
