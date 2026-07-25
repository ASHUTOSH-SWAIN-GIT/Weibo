package auth

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// BuildKgoSASL maps a weibo SASLConfig to a franz-go SASL mechanism
// (used by the transactional Kafka sink, which is built on franz-go).
// Returns an error if the mechanism is unsupported.
func BuildKgoSASL(cfg SASLConfig) (sasl.Mechanism, error) {
	switch cfg.Mechanism {
	case SASLPlain:
		return plain.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsMechanism(), nil

	case SASLScramSHA256:
		return scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha256Mechanism(), nil

	case SASLScramSHA512:
		return scram.Auth{
			User: cfg.Username,
			Pass: cfg.Password,
		}.AsSha512Mechanism(), nil

	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cfg.Mechanism)
	}
}
