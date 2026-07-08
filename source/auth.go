package source

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

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
//
// If CertFile and KeyFile are both set, client certificates are loaded
// for mutual TLS. If CAFile is set, it is added to the root CAs used to
// verify the broker. InsecureSkipVerify disables certificate verification
// (for development only).
type TLSConfig struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
}

// buildDialer constructs a kafka-go Dialer with SASL and/or TLS applied.
// Returns nil if neither is configured so the caller uses kafka-go defaults.
func buildDialer(saslCfg *SASLConfig, tlsCfg *TLSConfig) *kafka.Dialer {
	if saslCfg == nil && tlsCfg == nil {
		return nil
	}

	dialer := &kafka.Dialer{}

	if saslCfg != nil {
		mechanism, err := buildSASLMechanism(saslCfg)
		if err != nil {
			panic(fmt.Sprintf("mailer/source: %v", err))
		}
		dialer.SASLMechanism = mechanism
	}

	if tlsCfg != nil {
		tlsConf, err := buildTLSConfig(tlsCfg)
		if err != nil {
			panic(fmt.Sprintf("mailer/source: %v", err))
		}
		dialer.TLS = tlsConf
	}

	return dialer
}

// buildSASLMechanism maps a mailer SASLConfig to a kafka-go SASL mechanism.
func buildSASLMechanism(cfg *SASLConfig) (sasl.Mechanism, error) {
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

// buildTLSConfig converts a mailer TLSConfig into a *tls.Config.
func buildTLSConfig(cfg *TLSConfig) (*tls.Config, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	// Load client certificate + key for mutual TLS.
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}

	// Load custom CA into the root pool.
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no valid certificates found in CA file %s", cfg.CAFile)
		}
		tlsConf.RootCAs = pool
	}

	return tlsConf, nil
}
