package source

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"

	"github.com/segmentio/kafka-go"
)

// buildDialer constructs a kafka-go Dialer with SASL and/or TLS applied.
// Returns nil if neither is configured so the caller uses kafka-go defaults.
func buildDialer(saslCfg *auth.SASLConfig, tlsCfg *auth.TLSConfig) *kafka.Dialer {
	if saslCfg == nil && tlsCfg == nil {
		return nil
	}

	dialer := &kafka.Dialer{}

	if saslCfg != nil {
		mechanism, err := auth.BuildSASLMechanism(*saslCfg)
		if err != nil {
			panic(fmt.Sprintf("weibo/source: %v", err))
		}
		dialer.SASLMechanism = mechanism
	}

	if tlsCfg != nil {
		tlsConf, err := auth.BuildTLSConfig(*tlsCfg)
		if err != nil {
			panic(fmt.Sprintf("weibo/source: %v", err))
		}
		dialer.TLS = tlsConf
	}

	return dialer
}
