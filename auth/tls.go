package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// BuildTLSConfig converts a weibo TLSConfig into a *tls.Config suitable
// for use with kafka-go's Dialer (source) or Transport (sink).
// Returns nil if no TLS fields are set (plain connection).
func BuildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg == (TLSConfig{}) {
		return nil, nil
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConf.Certificates = []tls.Certificate{cert}
	}

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
