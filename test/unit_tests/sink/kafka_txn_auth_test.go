package sink_test

import (
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/auth"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
)

// The transactional sink accepts SASL + TLS for hosted clusters and
// reflects them in Describe. Construction must not connect (no broker
// is running in this test).
func TestTxnKafkaSink_SASLAndTLSConfigured(t *testing.T) {
	s := sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("broker.hosted.example:9092"),
		sink.TxnKafkaTopic("out"),
		sink.TxnKafkaTransactionalID("pipeline-1"),
		sink.TxnKafkaSASL(auth.SASLConfig{
			Mechanism: auth.SASLScramSHA256,
			Username:  "svc-user",
			Password:  "secret",
		}),
		sink.TxnKafkaTLS(auth.TLSConfig{}), // TLS on, system CAs
	)

	info := s.Describe()
	if info.Props["sasl"] != "SCRAM-SHA-256" {
		t.Errorf("sasl prop: got %q", info.Props["sasl"])
	}
	if info.Props["tls"] != "enabled" {
		t.Errorf("tls prop: got %q", info.Props["tls"])
	}
	// Credentials must never leak into Describe output.
	for k, v := range info.Props {
		if v == "secret" || v == "svc-user" {
			t.Errorf("credential leaked into Describe prop %q", k)
		}
	}
}

// An unsupported SASL mechanism fails fast at construction, matching
// the other Kafka connectors.
func TestTxnKafkaSink_BadSASLMechanismPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unsupported SASL mechanism")
		}
	}()
	sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("b:9092"),
		sink.TxnKafkaTopic("t"),
		sink.TxnKafkaTransactionalID("id"),
		sink.TxnKafkaSASL(auth.SASLConfig{Mechanism: "KERBEROS"}),
	)
}

// A TLS config pointing at an unreadable CA file also fails fast.
func TestTxnKafkaSink_BadTLSConfigPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for unreadable CA file")
		}
	}()
	sink.NewTxnKafkaSink(
		sink.TxnKafkaBrokers("b:9092"),
		sink.TxnKafkaTopic("t"),
		sink.TxnKafkaTransactionalID("id"),
		sink.TxnKafkaTLS(auth.TLSConfig{CAFile: "/nonexistent/ca.pem"}),
	)
}
