package compiler

import (
	"errors"
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow"
)

type mapSecretResolver map[string]string

func (r mapSecretResolver) Resolve(name string) (string, error) {
	value, ok := r[name]
	if !ok {
		return "", errors.New("not found")
	}
	return value, nil
}

type leakySecretResolver struct{}

func (leakySecretResolver) Resolve(name string) (string, error) {
	return "", errors.New("backend error includes resolved-password")
}

func TestResolveSecrets_DSNAndSASL(t *testing.T) {
	wf := &workflow.Workflow{
		Name: "secret-test",
		Source: workflow.SourceSpec{
			Type: "kafka",
			Kafka: &workflow.KafkaSourceSpec{
				Brokers: []string{"localhost:9092"},
				Topic:   "input",
				GroupID: "g",
				SASL: &workflow.SASLSpec{
					Mechanism: "plain",
					Username:  "${KAFKA_USERNAME}",
					Password:  "${KAFKA_PASSWORD}",
				},
			},
		},
		Sink: workflow.SinkSpec{
			Type: "postgres",
			Postgres: &workflow.PostgresSinkSpec{
				DSN:     "${POSTGRES_DSN}",
				Table:   "orders",
				Mapping: map[string]string{"amount": "amount"},
			},
		},
	}

	resolved, err := resolveSecrets(wf, mapSecretResolver{
		"POSTGRES_DSN":   "postgres://user:db-secret@localhost/db",
		"KAFKA_USERNAME": "resolved-user",
		"KAFKA_PASSWORD": "resolved-password",
	})
	if err != nil {
		t.Fatalf("resolveSecrets: %v", err)
	}

	if resolved.Sink.Postgres.DSN != "postgres://user:db-secret@localhost/db" {
		t.Fatalf("dsn was not resolved: %q", resolved.Sink.Postgres.DSN)
	}
	if resolved.Source.Kafka.SASL.Username != "resolved-user" {
		t.Fatalf("username was not resolved: %q", resolved.Source.Kafka.SASL.Username)
	}
	if resolved.Source.Kafka.SASL.Password != "resolved-password" {
		t.Fatalf("password was not resolved: %q", resolved.Source.Kafka.SASL.Password)
	}
	if wf.Sink.Postgres.DSN != "${POSTGRES_DSN}" {
		t.Fatalf("original workflow was mutated: %q", wf.Sink.Postgres.DSN)
	}
}

// The transactional sink's SASL credentials resolve like every other
// connector's, and the original spec is not mutated.
func TestResolveSecrets_TxnKafkaSASL(t *testing.T) {
	wf := &workflow.Workflow{
		Name:   "txn-secrets",
		Source: workflow.SourceSpec{Type: "generator"},
		Sink: workflow.SinkSpec{
			Type: "txnKafka",
			TxnKafka: &workflow.TxnKafkaSinkSpec{
				Brokers:         []string{"broker:9092"},
				Topic:           "out",
				TransactionalID: "id",
				SASL: &workflow.SASLSpec{
					Mechanism: "plain",
					Username:  "${TXN_USER}",
					Password:  "${TXN_PASS}",
				},
			},
		},
	}

	resolved, err := resolveSecrets(wf, mapSecretResolver{
		"TXN_USER": "svc",
		"TXN_PASS": "hunter2",
	})
	if err != nil {
		t.Fatalf("resolveSecrets: %v", err)
	}
	got := resolved.Sink.TxnKafka.SASL
	if got.Username != "svc" || got.Password != "hunter2" {
		t.Errorf("txn sasl not resolved: %+v", got)
	}
	// Original untouched.
	if wf.Sink.TxnKafka.SASL.Username != "${TXN_USER}" {
		t.Errorf("original spec mutated: %+v", wf.Sink.TxnKafka.SASL)
	}

	// An unresolvable reference fails.
	if _, err := resolveSecrets(wf, mapSecretResolver{}); err == nil {
		t.Error("expected error for unresolvable txn sasl reference")
	}
}

func TestResolveSecrets_ErrorDoesNotIncludeResolvedSecret(t *testing.T) {
	_, err := resolveOne(leakySecretResolver{}, "password", "${KAFKA_PASSWORD}")
	if err == nil {
		t.Fatal("expected missing secret error")
	}
	if strings.Contains(err.Error(), "resolved-password") {
		t.Fatalf("error leaked a resolved secret: %v", err)
	}
}
