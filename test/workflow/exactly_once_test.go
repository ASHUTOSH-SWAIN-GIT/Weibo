package workflow_test

import (
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow"
	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
)

func TestKafkaExactlyOnceValidation(t *testing.T) {
	partial := `name: partial-eo
source:
  type: kafka
  kafka: {brokers: [localhost:9092], topic: in, groupID: g, exactlyOnce: true}
sink:
  type: kafka
  kafka: {brokers: [localhost:9092], topic: out}
`
	wf, err := workflow.ParseYAML([]byte(partial))
	if err != nil {
		t.Fatal(err)
	}
	err = workflow.Validate(wf)
	if err == nil || !strings.Contains(err.Error(), "exactly-once requires a txnKafka") {
		t.Fatalf("expected exactly-once validation error, got %v", err)
	}

	full := `name: full-eo
env:
  checkpointing: {interval: 1s, dir: ` + t.TempDir() + `}
source:
  type: kafka
  kafka: {brokers: [localhost:9092], topic: in, groupID: g, exactlyOnce: true}
sink:
  type: txnKafka
  txnKafka: {brokers: [localhost:9092], topic: out, transactionalID: full-eo}
`
	wf, err = workflow.ParseYAML([]byte(full))
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.Validate(wf); err != nil {
		t.Fatalf("full exactly-once workflow should validate: %v", err)
	}
}

func TestSecretsAreNeverExposedInValidationOrCompileErrors(t *testing.T) {
	t.Setenv("KAFKA_PASSWORD", "do-not-leak")

	doc := `name: missing-secret
source:
  type: kafka
  kafka:
    brokers: [localhost:9092]
    topic: in
    groupID: g
    sasl: {mechanism: plain, username: "${KAFKA_USERNAME_MISSING}", password: "${KAFKA_PASSWORD}"}
sink: {type: stdout}
`
	wf, err := workflow.ParseYAML([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.Validate(wf); err != nil && strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("validation leaked secret: %v", err)
	}
	_, err = runCompileOnly(wf, t.TempDir())
	if err == nil {
		t.Fatal("expected missing secret compile error")
	}
	if strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("compile error leaked secret: %v", err)
	}
}

func runCompileOnly(wf *workflow.Workflow, dir string) (any, error) {
	return (&compiler.Compiler{BaseDataDir: dir}).CompileWorkflow(wf)
}
