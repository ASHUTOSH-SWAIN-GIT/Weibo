package secrets_test

import (
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/secrets"
)

func TestEnvironmentResolve(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://user:secret@localhost/db")

	got, err := (secrets.Environment{}).Resolve("POSTGRES_DSN")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "postgres://user:secret@localhost/db" {
		t.Fatalf("Resolve: got %q", got)
	}
}

func TestEnvironmentResolveMissingDoesNotLeakValues(t *testing.T) {
	t.Setenv("KAFKA_PASSWORD", "super-secret")

	_, err := (secrets.Environment{}).Resolve("KAFKA_PASSWORD_MISSING")
	if err == nil {
		t.Fatal("expected missing secret error")
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("error leaked a secret value: %v", err)
	}
}
