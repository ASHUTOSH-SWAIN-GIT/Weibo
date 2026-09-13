package sink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

func TestNewPostgresSinkEValidatesWithoutOpeningPool(t *testing.T) {
	s, err := NewPostgresSinkE(
		PostgresDSN("postgres://user:pass@127.0.0.1:1/db?sslmode=disable"),
		PostgresMapper(func(types.Record) (string, []string, []any) {
			return "orders", []string{"id"}, []any{"1"}
		}),
	)
	if err != nil {
		t.Fatalf("NewPostgresSinkE: %v", err)
	}
	if s.pool != nil {
		t.Fatal("constructor should not open a Postgres pool")
	}
}

func TestNewPostgresSinkEReportsConfigErrors(t *testing.T) {
	if _, err := NewPostgresSinkE(PostgresDSN("postgres://example/db")); err == nil {
		t.Fatal("expected missing mapper error")
	}
	if _, err := NewPostgresSinkE(
		PostgresDSN("postgres://example/db"),
		PostgresMapper(func(types.Record) (string, []string, []any) { return "", nil, nil }),
		PostgresMode(PostgresUpsert),
	); err == nil || !strings.Contains(err.Error(), "upsert requires") {
		t.Fatalf("expected write-mode validation error, got %v", err)
	}
}

func TestPostgresSinkCheckVerifiesConnectivity(t *testing.T) {
	s, err := NewPostgresSinkE(
		PostgresDSN("postgres://user:pass@127.0.0.1:1/db?sslmode=disable"),
		PostgresMapper(func(types.Record) (string, []string, []any) {
			return "orders", []string{"id"}, []any{"1"}
		}),
		PostgresMaxRetries(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err = s.Check(ctx)
	if err == nil {
		t.Fatal("expected connectivity check to fail for unreachable Postgres")
	}
	s.Close()
}
