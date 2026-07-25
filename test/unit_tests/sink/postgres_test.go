package sink_test

import (
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/sink"
)

func TestPostgresWriteQuery_Insert(t *testing.T) {
	got := sink.BuildPostgresWriteQuery(
		"orders",
		[]string{"order_id", "amount"},
		2,
		sink.PostgresInsert,
		nil,
		nil,
	)
	want := `INSERT INTO "orders" ("order_id","amount") VALUES ($1,$2),($3,$4)`
	if got != want {
		t.Fatalf("query:\ngot  %s\nwant %s", got, want)
	}
}

func TestPostgresWriteQuery_UpsertDefaultUpdatesNonConflictColumns(t *testing.T) {
	got := sink.BuildPostgresWriteQuery(
		"orders",
		[]string{"order_id", "amount", "updated_at"},
		1,
		sink.PostgresUpsert,
		[]string{"order_id"},
		nil,
	)
	want := `INSERT INTO "orders" ("order_id","amount","updated_at") VALUES ($1,$2,$3) ON CONFLICT ("order_id") DO UPDATE SET "amount"=EXCLUDED."amount","updated_at"=EXCLUDED."updated_at"`
	if got != want {
		t.Fatalf("query:\ngot  %s\nwant %s", got, want)
	}
}

func TestPostgresWriteQuery_UpsertExplicitUpdateColumns(t *testing.T) {
	got := sink.BuildPostgresWriteQuery(
		"orders",
		[]string{"order_id", "amount", "updated_at"},
		1,
		sink.PostgresUpsert,
		[]string{"order_id"},
		[]string{"amount"},
	)
	want := `INSERT INTO "orders" ("order_id","amount","updated_at") VALUES ($1,$2,$3) ON CONFLICT ("order_id") DO UPDATE SET "amount"=EXCLUDED."amount"`
	if got != want {
		t.Fatalf("query:\ngot  %s\nwant %s", got, want)
	}
}

func TestPostgresWriteQuery_UpsertOnlyConflictColumnsDoesNothing(t *testing.T) {
	got := sink.BuildPostgresWriteQuery(
		"orders",
		[]string{"order_id"},
		1,
		sink.PostgresUpsert,
		[]string{"order_id"},
		nil,
	)
	want := `INSERT INTO "orders" ("order_id") VALUES ($1) ON CONFLICT ("order_id") DO NOTHING`
	if got != want {
		t.Fatalf("query:\ngot  %s\nwant %s", got, want)
	}
}
