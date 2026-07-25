package store_test

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/workflow/compiler"
	_ "modernc.org/sqlite"
)

// A database created by an older version (no kind/image columns) must be
// migrated on open so the current code works against it.
func TestMigration_UpgradesOldDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	// The pre-P7 jobs schema — no kind/image columns.
	_, err = db.Exec(`CREATE TABLE jobs (
		id TEXT PRIMARY KEY, name TEXT NOT NULL, spec TEXT NOT NULL,
		delivery TEXT NOT NULL, graph TEXT NOT NULL, desired_state TEXT NOT NULL,
		created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	// Opening it migrates in the missing columns.
	s, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatalf("open old db: %v", err)
	}
	defer s.Close()

	// A kind/image-using write now succeeds.
	j := &store.Job{ID: "j", Name: "n", Kind: store.KindSDK, Image: "img", Spec: "x",
		Desired: store.DesiredRunning, Created: time.Now(), Updated: time.Now()}
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("CreateJob after migration: %v", err)
	}
	got, err := s.GetJob("j")
	if err != nil || got.Kind != store.KindSDK || got.Image != "img" {
		t.Fatalf("migrated job wrong: %+v err=%v", got, err)
	}
}

func open(t *testing.T) *store.SQLite {
	t.Helper()
	s, err := store.OpenSQLite(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestJobRoundTrip(t *testing.T) {
	s := open(t)
	now := time.Now().UTC().Truncate(time.Second)
	j := &store.Job{
		ID: "job-1", Name: "wordcount", Spec: "name: wordcount",
		Delivery: compiler.AtLeastOnce,
		Graph:    compiler.PipelineGraph{Source: "generator", Sink: "stdout", Operators: []compiler.GraphNode{{ID: "count", Type: "reduce"}}},
		Desired:  store.DesiredRunning, Created: now, Updated: now,
	}
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	got, err := s.GetJob("job-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Name != "wordcount" || got.Delivery != compiler.AtLeastOnce {
		t.Errorf("job mismatch: %+v", got)
	}
	if len(got.Graph.Operators) != 1 || got.Graph.Operators[0].ID != "count" {
		t.Errorf("graph not round-tripped: %+v", got.Graph)
	}

	if err := s.SetDesired("job-1", store.DesiredStopped); err != nil {
		t.Fatalf("SetDesired: %v", err)
	}
	got, _ = s.GetJob("job-1")
	if got.Desired != store.DesiredStopped {
		t.Errorf("desired not updated: %q", got.Desired)
	}

	jobs, err := s.ListJobs()
	if err != nil || len(jobs) != 1 {
		t.Fatalf("ListJobs: %v (n=%d)", err, len(jobs))
	}
}

func TestRunsAndActive(t *testing.T) {
	s := open(t)
	j := &store.Job{ID: "j", Name: "n", Spec: "x", Desired: store.DesiredRunning, Created: time.Now(), Updated: time.Now()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}

	// No runs yet.
	if r, err := s.LatestRun("j"); err != nil || r != nil {
		t.Fatalf("LatestRun on fresh job: r=%v err=%v", r, err)
	}

	r1 := &store.Run{ID: "r1", JobID: "j", ContainerID: "c1", HostPort: 32000, Phase: "running", Attempt: 1, Started: time.Now()}
	if err := s.CreateRun(r1); err != nil {
		t.Fatal(err)
	}

	active, err := s.ActiveRuns()
	if err != nil || len(active) != 1 {
		t.Fatalf("ActiveRuns: %v (n=%d)", err, len(active))
	}
	if active[0].HostPort != 32000 {
		t.Errorf("host port not persisted: %d", active[0].HostPort)
	}

	// Terminate r1: it leaves the active set.
	stopped := time.Now()
	r1.Phase, r1.Stopped = "finished", &stopped
	if err := s.UpdateRun(r1); err != nil {
		t.Fatal(err)
	}
	if active, _ := s.ActiveRuns(); len(active) != 0 {
		t.Fatalf("expected no active runs, got %d", len(active))
	}

	latest, err := s.LatestRun("j")
	if err != nil || latest == nil || latest.Phase != "finished" {
		t.Fatalf("LatestRun: %+v err=%v", latest, err)
	}
}

func TestTransitions(t *testing.T) {
	s := open(t)
	j := &store.Job{ID: "j", Name: "n", Spec: "x", Desired: store.DesiredRunning, Created: time.Now(), Updated: time.Now()}
	s.CreateJob(j)
	for _, to := range []string{"starting", "running", "finished"} {
		if err := s.AppendTransition(&store.Transition{JobID: "j", To: to, From: "prev", At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	ts, err := s.ListTransitions("j")
	if err != nil || len(ts) != 3 {
		t.Fatalf("ListTransitions: %v (n=%d)", err, len(ts))
	}
	if ts[2].To != "finished" {
		t.Errorf("transitions out of order: %+v", ts)
	}
}
