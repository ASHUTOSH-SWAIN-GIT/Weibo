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
		Secrets:  map[string]store.SecretRef{"API_KEY": {Provider: "env", Name: "API_KEY"}},
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
	if got.Secrets["API_KEY"].Provider != "env" || got.Secrets["API_KEY"].Name != "API_KEY" {
		t.Fatalf("secret refs not round-tripped: %+v", got.Secrets)
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

	r1 := &store.Run{ID: "r1", JobID: "j", ContainerID: "c1", HostPort: 32000, Phase: "running", Attempt: 1, Error: "temporary backend unavailable", FailureKind: store.FailureLaunchTransient, Started: time.Now()}
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
	if active[0].FailureKind != store.FailureLaunchTransient {
		t.Errorf("failure kind not persisted: %q", active[0].FailureKind)
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

func TestOnlyOneActiveRunPerJob(t *testing.T) {
	s := open(t)
	j := &store.Job{ID: "j", Name: "n", Spec: "x", Desired: store.DesiredRunning, Created: time.Now(), Updated: time.Now()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}

	r1 := &store.Run{ID: "r1", JobID: "j", ContainerID: "c1", Phase: "running", Attempt: 1, Started: time.Now()}
	if err := s.CreateRun(r1); err != nil {
		t.Fatal(err)
	}
	r2 := &store.Run{ID: "r2", JobID: "j", ContainerID: "c2", Phase: "starting", Attempt: 2, Started: time.Now().Add(time.Second)}
	if err := s.CreateRun(r2); err == nil {
		t.Fatal("expected second active run for same job to fail")
	}

	stopped := time.Now()
	r1.Phase = "finished"
	r1.Stopped = &stopped
	if err := s.UpdateRun(r1); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(r2); err != nil {
		t.Fatalf("CreateRun after stopping prior active run: %v", err)
	}
}

func TestPruneTerminalRuns(t *testing.T) {
	s := open(t)
	j := &store.Job{ID: "j", Name: "n", Spec: "x", Desired: store.DesiredRunning, Created: time.Now(), Updated: time.Now()}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	seq := 0
	mkRun := func(id string, stopped bool) {
		seq++
		r := &store.Run{ID: id, JobID: "j", ContainerID: "c-" + id, Phase: "running", Attempt: 1, Started: base.Add(time.Duration(seq) * time.Second)}
		if err := s.CreateRun(r); err != nil {
			t.Fatal(err)
		}
		if err := s.AppendTransition(&store.Transition{JobID: "j", RunID: id, From: "a", To: "b", At: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if stopped {
			finished := base.Add(time.Hour)
			r.Phase, r.Stopped = "finished", &finished
			if err := s.UpdateRun(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	// r1 stays active (oldest Started); r2..r4 are terminal, r4 newest.
	// Terminals are created first so the one-active-run constraint is
	// never violated mid-test; Started values still order r1 oldest.
	mkRun("r2", true)
	mkRun("r3", true)
	mkRun("r4", true)
	mkRun("r1", false)
	r1, err := s.GetRun("r1")
	if err != nil {
		t.Fatal(err)
	}
	older := base.Add(-time.Hour)
	r1.Started = older
	if err := s.UpdateRun(r1); err != nil {
		t.Fatal(err)
	}

	deleted, err := s.PruneTerminalRuns("j", 2)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d, want 1 (oldest terminal r2)", deleted)
	}
	runs, err := s.ListRuns("j")
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, r := range runs {
		ids[r.ID] = true
	}
	if len(runs) != 3 || !ids["r1"] || !ids["r3"] || !ids["r4"] {
		t.Fatalf("wrong survivors: %+v", runs)
	}
	ts, err := s.ListTransitions("j")
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range ts {
		if tr.RunID == "r2" {
			t.Fatal("transitions of pruned run r2 should be deleted")
		}
	}
	if len(ts) != 3 {
		t.Fatalf("transitions=%d, want 3", len(ts))
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
