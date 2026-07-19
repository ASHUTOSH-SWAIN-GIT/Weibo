package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/workflow/compiler"
	_ "modernc.org/sqlite" // pure-Go driver: keeps the controller CGO-free
)

// SQLite is a Store backed by a single SQLite database file (or ":memory:").
// modernc's driver is pure Go, so the controller image needs no CGO.
type SQLite struct {
	db *sql.DB
	mu sync.Mutex // serialize writes; SQLite allows one writer at a time
}

// OpenSQLite opens (creating if needed) the database at path and applies
// the schema. Use ":memory:" for tests.
func OpenSQLite(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// One connection avoids "database is locked" churn for :memory: and
	// keeps writes serialized without surprising the reconciler.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	// Migrate databases created by an older version: CREATE TABLE IF NOT
	// EXISTS does not add columns to a pre-existing table, so add them
	// here. A "duplicate column" error means the DB is already current.
	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("store: migrate: %w", err)
		}
	}
	return &SQLite{db: db}, nil
}

// migrations are applied after the base schema to upgrade older databases.
// Each must be safe to run on an already-current DB (idempotent; a
// "duplicate column" error is ignored).
var migrations = []string{
	`ALTER TABLE jobs ADD COLUMN kind TEXT NOT NULL DEFAULT 'yaml'`,
	`ALTER TABLE jobs ADD COLUMN image TEXT NOT NULL DEFAULT ''`,
}

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    kind          TEXT NOT NULL DEFAULT 'yaml',
    image         TEXT NOT NULL DEFAULT '',
    spec          TEXT NOT NULL,
    delivery      TEXT NOT NULL,
    graph         TEXT NOT NULL,
    desired_state TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id           TEXT PRIMARY KEY,
    job_id       TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    container_id TEXT,
    host_port    INTEGER,
    phase        TEXT NOT NULL,
    attempt      INTEGER NOT NULL,
    error        TEXT,
    started_at   TEXT NOT NULL,
    stopped_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON runs(job_id);
CREATE INDEX IF NOT EXISTS idx_runs_active ON runs(stopped_at);
CREATE TABLE IF NOT EXISTS transitions (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id  TEXT NOT NULL,
    run_id  TEXT,
    from_p  TEXT NOT NULL,
    to_p    TEXT NOT NULL,
    reason  TEXT,
    at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trans_job ON transitions(job_id);
`

const rfc = time.RFC3339Nano

func (s *SQLite) CreateJob(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	graph, err := json.Marshal(j.Graph)
	if err != nil {
		return err
	}
	kind := j.Kind
	if kind == "" {
		kind = KindYAML
	}
	_, err = s.db.Exec(
		`INSERT INTO jobs (id,name,kind,image,spec,delivery,graph,desired_state,created_at,updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Name, kind, j.Image, j.Spec, string(j.Delivery), string(graph),
		string(j.Desired), j.Created.Format(rfc), j.Updated.Format(rfc))
	return err
}

func (s *SQLite) GetJob(id string) (*Job, error) {
	row := s.db.QueryRow(
		`SELECT id,name,kind,image,spec,delivery,graph,desired_state,created_at,updated_at
		 FROM jobs WHERE id=?`, id)
	return scanJob(row)
}

func (s *SQLite) ListJobs() ([]*Job, error) {
	rows, err := s.db.Query(
		`SELECT id,name,kind,image,spec,delivery,graph,desired_state,created_at,updated_at
		 FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *SQLite) SetDesired(jobID string, d DesiredState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(`UPDATE jobs SET desired_state=?, updated_at=? WHERE id=?`,
		string(d), time.Now().UTC().Format(rfc), jobID)
	if err != nil {
		return err
	}
	return mustAffect(res, "job")
}

func (s *SQLite) DeleteJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM transitions WHERE job_id=?`, id)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`DELETE FROM runs WHERE job_id=?`, id)
	if err != nil {
		return err
	}
	res, err := s.db.Exec(`DELETE FROM jobs WHERE id=?`, id)
	if err != nil {
		return err
	}
	return mustAffect(res, "job")
}

func (s *SQLite) CreateRun(r *Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO runs (id,job_id,container_id,host_port,phase,attempt,error,started_at,stopped_at)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		r.ID, r.JobID, r.ContainerID, r.HostPort, r.Phase, r.Attempt, r.Error,
		r.Started.Format(rfc), nullTime(r.Stopped))
	return err
}

func (s *SQLite) UpdateRun(r *Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(
		`UPDATE runs SET container_id=?, host_port=?, phase=?, attempt=?, error=?, stopped_at=?
		 WHERE id=?`,
		r.ContainerID, r.HostPort, r.Phase, r.Attempt, r.Error, nullTime(r.Stopped), r.ID)
	if err != nil {
		return err
	}
	return mustAffect(res, "run")
}

func (s *SQLite) GetRun(id string) (*Run, error) {
	row := s.db.QueryRow(runCols+` WHERE id=?`, id)
	return scanRun(row)
}

func (s *SQLite) LatestRun(jobID string) (*Run, error) {
	row := s.db.QueryRow(runCols+` WHERE job_id=? ORDER BY started_at DESC LIMIT 1`, jobID)
	r, err := scanRun(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return r, err
}

func (s *SQLite) ListRuns(jobID string) ([]*Run, error) {
	rows, err := s.db.Query(runCols+` WHERE job_id=? ORDER BY started_at DESC`, jobID)
	if err != nil {
		return nil, err
	}
	return collectRuns(rows)
}

func (s *SQLite) ActiveRuns() ([]*Run, error) {
	rows, err := s.db.Query(runCols + ` WHERE stopped_at IS NULL`)
	if err != nil {
		return nil, err
	}
	return collectRuns(rows)
}

func (s *SQLite) AppendTransition(t *Transition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(
		`INSERT INTO transitions (job_id,run_id,from_p,to_p,reason,at) VALUES (?,?,?,?,?,?)`,
		t.JobID, t.RunID, t.From, t.To, t.Reason, t.At.Format(rfc))
	return err
}

func (s *SQLite) ListTransitions(jobID string) ([]*Transition, error) {
	rows, err := s.db.Query(
		`SELECT id,job_id,run_id,from_p,to_p,reason,at FROM transitions
		 WHERE job_id=? ORDER BY id ASC`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Transition
	for rows.Next() {
		var t Transition
		var runID, reason sql.NullString
		var at string
		if err := rows.Scan(&t.ID, &t.JobID, &runID, &t.From, &t.To, &reason, &at); err != nil {
			return nil, err
		}
		t.RunID, t.Reason = runID.String, reason.String
		t.At, _ = time.Parse(rfc, at)
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *SQLite) Close() error { return s.db.Close() }

// --- scanning helpers ---

const runCols = `SELECT id,job_id,container_id,host_port,phase,attempt,error,started_at,stopped_at FROM runs`

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(sc scanner) (*Job, error) {
	var j Job
	var delivery, graph, created, updated, desired string
	if err := sc.Scan(&j.ID, &j.Name, &j.Kind, &j.Image, &j.Spec, &delivery, &graph, &desired, &created, &updated); err != nil {
		return nil, err
	}
	j.Delivery = compiler.DeliveryGuarantee(delivery)
	j.Desired = DesiredState(desired)
	if err := json.Unmarshal([]byte(graph), &j.Graph); err != nil {
		return nil, err
	}
	j.Created, _ = time.Parse(rfc, created)
	j.Updated, _ = time.Parse(rfc, updated)
	return &j, nil
}

func scanRun(sc scanner) (*Run, error) {
	var r Run
	var containerID, errStr sql.NullString
	var hostPort sql.NullInt64
	var started string
	var stopped sql.NullString
	if err := sc.Scan(&r.ID, &r.JobID, &containerID, &hostPort, &r.Phase, &r.Attempt, &errStr, &started, &stopped); err != nil {
		return nil, err
	}
	r.ContainerID, r.Error = containerID.String, errStr.String
	r.HostPort = int(hostPort.Int64)
	r.Started, _ = time.Parse(rfc, started)
	if stopped.Valid {
		t, _ := time.Parse(rfc, stopped.String)
		r.Stopped = &t
	}
	return &r, nil
}

func collectRuns(rows *sql.Rows) ([]*Run, error) {
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.Format(rfc)
}

func mustAffect(res sql.Result, what string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("store: %s not found", what)
	}
	return nil
}
