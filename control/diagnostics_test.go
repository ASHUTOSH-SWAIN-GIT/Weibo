package control_test

import (
	"context"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/lifecycle"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

func TestDiagnoseFailureKinds(t *testing.T) {
	if d := control.DiagnoseFailure(nil); d != nil {
		t.Errorf("nil run should diagnose nothing: %+v", d)
	}
	healthy := &store.Run{Phase: string(lifecycle.Running)}
	if d := control.DiagnoseFailure(healthy); d != nil {
		t.Errorf("running run should diagnose nothing: %+v", d)
	}
	transient := &store.Run{Phase: string(lifecycle.Restarting), FailureKind: store.FailureLaunchTransient, Error: "boom"}
	d := control.DiagnoseFailure(transient)
	if d == nil || d.Kind != store.FailureLaunchTransient || d.Title == "" || d.Hint == "" || d.Message != "boom" {
		t.Fatalf("transient diagnosis: %+v", d)
	}
	// A failed phase with no recorded kind still explains itself.
	crashed := &store.Run{Phase: string(lifecycle.Failed), Error: "nonzero exit"}
	d = control.DiagnoseFailure(crashed)
	if d == nil || d.Kind != "run_failed" || d.Message != "nonzero exit" {
		t.Fatalf("crash diagnosis: %+v", d)
	}
	// A scheduled restart is unhealthy-in-progress, not silence.
	retrying := &store.Run{Phase: string(lifecycle.Restarting), Error: "nonzero exit"}
	d = control.DiagnoseFailure(retrying)
	if d == nil || d.Kind != "restarting" {
		t.Fatalf("restart diagnosis: %+v", d)
	}
}

func TestDiagnosticsAssembles(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	c := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}

	at := time.Now().UTC()
	c.History().Add(job.ID, control.Sample{
		At: at, Phase: "running", RecordsIn: 10, RecordsOut: 8,
		CheckpointID: "cp-1", CheckpointDurationMs: 250, CheckpointSizeBytes: 1024,
	})

	d, err := c.Diagnostics(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Phase != string(lifecycle.Running) || d.Desired != store.DesiredRunning {
		t.Fatalf("identity: %+v", d)
	}
	if d.Failure != nil {
		t.Fatalf("healthy job should have no failure: %+v", d.Failure)
	}
	if d.Activity == nil || !d.Activity.At.Equal(at) || d.Activity.RecordsOut != 8 {
		t.Fatalf("activity: %+v", d.Activity)
	}
	if d.Checkpoint == nil || d.Checkpoint.ID != "cp-1" || d.Checkpoint.DurationMs != 250 || d.Checkpoint.SizeBytes != 1024 {
		t.Fatalf("checkpoint: %+v", d.Checkpoint)
	}
	if d.Restart != nil {
		t.Fatalf("no restart scheduled: %+v", d.Restart)
	}
}

func TestDiagnosticsRestartCountdown(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	c := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := c.LatestRun(job.ID)
	fake.SetPhase(run.ContainerID, backend.PhaseExited, 1)
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	// With the default policy the crash schedules a future restart.
	d, err := c.Diagnostics(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Restart == nil || d.Restart.InSeconds <= 0 {
		t.Fatalf("restart countdown: %+v", d.Restart)
	}
	if d.Failure == nil {
		t.Fatal("crashed job should carry a failure diagnosis")
	}
}

func TestRunLogsGoneContainer(t *testing.T) {
	fake := backend.NewFake()
	st := openStore(t)
	c := control.New(control.Options{Store: st, Backend: fake, Image: "img", StopTimeout: time.Second})
	job, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, status, err := c.RunLogs(context.Background(), job.ID, "nope", 100); err == nil || status != 404 {
		t.Fatalf("unknown run: status=%d err=%v", status, err)
	}
	run, _ := c.LatestRun(job.ID)
	fake.SetLogs(run.ContainerID, "hello\n")
	if out, status, err := c.RunLogs(context.Background(), job.ID, run.ID, 100); err != nil || status != 200 || out != "hello\n" {
		t.Fatalf("run logs: status=%d err=%v out=%q", status, err, out)
	}
	if err := c.Delete(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	// Recreate the job row alone to simulate a pruned container: the run
	// row is gone with the job, so re-submit and wipe the container.
	job2, err := c.Submit(context.Background(), []byte(validSDKManifest), nil)
	if err != nil {
		t.Fatal(err)
	}
	run2, _ := c.LatestRun(job2.ID)
	if err := fake.Remove(context.Background(), run2.ContainerID); err != nil {
		t.Fatal(err)
	}
	if _, status, err := c.RunLogs(context.Background(), job2.ID, run2.ID, 100); err == nil || status != 410 {
		t.Fatalf("removed container: status=%d err=%v", status, err)
	}
}
