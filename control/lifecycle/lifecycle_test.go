package lifecycle_test

import (
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/control/lifecycle"
)

func TestTerminalAndTransitions(t *testing.T) {
	if !lifecycle.Finished.Terminal() || !lifecycle.Cancelled.Terminal() || !lifecycle.Failed.Terminal() {
		t.Error("terminal phases misclassified")
	}
	if lifecycle.Running.Terminal() {
		t.Error("running should not be terminal")
	}
	if !lifecycle.CanTransition(lifecycle.Running, lifecycle.Cancelling) {
		t.Error("running→cancelling should be legal")
	}
	if lifecycle.CanTransition(lifecycle.Finished, lifecycle.Running) {
		t.Error("finished→running must not be legal (restart makes a new run)")
	}
}

func TestFromAgent(t *testing.T) {
	if lifecycle.FromAgent("running") != lifecycle.Running {
		t.Error("running not mapped")
	}
	if lifecycle.FromAgent("garbage") != lifecycle.Failed {
		t.Error("unknown agent phase must map to failed, not look healthy")
	}
}

func TestRestartPolicy(t *testing.T) {
	p := lifecycle.RestartPolicy{MaxAttempts: 3, BaseBackoff: time.Second, MaxBackoff: 10 * time.Second}

	if !p.ShouldRestart(lifecycle.Failed, 1) {
		t.Error("first failure should restart")
	}
	if p.ShouldRestart(lifecycle.Failed, 3) {
		t.Error("must stop restarting at MaxAttempts")
	}
	if p.ShouldRestart(lifecycle.Finished, 1) {
		t.Error("clean finish must not restart")
	}
	if p.ShouldRestart(lifecycle.Cancelled, 1) {
		t.Error("user cancel must not restart")
	}

	// Exponential, capped.
	if got := p.Backoff(1); got != time.Second {
		t.Errorf("backoff(1): got %v", got)
	}
	if got := p.Backoff(2); got != 2*time.Second {
		t.Errorf("backoff(2): got %v", got)
	}
	if got := p.Backoff(100); got != 10*time.Second {
		t.Errorf("backoff cap: got %v", got)
	}

	if (lifecycle.RestartPolicy{MaxAttempts: 0}).ShouldRestart(lifecycle.Failed, 1) {
		t.Error("MaxAttempts=0 must disable restart")
	}
}
