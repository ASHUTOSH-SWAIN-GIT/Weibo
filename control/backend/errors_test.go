package backend

import (
	"errors"
	"testing"
)

func TestLaunchErrorMessages(t *testing.T) {
	if got := (LaunchError{Permanent: true}).Error(); got != "permanent launch failure" {
		t.Errorf("permanent, no wrapped err: got %q", got)
	}
	if got := (LaunchError{}).Error(); got != "launch failure" {
		t.Errorf("transient, no wrapped err: got %q", got)
	}
	wrapped := errors.New("boom")
	if got := (LaunchError{Err: wrapped}).Error(); got != "boom" {
		t.Errorf("wrapped err: got %q", got)
	}
	if got := (LaunchError{Err: wrapped}).Unwrap(); got != wrapped {
		t.Errorf("Unwrap() = %v, want %v", got, wrapped)
	}
}

func TestLaunchErrorPermanence(t *testing.T) {
	if !(LaunchError{Permanent: true}).IsPermanentLaunchFailure() {
		t.Error("expected permanent flag to report true")
	}
	if (LaunchError{}).IsPermanentLaunchFailure() {
		t.Error("expected transient (zero value) to report false")
	}
}

func TestPermanentAndTransientLaunchErrorConstructors(t *testing.T) {
	base := errors.New("root cause")

	perm := PermanentLaunchError(base)
	var le LaunchError
	if !errors.As(perm, &le) || !le.Permanent || le.Err != base {
		t.Fatalf("PermanentLaunchError: got %#v", perm)
	}

	trans := TransientLaunchError(base)
	if !errors.As(trans, &le) || le.Permanent || le.Err != base {
		t.Fatalf("TransientLaunchError: got %#v", trans)
	}

	permf := PermanentLaunchErrorf("bad image %q", "x:y")
	if !errors.As(permf, &le) || !le.Permanent || le.Error() != `bad image "x:y"` {
		t.Fatalf("PermanentLaunchErrorf: got %#v", permf)
	}

	transf := TransientLaunchErrorf("retry %d", 3)
	if !errors.As(transf, &le) || le.Permanent || le.Error() != "retry 3" {
		t.Fatalf("TransientLaunchErrorf: got %#v", transf)
	}
}
