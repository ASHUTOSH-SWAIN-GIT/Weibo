package backend

import "fmt"

// LaunchError classifies backend launch failures so the controller can decide
// whether to retry with backoff or fail the run permanently.
type LaunchError struct {
	Err       error
	Permanent bool
}

func (e LaunchError) Error() string {
	if e.Err == nil {
		if e.Permanent {
			return "permanent launch failure"
		}
		return "launch failure"
	}
	return e.Err.Error()
}

func (e LaunchError) Unwrap() error { return e.Err }

func (e LaunchError) IsPermanentLaunchFailure() bool { return e.Permanent }

func PermanentLaunchError(err error) error {
	return LaunchError{Err: err, Permanent: true}
}

func TransientLaunchError(err error) error {
	return LaunchError{Err: err}
}

func PermanentLaunchErrorf(format string, args ...any) error {
	return PermanentLaunchError(fmt.Errorf(format, args...))
}

func TransientLaunchErrorf(format string, args ...any) error {
	return TransientLaunchError(fmt.Errorf(format, args...))
}
