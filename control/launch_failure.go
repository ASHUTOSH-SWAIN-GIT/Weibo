package control

import (
	"errors"
	"strings"
)

type permanentLaunchFailure interface {
	IsPermanentLaunchFailure() bool
}

func retryableLaunchFailure(err error) bool {
	if err == nil {
		return false
	}
	return launchFailureKind(err) == "transient"
}

func launchFailureKind(err error) string {
	if err == nil {
		return ""
	}
	var classified permanentLaunchFailure
	if errors.As(err, &classified) {
		if classified.IsPermanentLaunchFailure() {
			return "permanent"
		}
		return "transient"
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"invalid reference format",
		"pull access denied",
		"repository does not exist",
		"manifest unknown",
		"not found: manifest",
		"no such image",
	} {
		if strings.Contains(msg, marker) {
			return "permanent"
		}
	}
	return "transient"
}
