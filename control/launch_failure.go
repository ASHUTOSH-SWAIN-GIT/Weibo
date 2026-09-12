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
	var classified permanentLaunchFailure
	if errors.As(err, &classified) {
		return !classified.IsPermanentLaunchFailure()
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
			return false
		}
	}
	return true
}
