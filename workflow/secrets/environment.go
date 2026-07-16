package secrets

import (
	"fmt"
	"os"
)

// Environment resolves workflow secrets from process environment variables.
type Environment struct{}

// Resolve returns the value of the named environment variable.
func (Environment) Resolve(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("secret %q is not set", name)
	}
	return value, nil
}
