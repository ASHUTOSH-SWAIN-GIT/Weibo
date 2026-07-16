package secrets

// SecretResolver resolves a named secret for workflow configuration.
//
// Implementations must not include resolved secret values in returned
// errors. Returning the secret name in an error is acceptable.
type SecretResolver interface {
	Resolve(name string) (string, error)
}
