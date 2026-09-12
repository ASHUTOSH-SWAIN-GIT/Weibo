package control

import (
	"fmt"
	"os"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

// SecretProvider resolves durable secret references at launch time.
// Implementations must never include resolved secret values in returned errors.
type SecretProvider interface {
	ResolveSecret(ref store.SecretRef) (string, error)
}

type envSecretProvider struct{}

// SecretProviders tries each provider in order.
type SecretProviders []SecretProvider

func (providers SecretProviders) ResolveSecret(ref store.SecretRef) (string, error) {
	var last error
	for _, provider := range providers {
		value, err := provider.ResolveSecret(ref)
		if err == nil {
			return value, nil
		}
		last = err
	}
	if last != nil {
		return "", last
	}
	return "", fmt.Errorf("no secret provider configured for %q", ref.Name)
}

func (envSecretProvider) ResolveSecret(ref store.SecretRef) (string, error) {
	if ref.Provider != "" && ref.Provider != "env" {
		return "", fmt.Errorf("unsupported secret provider %q for %q", ref.Provider, ref.Name)
	}
	name := ref.Name
	if name == "" {
		name = ref.Key
	}
	value, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("secret %q is not set", name)
	}
	return value, nil
}

func secretRefsFromEnv(env map[string]string) map[string]store.SecretRef {
	if len(env) == 0 {
		return nil
	}
	refs := make(map[string]store.SecretRef, len(env))
	for key := range env {
		refs[key] = store.SecretRef{Provider: "env", Name: key}
	}
	return refs
}

func mergeSecretRefs(base, override map[string]store.SecretRef) map[string]store.SecretRef {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]store.SecretRef, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

func (c *Controller) resolveEnvRefs(env map[string]string, refs map[string]store.SecretRef) (map[string]string, error) {
	out := make(map[string]string, len(env)+len(refs))
	for k, v := range env {
		out[k] = v
	}
	for key, ref := range refs {
		if _, ok := out[key]; ok {
			continue
		}
		value, err := c.secretStore.ResolveSecret(ref)
		if err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}
