//go:build kubernetes

package control

import (
	"context"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// KubernetesSecretProvider resolves refs of the form
// {provider:"kubernetes", name:"secret-name", key:"data-key"}.
type KubernetesSecretProvider struct {
	cs        kubernetes.Interface
	namespace string
}

func NewKubernetesSecretProvider(namespace, kubeconfig string) (*KubernetesSecretProvider, error) {
	cfg, err := kubernetesRestConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return NewKubernetesSecretProviderClient(cs, namespace), nil
}

func NewKubernetesSecretProviderClient(cs kubernetes.Interface, namespace string) *KubernetesSecretProvider {
	if namespace == "" {
		namespace = "default"
	}
	return &KubernetesSecretProvider{cs: cs, namespace: namespace}
}

func (p *KubernetesSecretProvider) ResolveSecret(ref store.SecretRef) (string, error) {
	if ref.Provider != "kubernetes" {
		return "", fmt.Errorf("unsupported secret provider %q for %q", ref.Provider, ref.Name)
	}
	if ref.Name == "" || ref.Key == "" {
		return "", fmt.Errorf("kubernetes secret reference requires name and key")
	}
	secret, err := p.cs.CoreV1().Secrets(p.namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", fmt.Errorf("kubernetes secret %q is not found", ref.Name)
	}
	if err != nil {
		return "", fmt.Errorf("kubernetes secret %q cannot be read: %w", ref.Name, err)
	}
	value, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("kubernetes secret %q key %q is not set", ref.Name, ref.Key)
	}
	return string(value), nil
}

func kubernetesRestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if c, err := rest.InClusterConfig(); err == nil {
			return c, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

var _ SecretProvider = (*KubernetesSecretProvider)(nil)
