//go:build !kubernetes

package backend

import (
	"context"
	"fmt"
	"time"
)

// KubernetesOptions configures the Kubernetes backend. The default binary is
// built without Kubernetes support to keep Docker-only installs lightweight.
type KubernetesOptions struct {
	Kubeconfig       string
	Namespace        string
	Image            string
	PVCSize          string
	StorageClass     string
	ImagePullSecrets []string
}

// Kubernetes is a placeholder when the binary is built without the kubernetes
// build tag.
type Kubernetes struct{}

// NewKubernetes reports how to build a Kubernetes-enabled controller.
func NewKubernetes(opts KubernetesOptions) (*Kubernetes, error) {
	return nil, fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Ping(ctx context.Context) error {
	return fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Launch(ctx context.Context, spec LaunchSpec) (string, error) {
	return "", fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Stop(ctx context.Context, containerID string, timeout time.Duration) error {
	return fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Status(ctx context.Context, containerID string) (Status, error) {
	return Status{Phase: PhaseGone}, fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Logs(ctx context.Context, containerID string, tail int) (string, error) {
	return "", fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Remove(ctx context.Context, containerID string) error {
	return fmt.Errorf("kubernetes backend not included; build with -tags kubernetes")
}

func (k *Kubernetes) Capacity(ctx context.Context, cfg CapacityConfig) (CapacitySnapshot, error) {
	return CapacitySnapshot{
		Backend:     "kubernetes",
		Health:      "unreachable",
		Reason:      "kubernetes backend not included; build with -tags kubernetes",
		At:          time.Now().UTC(),
		Unsupported: true,
	}, nil
}
