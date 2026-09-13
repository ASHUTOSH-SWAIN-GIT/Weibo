//go:build kubernetes

package backend

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sResources(t *testing.T) {
	if r := k8sResources(nil); r.Requests != nil || r.Limits != nil {
		t.Error("nil -> non-empty ResourceRequirements")
	}
	if r := k8sResources(&ResourceLimits{}); r.Requests != nil || r.Limits != nil {
		t.Error("empty -> non-empty ResourceRequirements")
	}
	r := k8sResources(&ResourceLimits{CPU: "500m", Memory: "256Mi", EphemeralStorage: "1Gi"})
	// requests must equal limits (guaranteed QoS)
	if r.Requests.Cpu().String() != "500m" || r.Limits.Cpu().String() != "500m" {
		t.Errorf("cpu req/limit = %s/%s, want 500m/500m", r.Requests.Cpu(), r.Limits.Cpu())
	}
	if r.Requests.Memory().String() != "256Mi" || r.Limits.Memory().String() != "256Mi" {
		t.Errorf("mem req/limit = %s/%s, want 256Mi/256Mi", r.Requests.Memory(), r.Limits.Memory())
	}
	reqEphemeral := r.Requests[corev1.ResourceEphemeralStorage]
	limEphemeral := r.Limits[corev1.ResourceEphemeralStorage]
	if reqEphemeral.String() != "1Gi" || limEphemeral.String() != "1Gi" {
		t.Errorf("ephemeral req/limit = %s/%s, want 1Gi/1Gi", reqEphemeral.String(), limEphemeral.String())
	}
}

func TestK8sCapacityUsesResourceQuota(t *testing.T) {
	q := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "compute", Namespace: "weibo"}, Status: corev1.ResourceQuotaStatus{
		Hard: corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse("4"), corev1.ResourceRequestsMemory: resource.MustParse("8Gi")},
		Used: corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse("1"), corev1.ResourceRequestsMemory: resource.MustParse("2Gi")},
	}}
	k := newK8s(fake.NewSimpleClientset(q), KubernetesOptions{Namespace: "weibo"})
	snap, err := k.Capacity(context.Background(), CapacityConfig{DefaultJobCPU: "1", DefaultJobMemory: "1Gi"})
	if err != nil {
		t.Fatal(err)
	}
	if snap.Unsupported || snap.Source != "resource_quota" || snap.AvailableSlots == nil || *snap.AvailableSlots != 3 {
		t.Fatalf("quota capacity not derived correctly: %+v", snap)
	}
}
