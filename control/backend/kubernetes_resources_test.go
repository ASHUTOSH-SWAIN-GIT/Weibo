//go:build kubernetes

package backend

import "testing"

func TestK8sResources(t *testing.T) {
	if r := k8sResources(nil); r.Requests != nil || r.Limits != nil {
		t.Error("nil -> non-empty ResourceRequirements")
	}
	if r := k8sResources(&ResourceLimits{}); r.Requests != nil || r.Limits != nil {
		t.Error("empty -> non-empty ResourceRequirements")
	}
	r := k8sResources(&ResourceLimits{CPU: "500m", Memory: "256Mi"})
	// requests must equal limits (guaranteed QoS)
	if r.Requests.Cpu().String() != "500m" || r.Limits.Cpu().String() != "500m" {
		t.Errorf("cpu req/limit = %s/%s, want 500m/500m", r.Requests.Cpu(), r.Limits.Cpu())
	}
	if r.Requests.Memory().String() != "256Mi" || r.Limits.Memory().String() != "256Mi" {
		t.Errorf("mem req/limit = %s/%s, want 256Mi/256Mi", r.Requests.Memory(), r.Limits.Memory())
	}
}
