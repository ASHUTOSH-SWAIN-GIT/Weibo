package backend

import (
	"testing"

	"github.com/docker/docker/api/types/container"
)

func TestCPUToNanoCPUs(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"500m", 500_000_000},    // half a CPU
		{"1000m", 1_000_000_000}, // one CPU via millicores
		{"2", 2_000_000_000},     // two whole CPUs
		{"1.5", 1_500_000_000},   // fractional
		{"250m", 250_000_000},
	}
	for _, c := range cases {
		got, err := cpuToNanoCPUs(c.in)
		if err != nil {
			t.Fatalf("cpuToNanoCPUs(%q): unexpected error %v", c.in, err)
		}
		if got != c.want {
			t.Errorf("cpuToNanoCPUs(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	if _, err := cpuToNanoCPUs("garbage"); err == nil {
		t.Error("cpuToNanoCPUs(garbage): expected error, got nil")
	}
}

func TestApplyResources(t *testing.T) {
	// nil → no constraints
	host := &container.HostConfig{}
	if err := applyResources(host, nil); err != nil {
		t.Fatalf("nil resources: %v", err)
	}
	if host.NanoCPUs != 0 || host.Memory != 0 {
		t.Errorf("nil resources set limits: cpu=%d mem=%d", host.NanoCPUs, host.Memory)
	}

	// "500m" CPU + "1Gi" memory → the plan's exact expected byte/nano values
	host = &container.HostConfig{}
	if err := applyResources(host, &ResourceLimits{CPU: "500m", Memory: "1Gi"}); err != nil {
		t.Fatalf("applyResources: %v", err)
	}
	if host.NanoCPUs != 500_000_000 {
		t.Errorf("NanoCPUs = %d, want 500000000", host.NanoCPUs)
	}
	if host.Memory != 1073741824 {
		t.Errorf("Memory = %d, want 1073741824", host.Memory)
	}

	// only one dimension set → the other stays unconstrained
	host = &container.HostConfig{}
	if err := applyResources(host, &ResourceLimits{Memory: "256Mi"}); err != nil {
		t.Fatalf("applyResources memory-only: %v", err)
	}
	if host.NanoCPUs != 0 {
		t.Errorf("NanoCPUs = %d, want 0 (unset)", host.NanoCPUs)
	}
	if host.Memory != 268435456 {
		t.Errorf("Memory = %d, want 268435456", host.Memory)
	}

	// bad memory → error
	if err := applyResources(&container.HostConfig{}, &ResourceLimits{Memory: "notabyte"}); err == nil {
		t.Error("applyResources(bad memory): expected error, got nil")
	}
}
