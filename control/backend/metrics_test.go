package backend

import (
	"math"
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
)

func TestContainerSnapshotMetadata(t *testing.T) {
	c := types.Container{
		ID: "1234567890abcdef", Names: []string{"/orders"}, Image: "registry/orders:v2",
		ImageID: "sha256:abcdef1234567890", State: "running", Created: 123,
		Labels: map[string]string{"weibo.job": "job-1"},
	}
	got := containerSnapshot(c, nil)
	if got.ID != "1234567890ab" || got.Name != "orders" || got.ImageID != "abcdef123456" {
		t.Fatalf("identity not normalized: %+v", got)
	}
	if !got.Managed || got.JobID != "job-1" || got.Image != "registry/orders:v2" || got.StartedAt != 123 {
		t.Fatalf("metadata missing: %+v", got)
	}
}

func TestContainerSnapshotComputeStats(t *testing.T) {
	c := types.Container{ID: "container", Names: []string{"/job"}, State: "running"}
	s := &container.StatsResponse{
		Stats: container.Stats{
			CPUStats: container.CPUStats{
				CPUUsage:    container.CPUUsage{TotalUsage: 300, PercpuUsage: []uint64{1, 1}},
				SystemUsage: 2000, OnlineCPUs: 2,
			},
			PreCPUStats: container.CPUStats{CPUUsage: container.CPUUsage{TotalUsage: 100}, SystemUsage: 1000},
			MemoryStats: container.MemoryStats{Usage: 600, Limit: 1000, Stats: map[string]uint64{"cache": 100}},
			PidsStats:   container.PidsStats{Current: 7},
			BlkioStats: container.BlkioStats{IoServiceBytesRecursive: []container.BlkioStatEntry{
				{Op: "Read", Value: 10}, {Op: "read", Value: 15}, {Op: "Write", Value: 20},
			}},
		},
		Networks: map[string]container.NetworkStats{
			"eth0": {RxBytes: 11, TxBytes: 12}, "eth1": {RxBytes: 20, TxBytes: 30},
		},
	}
	got := containerSnapshot(c, s)
	if math.Abs(got.CPUPercent-40) > 0.001 {
		t.Errorf("CPUPercent=%v, want 40", got.CPUPercent)
	}
	if got.MemoryUsedBytes != 500 || got.MemoryLimitBytes != 1000 || got.MemoryPercent != 50 {
		t.Errorf("memory stats wrong: %+v", got)
	}
	if got.NetworkRxBytes != 31 || got.NetworkTxBytes != 42 {
		t.Errorf("network stats wrong: %+v", got)
	}
	if got.BlockReadBytes != 25 || got.BlockWriteBytes != 20 || got.PIDs != 7 {
		t.Errorf("I/O or PID stats wrong: %+v", got)
	}
}

func TestHostStatsReadsCurrentLinuxHost(t *testing.T) {
	d := &Docker{}
	first := d.hostStats("node-1", "Linux", "amd64", "6.0", "29", 4, 8<<30)
	if first.Hostname != "node-1" || first.CPUCores != 4 || first.OperatingSystem != "Linux" {
		t.Fatalf("host metadata missing: %+v", first)
	}
	if first.MemoryTotalBytes <= 0 || first.MemoryUsedBytes < 0 || first.Load1 < 0 {
		t.Fatalf("invalid live host values: %+v", first)
	}
	second := d.hostStats("node-1", "Linux", "amd64", "6.0", "29", 4, 8<<30)
	if second.CPUPercent < 0 || second.CPUPercent > 100 {
		t.Fatalf("CPU percent out of range: %v", second.CPUPercent)
	}
}
