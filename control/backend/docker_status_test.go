package backend

import (
	"testing"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
)

func containerJSON(state *types.ContainerState, ports nat.PortMap) types.ContainerJSON {
	return types.ContainerJSON{
		ContainerJSONBase: &types.ContainerJSONBase{State: state},
		NetworkSettings: &types.NetworkSettings{
			NetworkSettingsBase: types.NetworkSettingsBase{Ports: ports},
		},
	}
}

func TestStatusFromInspect_Running(t *testing.T) {
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: true}, nil))
	if st.Phase != PhaseRunning {
		t.Fatalf("Phase = %q, want %q", st.Phase, PhaseRunning)
	}
}

func TestStatusFromInspect_Paused(t *testing.T) {
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: true, Paused: true}, nil))
	if st.Phase != PhaseUnhealthy {
		t.Fatalf("Phase = %q, want %q", st.Phase, PhaseUnhealthy)
	}
	if st.Reason == "" {
		t.Error("expected a non-empty reason for a paused container")
	}
}

func TestStatusFromInspect_Exited(t *testing.T) {
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: false, ExitCode: 3}, nil))
	if st.Phase != PhaseExited {
		t.Fatalf("Phase = %q, want %q", st.Phase, PhaseExited)
	}
	if st.ExitCode != 3 {
		t.Fatalf("ExitCode = %d, want 3", st.ExitCode)
	}
	if st.OOMKilled {
		t.Fatal("OOMKilled = true for an ordinary nonzero exit, want false")
	}
}

func TestStatusFromInspect_OOMKilled(t *testing.T) {
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: false, ExitCode: 137, OOMKilled: true}, nil))
	if st.Phase != PhaseExited {
		t.Fatalf("Phase = %q, want %q", st.Phase, PhaseExited)
	}
	if !st.OOMKilled {
		t.Fatal("OOMKilled = false for a container killed by the OOM killer, want true")
	}
}

func TestStatusFromInspect_NilState(t *testing.T) {
	st := statusFromInspect(containerJSON(nil, nil))
	if st.Phase != PhaseExited {
		t.Fatalf("Phase = %q, want %q for a nil state", st.Phase, PhaseExited)
	}
}

func TestStatusFromInspect_ResolvesPublishedPort(t *testing.T) {
	ports := nat.PortMap{
		"8080/tcp": []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "30001"}},
	}
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: true}, ports))
	if st.HostPort != 30001 {
		t.Fatalf("HostPort = %d, want 30001", st.HostPort)
	}
	if st.Address != "127.0.0.1:30001" {
		t.Fatalf("Address = %q, want 127.0.0.1:30001 (0.0.0.0 rewritten to loopback)", st.Address)
	}
}

func TestStatusFromInspect_PreservesExplicitHostIP(t *testing.T) {
	ports := nat.PortMap{
		"8080/tcp": []nat.PortBinding{{HostIP: "10.0.0.5", HostPort: "30002"}},
	}
	st := statusFromInspect(containerJSON(&types.ContainerState{Running: true}, ports))
	if st.Address != "10.0.0.5:30002" {
		t.Fatalf("Address = %q, want 10.0.0.5:30002", st.Address)
	}
}
