package source

import (
	"context"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type capabilityCheckpointSource struct{}

func (capabilityCheckpointSource) Run(context.Context, chan<- types.Record) error { return nil }
func (capabilityCheckpointSource) CheckpointOffset() ([]byte, error)              { return nil, nil }
func (capabilityCheckpointSource) RestoreOffset([]byte) error                     { return nil }

type explicitSourceCapabilities struct {
	caps Capabilities
}

func (s explicitSourceCapabilities) Run(context.Context, chan<- types.Record) error { return nil }
func (s explicitSourceCapabilities) SourceCapabilities() Capabilities               { return s.caps }

func TestCapabilitiesOfDerivesLegacySourceInterfaces(t *testing.T) {
	caps := CapabilitiesOf(capabilityCheckpointSource{})
	if !caps.CheckpointOffsets {
		t.Fatal("CheckpointOffsets: got false, want true")
	}
}

func TestCapabilitiesOfPreservesExplicitSourceCapabilities(t *testing.T) {
	caps := CapabilitiesOf(explicitSourceCapabilities{
		caps: Capabilities{Drain: true, OperationalState: true},
	})
	if !caps.Drain || !caps.OperationalState {
		t.Fatalf("explicit capabilities not preserved: %+v", caps)
	}
	if caps.CheckpointOffsets {
		t.Fatalf("CheckpointOffsets: got true, want false: %+v", caps)
	}
}
