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

// TestCapabilitiesOfSeesThroughWatermarkSource guards the exact bug behind
// issue #25: wrapping a checkpoint-capable source (e.g. Kafka) in
// WatermarkSource — the standard way to drive event-time windows — must
// not silently hide its checkpoint/offset capabilities from the engine.
func TestCapabilitiesOfSeesThroughWatermarkSource(t *testing.T) {
	inner := capabilityCheckpointSource{}
	wrapped := NewWatermarkSource(inner, nil, 0)

	caps := CapabilitiesOf(wrapped)
	if !caps.CheckpointOffsets {
		t.Fatalf("CheckpointOffsets: got false for a WatermarkSource-wrapped CheckpointSource, want true: %+v", caps)
	}

	cps, ok := As[CheckpointSource](wrapped)
	if !ok {
		t.Fatal("As[CheckpointSource] on a wrapped source: got false, want true")
	}
	if _, err := cps.CheckpointOffset(); err != nil {
		t.Fatalf("CheckpointOffset() through the unwrap chain: %v", err)
	}
}

// TestAsDoesNotFalselyReportCapabilityForBareWrapper guards the other side:
// a WatermarkSource wrapping a source with NO checkpoint capability must
// not claim one just because WatermarkSource itself became Unwrapper.
func TestAsDoesNotFalselyReportCapabilityForBareWrapper(t *testing.T) {
	wrapped := NewWatermarkSource(plainSource{}, nil, 0)
	if _, ok := As[CheckpointSource](wrapped); ok {
		t.Fatal("As[CheckpointSource] on a wrapped plain source: got true, want false")
	}
	caps := CapabilitiesOf(wrapped)
	if caps.CheckpointOffsets {
		t.Fatalf("CheckpointOffsets: got true for a non-checkpointing wrapped source, want false: %+v", caps)
	}
}

type plainSource struct{}

func (plainSource) Run(context.Context, chan<- types.Record) error { return nil }
