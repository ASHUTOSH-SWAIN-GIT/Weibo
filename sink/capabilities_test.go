package sink

import (
	"context"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

type capabilityCheckpointedSink struct{}

func (capabilityCheckpointedSink) Write(context.Context, <-chan types.Record) error { return nil }
func (capabilityCheckpointedSink) SetOnPrepared(func(string, error))                {}
func (capabilityCheckpointedSink) Commit(context.Context, string) error             { return nil }
func (capabilityCheckpointedSink) Abort(context.Context, string) error              { return nil }
func (capabilityCheckpointedSink) WasCommitted(context.Context, string) (bool, error) {
	return false, nil
}

type explicitSinkCapabilities struct {
	caps Capabilities
}

func (s explicitSinkCapabilities) Write(context.Context, <-chan types.Record) error { return nil }
func (s explicitSinkCapabilities) SinkCapabilities() Capabilities                   { return s.caps }

func TestCapabilitiesOfDerivesLegacySinkInterfaces(t *testing.T) {
	caps := CapabilitiesOf(capabilityCheckpointedSink{})
	if !caps.CoordinatedCheckpoints {
		t.Fatal("CoordinatedCheckpoints: got false, want true")
	}
}

func TestCapabilitiesOfPreservesExplicitSinkCapabilities(t *testing.T) {
	caps := CapabilitiesOf(explicitSinkCapabilities{
		caps: Capabilities{Describe: true},
	})
	if !caps.Describe {
		t.Fatalf("explicit capabilities not preserved: %+v", caps)
	}
	if caps.CoordinatedCheckpoints {
		t.Fatalf("CoordinatedCheckpoints: got true, want false: %+v", caps)
	}
}
