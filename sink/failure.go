package sink

import (
	"context"
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

// FailurePolicy determines what happens when a sink write fails
// after all retry attempts have been exhausted.
type FailurePolicy int

const (
	// FailurePolicyDrop silently discards the record.
	FailurePolicyDrop FailurePolicy = iota

	// FailurePolicyDLQ forwards the record to a dead-letter-queue sink.
	FailurePolicyDLQ

	// FailurePolicyFail returns an error and stops the pipeline.
	FailurePolicyFail
)

// DLQ is a sink that receives records that could not be written
// to the primary sink after all retries. It is used together with
// FailurePolicyDLQ.
//
// Common DLQ implementations: a separate Kafka topic, a file on disk,
// or a Postgres error table.
type DLQ interface {
	Write(ctx context.Context, record types.Record) error
}

// applyFailurePolicy handles a failed record according to the configured
// policy. Returns an error only when the policy is FailurePolicyFail.
func applyFailurePolicy(ctx context.Context, policy FailurePolicy, dlq DLQ, r types.Record) error {
	switch policy {
	case FailurePolicyDrop:
		return nil

	case FailurePolicyDLQ:
		if dlq == nil {
			return fmt.Errorf("weibo/sink: DLQ is nil but FailurePolicyDLQ is configured")
		}
		return dlq.Write(ctx, r)

	case FailurePolicyFail:
		return fmt.Errorf("weibo/sink: write failed after all retries, record key=%q", string(r.Key))

	default:
		return fmt.Errorf("weibo/sink: write failed after all retries")
	}
}
