package source

import (
	"context"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// CommitPolicy determines what happens when a Kafka offset commit
// fails after all retry attempts have been exhausted.
type CommitPolicy int

const (
	// CommitPolicySkip logs the error and continues processing.
	// Records may be reprocessed on restart because the offset was
	// never committed.
	CommitPolicySkip CommitPolicy = iota

	// CommitPolicyFail returns an error and stops the pipeline.
	CommitPolicyFail
)

// DeserFailurePolicy determines what happens when deserialization
// fails for a record read from the source.
type DeserFailurePolicy int

const (
	// DeserFailureDrop silently drops the record.
	DeserFailureDrop DeserFailurePolicy = iota

	// DeserFailureDLQ forwards the raw record to a dead-letter queue.
	DeserFailureDLQ

	// DeserFailureFail returns an error and stops the pipeline.
	DeserFailureFail
)

// RecordSink receives failed records for dead-letter-queue routing.
// Implemented by any sink (KafkaSink, PostgresSink, etc.).
type RecordSink interface {
	Write(ctx context.Context, r types.Record) error
}
