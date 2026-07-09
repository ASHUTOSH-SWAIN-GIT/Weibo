package source

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
