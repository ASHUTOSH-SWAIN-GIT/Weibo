package operator

import (
	"fmt"
	"hash/fnv"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

const defaultPartitions = 16

// KeyByOperator marks the start of a keyed stage.  Records flow
// through a router that hash-dispatches each record to one of N
// worker channels.  Each worker runs its own copy of the downstream
// stateful operators (Window, Reduce) with isolated state.
//
// KeyBy is not a processing operator — Execute() detects it and
// builds the worker topology. Process() is a no-op.
type KeyByOperator struct {
	KeySelector KeySelector
	Partitions  int
	Label       string
}

// KeyBy creates a KeyByOperator with the given key selector.
// Default partition count is 16. Use WithPartitions to change it.
func KeyBy(fn KeySelector) *KeyByOperator {
	return &KeyByOperator{
		KeySelector: fn,
		Partitions:  defaultPartitions,
	}
}

// WithPartitions sets the number of keyed workers and returns
// the operator. More workers = more parallelism for stateful processing.
func (op *KeyByOperator) WithPartitions(n int) *KeyByOperator {
	op.Partitions = n
	return op
}

func (op *KeyByOperator) Name() string     { return "KeyBy" }
func (op *KeyByOperator) GetLabel() string { return op.Label }

func (op *KeyByOperator) DescribeOp() OperatorMeta {
	return OperatorMeta{
		Type:  "KeyBy",
		Label: op.Label,
		Config: map[string]string{
			"partitions": fmt.Sprintf("%d", op.Partitions),
		},
	}
}

// IsRouter returns true — KeyBy is handled by Execute() as a
// worker topology builder, not a regular channel operator.
func (op *KeyByOperator) IsRouter() bool { return true }

// Process is never called by Execute() when IsRouter() is true.
func (op *KeyByOperator) Process(in <-chan types.Record, out chan<- types.Record) {
	defer close(out)
	for r := range in {
		out <- r
	}
}

// Route hashes the record key and returns the target worker index.
func (op *KeyByOperator) Route(r types.Record) int {
	key := op.KeySelector(r)
	if len(key) == 0 || op.Partitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % op.Partitions
}
