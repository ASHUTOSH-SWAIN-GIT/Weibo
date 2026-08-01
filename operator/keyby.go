package operator

import (
	"fmt"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/types"
)

const defaultPartitions = 16

// FNV-1a 32-bit constants (hash/fnv). Inlined so RouteKey hashes a key
// without allocating a hasher on every record — the value is identical to
// fnv.New32a().Sum32().
const (
	fnvOffset32 = 2166136261
	fnvPrime32  = 16777619
)

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

// SelectKey extracts the key used by the keyed stage. The router writes this
// key onto the record before downstream stateful operators process it.
func (op *KeyByOperator) SelectKey(r types.Record) []byte {
	return op.KeySelector(r)
}

// Route hashes the selected key and returns the target worker index.
func (op *KeyByOperator) Route(r types.Record) int {
	return op.RouteKey(op.SelectKey(r))
}

// RouteKey hashes key and returns the target worker index.
func (op *KeyByOperator) RouteKey(key []byte) int {
	if len(key) == 0 || op.Partitions <= 1 {
		return 0
	}
	h := uint32(fnvOffset32)
	for _, b := range key {
		h ^= uint32(b)
		h *= fnvPrime32
	}
	return int(h) % op.Partitions
}
