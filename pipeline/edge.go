// Package pipeline implements stage-based execution for stream
// pipelines. Operators are grouped into stages that execute as direct
// function calls; bounded edges between stages provide backpressure:
// a full edge blocks the upstream stage, and the pressure propagates
// back to the source.
package pipeline

import (
	"context"

	"github.com/ASHUTOSH-SWAIN-GIT/mailer/types"
)

// Edge is a bounded channel connecting two stages. A full edge blocks
// the upstream stage — that blocking is the backpressure mechanism.
type Edge struct {
	Name string
	Ch   chan types.Record
}

// NewEdge creates an edge with the given name and capacity.
func NewEdge(name string, capacity int) *Edge {
	return &Edge{Name: name, Ch: make(chan types.Record, capacity)}
}

// sendRecord writes r to out, blocking while the channel is full.
// It gives up only when hardCtx is cancelled (shutdown-timeout expiry
// or a fatal pipeline error) — graceful shutdown drains through normal
// channel closes instead, so no records are lost. No default branch:
// a full channel means downstream is slower, and blocking here is how
// backpressure propagates upstream.
func sendRecord(hardCtx context.Context, out chan<- types.Record, r types.Record) error {
	select {
	case out <- r:
		return nil
	case <-hardCtx.Done():
		return hardCtx.Err()
	}
}

// recvRecord reads the next record from in, aborting if hardCtx fires
// so stages never block forever on a stalled upstream during forced
// shutdown. ok is false when the channel is closed.
func recvRecord(hardCtx context.Context, in <-chan types.Record) (r types.Record, ok bool, err error) {
	select {
	case r, ok = <-in:
		return r, ok, nil
	case <-hardCtx.Done():
		return types.Record{}, false, hardCtx.Err()
	}
}
