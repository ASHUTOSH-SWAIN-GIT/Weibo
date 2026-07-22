package checkpoint

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestCoordinator_StopRaceWithOnSinkPrepared stresses the window where a
// sink's OnSinkPrepared send races Stop closing the events channel. Before
// the sendWg guard this panicked with "send on closed channel" (and the race
// detector flagged the halted/close ordering). Run with -race.
func TestCoordinator_StopRaceWithOnSinkPrepared(t *testing.T) {
	for iter := 0; iter < 40; iter++ {
		c := NewCoordinator(NewFileStorage(t.TempDir()), "txn")
		c.CommitSink = func(context.Context, string) error { return nil }
		c.AbortSink = func(context.Context, string) error { return nil }
		c.Start(context.Background())

		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c.OnSinkPrepared(fmt.Sprintf("cp-%d", i), nil)
			}(i)
		}
		// Stop concurrently with the in-flight prepares.
		go c.Stop()

		wg.Wait()
		c.Stop() // idempotent; also blocks until the first Stop finished
	}
}
