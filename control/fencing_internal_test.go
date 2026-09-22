package control

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/backend"
	"github.com/ASHUTOSH-SWAIN-GIT/weibo/control/store"
)

// TestStartRunFencesAcrossProcessesSharingStoreFile guards the cross-process
// single-live-run guarantee that startRun's in-process checks alone cannot
// provide: two controller processes racing to launch the same job must
// still result in exactly one live container, even though each process
// only holds an in-process mutex over its own jobLocks map.
//
// The fence is `idx_runs_one_active_per_job`, a UNIQUE index enforced by
// SQLite itself on the run row insert (see store/sqlite.go) — it works
// regardless of which process performs the write, as long as both point at
// the same store file. This test opens two independent SQLite connections
// against one on-disk file to simulate that scenario, since two Controllers
// sharing a single in-memory *SQLite Go value would only prove the
// in-process mutex works.
func TestStartRunFencesAcrossProcessesSharingStoreFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "weibo.db")

	st1, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st1.Close()
	st2, err := store.OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	now := time.Now().UTC()
	job := &store.Job{
		ID:      "job-fence",
		Name:    "orders-sdk",
		Kind:    store.KindSDK,
		Image:   "my-registry/orders-sdk:v1",
		Spec:    "kind: sdk\nname: orders-sdk\nimage: my-registry/orders-sdk:v1\n",
		Desired: store.DesiredRunning,
		Created: now,
		Updated: now,
	}
	if err := st1.CreateJob(job); err != nil {
		t.Fatal(err)
	}

	fake1, fake2 := backend.NewFake(), backend.NewFake()
	c1 := New(Options{Store: st1, Backend: fake1, Image: "img", StopTimeout: time.Second})
	c2 := New(Options{Store: st2, Backend: fake2, Image: "img", StopTimeout: time.Second})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs[0] = c1.launch(context.Background(), job, 1, "") }()
	go func() { defer wg.Done(); errs[1] = c2.launch(context.Background(), job, 1, "") }()
	wg.Wait()

	successes := 0
	for _, e := range errs {
		if e == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one of two racing launches to win the store-level fence, got %d successes: %v", successes, errs)
	}
	if total := fake1.Launched() + fake2.Launched(); total != 1 {
		t.Fatalf("expected exactly one container launched across both backends, got %d", total)
	}
}
