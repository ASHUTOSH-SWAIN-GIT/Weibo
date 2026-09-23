package state_test

import (
	"strings"
	"testing"

	"github.com/ASHUTOSH-SWAIN-GIT/weibo/state"
)

// listContents returns the entries of one list key as a comma-joined string.
func listContents(b state.StateBackend, ns, key string) string {
	ls := b.ListState(ns)
	ls.SetKey(key)
	var out []string
	for _, e := range ls.GetAll() {
		out = append(out, string(e))
	}
	return strings.Join(out, ",")
}

func appendAll(b state.StateBackend, ns, key string, vals ...string) {
	ls := b.ListState(ns)
	ls.SetKey(key)
	for _, v := range vals {
		ls.Append([]byte(v))
	}
}

// A ListState append after the DB has been reopened must add to the existing
// entries, not overwrite them. Append picks its slot from an in-memory counter;
// if that counter starts at 0 for a list that already has entries on disk, the
// new records silently replace the oldest ones. In a windowed job that meant a
// restart lost every record that was buffered in the window at checkpoint time.
func TestPebbleListState_AppendAfterReopenKeepsExistingEntries(t *testing.T) {
	dir := t.TempDir()
	first, err := state.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendAll(first, "win", "w1", "a", "b", "c")
	appendAll(first, "win", "w2", "x")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := state.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	appendAll(second, "win", "w1", "d", "e")
	appendAll(second, "win", "w2", "y")
	appendAll(second, "win", "w3", "fresh") // a key with no prior entries still starts clean

	if got := listContents(second, "win", "w1"); got != "a,b,c,d,e" {
		t.Errorf("w1 = %q, want a,b,c,d,e (new appends overwrote restored entries)", got)
	}
	if got := listContents(second, "win", "w2"); got != "x,y" {
		t.Errorf("w2 = %q, want x,y", got)
	}
	if got := listContents(second, "win", "w3"); got != "fresh" {
		t.Errorf("w3 = %q, want fresh", got)
	}
}

// The engine restores state through RestoreFrom (checkpoint dir -> live DB),
// which also replaces the counter map.
func TestPebbleListState_AppendAfterRestoreFromKeepsExistingEntries(t *testing.T) {
	live, err := state.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	appendAll(live, "win", "w1", "a", "b", "c")

	ckpt := t.TempDir() + "/ckpt"
	if err := live.CheckpointTo(ckpt); err != nil {
		t.Fatal(err)
	}
	// Records arriving after the checkpoint are discarded by the restore.
	appendAll(live, "win", "w1", "post-checkpoint")

	if err := live.RestoreFrom(ckpt); err != nil {
		t.Fatal(err)
	}
	if got := listContents(live, "win", "w1"); got != "a,b,c" {
		t.Fatalf("after restore w1 = %q, want a,b,c", got)
	}
	appendAll(live, "win", "w1", "d", "e")
	if got := listContents(live, "win", "w1"); got != "a,b,c,d,e" {
		t.Errorf("after restore + appends w1 = %q, want a,b,c,d,e", got)
	}
}

// Reset discards everything, so appends must start from an empty list.
func TestPebbleListState_AppendAfterResetStartsEmpty(t *testing.T) {
	b, err := state.OpenPebble(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	appendAll(b, "win", "w1", "a", "b")
	if err := b.Reset(); err != nil {
		t.Fatal(err)
	}
	appendAll(b, "win", "w1", "c")
	if got := listContents(b, "win", "w1"); got != "c" {
		t.Errorf("after reset w1 = %q, want c", got)
	}
}

// Clear followed by more appends (window fired, key reused) must not resurrect
// or clobber anything, including after a reopen.
func TestPebbleListState_AppendAfterClearAndReopen(t *testing.T) {
	dir := t.TempDir()
	first, _ := state.OpenPebble(dir)
	appendAll(first, "win", "w1", "a", "b", "c")
	ls := first.ListState("win")
	ls.SetKey("w1")
	ls.Clear()
	appendAll(first, "win", "w1", "d")
	first.Close()

	second, err := state.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	appendAll(second, "win", "w1", "e")
	if got := listContents(second, "win", "w1"); got != "d,e" {
		t.Errorf("w1 = %q, want d,e", got)
	}
}

// Keys sharing a byte prefix ("a" / "ab") are separate lists: seeding one must
// not be thrown off by, or disturb, the other.
func TestPebbleListState_SeedIsPerKeyForPrefixSharingKeys(t *testing.T) {
	dir := t.TempDir()
	first, _ := state.OpenPebble(dir)
	appendAll(first, "win", "a", "1", "2")
	appendAll(first, "win", "ab", "L1", "L2", "L3", "L4", "L5")
	first.Close()

	second, err := state.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	appendAll(second, "win", "a", "3")
	appendAll(second, "win", "ab", "L6")
	if got := listContents(second, "win", "a"); got != "1,2,3" {
		t.Errorf("a = %q, want 1,2,3", got)
	}
	if got := listContents(second, "win", "ab"); got != "L1,L2,L3,L4,L5,L6" {
		t.Errorf("ab = %q, want L1..L6", got)
	}
}
