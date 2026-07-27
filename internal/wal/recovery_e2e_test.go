package wal_test

import (
	"fmt"
	"testing"

	"github.com/sanskarpan/TransactionManager/internal/wal"
)

// TestWAL_CommitSurvivesRestart writes 5 committed txns and 1 uncommitted txn,
// then runs ARIES recovery and verifies committed set and undo set.
func TestWAL_CommitSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write and commit 5 transactions; begin a 6th but crash.
	func() {
		mgr, err := wal.OpenManager(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer mgr.Close()

		for i := uint64(1); i <= 5; i++ {
			lsn, err := mgr.Begin(i)
			if err != nil {
				t.Fatalf("begin txn %d: %v", i, err)
			}
			writeLSN, err := mgr.Write(i, lsn, 1, "accounts", fmt.Sprintf("key%d", i), nil, []byte("data"))
			if err != nil {
				t.Fatalf("write txn %d: %v", i, err)
			}
			commitLSN, err := mgr.Commit(i, writeLSN)
			if err != nil {
				t.Fatalf("commit txn %d: %v", i, err)
			}
			if err := mgr.Flush(commitLSN); err != nil {
				t.Fatalf("flush txn %d: %v", i, err)
			}
		}

		// Txn 6: begin + write, but never commit — simulates crash.
		lsn6, _ := mgr.Begin(6)
		_, _ = mgr.Write(6, lsn6, 1, "accounts", "key6", nil, []byte("crash"))
		// intentionally no Commit call
	}()

	// Phase 2: run ARIES recovery.
	rec, err := wal.RunRecovery(dir)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}

	for i := uint64(1); i <= 5; i++ {
		if _, ok := rec.Committed[i]; !ok {
			t.Errorf("txn %d should be in Committed", i)
		}
	}
	if _, ok := rec.Committed[6]; ok {
		t.Error("txn 6 (uncommitted) must NOT be in Committed")
	}
	if _, ok := rec.UndoTxns[6]; !ok {
		t.Error("txn 6 should be in UndoTxns (needs undo)")
	}
	if len(rec.RedoOps) != 5 {
		t.Errorf("expected 5 redo ops for committed writes, got %d", len(rec.RedoOps))
	}
}

// TestWAL_UncommittedNotRecovered confirms a begin+write with no commit
// produces no Committed entry and appears in UndoTxns.
func TestWAL_UncommittedNotRecovered(t *testing.T) {
	dir := t.TempDir()

	func() {
		mgr, err := wal.OpenManager(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer mgr.Close()
		lsn, _ := mgr.Begin(1)
		_, _ = mgr.Write(1, lsn, 1, "t", "k", nil, []byte("v"))
		// no Commit — simulates crash
	}()

	rec, err := wal.RunRecovery(dir)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if _, ok := rec.Committed[1]; ok {
		t.Error("uncommitted txn must NOT be in Committed")
	}
	if _, ok := rec.UndoTxns[1]; !ok {
		t.Error("uncommitted txn should be in UndoTxns")
	}
}

// TestWAL_EmptyLogRecovery verifies that recovery on an empty WAL dir succeeds
// and returns zero committed/undo/redo.
func TestWAL_EmptyLogRecovery(t *testing.T) {
	dir := t.TempDir()
	rec, err := wal.RunRecovery(dir)
	if err != nil {
		t.Fatalf("recovery on empty dir: %v", err)
	}
	if len(rec.Committed) != 0 || len(rec.UndoTxns) != 0 || len(rec.RedoOps) != 0 {
		t.Errorf("expected empty recovery result on empty dir, got committed=%d undo=%d redo=%d",
			len(rec.Committed), len(rec.UndoTxns), len(rec.RedoOps))
	}
}
