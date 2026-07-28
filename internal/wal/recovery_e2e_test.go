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

// TestWAL_CLR_MidAbortRecovery simulates a crash that occurs in the middle of
// an abort (after one CLR has been written but before the ABORT record).
// ARIES recovery must recognise that the txn is still in the undo set and
// that the CLR's UndoNextLSN correctly identifies the next record to undo.
func TestWAL_CLR_MidAbortRecovery(t *testing.T) {
	const txnID = uint64(7)
	dir := t.TempDir()

	// Phase 1: write two rows, then write one CLR (simulating partial abort of
	// the second write), then crash before the ABORT record is written.
	var lsnWriteA, lsnWriteB, clrLSN wal.LSN
	func() {
		mgr, err := wal.OpenManager(dir)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer mgr.Close()

		beginLSN, err := mgr.Begin(txnID)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}

		// Write row "a"
		lsnWriteA, err = mgr.Write(txnID, beginLSN, 0, "accounts", "row-a", nil, []byte("val-a"))
		if err != nil {
			t.Fatalf("write row-a: %v", err)
		}

		// Write row "b"
		lsnWriteB, err = mgr.Write(txnID, lsnWriteA, 0, "accounts", "row-b", nil, []byte("val-b"))
		if err != nil {
			t.Fatalf("write row-b: %v", err)
		}

		// Write CLR compensating row "b" (the last write). The CLR's prevLSN is
		// lsnWriteB (the record being compensated) and undoNextLSN is lsnWriteA
		// (the next record to undo after this compensation).
		clrLSN, err = mgr.CLR(txnID, lsnWriteB, lsnWriteA)
		if err != nil {
			t.Fatalf("CLR: %v", err)
		}

		// Flush so the CLR reaches disk before the simulated crash.
		if err := mgr.Flush(clrLSN); err != nil {
			t.Fatalf("flush: %v", err)
		}

		// Crash here: no ABORT record written.
	}()

	// Phase 2: run ARIES recovery.
	rec, err := wal.RunRecovery(dir)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}

	// The txn must appear in UndoTxns because no ABORT record was written.
	undoLSN, inUndo := rec.UndoTxns[txnID]
	if !inUndo {
		t.Fatalf("txn %d must be in UndoTxns after mid-abort crash", txnID)
	}
	// The analysis pass tracks lastLSN; after the CLR the lastLSN should be
	// the CLR's own LSN.
	if undoLSN != clrLSN {
		t.Errorf("UndoTxns[%d] = %d, want CLR LSN %d", txnID, undoLSN, clrLSN)
	}
	// The txn must NOT appear in Committed or Aborted.
	if _, committed := rec.Committed[txnID]; committed {
		t.Error("mid-abort txn must NOT be in Committed")
	}
	if _, aborted := rec.Aborted[txnID]; aborted {
		t.Error("mid-abort txn must NOT be in Aborted (no ABORT record was written)")
	}

	// Phase 3: re-open the WAL and iterate to verify the CLR record carries the
	// correct UndoNextLSN (the LSN of the next write record to undo).
	mgr2, err := wal.OpenManager(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer mgr2.Close()

	var foundCLR bool
	if err := mgr2.Iterate(1, func(rec wal.LogRecord) bool {
		if rec.Type == wal.RecordCLR && rec.TxnID == txnID {
			foundCLR = true
			if rec.UndoNextLSN != lsnWriteA {
				t.Errorf("CLR.UndoNextLSN = %d, want lsnWriteA=%d", rec.UndoNextLSN, lsnWriteA)
			}
			// The write record that was compensated (lsnWriteB) should appear
			// as the CLR's PrevLSN.
			if rec.PrevLSN != lsnWriteB {
				t.Errorf("CLR.PrevLSN = %d, want lsnWriteB=%d", rec.PrevLSN, lsnWriteB)
			}
		}
		return true
	}); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	if !foundCLR {
		t.Error("no CLR record found in the WAL")
	}

	// lsnWriteA and lsnWriteB are captured only to verify CLR fields; suppress
	// any unused-variable warnings by referencing them here.
	_ = lsnWriteA
	_ = lsnWriteB
}
