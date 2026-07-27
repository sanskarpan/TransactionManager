package wal

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("tempDir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestWALRoundTrip writes Begin/Write/Commit records, Iterates and verifies
// all are returned with correct fields.
func TestWALRoundTrip(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	const txnID = uint64(42)

	lsnBegin, err := m.Begin(txnID)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if lsnBegin == LSNInvalid {
		t.Fatal("Begin returned LSNInvalid")
	}

	before := []byte("before-data")
	after := []byte("after-data")
	lsnWrite, err := m.Write(txnID, lsnBegin, 1, "users", "row-1", before, after)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	lsnCommit, err := m.Commit(txnID, lsnWrite)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if lsnCommit <= lsnWrite {
		t.Errorf("Commit LSN %d should be > Write LSN %d", lsnCommit, lsnWrite)
	}

	// Iterate from the beginning and verify all three records
	var records []LogRecord
	if err := m.Iterate(LSNInvalid+1, func(rec LogRecord) bool {
		records = append(records, rec)
		return true
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Check BEGIN record
	if records[0].Type != RecordBegin {
		t.Errorf("record[0]: expected RecordBegin, got %v", records[0].Type)
	}
	if records[0].TxnID != txnID {
		t.Errorf("record[0]: expected TxnID %d, got %d", txnID, records[0].TxnID)
	}

	// Check WRITE record
	if records[1].Type != RecordWrite {
		t.Errorf("record[1]: expected RecordWrite, got %v", records[1].Type)
	}
	if records[1].Table != "users" {
		t.Errorf("record[1]: expected Table 'users', got %q", records[1].Table)
	}
	if records[1].Key != "row-1" {
		t.Errorf("record[1]: expected Key 'row-1', got %q", records[1].Key)
	}
	if string(records[1].Before) != string(before) {
		t.Errorf("record[1]: Before mismatch")
	}
	if string(records[1].After) != string(after) {
		t.Errorf("record[1]: After mismatch")
	}

	// Check COMMIT record
	if records[2].Type != RecordCommit {
		t.Errorf("record[2]: expected RecordCommit, got %v", records[2].Type)
	}
}

// TestWALGroupCommit spawns 10 concurrent goroutines each appending a record,
// then verifies all records are flushed.
func TestWALGroupCommit(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	const nGoroutines = 10
	var wg sync.WaitGroup
	lsns := make([]LSN, nGoroutines)
	errs := make([]error, nGoroutines)

	for i := range nGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lsn, err := m.Begin(uint64(100 + idx))
			lsns[idx] = lsn
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: Begin error: %v", i, err)
		}
	}

	// Find max LSN and flush up to it
	var maxLSN LSN
	for _, lsn := range lsns {
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}

	if err := m.Flush(maxLSN); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if m.FlushedLSN() < maxLSN {
		t.Errorf("FlushedLSN() %d < maxLSN %d after Flush", m.FlushedLSN(), maxLSN)
	}

	// Count records
	var count int
	if err := m.Iterate(1, func(_ LogRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if count != nGoroutines {
		t.Errorf("expected %d records, got %d", nGoroutines, count)
	}
}

// TestWALReopen writes records, closes the manager, reopens it, and verifies
// that all records are still present.
func TestWALReopen(t *testing.T) {
	dir := tempDir(t)

	// Write some records in the first manager
	m1, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager (first): %v", err)
	}

	const txnID = uint64(7)
	lsnBegin, err := m1.Begin(txnID)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	lsnWrite, err := m1.Write(txnID, lsnBegin, 0, "orders", "key-99", nil, []byte("data"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := m1.Commit(txnID, lsnWrite); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if err := m1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify
	m2, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager (second): %v", err)
	}
	defer m2.Close()

	var records []LogRecord
	if err := m2.Iterate(1, func(rec LogRecord) bool {
		records = append(records, rec)
		return true
	}); err != nil {
		t.Fatalf("Iterate after reopen: %v", err)
	}

	if len(records) != 3 {
		t.Fatalf("after reopen: expected 3 records, got %d", len(records))
	}

	types := []RecordType{RecordBegin, RecordWrite, RecordCommit}
	for i, rec := range records {
		if rec.Type != types[i] {
			t.Errorf("record[%d]: expected type %v, got %v", i, types[i], rec.Type)
		}
	}
}

// TestRecovery_CommittedAndAborted writes:
//   - txn1: Begin + Write + Commit (committed)
//   - txn2: Begin + Write        (no commit — crashed)
//
// Then runs RunRecovery and verifies:
//   - txn1 is in Committed
//   - txn2 is in UndoTxns
func TestRecovery_CommittedAndAborted(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	// txn1: committed
	const txn1 = uint64(1)
	lsn1Begin, _ := m.Begin(txn1)
	lsn1Write, _ := m.Write(txn1, lsn1Begin, 0, "t", "k1", nil, []byte("v1"))
	if _, err := m.Commit(txn1, lsn1Write); err != nil {
		t.Fatalf("txn1 Commit: %v", err)
	}

	// txn2: no commit (simulates crash)
	const txn2 = uint64(2)
	lsn2Begin, _ := m.Begin(txn2)
	_, _ = m.Write(txn2, lsn2Begin, 1, "t", "k2", []byte("old"), []byte("new"))

	// Flush so the data is on disk (flush up to the last assigned LSN)
	lastAssigned := m.nextLSN.Load() - 1
	if lastAssigned > 0 {
		_ = m.Flush(lastAssigned)
	}

	// Close without writing Abort/Commit for txn2
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Run recovery on the closed WAL
	result, err := RunRecovery(dir)
	if err != nil {
		t.Fatalf("RunRecovery: %v", err)
	}

	// txn1 should be in Committed
	if _, ok := result.Committed[txn1]; !ok {
		t.Errorf("txn1 should be in Committed, got Committed=%v", result.Committed)
	}

	// txn2 should be in UndoTxns (still active at crash time)
	if _, ok := result.UndoTxns[txn2]; !ok {
		t.Errorf("txn2 should be in UndoTxns, got UndoTxns=%v", result.UndoTxns)
	}

	// txn1's write should appear in RedoOps
	found := false
	for _, rop := range result.RedoOps {
		if rop.TxnID == txn1 && rop.Table == "t" && rop.Key == "k1" {
			found = true
		}
	}
	if !found {
		t.Errorf("txn1 write not found in RedoOps: %v", result.RedoOps)
	}
}

// TestNoopWriter verifies the NoopWriter satisfies the WALWriter interface and
// returns LSNInvalid for all operations.
func TestNoopWriter(t *testing.T) {
	var w WALWriter = NoopWriter{}

	lsn, err := w.Begin(1)
	if err != nil || lsn != LSNInvalid {
		t.Errorf("Begin: lsn=%d err=%v", lsn, err)
	}
	lsn, err = w.Write(1, 0, 0, "t", "k", nil, nil)
	if err != nil || lsn != LSNInvalid {
		t.Errorf("Write: lsn=%d err=%v", lsn, err)
	}
	lsn, err = w.Commit(1, 0)
	if err != nil || lsn != LSNInvalid {
		t.Errorf("Commit: lsn=%d err=%v", lsn, err)
	}
	if w.FlushedLSN() != LSNInvalid {
		t.Error("FlushedLSN should be LSNInvalid")
	}
}

// TestCheckpointRoundTrip writes a checkpoint and finds it via FindLastCheckpoint.
func TestCheckpointRoundTrip(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	att := map[uint64]ATTEntry{
		1: {LastLSN: 5, FirstLSN: 1, Status: 0},
		2: {LastLSN: 8, FirstLSN: 3, Status: 1},
	}

	endLSN, err := m.WriteCheckpoint(att)
	if err != nil {
		t.Fatalf("WriteCheckpoint: %v", err)
	}
	if endLSN == LSNInvalid {
		t.Fatal("WriteCheckpoint returned LSNInvalid")
	}

	cp, err := FindLastCheckpoint(dir)
	if err != nil {
		t.Fatalf("FindLastCheckpoint: %v", err)
	}
	if cp.EndLSN == LSNInvalid {
		t.Fatal("FindLastCheckpoint: no checkpoint found")
	}
	if len(cp.ATT) != 2 {
		t.Errorf("expected 2 ATT entries, got %d", len(cp.ATT))
	}
	e1, ok := cp.ATT[1]
	if !ok || e1.FirstLSN != 1 || e1.LastLSN != 5 {
		t.Errorf("ATT entry for txn 1 wrong: %+v", e1)
	}
}

// TestRecordEncodeDecodeAllTypes exercises every record type through
// encodeRecord/decodeRecord.
func TestRecordEncodeDecodeAllTypes(t *testing.T) {
	recs := []LogRecord{
		{
			Type:      RecordBegin,
			LSN:       1,
			TxnID:     10,
			PrevLSN:   0,
			Timestamp: time.Now().UnixNano(),
		},
		{
			Type:        RecordWrite,
			LSN:         2,
			TxnID:       10,
			PrevLSN:     1,
			UndoNextLSN: 1,
			Op:          1,
			Table:       "mytable",
			Key:         "mykey",
			Before:      []byte("before"),
			After:       []byte("after"),
		},
		{
			Type:    RecordCommit,
			LSN:     3,
			TxnID:   10,
			PrevLSN: 2,
		},
		{
			Type:    RecordAbort,
			LSN:     4,
			TxnID:   11,
			PrevLSN: 0,
		},
		{
			Type:        RecordCLR,
			LSN:         5,
			TxnID:       11,
			PrevLSN:     3,
			UndoNextLSN: 1,
		},
		{
			Type: RecordCheckpointBegin,
			LSN:  6,
			ATT: map[uint64]ATTEntry{
				99: {LastLSN: 10, FirstLSN: 1, Status: 0},
			},
		},
		{
			Type:     RecordCheckpointEnd,
			LSN:      7,
			BeginLSN: 6,
		},
	}

	for _, orig := range recs {
		encoded := encodeRecord(orig)
		decoded, err := decodeRecord(encoded)
		if err != nil {
			t.Errorf("type %v: decode error: %v", orig.Type, err)
			continue
		}
		if decoded.Type != orig.Type {
			t.Errorf("type %v: Type mismatch: got %v", orig.Type, decoded.Type)
		}
		if decoded.LSN != orig.LSN {
			t.Errorf("type %v: LSN mismatch: want %d got %d", orig.Type, orig.LSN, decoded.LSN)
		}
		if decoded.TxnID != orig.TxnID {
			t.Errorf("type %v: TxnID mismatch", orig.Type)
		}
		switch orig.Type {
		case RecordWrite:
			if decoded.Table != orig.Table {
				t.Errorf("WRITE: Table mismatch: %q vs %q", decoded.Table, orig.Table)
			}
			if decoded.Key != orig.Key {
				t.Errorf("WRITE: Key mismatch")
			}
			if string(decoded.Before) != string(orig.Before) {
				t.Errorf("WRITE: Before mismatch")
			}
			if string(decoded.After) != string(orig.After) {
				t.Errorf("WRITE: After mismatch")
			}
		case RecordCheckpointBegin:
			if len(decoded.ATT) != len(orig.ATT) {
				t.Errorf("CHECKPOINT_BEGIN: ATT length mismatch")
			}
		case RecordCheckpointEnd:
			if decoded.BeginLSN != orig.BeginLSN {
				t.Errorf("CHECKPOINT_END: BeginLSN mismatch")
			}
		}
	}
}

// TestWALLSNMonotonic verifies that successive writes produce strictly
// increasing LSNs.
func TestWALLSNMonotonic(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	const n = 20
	lsns := make([]LSN, n)
	for i := range n {
		lsn, err := m.Begin(uint64(i + 1))
		if err != nil {
			t.Fatalf("Begin[%d]: %v", i, err)
		}
		lsns[i] = lsn
	}
	for i := 1; i < n; i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSNs not monotonic: lsns[%d]=%d <= lsns[%d]=%d", i, lsns[i], i-1, lsns[i-1])
		}
	}
}

// Compile-time check: Manager implements WALWriter.
var _ WALWriter = (*Manager)(nil)

// TestManagerSequentialWriteRead writes N records and reads them back to
// verify the on-disk format is parseable across a flush boundary.
func TestManagerSequentialWriteRead(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	const N = 50
	for i := range N {
		_, err := m.Begin(uint64(i + 1))
		if err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
	}

	lastAssigned := m.nextLSN.Load() - 1
	if err := m.Flush(lastAssigned); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var count int
	if err := m.Iterate(1, func(_ LogRecord) bool {
		count++
		return true
	}); err != nil {
		t.Fatalf("Iterate: %v", err)
	}
	if count != N {
		t.Errorf("expected %d records, got %d", N, count)
	}
}

// Benchmark for raw append throughput.
func BenchmarkWALAppend(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	m, err := OpenManager(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()

	b.ResetTimer()
	for i := range b.N {
		_, _ = m.Begin(uint64(i + 1))
	}

	// Flush all
	last := m.nextLSN.Load() - 1
	if last > 0 {
		_ = m.Flush(last)
	}
}

// TestWALConcurrentMixed exercises concurrent Begin, Write, Commit, and Abort
// calls to verify no data races with the -race flag.
func TestWALConcurrentMixed(t *testing.T) {
	dir := tempDir(t)
	m, err := OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	defer m.Close()

	const nTxns = 20
	var wg sync.WaitGroup
	for i := range nTxns {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			lsn, err := m.Begin(id)
			if err != nil {
				return
			}
			lsn, err = m.Write(id, lsn, 0, fmt.Sprintf("tbl%d", id), "key", nil, []byte("val"))
			if err != nil {
				return
			}
			if id%2 == 0 {
				_, _ = m.Commit(id, lsn)
			} else {
				_, _ = m.Abort(id, lsn)
			}
		}(uint64(i + 1))
	}
	wg.Wait()

	// Verify we can iterate all records
	var total int
	_ = m.Iterate(1, func(_ LogRecord) bool {
		total++
		return true
	})
	// Each txn writes: Begin + Write + Commit or Abort = 3 records
	if total != nTxns*3 {
		t.Errorf("expected %d records, got %d", nTxns*3, total)
	}
}
