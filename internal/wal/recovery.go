package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// RecoveryResult contains the state reconstructed by ARIES recovery.
type RecoveryResult struct {
	// Transactions that committed: their IDs
	Committed map[uint64]struct{}
	// Transactions that aborted explicitly
	Aborted map[uint64]struct{}
	// Operations to redo: list in log order
	RedoOps []RedoOp
	// Transactions to undo: still active at crash time
	UndoTxns map[uint64]LSN // txnID → lastLSN (to walk chain)
}

// RedoOp describes a single write operation to replay during redo.
type RedoOp struct {
	TxnID uint64
	LSN   LSN
	Op    uint8
	Table string
	Key   string
	After []byte // the value to restore (encoded)
}

// UndoOp describes a single write operation to reverse during undo.
type UndoOp struct {
	TxnID   uint64
	LSN     LSN
	Op      uint8 // operation to REVERSE
	Table   string
	Key     string
	Before  []byte // the value to restore (encoded)
	PrevLSN LSN
}

// attEntry tracks a transaction's state during the analysis pass.
type attEntry struct {
	lastLSN  LSN
	firstLSN LSN
	status   uint8 // 0=active, 1=committed, 2=aborted
}

// RunRecovery performs ARIES three-pass recovery on the WAL in dir.
// It returns a RecoveryResult describing what must be applied to storage.
func RunRecovery(dir string) (*RecoveryResult, error) {
	// Find last checkpoint
	cp, err := FindLastCheckpoint(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: recovery find checkpoint: %w", err)
	}

	// ── Analysis Pass ──────────────────────────────────────────────────────
	// Build ATT from checkpoint state, then scan from checkpoint endLSN
	// (or the beginning of the log) to the end.
	att := make(map[uint64]*attEntry)

	// Seed ATT from checkpoint
	for txnID, e := range cp.ATT {
		att[txnID] = &attEntry{
			lastLSN:  e.LastLSN,
			firstLSN: e.FirstLSN,
			status:   e.Status,
		}
	}

	// scanFromLSN is the first LSN we need to process in the analysis pass.
	// If we have a checkpoint, we start from its EndLSN (inclusive).
	// If not, we start from the very beginning (LSN 1).
	scanFromLSN := LSN(1)
	if cp.EndLSN != LSNInvalid {
		scanFromLSN = cp.EndLSN
	}

	// Scan all log records from scanFromLSN onward
	allRecs, err := readAllRecordsFrom(dir, scanFromLSN)
	if err != nil {
		return nil, fmt.Errorf("wal: recovery analysis scan: %w", err)
	}

	for _, rec := range allRecs {
		switch rec.Type {
		case RecordBegin:
			if _, exists := att[rec.TxnID]; !exists {
				att[rec.TxnID] = &attEntry{
					firstLSN: rec.LSN,
					lastLSN:  rec.LSN,
					status:   0,
				}
			}
		case RecordWrite, RecordCLR:
			if e, ok := att[rec.TxnID]; ok {
				e.lastLSN = rec.LSN
			} else {
				att[rec.TxnID] = &attEntry{
					firstLSN: rec.LSN,
					lastLSN:  rec.LSN,
					status:   0,
				}
			}
		case RecordCommit:
			if e, ok := att[rec.TxnID]; ok {
				e.lastLSN = rec.LSN
				e.status = 1
			}
		case RecordAbort:
			if e, ok := att[rec.TxnID]; ok {
				e.lastLSN = rec.LSN
				e.status = 2
			}
		}
	}

	// ── Redo Pass ──────────────────────────────────────────────────────────
	// Find min(firstLSN) across all ATT entries to know where redo starts.
	var redoStart LSN
	redoStart = LSNInvalid
	for _, e := range att {
		if redoStart == LSNInvalid || e.firstLSN < redoStart {
			redoStart = e.firstLSN
		}
	}

	var redoOps []RedoOp
	if redoStart != LSNInvalid {
		redoRecs, err := readAllRecordsFrom(dir, redoStart)
		if err != nil {
			return nil, fmt.Errorf("wal: recovery redo scan: %w", err)
		}
		for _, rec := range redoRecs {
			if rec.Type == RecordWrite {
				// Only redo writes from transactions that committed
				if e, ok := att[rec.TxnID]; ok && e.status == 1 {
					redoOps = append(redoOps, RedoOp{
						TxnID: rec.TxnID,
						LSN:   rec.LSN,
						Op:    rec.Op,
						Table: rec.Table,
						Key:   rec.Key,
						After: rec.After,
					})
				}
			}
		}
	}

	// ── Undo Pass ──────────────────────────────────────────────────────────
	// Collect UndoTxns: transactions still active at crash time (status=active).
	undoTxns := make(map[uint64]LSN)
	for txnID, e := range att {
		if e.status == 0 {
			undoTxns[txnID] = e.lastLSN
		}
	}

	// Build committed and aborted sets
	committed := make(map[uint64]struct{})
	aborted := make(map[uint64]struct{})
	for txnID, e := range att {
		switch e.status {
		case 1:
			committed[txnID] = struct{}{}
		case 2:
			aborted[txnID] = struct{}{}
		}
	}

	return &RecoveryResult{
		Committed: committed,
		Aborted:   aborted,
		RedoOps:   redoOps,
		UndoTxns:  undoTxns,
	}, nil
}

// readAllRecordsFrom reads all valid log records with LSN >= fromLSN.
func readAllRecordsFrom(dir string, fromLSN LSN) ([]LogRecord, error) {
	path := filepath.Join(dir, "wal.log")
	rf, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rf.Close()

	var recs []LogRecord
	lenBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(rf, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return recs, fmt.Errorf("read length: %w", err)
		}
		totalLen := binary.LittleEndian.Uint32(lenBuf)
		if totalLen < uint32(hdrFixed) {
			break
		}
		recBuf := make([]byte, totalLen)
		copy(recBuf, lenBuf)
		_, err = io.ReadFull(rf, recBuf[4:])
		if err != nil {
			break
		}
		rec, err := decodeRecord(recBuf)
		if err != nil {
			break
		}
		if rec.LSN >= fromLSN {
			recs = append(recs, rec)
		}
	}
	return recs, nil
}

// StorageApplier is implemented by callers that want to apply redo/undo ops
// to a storage backend.
type StorageApplier interface {
	SetRow(table, key string, vals []byte)
	DeleteRow(table, key string)
}

// ApplyRedoOps applies redo operations to a storage backend in log order.
func ApplyRedoOps(ops []RedoOp, store StorageApplier) {
	for _, op := range ops {
		switch op.Op {
		case 0: // UndoInsert → Insert was done → redo means set the row
			store.SetRow(op.Table, op.Key, op.After)
		case 1: // UndoUpdate → Update was done → redo means set the row to new value
			store.SetRow(op.Table, op.Key, op.After)
		case 2: // UndoDelete → Delete was done → redo means delete the row
			store.DeleteRow(op.Table, op.Key)
		}
	}
}

// ApplyUndoOps applies undo operations to a storage backend (reverse log order).
// It writes CLR records to w as it processes each undo op.
func ApplyUndoOps(ops []UndoOp, store StorageApplier, w WALWriter) {
	// Apply in reverse order (newest first)
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		switch op.Op {
		case 0: // UndoInsert → original was an insert → undo means delete
			store.DeleteRow(op.Table, op.Key)
		case 1, 2: // UndoUpdate or UndoDelete → restore before image
			store.SetRow(op.Table, op.Key, op.Before)
		}
		if w != nil {
			_, _ = w.CLR(op.TxnID, op.LSN, op.PrevLSN)
		}
	}
}
