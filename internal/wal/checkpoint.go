package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CheckpointState is the ATT snapshot written at checkpoint time.
type CheckpointState struct {
	ATT    map[uint64]ATTEntry
	EndLSN LSN
}

// WriteCheckpoint writes a BEGIN_CHECKPOINT and END_CHECKPOINT record pair.
// activeTxns is a snapshot of all active transactions and their lastLSN.
// Returns the LSN of the END_CHECKPOINT record.
func (m *Manager) WriteCheckpoint(activeTxns map[uint64]ATTEntry) (LSN, error) {
	// Write BEGIN_CHECKPOINT with the ATT
	beginLSN, err := m.appendRecord(LogRecord{
		Type: RecordCheckpointBegin,
		ATT:  activeTxns,
	})
	if err != nil {
		return LSNInvalid, fmt.Errorf("wal: checkpoint begin: %w", err)
	}

	// Write END_CHECKPOINT referencing the begin LSN
	endLSN, err := m.appendRecord(LogRecord{
		Type:     RecordCheckpointEnd,
		BeginLSN: beginLSN,
	})
	if err != nil {
		return LSNInvalid, fmt.Errorf("wal: checkpoint end: %w", err)
	}

	// Flush both records to disk
	if err := m.Flush(endLSN); err != nil {
		return LSNInvalid, fmt.Errorf("wal: checkpoint flush: %w", err)
	}

	return endLSN, nil
}

// FindLastCheckpoint scans the WAL backwards to find the most recent
// END_CHECKPOINT record and returns the corresponding CheckpointState.
// Returns a zero CheckpointState (EndLSN == LSNInvalid) if no checkpoint found.
func FindLastCheckpoint(dir string) (CheckpointState, error) {
	path := filepath.Join(dir, "wal.log")
	rf, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return CheckpointState{}, nil
		}
		return CheckpointState{}, fmt.Errorf("wal: open for checkpoint scan: %w", err)
	}
	defer rf.Close()

	// Collect all records in order; find the last END_CHECKPOINT.
	// We scan forward because the file is append-only; the last
	// END_CHECKPOINT is the most recent.
	type cpRecord struct {
		endLSN   LSN
		beginLSN LSN
	}
	var lastCP *cpRecord

	// Map from beginLSN → ATT (from BEGIN_CHECKPOINT records)
	attMap := make(map[LSN]map[uint64]ATTEntry)

	lenBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(rf, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return CheckpointState{}, fmt.Errorf("wal: checkpoint scan read: %w", err)
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
		switch rec.Type {
		case RecordCheckpointBegin:
			attMap[rec.LSN] = rec.ATT
		case RecordCheckpointEnd:
			lastCP = &cpRecord{endLSN: rec.LSN, beginLSN: rec.BeginLSN}
		}
	}

	if lastCP == nil {
		return CheckpointState{}, nil
	}

	att := attMap[lastCP.beginLSN]
	if att == nil {
		att = make(map[uint64]ATTEntry)
	}

	return CheckpointState{
		ATT:    att,
		EndLSN: lastCP.endLSN,
	}, nil
}
