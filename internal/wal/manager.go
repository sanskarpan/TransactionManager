package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxSegmentSize = 64 * 1024 * 1024 // 64 MB per log segment
	flushInterval  = 2 * time.Millisecond
)

// Manager is the production WAL writer. It is safe for concurrent use.
// It uses group commit: multiple goroutines call Append; a background
// goroutine batches their writes and calls fsync once per group.
type Manager struct {
	mu         sync.Mutex
	dir        string
	file       *os.File
	buf        []byte        // unflushed record bytes
	nextLSN    atomic.Uint64 // next LSN to assign (starts at 1)
	flushedLSN atomic.Uint64 // highest LSN fsynced to disk

	// Group commit machinery
	flushCh   chan struct{} // signal the flush goroutine
	flushMu   sync.Mutex
	flushCond *sync.Cond // waiters block on this
	stopCh    chan struct{}
	done      chan struct{}

	// per-txn last LSN tracking (for PrevLSN chain)
	txnLSN   map[uint64]LSN
	txnLSNmu sync.Mutex
}

// OpenManager opens (or creates) the WAL in dir and starts the background
// flush goroutine. The highest LSN found in the existing file is used to
// seed nextLSN so new records have globally unique, monotonically increasing
// sequence numbers.
func OpenManager(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	m := &Manager{
		dir:     dir,
		file:    f,
		buf:     make([]byte, 0, 64*1024),
		flushCh: make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
		txnLSN:  make(map[uint64]LSN),
	}
	m.flushCond = sync.NewCond(&m.flushMu)

	// Scan existing file to find highest LSN
	maxLSN, err := m.scanHighestLSN()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("wal: scan existing log: %w", err)
	}
	m.nextLSN.Store(maxLSN + 1)
	m.flushedLSN.Store(maxLSN)

	go m.flushLoop()
	return m, nil
}

// scanHighestLSN reads through the WAL file to find the highest LSN.
func (m *Manager) scanHighestLSN() (LSN, error) {
	// Re-open for reading from the start
	path := filepath.Join(m.dir, "wal.log")
	rf, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer rf.Close()

	var maxLSN LSN
	lenBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(rf, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return maxLSN, err
		}
		totalLen := binary.LittleEndian.Uint32(lenBuf)
		if totalLen < uint32(hdrFixed) {
			// Corrupt record, stop scanning
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
			// Corrupt record at tail — stop here
			break
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
	}
	return maxLSN, nil
}

// assignLSN atomically allocates the next LSN.
func (m *Manager) assignLSN() LSN {
	return m.nextLSN.Add(1) - 1
}

// setTxnLSN records the last LSN for txnID.
func (m *Manager) setTxnLSN(txnID uint64, lsn LSN) {
	m.txnLSNmu.Lock()
	m.txnLSN[txnID] = lsn
	m.txnLSNmu.Unlock()
}

// appendRecord encodes rec, assigns it an LSN, appends to buf, and
// signals the flush goroutine.
func (m *Manager) appendRecord(rec LogRecord) (LSN, error) {
	rec.LSN = m.assignLSN()

	encoded := encodeRecord(rec)

	m.mu.Lock()
	m.buf = append(m.buf, encoded...)
	m.mu.Unlock()

	// Signal the flush goroutine (non-blocking; it will pick up the batch)
	select {
	case m.flushCh <- struct{}{}:
	default:
	}

	m.setTxnLSN(rec.TxnID, rec.LSN)
	return rec.LSN, nil
}

// flushLoop is the background goroutine that drains buf to disk and fsyncs.
func (m *Manager) flushLoop() {
	defer close(m.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.flushCh:
		case <-ticker.C:
		case <-m.stopCh:
			m.doFlush()
			return
		}
		m.doFlush()
	}
}

// doFlush writes any buffered bytes to the file and fsyncs.
// Must be called from the flush goroutine only (not concurrently).
func (m *Manager) doFlush() {
	m.mu.Lock()
	if len(m.buf) == 0 {
		m.mu.Unlock()
		// Still broadcast to unblock any Flush waiters that may have
		// been waiting for a previously-flushed LSN.
		m.flushMu.Lock()
		m.flushCond.Broadcast()
		m.flushMu.Unlock()
		return
	}
	toWrite := m.buf
	m.buf = make([]byte, 0, 64*1024)
	m.mu.Unlock()

	_, err := m.file.Write(toWrite)
	if err == nil {
		err = m.file.Sync()
	}

	// Find the highest LSN in the flushed data so we can update flushedLSN.
	if err == nil {
		maxLSN := m.highestLSNInBytes(toWrite)
		m.flushMu.Lock()
		if maxLSN > m.flushedLSN.Load() {
			m.flushedLSN.Store(maxLSN)
		}
		m.flushCond.Broadcast()
		m.flushMu.Unlock()
	} else {
		// On error, still broadcast to unblock waiters (they will re-check).
		m.flushMu.Lock()
		m.flushCond.Broadcast()
		m.flushMu.Unlock()
	}
}

// highestLSNInBytes parses the record batch to find the highest LSN.
func (m *Manager) highestLSNInBytes(data []byte) LSN {
	var maxLSN LSN
	pos := 0
	for pos+hdrFixed <= len(data) {
		totalLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		if totalLen < hdrFixed || pos+totalLen > len(data) {
			break
		}
		rec, err := decodeRecord(data[pos : pos+totalLen])
		if err == nil && rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		pos += totalLen
	}
	return maxLSN
}

// Begin writes a BEGIN record and returns its LSN.
func (m *Manager) Begin(txnID uint64) (LSN, error) {
	return m.appendRecord(LogRecord{
		Type:      RecordBegin,
		TxnID:     txnID,
		PrevLSN:   LSNInvalid,
		Timestamp: time.Now().UnixNano(),
	})
}

// Write writes a data-change record before the storage mutation.
func (m *Manager) Write(txnID uint64, prevLSN LSN, op uint8, table, key string, before, after []byte) (LSN, error) {
	return m.appendRecord(LogRecord{
		Type:        RecordWrite,
		TxnID:       txnID,
		PrevLSN:     prevLSN,
		UndoNextLSN: prevLSN,
		Op:          op,
		Table:       table,
		Key:         key,
		Before:      before,
		After:       after,
	})
}

// Commit writes a COMMIT record and flushes the WAL to disk.
func (m *Manager) Commit(txnID uint64, lastLSN LSN) (LSN, error) {
	lsn, err := m.appendRecord(LogRecord{
		Type:    RecordCommit,
		TxnID:   txnID,
		PrevLSN: lastLSN,
	})
	if err != nil {
		return LSNInvalid, err
	}
	return lsn, m.Flush(lsn)
}

// Abort writes an ABORT record (called after undo log is applied).
func (m *Manager) Abort(txnID uint64, lastLSN LSN) (LSN, error) {
	return m.appendRecord(LogRecord{
		Type:    RecordAbort,
		TxnID:   txnID,
		PrevLSN: lastLSN,
	})
}

// CLR writes a Compensation Log Record during undo (ABORT path).
func (m *Manager) CLR(txnID uint64, prevLSN LSN, undoNextLSN LSN) (LSN, error) {
	return m.appendRecord(LogRecord{
		Type:        RecordCLR,
		TxnID:       txnID,
		PrevLSN:     prevLSN,
		UndoNextLSN: undoNextLSN,
	})
}

// Flush ensures all records up to and including lsn are durable on disk.
func (m *Manager) Flush(lsn LSN) error {
	if m.flushedLSN.Load() >= lsn {
		return nil
	}

	// Signal flush goroutine and wait
	select {
	case m.flushCh <- struct{}{}:
	default:
	}

	m.flushMu.Lock()
	for m.flushedLSN.Load() < lsn {
		m.flushCond.Wait()
	}
	m.flushMu.Unlock()
	return nil
}

// FlushedLSN returns the highest LSN that has been fsynced.
func (m *Manager) FlushedLSN() LSN {
	return m.flushedLSN.Load()
}

// Iterate scans the log file from the beginning and calls fn for each
// record with LSN >= fromLSN. Iteration stops if fn returns false.
func (m *Manager) Iterate(fromLSN LSN, fn func(LogRecord) bool) error {
	// Flush any pending writes so the file contains the latest records
	m.doFlush()

	path := filepath.Join(m.dir, "wal.log")
	rf, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("wal: iterate open: %w", err)
	}
	defer rf.Close()

	lenBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(rf, lenBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return fmt.Errorf("wal: iterate read length: %w", err)
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
			if !fn(rec) {
				break
			}
		}
	}
	return nil
}

// Close flushes all pending records, closes the file, and stops the goroutine.
func (m *Manager) Close() error {
	close(m.stopCh)
	<-m.done
	return m.file.Close()
}
