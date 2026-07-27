package wal

// WALWriter is the interface for writing transaction log records.
// A nil WALWriter means in-memory-only mode; use NoopWriter for safe no-ops.
type WALWriter interface {
	// Begin writes a BEGIN record and returns its LSN.
	Begin(txnID uint64) (LSN, error)
	// Write writes a data-change record before the storage mutation.
	Write(txnID uint64, prevLSN LSN, op uint8, table, key string, before, after []byte) (LSN, error)
	// Commit writes a COMMIT record and flushes the WAL to disk.
	// The caller must NOT release locks or acknowledge success until this returns.
	Commit(txnID uint64, lastLSN LSN) (LSN, error)
	// Abort writes an ABORT record (called after undo log is applied).
	Abort(txnID uint64, lastLSN LSN) (LSN, error)
	// CLR writes a Compensation Log Record during undo (ABORT path).
	CLR(txnID uint64, prevLSN LSN, undoNextLSN LSN) (LSN, error)
	// Flush ensures all records up to and including lsn are durable on disk.
	Flush(lsn LSN) error
	// FlushedLSN returns the highest LSN that has been fsynced.
	FlushedLSN() LSN
}

// NoopWriter implements WALWriter with all no-ops. Used when WAL_DIR is empty.
type NoopWriter struct{}

func (NoopWriter) Begin(_ uint64) (LSN, error) { return LSNInvalid, nil }
func (NoopWriter) Write(_ uint64, _ LSN, _ uint8, _, _ string, _, _ []byte) (LSN, error) {
	return LSNInvalid, nil
}
func (NoopWriter) Commit(_ uint64, _ LSN) (LSN, error)     { return LSNInvalid, nil }
func (NoopWriter) Abort(_ uint64, _ LSN) (LSN, error)      { return LSNInvalid, nil }
func (NoopWriter) CLR(_ uint64, _ LSN, _ LSN) (LSN, error) { return LSNInvalid, nil }
func (NoopWriter) Flush(_ LSN) error                       { return nil }
func (NoopWriter) FlushedLSN() LSN                         { return LSNInvalid }
