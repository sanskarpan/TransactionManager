package txn

import (
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// UndoOpType distinguishes between insert, update, and delete undo operations.
type UndoOpType int

const (
	// UndoInsert reverses an insert operation (row deletion).
	UndoInsert UndoOpType = iota
	// UndoUpdate reverses an update operation (restore before-image).
	UndoUpdate
	// UndoDelete reverses a delete operation (restore before-image).
	UndoDelete
)

// UndoEntry records a single operation's before-image for rollback.
type UndoEntry struct {
	Op          UndoOpType
	Table       string
	Key         storage.RowKey
	BeforeImage []types.Value
	SavepointID int
}

// UndoLog is a thread-safe, append-only log of undo entries for a transaction.
type UndoLog struct {
	mu      sync.Mutex
	entries []UndoEntry
}

// NewUndoLog creates an empty undo log.
func NewUndoLog() *UndoLog {
	return &UndoLog{}
}

// Append records a new undo entry for an insert, update, or delete.
func (u *UndoLog) Append(op UndoOpType, table string, key storage.RowKey, before []types.Value) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.entries = append(u.entries, UndoEntry{Op: op, Table: table, Key: key, BeforeImage: before})
}

// Len returns the number of entries in the undo log.
func (u *UndoLog) Len() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.entries)
}

// EntriesFrom returns a copy of all entries from position to the end.
func (u *UndoLog) EntriesFrom(position int) []UndoEntry {
	u.mu.Lock()
	defer u.mu.Unlock()
	if position >= len(u.entries) {
		return nil
	}
	result := make([]UndoEntry, len(u.entries)-position)
	copy(result, u.entries[position:])
	return result
}

// TruncateTo discards all entries at and after position.
func (u *UndoLog) TruncateTo(position int) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if position < len(u.entries) {
		u.entries = u.entries[:position]
	}
}

// All returns a copy of every undo entry in order.
func (u *UndoLog) All() []UndoEntry {
	u.mu.Lock()
	defer u.mu.Unlock()
	result := make([]UndoEntry, len(u.entries))
	copy(result, u.entries)
	return result
}
