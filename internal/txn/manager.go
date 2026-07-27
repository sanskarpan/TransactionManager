package txn

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/isolation"
	"github.com/sanskarpan/TransactionManager/internal/lock"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
	"github.com/sanskarpan/TransactionManager/internal/wal"
)

// ErrTooManyTransactions is returned by Manager.Begin when MaxActive is
// set and the number of active transactions has reached that limit.
var ErrTooManyTransactions = errors.New("too many active transactions")

// Manager is the central orchestrator that owns every transaction's
// lifecycle, hands out monotonic IDs, and bridges the two concurrency
// protocols (2PL and MVCC) to the lock table, MVCC store, and SSI
// tracker. All exported mutators are safe for concurrent use; status
// queries take a read lock on m.mu.
type Manager struct {
	mu        sync.RWMutex
	txns      map[ID]*Transaction
	committed map[ID]struct{}
	aborted   map[ID]struct{}
	nextID    atomic.Uint64

	// MaxActive caps the number of concurrently-active transactions.
	// Zero means no cap. Enforced atomically inside Begin under m.mu.
	MaxActive int

	// epochMu serialises opEpoch reads (from MVCC goroutines) and
	// writes (from Reset). A separate mutex avoids the self-deadlock
	// that would arise if epoch were protected by m.mu — Reset already
	// holds m.mu.Lock when it calls applyMVCCUndo, and applyMVCCUndo
	// must also be able to snapshot the epoch to protect its chain
	// accesses (CT-28/29/30).
	epochMu sync.RWMutex
	opEpoch uint64

	LockTable  *lock.Table
	LockAcq    *lock.Acquirer
	Catalog    *storage.Catalog
	WFG        *WFGAdapter
	MVCCStore  *mvcc.Store
	SSITracker *isolation.SSITracker
	WAL        wal.WALWriter

	// Events (optional - set after creation)
	OnBegin  func(txn *Transaction)
	OnCommit func(txn *Transaction)
	OnAbort  func(txn *Transaction, reason error)
	// OnLockTimeout is invoked when a 2PL lock acquisition fails because
	// the lock was not granted within the configured timeout (or the
	// request context was cancelled, or the txn was aborted). It is
	// NOT an abort — the txn stays active and the caller surfaces the
	// error to the API. CT-17: previously no signal was emitted and
	// the LockTimeouts metric never incremented.
	OnLockTimeout func(txn *Transaction, resource lock.ResourceID)
}

// WFGAdapter wraps the deadlock.WFG so the txn package can call into the
// wait-for graph without importing the deadlock package (which would be a
// cycle: deadlock → txn → deadlock). The adapter is just function
// pointers; the real graph is wired by cmd/server at startup.
type WFGAdapter struct {
	AddEdgeFn     func(from, to uint64)
	RemoveEdgesFn func(txnID uint64)
	RemoveNodeFn  func(txnID uint64)
}

// AddEdge records that `from` is waiting for `to` to release a lock. No-op
// when no WFG is wired (e.g. in unit tests that don't exercise deadlock
// detection). Safe to call from any goroutine.
func (w *WFGAdapter) AddEdge(from, to uint64) {
	if w.AddEdgeFn != nil {
		w.AddEdgeFn(from, to)
	}
}

// RemoveEdges drops every edge originating from txnID. Called when the
// txn waits no more (granted or aborted) so the deadlock detector does not
// see stale predecessors. No-op when no WFG is wired.
func (w *WFGAdapter) RemoveEdges(txnID uint64) {
	if w.RemoveEdgesFn != nil {
		w.RemoveEdgesFn(txnID)
	}
}

// NewManager constructs a manager with an empty active map, a freshly
// allocated lock table, and a fresh MVCC store. The supplied catalog is
// the source of truth for table/row data; the manager will not modify it
// except through the 2PL write path. ID generation starts at 0 (the first
// Begin returns ID 1 — 0 is reserved for the seed transaction).
func NewManager(catalog *storage.Catalog) *Manager {
	wfg := &WFGAdapter{}
	lockTable := lock.NewTable()
	lockAcq := lock.NewAcquirer(lockTable, wfg)

	m := &Manager{
		txns:       make(map[ID]*Transaction),
		committed:  make(map[ID]struct{}),
		aborted:    make(map[ID]struct{}),
		LockTable:  lockTable,
		LockAcq:    lockAcq,
		Catalog:    catalog,
		WFG:        wfg,
		MVCCStore:  mvcc.NewStore(),
		SSITracker: isolation.NewSSITracker(),
	}
	// Start from ID 1 (0 is invalid)
	m.nextID.Store(0)
	return m
}

func (m *Manager) nextTxnID() ID {
	return ID(m.nextID.Add(1))
}

// Begin starts a new transaction, assigns it the next monotonic ID, and
// (for MVCC + non-ReadCommitted) takes a snapshot of the active set so
// RR / Serializable reads see a consistent view. The lockTimeout defaults
// to 5s when zero; the returned txn is active and ready for read/write.
// If MaxActive is non-zero and the active count has reached that limit,
// Begin returns ErrTooManyTransactions without allocating an ID.
func (m *Manager) Begin(protocol ConcurrencyProtocol, isoLevel IsolationLevel, lockTimeout time.Duration) (*Transaction, error) {
	if lockTimeout == 0 {
		lockTimeout = 5 * time.Second
	}

	m.mu.Lock()
	if m.MaxActive > 0 && len(m.txns) >= m.MaxActive {
		m.mu.Unlock()
		return nil, ErrTooManyTransactions
	}
	id := m.nextTxnID()
	txn := NewTransaction(id, protocol, isoLevel, lockTimeout)
	// Take MVCC snapshot if needed
	if protocol == ProtocolMVCC && isoLevel != ReadCommitted {
		txn.Snapshot = m.takeSnapshot()
	}
	m.txns[id] = txn
	m.mu.Unlock()

	if m.WAL != nil {
		if lsn, err := m.WAL.Begin(uint64(txn.ID)); err == nil {
			txn.LastWALLSN = lsn
		}
	}

	if m.OnBegin != nil {
		m.OnBegin(txn)
	}

	return txn, nil
}

// TakeSnapshot takes an MVCC snapshot of the current active transaction
// set. Used by external callers (e.g. the /api/snapshot endpoint) that
// need a snapshot without holding any manager lock. MVCC reads inside
// the manager use the unexported takeSnapshot helper that requires the
// caller to already hold m.mu to avoid deadlocks.
func (m *Manager) TakeSnapshot() *Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.takeSnapshot()
}

func (m *Manager) takeSnapshot() *Snapshot {
	snap := &Snapshot{
		Xmax:    ID(m.nextID.Load() + 1),
		TakenAt: time.Now(),
	}
	var active []ID
	var xmin ID
	first := true
	for id := range m.txns {
		active = append(active, id)
		if first || id < xmin {
			xmin = id
			first = false
		}
	}
	if first {
		// No active transactions
		snap.Xmin = snap.Xmax
	} else {
		snap.Xmin = xmin
	}
	snap.Active = active
	return snap
}

// Commit marks the transaction as durable, releases its 2PL locks, and
// cleans up SSI tracking. For Serializable MVCC, an SSI commit-time check
// runs first and may convert the call into an abort with
// ErrSerializationFailure. Idempotent on terminal states: a repeat Commit
// of a committed txn returns nil; of an aborted txn returns ErrTxnAborted.
func (m *Manager) Commit(txnID ID) error {
	m.mu.Lock()
	txn, ok := m.txns[txnID]
	if !ok {
		// C-05: the txn is no longer active. Distinguish terminal states so we
		// do not fail open. Previously Commit returned TxnNotFound for any
		// absent txn, which is correct for an unknown id, but a client whose
		// txn was aborted (deadlock detector, SSI, lock timeout) and who then
		// called Commit would get "not found" — better to surface the actual
		// terminal state so the caller knows its work was discarded. A
		// committed txn returns nil (benign idempotent). The committed/aborted
		// maps MUST be read under m.mu — Reset mutates them under m.mu.
		_, aborted := m.aborted[txnID]
		_, committed := m.committed[txnID]
		m.mu.Unlock()
		if aborted {
			return types.NewTxnAbortedError()
		}
		if committed {
			return nil
		}
		return types.NewTxnNotFoundError(uint64(txnID))
	}
	if txn.GetStatus() != TxnActive {
		m.mu.Unlock()
		// Defensive: txn is in the active map but has a terminal status
		// (should not happen in normal flow since terminal-state transitions
		// delete from m.txns under m.mu). Treat as the terminal state.
		if txn.GetStatus() == TxnAborted {
			return types.NewTxnAbortedError()
		}
		return nil
	}

	// SSI check for Serializable MVCC transactions (before marking committed)
	if txn.Protocol == ProtocolMVCC && txn.Isolation == Serializable && m.SSITracker != nil {
		if err := m.SSITracker.CheckCommit(txn); err != nil {
			// Abort the transaction and return serialization failure
			txn.setStatus(TxnAborted)
			delete(m.txns, txnID)
			m.aborted[txnID] = struct{}{}
			m.mu.Unlock()
			// H-13: close AbortCh so any goroutine waiting on it (e.g. a
			// concurrent AcquireLockCtx on a 2PL/SERIALIZABLE hybrid, or any
			// future caller) is unblocked. Previously this branch never called
			// closeAbort, leaving waiters stuck until lockTimeout.
			txn.closeAbort()
			m.SSITracker.Cleanup(uint64(txnID))
			m.applyMVCCUndo(txn)
			if m.OnAbort != nil {
				m.OnAbort(txn, types.NewSerializationFailure())
			}
			return types.NewSerializationFailure()
		}
	}

	// WAL: write COMMIT record and flush before releasing locks or acknowledging
	// success. If the flush fails we return an error and leave the txn active
	// so the caller can retry or abort.
	if m.WAL != nil {
		if lsn, walErr := m.WAL.Commit(uint64(txnID), txn.LastWALLSN); walErr == nil {
			if flushErr := m.WAL.Flush(lsn); flushErr != nil {
				m.mu.Unlock()
				return flushErr
			}
		}
	}

	txn.setStatus(TxnCommitted)
	delete(m.txns, txnID)
	m.committed[txnID] = struct{}{}
	m.mu.Unlock()

	// Cleanup SSI tracker
	if txn.Protocol == ProtocolMVCC && txn.Isolation == Serializable && m.SSITracker != nil {
		m.SSITracker.Cleanup(uint64(txnID))
	}

	// Release 2PL locks
	if txn.Protocol == Protocol2PL {
		m.LockAcq.ReleaseAllLocks(uint64(txn.ID), txn.LocksHeld)
	}

	if m.OnCommit != nil {
		m.OnCommit(txn)
	}
	return nil
}

// Abort rolls back the transaction: applies the undo log (2PL) or
// rewinds version-chain changes (MVCC), releases locks, closes the
// abort channel so any waiters unblock, and fires the OnAbort callback.
// Idempotent on active/aborted/unknown txns; returns ErrTxnCommitted
// if the txn was already committed (fail-closed at the API boundary).
func (m *Manager) Abort(txnID ID, reason error) error {
	m.mu.Lock()
	txn, ok := m.txns[txnID]
	if !ok {
		// C-06: previously Abort returned nil for any absent txn, including
		// ones that had already committed — a client that aborted a committed
		// txn received nil and could believe the rollback succeeded while the
		// commit's writes were durable (fail-open). Distinguish: committed →
		// error; aborted/unknown → nil (idempotent). Read under m.mu.
		_, committed := m.committed[txnID]
		m.mu.Unlock()
		if committed {
			return types.NewTxnCommittedError(uint64(txnID))
		}
		return nil
	}
	if txn.GetStatus() != TxnActive {
		m.mu.Unlock()
		// Defensive (see Commit for rationale).
		if txn.GetStatus() == TxnCommitted {
			return types.NewTxnCommittedError(uint64(txnID))
		}
		return nil
	}
	txn.setStatus(TxnAborted)
	delete(m.txns, txnID)
	m.aborted[txnID] = struct{}{}
	m.mu.Unlock()

	// Signal goroutines waiting on this txn. Idempotent: safe to call
	// repeatedly from any goroutine (abort path, lock-acquirer timeout,
	// deadlock detector). sync.Once guards the close.
	txn.closeAbort()

	// 2PL: apply undo log, release locks
	if txn.Protocol == Protocol2PL {
		m.applyUndoLog(txn)
		m.LockAcq.ReleaseAllLocks(uint64(txn.ID), txn.LocksHeld)
	}

	// MVCC: undo version chain changes
	if txn.Protocol == ProtocolMVCC {
		m.applyMVCCUndo(txn)
	}

	// WAL: write ABORT record after undo is complete
	if m.WAL != nil {
		_, _ = m.WAL.Abort(uint64(txnID), txn.LastWALLSN)
	}

	// Cleanup SSI tracker
	if txn.Protocol == ProtocolMVCC && txn.Isolation == Serializable && m.SSITracker != nil {
		m.SSITracker.Cleanup(uint64(txnID))
	}

	if m.OnAbort != nil {
		m.OnAbort(txn, reason)
	}
	return nil
}

// applyUndoEntries restores before-images for the supplied undo entries,
// applied in reverse order. Used by applyUndoLog (full abort) and
// RollbackToSavepoint (partial rollback) to avoid duplicating the switch.
func (m *Manager) applyUndoEntries(entries []UndoEntry) {
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		table, ok := m.Catalog.Lookup(e.Table)
		if !ok {
			continue
		}
		switch e.Op {
		case UndoInsert:
			table.DeleteRow(e.Key)
		case UndoUpdate, UndoDelete:
			if e.BeforeImage != nil {
				table.PutRow(e.Key, e.BeforeImage)
			}
		}
	}
}

// applyUndoLog restores all before-images in the catalog for a 2PL abort.
func (m *Manager) applyUndoLog(txn *Transaction) {
	m.applyUndoEntries(txn.UndoLog.All())
}

// CreateSavepoint saves a named savepoint
func (m *Manager) CreateSavepoint(txnID ID, name string) error {
	m.mu.RLock()
	txn, ok := m.txns[txnID]
	m.mu.RUnlock()
	if !ok {
		return types.NewTxnNotFoundError(uint64(txnID))
	}
	// H-12: guard Savepoints with txn.mu. Previously these methods mutated the
	// slice under only m.mu.RLock (which protects the manager map, not the
	// transaction's fields), racing concurrent calls on the same txn ID.
	pos := txn.UndoLog.Len()
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.Savepoints == nil {
		txn.Savepoints = make([]*Savepoint, 0)
	}
	txn.Savepoints = append(txn.Savepoints, &Savepoint{Name: name, UndoPosition: pos})
	return nil
}

// RollbackToSavepoint applies undo log back to the named savepoint
func (m *Manager) RollbackToSavepoint(txnID ID, name string) error {
	m.mu.RLock()
	txn, ok := m.txns[txnID]
	m.mu.RUnlock()
	if !ok {
		return types.NewTxnNotFoundError(uint64(txnID))
	}

	// H-12: take txn.mu for the Savepoints scan/mutation.
	txn.mu.Lock()
	defer txn.mu.Unlock()

	var sp *Savepoint
	for i := len(txn.Savepoints) - 1; i >= 0; i-- {
		if txn.Savepoints[i].Name == name {
			sp = txn.Savepoints[i]
			break
		}
	}
	if sp == nil {
		return &types.TxnError{Code: types.ErrSavepointNotFound, Message: fmt.Sprintf("savepoint %q not found", name)}
	}

	// Apply undo entries from current pos back to savepoint. MVCC protocol
	// stores no before-images in the catalog (writes go to version chains),
	// so only 2PL needs the catalog restoration.
	entries := txn.UndoLog.EntriesFrom(sp.UndoPosition)
	if txn.Protocol == Protocol2PL {
		m.applyUndoEntries(entries)
	}
	txn.UndoLog.TruncateTo(sp.UndoPosition)
	return nil
}

// ReleaseSavepoint removes a savepoint
func (m *Manager) ReleaseSavepoint(txnID ID, name string) error {
	m.mu.RLock()
	txn, ok := m.txns[txnID]
	m.mu.RUnlock()
	if !ok {
		return types.NewTxnNotFoundError(uint64(txnID))
	}
	// H-12: guard the slice mutation with txn.mu.
	txn.mu.Lock()
	defer txn.mu.Unlock()
	for i := len(txn.Savepoints) - 1; i >= 0; i-- {
		if txn.Savepoints[i].Name == name {
			txn.Savepoints = append(txn.Savepoints[:i], txn.Savepoints[i+1:]...)
			return nil
		}
	}
	return &types.TxnError{Code: types.ErrSavepointNotFound, Message: fmt.Sprintf("savepoint %q not found", name)}
}

// Reset aborts all active transactions, reseeds the MVCC store from the catalog,
// and clears SSI state. Used by the "Reset Database" API endpoint.
//
// The entire reset runs under m.mu so that concurrently in-flight reads/writes
// on other goroutines see a consistent before/after snapshot rather than a
// half-cleared store. This closes the race that previously existed between
// MVCCStore.Clear() and a concurrent MVCCRead/MVCCWrite dereferencing a
// chain deleted out from under it.
func (m *Manager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Inline abort of every active transaction. We cannot call m.Abort
	// here (it re-acquires m.mu); instead we apply the same teardown steps
	// directly under the lock we already hold.
	for id, txn := range m.txns {
		txn.setStatus(TxnAborted)
		txn.closeAbort()
		if txn.Protocol == Protocol2PL {
			m.applyUndoLog(txn)
			m.LockAcq.ReleaseAllLocks(uint64(txn.ID), txn.LocksHeld)
		}
		if txn.Protocol == ProtocolMVCC {
			m.applyMVCCUndo(txn)
		}
		if txn.Protocol == ProtocolMVCC && txn.Isolation == Serializable && m.SSITracker != nil {
			m.SSITracker.Cleanup(uint64(id))
		}
		m.aborted[id] = struct{}{}
		delete(m.txns, id)
	}

	// Bump opEpoch before Clear so any in-flight MVCCRead/Write/Scan
	// that loaded a pre-Clear chain can detect the store was torn down
	// (CT-28/29/30).  We use the dedicated epochMu to avoid the
	// reentrant-lock deadlock that would arise with m.mu (Reset already
	// holds m.mu.Lock and applyMVCCUndo may snapshot epoch under
	// epochMu.RLock).
	m.epochMu.Lock()
	m.opEpoch++
	m.epochMu.Unlock()

	// Clear MVCC chains and SSI state.
	m.MVCCStore.Clear()
	m.SSITracker.Reset()

	// Reseed catalog tables back to original data.
	storage.SeedCatalog(m.Catalog)

	// Reseed MVCC from catalog.
	seedID := ID(m.nextID.Add(1))
	for _, name := range m.Catalog.List() {
		tbl, ok := m.Catalog.Lookup(name)
		if !ok {
			continue
		}
		rows := tbl.Scan(nil)
		mvcc.SeedMVCCStore(m.MVCCStore, name, rows, mvcc.TxnID(seedID))
	}
	m.committed[seedID] = struct{}{}
}

// MaxHistoryRetained caps the size of the committed/aborted history maps.
// The vacuum calls PruneHistory on every tick; once a map exceeds this cap,
// the oldest entries are removed regardless of the oldest-active horizon.
//
// CT-02: the history maps previously grew for the lifetime of the process
// (~28 GB/day at 10k committed txns/sec), so the cap is a hard memory
// bound. The vacuum's IsCommitted(XMax) check still works for entries that
// have NOT been pruned; for pruned entries, pruneVersions falls through and
// the version simply lingers (ReapEmpty removes the chain when empty). The
// cap is large enough that pruning only affects entries whose chains have
// already been reaped.
const MaxHistoryRetained = 1 << 16 // 65 536

// PruneHistory drops committed/aborted entries that are either (a) older
// than the oldest active transaction or (b) above the MaxHistoryRetained
// cap. Returns the number of entries removed.
//
// CT-02: this is now called from the vacuum tick (was previously defined
// but never invoked, so the maps grew without bound). Aborted entries are
// always safe to drop because applyMVCCUndo has already removed the
// version chain (versions with XMin=aborted or XMax=aborted are gone by
// the time the abort completes).
func (m *Manager) PruneHistory() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldest := m.oldestActiveTxnIDLocked()
	if oldest == 0 {
		// No active transactions: prune everything below the next-to-allocate ID.
		oldest = ID(m.nextID.Load())
	}
	removed := 0
	for id := range m.committed {
		if id < oldest {
			delete(m.committed, id)
			removed++
		}
	}
	for id := range m.aborted {
		if id < oldest {
			delete(m.aborted, id)
			removed++
		}
	}
	// Cap: if the committed map exceeds MaxHistoryRetained, drop the oldest
	// entries. We intentionally do NOT cap-prune the aborted map: cap-pruning
	// could remove abort records for IDs above the oldest-active horizon,
	// causing IsAborted to return false for a genuinely aborted txn — and
	// the committedBeforeSnapshot check in IsVisible would treat that txn's
	// rolled-back writes as visible (dirty read). The normal horizon-based
	// pruning above keeps the aborted map bounded in practice.
	if excess := len(m.committed) - MaxHistoryRetained; excess > 0 {
		sorted := make([]ID, 0, len(m.committed))
		for id := range m.committed {
			sorted = append(sorted, id)
		}
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		for i := 0; i < excess; i++ {
			delete(m.committed, sorted[i])
			removed++
		}
	}
	return removed
}

// MarkCommitted marks an external (e.g. seed) transaction ID as committed
// without going through the normal Begin/Commit lifecycle.
func (m *Manager) MarkCommitted(id ID) {
	m.mu.Lock()
	m.committed[id] = struct{}{}
	m.mu.Unlock()
}

// MarkAborted marks an external (e.g. WAL recovery) transaction ID as aborted
// without going through the normal Abort path.
func (m *Manager) MarkAborted(id ID) {
	m.mu.Lock()
	m.aborted[id] = struct{}{}
	m.mu.Unlock()
}

// VacuumChecker wraps Manager to satisfy both the mvcc.Vacuum interface and
// mvcc.TxnStatusChecker. It exists because mvcc.TxnID is a type alias for
// uint64 while ID is a distinct named type; this adapter bridges the two
// without exporting the manager's internal methods to the mvcc package.
type VacuumChecker struct{ M *Manager }

// IsCommitted reports whether id is in the manager's committed history map.
func (c VacuumChecker) IsCommitted(id mvcc.TxnID) bool { return c.M.IsCommitted(ID(id)) }

// IsAborted reports whether id is in the manager's aborted history map.
func (c VacuumChecker) IsAborted(id mvcc.TxnID) bool { return c.M.IsAborted(ID(id)) }

// IsActive reports whether id is a currently-active (uncommitted) transaction.
func (c VacuumChecker) IsActive(id mvcc.TxnID) bool { return c.M.IsActive(ID(id)) }

// TxnStatus returns active and committed flags in a single read-locked call.
func (c VacuumChecker) TxnStatus(id mvcc.TxnID) (active, committed bool) {
	return c.M.TxnStatus(ID(id))
}

// OldestActiveTxnID returns the smallest active transaction ID, or 0 if
// no transactions are active. Part of the mvcc.Vacuum interface; the
// returned value is the prune horizon.
func (c VacuumChecker) OldestActiveTxnID() mvcc.TxnID { return mvcc.TxnID(c.M.OldestActiveTxnID()) }

// PruneHistory delegates to Manager.PruneHistory so the vacuum tick can
// bound the committed/aborted history maps (CT-02).
func (c VacuumChecker) PruneHistory() int { return c.M.PruneHistory() }

// Status query helpers

// IsCommitted reports whether id has previously committed. Safe to call
// from any goroutine. Read under m.mu so the result is consistent with
// concurrent commits/aborts.
func (m *Manager) IsCommitted(id ID) bool {
	m.mu.RLock()
	_, ok := m.committed[id]
	m.mu.RUnlock()
	return ok
}

// IsAborted reports whether id has previously aborted. Note that older
// aborted IDs may have been pruned by Manager.PruneHistory, in which
// case this returns false for a previously-aborted txn.
func (m *Manager) IsAborted(id ID) bool {
	m.mu.RLock()
	_, ok := m.aborted[id]
	m.mu.RUnlock()
	return ok
}

// IsActive reports whether id is currently in the active set. Once a
// txn transitions to a terminal state it is removed from m.txns, so
// this returns false for committed/aborted txn IDs.
func (m *Manager) IsActive(id ID) bool {
	m.mu.RLock()
	_, ok := m.txns[id]
	m.mu.RUnlock()
	return ok
}

// TxnStatus returns the active and committed status of id under a single lock,
// preventing a race between separate IsActive/IsCommitted calls (e.g. a
// concurrent PruneHistory evicting a committed record between them — a TOCTOU
// that would cause IsCommitted to return false for a txn that DID commit,
// yielding a missed write-write conflict / lost update).
func (m *Manager) TxnStatus(id ID) (active, committed bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, active = m.txns[id]
	_, committed = m.committed[id]
	return
}

// GetTxn returns the active transaction for id, if present. Note that
// terminal-state txns are NOT returned (they are removed from the active
// map at commit/abort time); use IsCommitted/IsAborted for those.
func (m *Manager) GetTxn(id ID) (*Transaction, bool) {
	m.mu.RLock()
	txn, ok := m.txns[id]
	m.mu.RUnlock()
	return txn, ok
}

// WithRequestCtx decorates a function so that the transaction's lock
// acquisitions are tied to the inbound HTTP request context (H-08).
// Handlers call this around read/write/scan/insert/delete so that a
// client disconnect unblocks any contended AcquireLock call and aborts
// the in-flight operation.
func (m *Manager) WithRequestCtx(ctx context.Context, id ID, fn func()) {
	if txn, ok := m.GetTxn(id); ok {
		txn.setRequestCtx(ctx)
		defer txn.setRequestCtx(context.Background())
	}
	fn()
}

// ActiveTxns returns a snapshot of every currently-active transaction.
// The returned slice is a copy; callers may iterate it without holding
// any manager lock. Used by the /api/txn/list endpoint and the deadlock
// detector.
func (m *Manager) ActiveTxns() []*Transaction {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Transaction, 0, len(m.txns))
	for _, txn := range m.txns {
		result = append(result, txn)
	}
	return result
}

// ActiveCount returns the number of currently-active transactions. Used by
// the API layer to enforce MaxActiveTxns (H-16) without iterating the whole
// slice.
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.txns)
}

// OldestActiveTxnID returns the smallest active transaction ID, or 0 if no
// transactions are active. Used by the vacuum loop to compute a horizon below
// which committed/aborted entries can be pruned.
func (m *Manager) OldestActiveTxnID() ID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.oldestActiveTxnIDLocked()
}

// oldestActiveTxnIDLocked is the lock-free inner helper. Caller MUST hold m.mu
// (either read or write).
func (m *Manager) oldestActiveTxnIDLocked() ID {
	var oldest ID
	first := true
	for id := range m.txns {
		if first || id < oldest {
			oldest = id
			first = false
		}
	}
	return oldest
}

// TwoPLRead reads a row under the 2PL protocol. For non-ReadUncommitted
// isolation, it acquires an IS lock on the table and an S lock on the row
// before reading from the catalog. ReadUncommitted skips locking entirely
// (and accepts dirty / non-repeatable reads).
func (m *Manager) TwoPLRead(txn *Transaction, table, key string) ([]types.Value, bool, error) {
	if txn.Isolation != ReadUncommitted {
		res := lock.ResourceForRow(table, key)
		// Also need IS lock on table
		tableRes := lock.ResourceForTable(table)
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockIS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			m.fireLockTimeout(txn, tableRes, err)
			return nil, false, err
		}
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), res, lock.LockS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			m.fireLockTimeout(txn, res, err)
			return nil, false, err
		}
	}
	t, ok := m.Catalog.Lookup(table)
	if !ok {
		return nil, false, types.NewTableNotFoundError(table)
	}
	v, ok := t.GetRow(storage.RowKey(key))
	return v, ok, nil
}

// applyMVCCUndo undoes MVCC version chain changes for an aborted transaction.
// For every written key, apply BOTH RemoveByXMin AND ClearXMax:
//   - RemoveByXMin removes any version this txn created (XMin == txnID).
//   - ClearXMax restores any version this txn marked deleted (XMax == txnID → reset to 0).
//
// This correctly handles all three op types (Insert/Update/Delete).
func (m *Manager) applyMVCCUndo(txn *Transaction) {
	if m.MVCCStore == nil {
		return
	}
	// CT-30: snapshot opEpoch before walking the undo log. If Reset
	// fires concurrently, the chain we load may be orphaned; bail
	// cleanly rather than appending to a chain about to be removed.
	m.epochMu.RLock()
	epoch := m.opEpoch
	m.epochMu.RUnlock()
	// Walk undo log to find all (table, key) pairs written by this transaction.
	// The undo log preserves table information that WriteSet (map[RowKey]) does not.
	entries := txn.UndoLog.All()
	type tableKey struct{ table, key string }
	seen := make(map[tableKey]bool)
	for _, e := range entries {
		tk := tableKey{e.Table, string(e.Key)}
		if seen[tk] {
			continue
		}
		seen[tk] = true
		chain, ok := m.MVCCStore.GetChain(e.Table, string(e.Key))
		if !ok {
			continue
		}
		// CT-30: verify the opEpoch did not advance before mutating the
		// chain. A concurrent Reset would have orphaned this chain; we
		// must not write to it.
		m.epochMu.RLock()
		resetHappened := m.opEpoch != epoch
		m.epochMu.RUnlock()
		if resetHappened {
			return
		}
		// Remove versions this txn created, then clear any XMax it set.
		chain.RemoveByXMin(mvcc.TxnID(txn.ID))
		chain.ClearXMax(mvcc.TxnID(txn.ID))
	}
}

// getSnapshot returns the effective snapshot for the transaction.
// For ReadCommitted, returns a fresh snapshot per call (per-statement).
// For RepeatableRead and Serializable, returns the transaction snapshot (set at Begin).
//
// Snapshot pointer writes are guarded by txn.mu so concurrent readers on
// the same transaction do not race on the CurrentStatementSnapshot / lazy
// Snapshot initialization fields.
func (m *Manager) getSnapshot(txn *Transaction) *Snapshot {
	if txn.Isolation == ReadCommitted {
		m.mu.RLock()
		snap := m.takeSnapshot()
		m.mu.RUnlock()
		txn.setCurrentStatementSnapshot(snap)
		return snap
	}
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.Snapshot == nil {
		m.mu.RLock()
		txn.Snapshot = m.takeSnapshot()
		m.mu.RUnlock()
	}
	return txn.Snapshot
}

// MVCCRead reads the row that is visible to the transaction's snapshot.
// The MVCC chain is walked from newest to oldest and the first version
// passing mvcc.IsVisible is returned. On Serializable, the read is also
// recorded with the SSI tracker for dangerous-structure detection at
// commit time.
func (m *Manager) MVCCRead(txn *Transaction, table, key string) ([]types.Value, bool, error) {
	// CT-28: snapshot the opEpoch under epochMu before any store
	// access. If Reset fires concurrently and bumps opEpoch, the
	// post-snapshot check below detects the change and aborts the
	// read rather than reading from a now-orphaned chain.
	m.epochMu.RLock()
	epoch := m.opEpoch
	m.epochMu.RUnlock()

	// H-10: a Reset aborts every active txn (sets status aborted under m.mu)
	// before clearing/reseeding the store. If our txn was aborted by a
	// concurrent Reset, fail fast rather than read against a half-cleared
	// store (stale/orphaned chain).
	if !txn.IsActive() {
		return nil, false, types.NewTxnAbortedError()
	}
	snap := m.getSnapshot(txn)

	chain, ok := m.MVCCStore.GetChain(table, key)
	if !ok {
		return nil, false, nil
	}

	// CT-28: verify the opEpoch did not advance. If it did, the store
	// was torn down and reseeded between our snapshot and the chain
	// access; abort cleanly rather than read pre-reset state.
	m.epochMu.RLock()
	resetHappened := m.opEpoch != epoch
	m.epochMu.RUnlock()
	if resetHappened {
		return nil, false, types.NewTxnAbortedError()
	}

	checker := VacuumChecker{M: m}
	v := chain.FindVisible(func(ver *mvcc.Version) bool {
		return mvcc.IsVisible(ver, uint64(txn.ID), snap, checker)
	})

	if v == nil {
		return nil, false, nil
	}

	// Record read for SSI
	if txn.Isolation == Serializable && m.SSITracker != nil {
		fullKey := table + ":" + key
		m.SSITracker.RecordRead(txn, fullKey)
		txn.addSIRead(storage.RowKey(fullKey))
	}

	txn.addRead(storage.RowKey(key))

	return v.Data, true, nil
}

// MVCCWrite performs an Insert/Update/Delete on the version chain for
// (table, key). Before mutating the chain, it scans the entire chain for
// a write-write conflict against any committed txn active at our snapshot
// (C-07: previously only the head was checked, missing concurrent
// committed writes buried under an aborted version). On conflict a
// TxnError with ErrWriteConflict is returned without mutating the chain.
func (m *Manager) MVCCWrite(txn *Transaction, table, key string, values []types.Value, op UndoOpType) error {
	// CT-28: snapshot opEpoch before any store access. If Reset
	// fires concurrently, the chain we load will be orphaned.
	m.epochMu.RLock()
	epoch := m.opEpoch
	m.epochMu.RUnlock()
	if !txn.IsActive() {
		return types.NewTxnAbortedError()
	}
	snap := m.getSnapshot(txn)
	// CT-19/CT-26: GetOrCreateChain returns the chain but does NOT
	// guarantee it is live — ReapEmpty may tombstone + delete the
	// entry from the sync.Map AFTER GetOrCreateChain returns and
	// BEFORE we acquire chain.Lock(). Re-check under our own lock.
	chain := m.MVCCStore.GetOrCreateChain(table, key)

	const maxRetry = 4
	for attempt := 0; attempt < maxRetry; attempt++ {
		chain.Lock()
		if !chain.IsTombstonedLocked() {
			break // live chain; proceed
		}
		chain.Unlock()
		// Tombstoned: another goroutine is about to delete the entry.
		// Force-evict and retry with a fresh chain.
		m.MVCCStore.EvictChain(table, key)
		chain = m.MVCCStore.GetOrCreateChain(table, key)
	}
	defer chain.Unlock()

	// CT-28: verify the opEpoch did not advance after we acquired
	// the chain lock. A concurrent Reset would have orphaned the
	// chain (or it was deleted after our retry). Abort cleanly.
	m.epochMu.RLock()
	resetHappened := m.opEpoch != epoch
	m.epochMu.RUnlock()
	if resetHappened {
		return types.NewTxnAbortedError()
	}

	// Check write-write conflict against the ENTIRE chain, not just the head
	// (C-07). The prior inline check inspected only chain.UnsafeHead(); if the
	// head was an aborted transaction's not-yet-removed version, the head
	// check reported no conflict while a deeper version written by a
	// concurrent *committed* transaction (active at our snapshot) was missed
	// — a lost update. Delegate to mvcc.CheckWriteConflictNoLock, which walks
	// the whole chain under our held write lock.
	if err := mvcc.CheckWriteConflictNoLock(mvcc.TxnID(txn.ID), chain, snap, VacuumChecker{M: m}); err != nil {
		var wcerr *mvcc.WriteConflictError
		if errors.As(err, &wcerr) {
			return types.NewWriteConflictError(uint64(wcerr.ConflictingTxn))
		}
		return err
	}

	switch op {
	case UndoInsert:
		newVer := &mvcc.Version{
			XMin:      uint64(txn.ID),
			XMax:      0,
			Data:      values,
			CreatedAt: time.Now(),
		}
		if m.WAL != nil {
			var afterBytes []byte
			if values != nil {
				afterBytes = types.EncodeValues(values)
			}
			if lsn, walErr := m.WAL.Write(uint64(txn.ID), txn.LastWALLSN, uint8(op), table, key, nil, afterBytes); walErr == nil {
				txn.LastWALLSN = lsn
				newVer.LSN = lsn
			}
		}
		chain.PrependUnsafe(newVer)
		txn.UndoLog.Append(UndoInsert, table, storage.RowKey(key), nil)

	case UndoUpdate:
		// Find the currently visible version to supersede
		checker := VacuumChecker{M: m}
		var visible *mvcc.Version
		for v := chain.UnsafeHead(); v != nil; v = v.Prev {
			if mvcc.IsVisible(v, uint64(txn.ID), snap, checker) {
				visible = v
				break
			}
		}
		if visible == nil {
			return types.NewRowNotFoundError()
		}
		txn.UndoLog.Append(UndoUpdate, table, storage.RowKey(key), visible.Data)
		visible.XMax = uint64(txn.ID)
		// Prepend the new version at the chain head. PrependUnsafe sets
		// newVer.Prev = current head and re-heads the chain. This preserves
		// ALL existing versions (including any un-pruned aborted-txn
		// versions sitting between the previous head and `visible`),
		// whereas the prior code did newVer.Prev = visible +
		// chain.SetHead(newVer), which orphaned everything newer than
		// visible from the reachable list and from RemoveByXMin cleanup.
		newVer := &mvcc.Version{
			XMin:      uint64(txn.ID),
			XMax:      0,
			Data:      values,
			CreatedAt: time.Now(),
		}
		if m.WAL != nil {
			var beforeBytes, afterBytes []byte
			beforeBytes = types.EncodeValues(visible.Data)
			if values != nil {
				afterBytes = types.EncodeValues(values)
			}
			if lsn, walErr := m.WAL.Write(uint64(txn.ID), txn.LastWALLSN, uint8(op), table, key, beforeBytes, afterBytes); walErr == nil {
				txn.LastWALLSN = lsn
				newVer.LSN = lsn
			}
		}
		chain.PrependUnsafe(newVer)

	case UndoDelete:
		checker2 := VacuumChecker{M: m}
		var visible *mvcc.Version
		for v := chain.UnsafeHead(); v != nil; v = v.Prev {
			if mvcc.IsVisible(v, uint64(txn.ID), snap, checker2) {
				visible = v
				break
			}
		}
		if visible == nil {
			return types.NewRowNotFoundError()
		}
		txn.UndoLog.Append(UndoDelete, table, storage.RowKey(key), visible.Data)
		if m.WAL != nil {
			beforeBytes := types.EncodeValues(visible.Data)
			if lsn, walErr := m.WAL.Write(uint64(txn.ID), txn.LastWALLSN, uint8(op), table, key, beforeBytes, nil); walErr == nil {
				txn.LastWALLSN = lsn
			}
		}
		visible.XMax = uint64(txn.ID)
	}

	// Record write for SSI
	if txn.Isolation == Serializable && m.SSITracker != nil {
		fullKey := table + ":" + key
		m.SSITracker.RecordWrite(txn, fullKey)
	}

	txn.addWrite(storage.RowKey(key))
	return nil
}

// MVCCScan iterates every chain in the named table and returns the rows
// visible to the transaction's snapshot, optionally filtered by the
// supplied predicate. Like MVCCRead, Serializable-isolation scans are
// recorded with the SSI tracker so the commit-time validator can detect
// phantoms and write skew.
func (m *Manager) MVCCScan(txn *Transaction, table string, filter func(storage.Row) bool) ([]storage.Row, error) {
	// H-10: see MVCCRead.
	if !txn.IsActive() {
		return nil, types.NewTxnAbortedError()
	}
	// CT-28: snapshot opEpoch. Reset fires concurrently could orphan
	// the chains we are about to iterate; check before the loop and
	// bail if the store was torn down.
	m.epochMu.RLock()
	epoch := m.opEpoch
	m.epochMu.RUnlock()
	snap := m.getSnapshot(txn)

	var result []storage.Row

	checker := VacuumChecker{M: m}
	ssiEnabled := txn.Isolation == Serializable && m.SSITracker != nil
	ssiPrefix := table + ":" // precomputed once; avoids a per-row alloc in the SSI hot path
	m.MVCCStore.ForEachChainInTable(table, func(key string, chain *mvcc.VersionChain) {
		v := chain.FindVisible(func(ver *mvcc.Version) bool {
			return mvcc.IsVisible(ver, uint64(txn.ID), snap, checker)
		})
		if v == nil {
			return
		}
		row := storage.Row{Key: storage.RowKey(key), Values: v.Data}
		if filter == nil || filter(row) {
			result = append(result, row)
		}
		// Record read for SSI
		if ssiEnabled {
			fullKey := ssiPrefix + key
			m.SSITracker.RecordRead(txn, fullKey)
			txn.addSIRead(storage.RowKey(fullKey))
		}
	})
	// CT-28: if Reset fired during the scan, the result is a mix of
	// pre- and post-reset rows. Discard and abort cleanly.
	m.epochMu.RLock()
	resetHappened := m.opEpoch != epoch
	m.epochMu.RUnlock()
	if resetHappened {
		return nil, types.NewTxnAbortedError()
	}
	return result, nil
}

// fireLockTimeout invokes the OnLockTimeout callback iff the error is a
// lock-timeout-class failure (ErrLockTimeout) and a callback is wired.
// Aborted/disconnected failures are not counted as "timeouts" because
// the user did not wait the full timeout — something else cancelled the
// wait (CT-17).
func (m *Manager) fireLockTimeout(txn *Transaction, res lock.ResourceID, err error) {
	if m.OnLockTimeout == nil {
		return
	}
	var txnErr *types.TxnError
	if !errors.As(err, &txnErr) || txnErr.Code != types.ErrLockTimeout {
		return
	}
	m.OnLockTimeout(txn, res)
}

// TwoPLWrite writes a row under the 2PL protocol. It acquires IX on the
// table and X on the row, records the before-image in the undo log for
// rollback, and applies the change to the catalog. Lock-acquisition
// failures fire the OnLockTimeout callback so the LockTimeouts metric
// can increment (CT-17).
func (m *Manager) TwoPLWrite(txn *Transaction, table, key string, values []types.Value, op UndoOpType) error {
	tableRes := lock.ResourceForTable(table)
	rowRes := lock.ResourceForRow(table, key)

	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockIX, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		m.fireLockTimeout(txn, tableRes, err)
		return err
	}
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), rowRes, lock.LockX, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		m.fireLockTimeout(txn, rowRes, err)
		return err
	}

	t, ok := m.Catalog.Lookup(table)
	if !ok {
		return types.NewTableNotFoundError(table)
	}

	before, _ := t.GetRow(storage.RowKey(key))
	txn.UndoLog.Append(op, table, storage.RowKey(key), before)

	if m.WAL != nil {
		var beforeBytes, afterBytes []byte
		beforeBytes = types.EncodeValues(before)
		if values != nil {
			afterBytes = types.EncodeValues(values)
		}
		if lsn, walErr := m.WAL.Write(uint64(txn.ID), txn.LastWALLSN, uint8(op), table, key, beforeBytes, afterBytes); walErr == nil {
			txn.LastWALLSN = lsn
		}
	}

	switch op {
	case UndoInsert, UndoUpdate:
		t.PutRow(storage.RowKey(key), values)
	case UndoDelete:
		t.DeleteRow(storage.RowKey(key))
	}
	txn.addWrite(storage.RowKey(key))
	return nil
}

// BeginWithID starts a new transaction using the provided id instead of
// allocating a new one. Used by the Raft FSM on followers to apply the
// leader-assigned TxnID deterministically.
func (m *Manager) BeginWithID(id ID, protocol ConcurrencyProtocol, isoLevel IsolationLevel, lockTimeout time.Duration) (*Transaction, error) {
	if lockTimeout == 0 {
		lockTimeout = 5 * time.Second
	}

	m.mu.Lock()
	if m.MaxActive > 0 && len(m.txns) >= m.MaxActive {
		m.mu.Unlock()
		return nil, ErrTooManyTransactions
	}
	t := NewTransaction(id, protocol, isoLevel, lockTimeout)
	if protocol == ProtocolMVCC && isoLevel != ReadCommitted {
		t.Snapshot = m.takeSnapshot()
	}
	m.txns[id] = t
	m.mu.Unlock()

	if m.WAL != nil {
		if lsn, err := m.WAL.Begin(uint64(id)); err == nil {
			t.LastWALLSN = lsn
		}
	}

	if m.OnBegin != nil {
		m.OnBegin(t)
	}
	return t, nil
}

// GetActiveTxn returns the active transaction for id, or nil if not found.
// Terminal-state transactions (committed/aborted) are not returned.
func (m *Manager) GetActiveTxn(id ID) *Transaction {
	m.mu.RLock()
	t := m.txns[id]
	m.mu.RUnlock()
	return t
}
