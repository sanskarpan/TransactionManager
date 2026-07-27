// Package txn implements the central transaction manager coordinating the
// two concurrency-control protocols (2PL and MVCC) offered by this service.
//
// A Transaction is the in-memory representation of a single database
// transaction. It is NOT safe to mutate a Transaction's tracking fields
// (ReadSet/WriteSet/SIReadKeys/AbortCh) from outside the package — callers
// must use the accessor methods defined below so that all mutations go
// through the transaction's internal mutex.
package txn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/lock"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/wal"
)

// Status is the lifecycle state of a transaction. Stored atomically so it
// can be read by concurrent observers without holding the transaction mutex.
type Status int32

const (
	// TxnActive is the initial state of a newly-created transaction. The
	// txn remains active until it transitions to TxnCommitted or TxnAborted
	// via Manager.Commit/Abort (or an abort path such as a deadlock).
	TxnActive Status = iota
	// TxnCommitted is a terminal state: the txn's writes are durable and the
	// row locks have been released. Reached only via Manager.Commit.
	TxnCommitted
	// TxnAborted is a terminal state: undo log has been applied and locks
	// released. Reached via Manager.Abort, the deadlock detector, or SSI
	// commit-time validation.
	TxnAborted
)

// ConcurrencyProtocol selects between strict two-phase locking and MVCC.
type ConcurrencyProtocol int

const (
	// Protocol2PL selects strict two-phase locking: rows are locked on first
	// access and held until Commit/Abort. Use for write-heavy workloads or
	// when serializable isolation is required without read-write conflicts.
	Protocol2PL ConcurrencyProtocol = iota
	// ProtocolMVCC selects multi-version concurrency control: writers create
	// new versions and readers see a snapshot, allowing non-blocking reads.
	// Use for read-heavy workloads or analytics-style queries.
	ProtocolMVCC
)

// IsolationLevel selects the isolation strength. Higher values are strictly
// stronger (per the SQL standard hierarchy adapted to this implementation).
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads. Use only for approximate aggregate
	// queries where stale data is acceptable; rows are read without locking.
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted takes a new snapshot per statement, so the same query
	// run twice in one txn can see different committed values. This is the
	// 2PL / MVCC default and offers the conventional "no dirty reads, but
	// non-repeatable reads are possible" semantics.
	ReadCommitted
	// RepeatableRead shares a snapshot across the whole transaction so
	// successive reads of the same row return the same value.
	RepeatableRead
	// Serializable adds SSI conflict tracking on top of MVCC/2PL to prevent
	// write skew and other read-write anomalies. Strictest level; may
	// return ErrSerializationFailure and require the client to retry.
	Serializable
)

// Transaction is the in-memory state of a single transaction.
//
// All fields are written under mu (for tracking maps) or via atomic ops
// (for Status). External code must not touch ReadSet/WriteSet/SIReadKeys
// directly — use the addRead/addWrite/addSIRead accessors.
type Transaction struct {
	ID        ID
	Status    Status
	Protocol  ConcurrencyProtocol
	Isolation IsolationLevel

	// MVCC snapshots. Snapshot is the begin-time snapshot for RR/Serializable;
	// CurrentStatementSnapshot is refreshed per statement under ReadCommitted.
	Snapshot                 *Snapshot
	CurrentStatementSnapshot *Snapshot

	// 2PL locks held by this transaction: resource → strongest mode held.
	// Guarded by the set's internal mutex (C-08): previously a raw map written
	// by AcquireLockCtx with no synchronization while ReleaseAllLocks iterated
	// it concurrently — a data race and runtime panic on map iteration+write.
	LocksHeld *lock.HeldSet

	// Read/Write tracking sets. Guarded by mu.
	ReadSet  map[storage.RowKey]ID
	WriteSet map[storage.RowKey]struct{}

	// Undo log (has its own internal mutex).
	UndoLog    *UndoLog
	Savepoints []*Savepoint

	// SSI conflict flags + SIREAD key set. Guarded by mu.
	InConflict  bool
	OutConflict bool
	SIReadKeys  map[storage.RowKey]struct{}

	// LastWALLSN is the last WAL LSN written for this transaction (for PrevLSN chain).
	LastWALLSN wal.LSN

	// Lifecycle observability.
	BeginAt time.Time

	// Per-transaction lock-acquisition timeout.
	LockTimeout time.Duration

	// Concurrency.
	mu        sync.Mutex
	abortOnce sync.Once
	AbortCh   chan struct{}

	// requestCtx, when non-nil, is the inbound HTTP request context. Set
	// by Manager.WithRequestCtx before invoking read/write/scan so
	// that AcquireLockCtx can cancel on client disconnect (H-08). Not
	// exported; manipulated only by the txn package.
	requestCtx context.Context
}

// setRequestCtx stores the inbound request context for the duration of
// one HTTP operation so lock acquisitions can be cancelled on client
// disconnect. Caller does not need to clear it; the next call replaces.
func (t *Transaction) setRequestCtx(ctx context.Context) {
	t.mu.Lock()
	t.requestCtx = ctx
	t.mu.Unlock()
}

// requestCtxOrBackground returns the stored request context or
// context.Background. Thread-safe.
func (t *Transaction) requestCtxOrBackground() context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.requestCtx == nil {
		return context.Background()
	}
	return t.requestCtx
}

// NewTransaction constructs an active transaction.
func NewTransaction(id ID, protocol ConcurrencyProtocol, isolation IsolationLevel, lockTimeout time.Duration) *Transaction {
	return &Transaction{
		ID:          id,
		Status:      TxnActive,
		Protocol:    protocol,
		Isolation:   isolation,
		LocksHeld:   lock.NewHeldSet(),
		ReadSet:     make(map[storage.RowKey]ID),
		WriteSet:    make(map[storage.RowKey]struct{}),
		UndoLog:     NewUndoLog(),
		SIReadKeys:  make(map[storage.RowKey]struct{}),
		BeginAt:     time.Now(),
		LockTimeout: lockTimeout,
		AbortCh:     make(chan struct{}),
	}
}

// GetStatus returns the transaction's status atomically.
func (t *Transaction) GetStatus() Status {
	return Status(atomic.LoadInt32((*int32)(&t.Status)))
}

func (t *Transaction) setStatus(s Status) {
	atomic.StoreInt32((*int32)(&t.Status), int32(s))
}

// IsActive reports whether the transaction is still running (not yet
// committed or aborted). Safe to call from any goroutine without locking.
func (t *Transaction) IsActive() bool { return t.GetStatus() == TxnActive }

// IsCommitted reports whether the transaction has reached the terminal
// committed state. Returns true only after Manager.Commit has run
// setStatus(TxnCommitted) and recorded the txn in the committed history.
func (t *Transaction) IsCommitted() bool { return t.GetStatus() == TxnCommitted }

// IsAborted reports whether the transaction has reached the terminal
// aborted state — either via Manager.Abort, the deadlock detector,
// or the SSI commit-time validator. Returns false for a txn that is
// still active or has been pruned past the history retention cap.
func (t *Transaction) IsAborted() bool { return t.GetStatus() == TxnAborted }

// closeAbort signals the abort channel exactly once. Safe to call from any
// goroutine — duplicate calls are no-ops. This replaces the racy
// `select { default: close(ch) }` pattern that could double-close under
// concurrent abort paths.
func (t *Transaction) closeAbort() {
	t.abortOnce.Do(func() { close(t.AbortCh) })
}

// addRead records a read of key. Thread-safe.
func (t *Transaction) addRead(key storage.RowKey) {
	t.mu.Lock()
	t.ReadSet[key] = t.ID
	t.mu.Unlock()
}

// addWrite records a write of key. Thread-safe.
func (t *Transaction) addWrite(key storage.RowKey) {
	t.mu.Lock()
	t.WriteSet[key] = struct{}{}
	t.mu.Unlock()
}

// addSIRead records a SIREAD key for Serializable SSI tracking. Thread-safe.
func (t *Transaction) addSIRead(key storage.RowKey) {
	t.mu.Lock()
	t.SIReadKeys[key] = struct{}{}
	t.mu.Unlock()
}

// SSIConflictTarget interface methods (used by isolation.SSITracker).

// SetInConflict marks the transaction as a victim of an incoming rw-antidependency
// (another txn read the data we wrote). Called by the SSI tracker on commit
// validation when the dangerous-structure check fires.
func (t *Transaction) SetInConflict() {
	t.mu.Lock()
	t.InConflict = true
	t.mu.Unlock()
}

// SetOutConflict marks the transaction as having observed an rw-antidependency
// against another txn's write. Called by the SSI tracker when a concurrent
// writer commits over a key we read.
func (t *Transaction) SetOutConflict() {
	t.mu.Lock()
	t.OutConflict = true
	t.mu.Unlock()
}

// GetInConflict reports whether this txn currently has an incoming
// rw-antidependency from another txn. Used by SSI to decide whether to
// abort at commit time.
func (t *Transaction) GetInConflict() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.InConflict
}

// GetOutConflict reports whether this txn currently has an outgoing
// rw-antidependency to another txn's recent write. Used by SSI commit-time
// validation to detect dangerous structures.
func (t *Transaction) GetOutConflict() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.OutConflict
}

// GetID returns the transaction ID as uint64 (for SSIConflictTarget interface).
func (t *Transaction) GetID() uint64 { return uint64(t.ID) }

// setCurrentStatementSnapshot stores the per-statement snapshot for
// ReadCommitted in a thread-safe way. Other isolation levels do not use
// this field after Begin, but the field is still mutated by getSnapshot
// and so must always go through this accessor.
func (t *Transaction) setCurrentStatementSnapshot(s *Snapshot) {
	t.mu.Lock()
	t.CurrentStatementSnapshot = s
	t.mu.Unlock()
}
