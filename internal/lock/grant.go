// Package lock implements a multi-granularity lock manager with FIFO
// queues, 2PL acquisition, upgrade support, and wait-for graph wiring.
package lock

import (
	"context"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/types"
)

// AcquireResult is the outcome of a synchronous lock acquire call before
// the wait-channel wiring was introduced. Retained for backwards
// compatibility with existing tests that still assert the constants.
type AcquireResult int

const (
	// AcquireGranted means the lock was immediately available and granted.
	AcquireGranted AcquireResult = iota
	// AcquireWaiting means the lock was not available; the caller must
	// observe the request's GrantedCh. The current code path (AcquireLockCtx)
	// blocks internally rather than returning this value.
	AcquireWaiting
	// AcquireConflict means the request was incompatible with the
	// current holders and the caller should retry/abort. Same caveat
	// as AcquireWaiting — retained for backwards compatibility.
	AcquireConflict
)

// Acquirer handles lock acquisition with the full 2PL protocol:
// upgrade detection, FIFO wait queues, context cancellation, and
// wait-for-graph edge wiring. It is separate from Table and
// Queue so the queue can stay a dumb data structure.
type Acquirer struct {
	table *Table
	wfg   WFGInterface
}

// WFGInterface abstracts the wait-for graph so the lock package does not
// import the deadlock package directly (which would cycle: deadlock →
// txn → lock → deadlock).
type WFGInterface interface {
	AddEdge(from, to uint64)
	RemoveEdges(txnID uint64)
}

// NewAcquirer wires a new acquirer to the given lock table and WFG
// adapter. Both are required: the WFG is updated on every wait and on
// every grant/abort, so passing nil would silently break deadlock
// detection.
func NewAcquirer(table *Table, wfg WFGInterface) *Acquirer {
	return &Acquirer{table: table, wfg: wfg}
}

// AcquireLock acquires the given mode on resource for the transaction.
// It blocks until granted, timed out, or the transaction is aborted
// (via abortCh). For H-08 (request context propagation), callers that
// have an inbound HTTP request context should prefer AcquireLockCtx,
// which also unblocks on ctx.Done() (client disconnect) and returns a
// TxnError with Code=ErrTxnAborted so the in-flight operation aborts
// rather than pinning its locks until lockTimeout.
//
// txnLocksHeld is a ref to txn.LocksHeld; txnID is the transaction's ID.
func (a *Acquirer) AcquireLock(
	txnID uint64,
	resource ResourceID,
	mode Mode,
	held *HeldSet,
	lockTimeout time.Duration,
	abortCh <-chan struct{},
) error {
	return a.AcquireLockCtx(context.Background(), txnID, resource, mode, held, lockTimeout, abortCh)
}

// AcquireLockCtx is the context-aware variant of AcquireLock. A client
// disconnect (ctx.Done()) cancels the wait and frees the wait-slot for
// the next acquirer. Callers should pass r.Context() from HTTP handlers.
func (a *Acquirer) AcquireLockCtx(
	ctx context.Context,
	txnID uint64,
	resource ResourceID,
	mode Mode,
	held *HeldSet,
	lockTimeout time.Duration,
	abortCh <-chan struct{},
) error {
	q := a.table.GetOrCreate(resource)
	q.mu.Lock()

	// Check if already holds sufficient or stronger lock
	if existing, ok := held.Get(resource); ok {
		if LockStrength[existing] >= LockStrength[mode] {
			q.mu.Unlock()
			return nil
		}
	}

	// CT-27: detect upgrade (we hold a weaker lock on the same
	// resource). CanGrant skips our own entries, but two bugs lurk:
	//
	//  1. No-conflict path: if no other holder conflicts, CanGrant
	//     returns true and we fall into AddGranted, which APPENDS
	//     the new entry — leaving the chain with both our old S
	//     and our new X. q.granted grows unboundedly for repeated
	//     upgrades.
	//  2. Conflict path: if another txn holds an incompatible lock,
	//     we go to waiting. When that other txn releases,
	//     GrantWaiters recomputes currentMode from ALL of q.granted
	//     — INCLUDING our own leftover S — and S is incompatible
	//     with the requested X, so the upgrade is never granted.
	//     We spin until lockTimeout (self-deadlock).
	//
	// Fix: drop the existing grant for our txn on this resource
	// before attempting to acquire the new mode. The new request
	// then proceeds with a clean q.granted for us on this resource.
	if existing, ok := held.Get(resource); ok && LockStrength[existing] < LockStrength[mode] {
		q.RemoveGranted(txnID)
		held.Delete(resource)
	}

	// Check compatibility
	if q.CanGrant(mode, txnID) {
		req := NewRequest(txnID, mode)
		q.AddGranted(req)
		held.Set(resource, mode)
		q.mu.Unlock()
		return nil
	}

	// Conflict: need to wait
	conflictors := q.ConflictingTxns(mode, txnID)
	req := NewRequest(txnID, mode)
	// H-14: if this transaction already holds a weaker lock on the resource,
	// this is an upgrade. Queue at the FRONT so subsequent compatible
	// acquirers (e.g. new S-lockers compatible with the held lock but not
	// with the requested stronger mode) do not cut ahead and starve the
	// upgrade indefinitely (the prior code always used AddWaiting, leaving
	// AddWaitingFront as dead code).
	if existing, ok := held.Get(resource); ok && LockStrength[existing] < LockStrength[mode] {
		q.AddWaitingFront(req)
	} else {
		q.AddWaiting(req)
	}
	for _, conflictorID := range conflictors {
		a.wfg.AddEdge(txnID, conflictorID)
	}
	q.mu.Unlock()

	// Block until granted, aborted, ctx cancelled, or timed out.
	//
	// C-03: there is a race between GrantedCh being closed (grant won) and
	// abortCh/ctx.Done/timer.C firing. If the select picks a non-grant case
	// even though the request was already moved from `waiting` to `granted`
	// (another goroutine's GrantWaiters closed GrantedCh), RemoveWaiting
	// returns false (the req is no longer in `waiting`) and the granted entry
	// is left dangling — `locksHeld[resource]` is never set, so
	// ReleaseAllLocks never releases it, and the lock is leaked forever,
	// blocking all future acquirers. We therefore also call RemoveGranted
	// (a no-op if the req was never granted) and always cascade GrantWaiters.
	timer := time.NewTimer(lockTimeout)
	defer timer.Stop()
	select {
	case <-req.GrantedCh:
		a.wfg.RemoveEdges(txnID)
		held.Set(resource, mode)
		return nil
	case <-abortCh:
		q.mu.Lock()
		q.RemoveWaiting(txnID)
		q.RemoveGranted(txnID)
		q.GrantWaiters()
		q.mu.Unlock()
		a.wfg.RemoveEdges(txnID)
		return &types.TxnError{Code: types.ErrTxnAborted, Message: "transaction aborted while waiting for lock"}
	case <-ctx.Done():
		q.mu.Lock()
		q.RemoveWaiting(txnID)
		q.RemoveGranted(txnID)
		q.GrantWaiters()
		q.mu.Unlock()
		a.wfg.RemoveEdges(txnID)
		return &types.TxnError{Code: types.ErrTxnAborted, Message: "client disconnected while waiting for lock"}
	case <-timer.C:
		q.mu.Lock()
		q.RemoveWaiting(txnID)
		q.RemoveGranted(txnID)
		q.GrantWaiters()
		q.mu.Unlock()
		a.wfg.RemoveEdges(txnID)
		return &types.TxnError{Code: types.ErrLockTimeout, Message: "lock acquisition timed out"}
	}
}

// ReleaseLock releases the lock on a single resource. After removing
// the grant, calls GrantWaiters so any X-waiters blocked on the holder
// can be reconsidered. Idempotent: releasing a lock the txn does not
// hold is a no-op.
func (a *Acquirer) ReleaseLock(txnID uint64, resource ResourceID, held *HeldSet) {
	q, ok := a.table.Get(resource)
	if !ok {
		return
	}
	q.mu.Lock()
	q.RemoveGranted(txnID)
	q.GrantWaiters()
	q.mu.Unlock()
	held.Delete(resource)
}

// ReleaseAllLocks releases all locks held by the transaction and reaps any
// now-empty lock queues so the lock table's sync.Map does not grow without
// bound (H-03).
func (a *Acquirer) ReleaseAllLocks(txnID uint64, held *HeldSet) {
	// Snapshot resources under the held-set lock, then release via the queues.
	// Releasing takes per-queue mutexes; we must not hold held.mu during that
	// to avoid lock-ordering coupling (C-08: the prior raw-map iteration raced
	// AcquireLockCtx's concurrent writes to the same map).
	resources := held.Keys()
	for _, resource := range resources {
		q, ok := a.table.Get(resource)
		if !ok {
			held.Delete(resource)
			continue
		}
		q.mu.Lock()
		q.RemoveGranted(txnID)
		q.GrantWaiters()
		empty := len(q.granted) == 0 && len(q.waiting) == 0
		q.mu.Unlock()
		if empty {
			a.table.Delete(resource)
		}
		held.Delete(resource)
	}
	a.wfg.RemoveEdges(txnID)
}
