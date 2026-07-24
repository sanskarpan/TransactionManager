package lock

import (
	"sync"
	"time"
)

// Queue holds the granted and waiting lock requests for a single
// resource. All operations take q.mu; the queue is the only place in
// the lock package that serialises access to a given resource's
// compatibility decisions.
type Queue struct {
	mu      sync.Mutex
	res     ResourceID
	granted []*Request
	waiting []*Request
}

// NewQueue constructs an empty queue for the given resource. The
// lock table calls this when GetOrCreate allocates a new per-resource
// entry.
func NewQueue(res ResourceID) *Queue {
	return &Queue{res: res}
}

// CanGrant returns true if mode is compatible with all currently granted locks
// (except for the transaction with the given txnID - used for upgrades)
func (q *Queue) CanGrant(mode Mode, exceptTxnID uint64) bool {
	for _, g := range q.granted {
		if g.TxnID == exceptTxnID {
			continue
		}
		if !LockCompatible[mode][g.Mode] {
			return false
		}
	}
	return true
}

// AddGranted appends a request to the granted list
func (q *Queue) AddGranted(req *Request) {
	req.Status = StatusGranted
	req.GrantedAt = time.Now()
	q.granted = append(q.granted, req)
}

// AddWaiting appends a request to the wait queue
func (q *Queue) AddWaiting(req *Request) {
	req.Status = StatusWaiting
	q.waiting = append(q.waiting, req)
}

// AddWaitingFront prepends a request to the wait queue (for upgrade priority)
func (q *Queue) AddWaitingFront(req *Request) {
	req.Status = StatusConverting
	q.waiting = append([]*Request{req}, q.waiting...)
}

// RemoveGranted removes ALL granted locks for the given txnID (handles upgrade duplicates).
func (q *Queue) RemoveGranted(txnID uint64) bool {
	var found bool
	kept := q.granted[:0]
	for _, g := range q.granted {
		if g.TxnID == txnID {
			found = true
		} else {
			kept = append(kept, g)
		}
	}
	q.granted = kept
	return found
}

// RemoveWaiting removes the waiting request for the given txnID
func (q *Queue) RemoveWaiting(txnID uint64) bool {
	for i, w := range q.waiting {
		if w.TxnID == txnID {
			q.waiting = append(q.waiting[:i], q.waiting[i+1:]...)
			return true
		}
	}
	return false
}

// GrantWaiters tries to grant waiting requests in FIFO order.
// Stops at first incompatible request to maintain fairness (prevent X starvation).
func (q *Queue) GrantWaiters() {
	currentMode := LockNL
	for _, g := range q.granted {
		currentMode = DominantMode(currentMode, g.Mode)
	}

	var remaining []*Request
	granted := 0
	for i, req := range q.waiting {
		if !LockCompatible[req.Mode][currentMode] {
			// Stop: preserve FIFO fairness to prevent starvation of X-lock
			// waiters. Keep this waiter and everything after it. Use the loop
			// index (NOT len(remaining), which the prior code did) so already-
			// granted waiters are not re-appended to `remaining` — re-adding
			// them would cause a double close(req.GrantedCh) on the next
			// GrantWaiters call (C-01).
			remaining = q.waiting[i:]
			break
		}
		req.Status = StatusGranted
		req.GrantedAt = time.Now()
		q.granted = append(q.granted, req)
		currentMode = DominantMode(currentMode, req.Mode)
		close(req.GrantedCh)
		granted++
	}
	if granted == len(q.waiting) {
		// All waiters were granted; nothing remains.
		q.waiting = nil
	} else {
		q.waiting = remaining
	}
}

// ConflictingTxns returns the TxnIDs of transactions whose grants conflict with mode
func (q *Queue) ConflictingTxns(mode Mode, requesterID uint64) []uint64 {
	var result []uint64
	seen := make(map[uint64]bool)
	for _, g := range q.granted {
		if g.TxnID == requesterID {
			continue
		}
		if !LockCompatible[mode][g.Mode] && !seen[g.TxnID] {
			result = append(result, g.TxnID)
			seen[g.TxnID] = true
		}
	}
	return result
}

// QueueSnapshot is a read-only copy of a Queue's state, returned
// by Queue.Snapshot for the /api/lock/queue endpoint. Decoupling the
// JSON shape from the live queue lets callers iterate without holding q.mu.
type QueueSnapshot struct {
	Resource ResourceID
	Granted  []RequestSnapshot
	Waiting  []RequestSnapshot
}

// RequestSnapshot is the read-only view of a single lock request,
// returned as part of QueueSnapshot. Keeping it value-typed lets
// callers store/format it without copying pointers to live requests.
type RequestSnapshot struct {
	TxnID     uint64
	Mode      Mode
	Status    Status
	RequestAt time.Time
	GrantedAt time.Time
}

// Snapshot returns a value-typed copy of the queue's granted and waiting
// lists. Used by the /api/lock/queue endpoint so a caller can inspect
// the queue without holding q.mu during JSON encoding.
func (q *Queue) Snapshot() QueueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	snap := QueueSnapshot{Resource: q.res}
	for _, g := range q.granted {
		snap.Granted = append(snap.Granted, RequestSnapshot{
			TxnID: g.TxnID, Mode: g.Mode, Status: g.Status,
			RequestAt: g.RequestAt, GrantedAt: g.GrantedAt,
		})
	}
	for _, w := range q.waiting {
		snap.Waiting = append(snap.Waiting, RequestSnapshot{
			TxnID: w.TxnID, Mode: w.Mode, Status: w.Status,
			RequestAt: w.RequestAt,
		})
	}
	return snap
}
