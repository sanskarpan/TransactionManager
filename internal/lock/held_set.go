package lock

import "sync"

// HeldSet is a thread-safe map of resource → strongest lock mode held by
// a transaction.
//
// It replaces a raw map[ResourceID]Mode to close the C-08 data race: the
// prior raw map was written by AcquireLockCtx (grant.go, with no synchronization
// on the txn's mutex) while ReleaseAllLocks iterated it from another goroutine
// (e.g. TxnManager.Reset), producing a concurrent map iteration + write that
// the race detector flags and that can panic at runtime
// ("concurrent map iteration and map write").
//
// The set's mutex is never held while a queue mutex is held in the same
// goroutine beyond a single accessor call (Set/Get/Delete return before queue
// ops), so there is no lock-ordering cycle between held.mu and queue.mu.
type HeldSet struct {
	mu sync.Mutex
	m  map[ResourceID]Mode
}

// NewHeldSet constructs an empty held-lock set. One set is owned
// per transaction (Transaction.LocksHeld) and persists for the lifetime
// of the txn; the lock acquirer reads/writes it via Get/Set/Delete.
func NewHeldSet() *HeldSet {
	return &HeldSet{m: make(map[ResourceID]Mode)}
}

// Get returns the strongest mode held for resource, if any.
func (s *HeldSet) Get(r ResourceID) (Mode, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.m[r]
	return m, ok
}

// Set records mode as the strongest mode held for resource (overwrites).
func (s *HeldSet) Set(r ResourceID, m Mode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[r] = m
}

// Delete removes the entry for resource.
func (s *HeldSet) Delete(r ResourceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, r)
}

// Keys returns a snapshot of all resources currently held. Order is
// non-deterministic; callers must not rely on ordering.
func (s *HeldSet) Keys() []ResourceID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ResourceID, 0, len(s.m))
	for r := range s.m {
		out = append(out, r)
	}
	return out
}

// Len returns the number of held resources.
func (s *HeldSet) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
