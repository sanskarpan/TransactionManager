// Package mvcc implements multi-version concurrency control with version
// chains, write-conflict detection, visibility rules, and vacuum.
package mvcc

import (
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/storage"
)

// VersionChain is a linked list of versions for a single row key, newest first.
type VersionChain struct {
	mu   sync.RWMutex
	head *Version
	key  storage.RowKey
	// tombstoned marks this chain for deletion by ReapEmpty (CT-19).
	// A concurrent MVCCWrite that observes tombstoned=true under the
	// chain's write lock creates a fresh chain via GetOrCreateChain
	// rather than appending to this one. Without this, a writer that
	// loaded the chain before ReapEmpty's Delete would silently lose
	// its write (the chain was removed from the sync.Map).
	tombstoned bool
}

// NewVersionChain constructs an empty chain for the given row key. The
// caller is expected to immediately Prepend a seed version (e.g. via
// SeedMVCCStore) before any concurrent reader sees the chain.
func NewVersionChain(key storage.RowKey) *VersionChain {
	return &VersionChain{key: key}
}

// Prepend inserts a new version at the head (newest).
func (c *VersionChain) Prepend(v *Version) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v.Prev = c.head
	c.head = v
}

// Head returns the newest version (may be nil).
func (c *VersionChain) Head() *Version {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.head
}

// All returns all versions from newest to oldest.
func (c *VersionChain) All() []*Version {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result []*Version
	for v := c.head; v != nil; v = v.Prev {
		result = append(result, v)
	}
	return result
}

// SetXMax sets XMax on the version with XMin == xmin.
func (c *VersionChain) SetXMax(xmin, xmax TxnID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for v := c.head; v != nil; v = v.Prev {
		if v.XMin == xmin && v.XMax == 0 {
			v.XMax = xmax
			return true
		}
	}
	return false
}

// ClearXMax resets XMax to 0 for all versions where XMax == txnID (abort undo).
func (c *VersionChain) ClearXMax(txnID TxnID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for v := c.head; v != nil; v = v.Prev {
		if v.XMax == txnID {
			v.XMax = 0
		}
	}
}

// RemoveByXMin removes all versions where XMin == txnID (abort undo).
func (c *VersionChain) RemoveByXMin(txnID TxnID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Rebuild the linked list without matching versions
	var dummy Version
	tail := &dummy
	for v := c.head; v != nil; v = v.Prev {
		if v.XMin != txnID {
			tail.Prev = v
			tail = v
		}
	}
	tail.Prev = nil
	c.head = dummy.Prev
}

// Prune removes versions where pred returns true.
func (c *VersionChain) Prune(pred func(*Version) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var dummy Version
	tail := &dummy
	for v := c.head; v != nil; v = v.Prev {
		if !pred(v) {
			tail.Prev = v
			tail = v
		}
	}
	tail.Prev = nil
	c.head = dummy.Prev
}

// FindVisible returns the first visible version for the given snapshot/txn.
// Caller provides the isVisible function.
func (c *VersionChain) FindVisible(isVisible func(*Version) bool) *Version {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for v := c.head; v != nil; v = v.Prev {
		if isVisible(v) {
			return v
		}
	}
	return nil
}

// Lock acquires an exclusive lock for external callers that need atomic read-modify-write.
func (c *VersionChain) Lock() { c.mu.Lock() }

// Unlock releases the exclusive lock.
func (c *VersionChain) Unlock() { c.mu.Unlock() }

// UnsafeHead returns the head version without acquiring a lock.
// Caller MUST hold the chain lock.
func (c *VersionChain) UnsafeHead() *Version { return c.head }

// PrependUnsafe inserts a new version at the head without acquiring a lock.
// Caller MUST hold the chain lock.
func (c *VersionChain) PrependUnsafe(v *Version) {
	v.Prev = c.head
	c.head = v
}

// SetHead sets the head version without acquiring a lock.
// Caller MUST hold the chain lock.
func (c *VersionChain) SetHead(v *Version) { c.head = v }

// IsTombstonedLocked reports whether ReapEmpty has marked this chain for
// deletion. Caller MUST hold the chain's write lock (CT-19).
func (c *VersionChain) IsTombstonedLocked() bool { return c.tombstoned }
