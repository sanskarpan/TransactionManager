package mvcc

import (
	"fmt"
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/storage"
)

// Store is the global version-chain map keyed by "table:key". It is
// safe for concurrent reads (sync.Map loads) and for concurrent writes
// (sync.Map stores). Chains are created lazily on first write and
// reaped by the vacuum when their version list becomes empty.
type Store struct {
	chains sync.Map // string key -> *VersionChain
}

// NewStore constructs an empty store. The first call to GetOrCreateChain
// lazily allocates the version chain for a (table, key) pair.
func NewStore() *Store {
	return &Store{}
}

func chainKey(table, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}

// GetChain returns the version chain for (table, key) if it exists.
// Returns (nil, false) if the chain has not been created or has been
// reaped by the vacuum. Callers that need to mutate the chain must
// use GetOrCreateChain instead.
func (s *Store) GetChain(table, key string) (*VersionChain, bool) {
	v, ok := s.chains.Load(chainKey(table, key))
	if !ok {
		return nil, false
	}
	return v.(*VersionChain), true
}

// GetOrCreateChain returns the chain for (table, key), creating one if
// necessary. CT-19/CT-26: a chain returned by this function may have been
// marked tombstoned by a concurrent ReapEmpty (ReapEmpty holds the
// chain's write lock just long enough to set the tombstone + delete
// from the sync.Map, but a Load racing with that delete may observe
// the chain anyway). When we see a tombstoned chain, we evict it
// (chains.Delete is idempotent) and LoadOrStore creates a fresh one.
// MVCCWrite additionally checks the tombstone under its own lock and
// retries via EvictChain + GetOrCreateChain if the chain was tombstoned
// between our Load and our Lock.
func (s *Store) GetOrCreateChain(table, key string) *VersionChain {
	k := chainKey(table, key)
	if v, ok := s.chains.Load(k); ok {
		chain := v.(*VersionChain)
		chain.mu.Lock()
		notTombstoned := !chain.tombstoned
		chain.mu.Unlock()
		if notTombstoned {
			return chain
		}
		// Tombstoned: evict and fall through to LoadOrStore. Delete
		// is safe to race with ReapEmpty's own Delete (both are
		// idempotent). LoadOrStore then creates a fresh chain
		// (no race winner) or returns a chain another racing
		// goroutine inserted (also fresh, because they all run
		// this same function).
		s.chains.Delete(k)
	}
	chain := NewVersionChain(storage.RowKey(key))
	actual, _ := s.chains.LoadOrStore(k, chain)
	return actual.(*VersionChain)
}

// EvictChain removes the chain entry for (table, key) from the sync.Map
// if it exists. CT-26: MVCCWrite calls this when it observes a
// tombstoned chain under its write lock — at that point ReapEmpty has
// set the tombstone + Delete may be racing, but chains.Delete is
// idempotent so the chain entry is reliably gone after this returns.
// MVCCWrite then re-GetOrCreateChain's a fresh chain.
func (s *Store) EvictChain(table, key string) {
	s.chains.Delete(chainKey(table, key))
}

// MarkChainTombstoned is a TEST-ONLY helper. It marks the chain for
// (table, key) as tombstoned and evicts it from the store, exactly as
// ReapEmpty does after setting tombstone=true. CT-26 regression guard
// uses this to simulate a concurrent ReapEmpty that observes a
// tombstone between MVCCWrite's GetOrCreateChain and its chain.Lock.
func (s *Store) MarkChainTombstoned(table, key string) bool {
	v, ok := s.chains.Load(chainKey(table, key))
	if !ok {
		return false
	}
	chain := v.(*VersionChain)
	chain.mu.Lock()
	chain.tombstoned = true
	chain.mu.Unlock()
	s.chains.Delete(chainKey(table, key))
	return true
}

// ForEachChain iterates every version chain in the store, regardless of
// table. Used by the vacuum; expensive on large stores. The callback
// must not call back into the store's mutating methods.
func (s *Store) ForEachChain(fn func(*VersionChain)) {
	s.chains.Range(func(_, v interface{}) bool {
		fn(v.(*VersionChain))
		return true
	})
}

// ForEachChainInTable iterates all chains whose key has the given table prefix.
func (s *Store) ForEachChainInTable(table string, fn func(rowKey string, chain *VersionChain)) {
	prefix := table + ":"
	s.chains.Range(func(k, v interface{}) bool {
		key := k.(string)
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			rowKey := key[len(prefix):]
			fn(rowKey, v.(*VersionChain))
		}
		return true
	})
}

// AllVersions returns a snapshot of every chain in the store, keyed by
// the "table:key" string. Used by the /api/mvcc/versions endpoint and
// by tests; expensive — never call from a hot path.
func (s *Store) AllVersions() map[string][]*Version {
	result := make(map[string][]*Version)
	s.chains.Range(func(k, v interface{}) bool {
		chain := v.(*VersionChain)
		result[k.(string)] = chain.All()
		return true
	})
	return result
}

// SeedMVCCStore directly populates MVCC store with initial data. Used by
// cmd/server at process start (after reading the catalog) and by the
// benchmark fixture to skip a cold start. Each row becomes a single
// version with XMin=seedTxnID and XMax=0.
func SeedMVCCStore(store *Store, table string, rows []storage.Row, seedTxnID TxnID) {
	for _, row := range rows {
		chain := store.GetOrCreateChain(table, string(row.Key))
		v := &Version{
			XMin: seedTxnID,
			XMax: 0,
			Data: row.Values,
		}
		chain.Prepend(v)
	}
}

// Clear removes all version chains from the store.
func (s *Store) Clear() {
	s.chains.Range(func(k, _ interface{}) bool {
		s.chains.Delete(k)
		return true
	})
}

// ReapEmpty deletes chain entries whose version list is empty. The vacuum
// loop calls this after a prune pass so the store's sync.Map does not grow
// without bound over workloads touching a high cardinality of distinct
// keys (H-03).
//
// CT-19: previously this used Head() (RLock) and then chains.Delete()
// outside the lock, racing a concurrent MVCCWrite that could prepend a
// version between the check and the delete — the write would succeed but
// the chain would be removed from the map, and subsequent GetChain
// would return (nil, false), silently losing the write. The fix: hold
// the chain's write lock across the check, the tombstone, AND the
// chains.Delete so MVCCWrite cannot interleave.
func (s *Store) ReapEmpty() {
	s.chains.Range(func(k, v interface{}) bool {
		chain := v.(*VersionChain)
		chain.mu.Lock()
		empty := chain.head == nil
		if empty {
			chain.tombstoned = true
		}
		// Hold the lock across chains.Delete to serialize with
		// MVCCWrite's chain.Lock(). MVCCWrite checks
		// IsTombstonedLocked under the chain's write lock, so it
		// cannot observe a tombstoned chain unless ReapEmpty has
		// already unlocked — in which case the Delete has happened
		// first, the Load in GetOrCreateChain returns no entry, and
		// LoadOrStore creates a fresh chain.
		if empty {
			s.chains.Delete(k)
		}
		chain.mu.Unlock()
		return true
	})
}
