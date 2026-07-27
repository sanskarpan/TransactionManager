package mvcc

import (
	"sync"

	"github.com/sanskarpan/TransactionManager/internal/storage"
)

// tableStore holds the version chains for a single table.
type tableStore struct {
	chains sync.Map // rowKey string -> *VersionChain
}

// Store is the global version-chain map keyed by table and then row key.
// It is safe for concurrent reads and writes. Chains are created lazily on
// first write and reaped by the vacuum when their version list becomes empty.
//
// The two-level structure (tables → chains) makes ForEachChainInTable O(rows
// in table) instead of O(total rows across all tables).
type Store struct {
	tables sync.Map // table string -> *tableStore
}

// NewStore constructs an empty store.
func NewStore() *Store {
	return &Store{}
}

// getTable returns the tableStore for table, creating one if necessary.
func (s *Store) getTable(table string) *tableStore {
	if v, ok := s.tables.Load(table); ok {
		return v.(*tableStore)
	}
	ts := &tableStore{}
	actual, _ := s.tables.LoadOrStore(table, ts)
	return actual.(*tableStore)
}

// GetChain returns the version chain for (table, key) if it exists.
// Returns (nil, false) if the chain has not been created or has been reaped.
func (s *Store) GetChain(table, key string) (*VersionChain, bool) {
	tv, ok := s.tables.Load(table)
	if !ok {
		return nil, false
	}
	cv, ok := tv.(*tableStore).chains.Load(key)
	if !ok {
		return nil, false
	}
	return cv.(*VersionChain), true
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
	ts := s.getTable(table)
	if v, ok := ts.chains.Load(key); ok {
		chain := v.(*VersionChain)
		chain.mu.Lock()
		notTombstoned := !chain.tombstoned
		chain.mu.Unlock()
		if notTombstoned {
			return chain
		}
		// Tombstoned: evict and fall through to LoadOrStore.
		ts.chains.Delete(key)
	}
	chain := NewVersionChain(storage.RowKey(key))
	actual, _ := ts.chains.LoadOrStore(key, chain)
	return actual.(*VersionChain)
}

// EvictChain removes the chain entry for (table, key) from the store.
// CT-26: MVCCWrite calls this when it observes a tombstoned chain under
// its write lock.
func (s *Store) EvictChain(table, key string) {
	if tv, ok := s.tables.Load(table); ok {
		tv.(*tableStore).chains.Delete(key)
	}
}

// MarkChainTombstoned is a TEST-ONLY helper. It marks the chain for
// (table, key) as tombstoned and evicts it from the store, exactly as
// ReapEmpty does. CT-26 regression guard uses this to simulate a concurrent
// ReapEmpty that observes a tombstone between MVCCWrite's GetOrCreateChain
// and its chain.Lock.
func (s *Store) MarkChainTombstoned(table, key string) bool {
	tv, ok := s.tables.Load(table)
	if !ok {
		return false
	}
	cv, ok := tv.(*tableStore).chains.Load(key)
	if !ok {
		return false
	}
	chain := cv.(*VersionChain)
	chain.mu.Lock()
	chain.tombstoned = true
	chain.mu.Unlock()
	tv.(*tableStore).chains.Delete(key)
	return true
}

// ForEachChain iterates every version chain in the store, regardless of
// table. Used by the vacuum; the callback must not call back into the
// store's mutating methods.
func (s *Store) ForEachChain(fn func(*VersionChain)) {
	s.tables.Range(func(_, tv interface{}) bool {
		tv.(*tableStore).chains.Range(func(_, cv interface{}) bool {
			fn(cv.(*VersionChain))
			return true
		})
		return true
	})
}

// ForEachChainInTable iterates all chains for the given table. O(rows in
// table) — unlike the old flat-map design which was O(total rows across
// all tables).
func (s *Store) ForEachChainInTable(table string, fn func(rowKey string, chain *VersionChain)) {
	tv, ok := s.tables.Load(table)
	if !ok {
		return
	}
	tv.(*tableStore).chains.Range(func(k, cv interface{}) bool {
		fn(k.(string), cv.(*VersionChain))
		return true
	})
}

// AllVersions returns a snapshot of every chain in the store, keyed by
// "table:key". Used by the /api/mvcc/versions endpoint and by tests;
// expensive — never call from a hot path.
func (s *Store) AllVersions() map[string][]*Version {
	result := make(map[string][]*Version)
	s.tables.Range(func(tk, tv interface{}) bool {
		table := tk.(string)
		tv.(*tableStore).chains.Range(func(rk, cv interface{}) bool {
			result[table+":"+rk.(string)] = cv.(*VersionChain).All()
			return true
		})
		return true
	})
	return result
}

// SeedMVCCStore directly populates the MVCC store with initial data. Used by
// cmd/server at process start and by the benchmark fixture to skip a cold
// start. Each row becomes a single version with XMin=seedTxnID and XMax=0.
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
	s.tables.Range(func(k, _ interface{}) bool {
		s.tables.Delete(k)
		return true
	})
}

// ReapEmpty deletes chain entries whose version list is empty. The vacuum
// loop calls this after a prune pass so the store does not grow without
// bound over workloads touching a high cardinality of distinct keys (H-03).
//
// CT-19: holds the chain's write lock across the tombstone AND the Delete
// so MVCCWrite cannot interleave.
func (s *Store) ReapEmpty() {
	s.tables.Range(func(_, tv interface{}) bool {
		ts := tv.(*tableStore)
		ts.chains.Range(func(k, cv interface{}) bool {
			chain := cv.(*VersionChain)
			chain.mu.Lock()
			empty := chain.head == nil
			if empty {
				chain.tombstoned = true
				ts.chains.Delete(k)
			}
			chain.mu.Unlock()
			return true
		})
		return true
	})
}
