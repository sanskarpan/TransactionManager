package txn

import (
	"sort"

	"github.com/sanskarpan/TransactionManager/internal/lock"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// gapInfinitySentinel is the upper-bound string used to represent the
// "open upper bound" of a gap when there is no larger key in the table
// (M-10 / docs/adr/0006-gap-locking.md). A real key containing the
// 0xFF byte would collide with this sentinel and produce a wrong
// placement; for that reason, gap locking REQUIRES row keys to be
// restricted to printable ASCII (0x20-0x7E). The zero-padded integer
// keys used by the seed data and the demo frontend satisfy this.
const gapInfinitySentinel = "\xff\xff\xff\xff"

func isGapSafeKey(k string) bool {
	for _, r := range k {
		if r < 0x20 || r > 0x7E {
			return false
		}
	}
	return true
}

// LockForRangeScan acquires gap locks for a range scan [lo, hi] (inclusive).
// This prevents phantom reads in Serializable 2PL by locking:
//   - IS on the table
//   - S on every row in the range
//   - S on every gap in the range (prev, lo], (row[i], row[i+1]] pairs, (hi, ∞)
//
// lo == "" means start of table; hi == "" means end of table.
func (m *Manager) LockForRangeScan(txn *Transaction, table, lo, hi string) error {
	tableRes := lock.ResourceForTable(table)
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockIS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		return err
	}

	t, ok := m.Catalog.Lookup(table)
	if !ok {
		return types.NewTableNotFoundError(table)
	}
	keys := t.Keys()
	sortedKeys := make([]string, len(keys))
	for i, k := range keys {
		sortedKeys[i] = string(k)
	}
	sort.Strings(sortedKeys)

	// Determine range bounds
	inRange := func(k string) bool {
		afterLo := lo == "" || k >= lo
		beforeHi := hi == "" || k <= hi
		return afterLo && beforeHi
	}

	var prev string // previous key (for gap computation)
	for _, k := range sortedKeys {
		if !inRange(k) {
			if lo != "" && k < lo {
				prev = k
			}
			continue
		}
		// Lock the gap (prev, k]
		gapRes := lock.ResourceForGap(table, prev, k)
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), gapRes, lock.LockS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			return err
		}
		// Lock the row
		rowRes := lock.ResourceForRow(table, k)
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), rowRes, lock.LockS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			return err
		}
		prev = k
	}

	// Lock trailing gap (last-in-range, nextExistingKeyAfterRange] (or
	// (last-in-range, ∞) if none). H-09: previously this used the caller-
	// supplied `hi` as the upper bound, but LockForInsert computes its gap as
	// (prev, nextExistingKey] using the next physically present key. When `hi`
	// is not an existing key (the typical predicate case) — or when `hi` IS
	// an existing key and the prior code produced a degenerate (hi, hi] gap —
	// the two gap ResourceIDs did not match, so the lock table reported no
	// conflict and an insert into the trailing gap slipped through as a
	// phantom read, violating Serializable isolation. Use the next existing
	// key > hi so the resource matches LockForInsert's computation.
	trailingGapHi := ""
	if hi == "" {
		trailingGapHi = gapInfinitySentinel
	} else {
		found := false
		for _, k := range sortedKeys {
			if k > hi {
				trailingGapHi = k
				found = true
				break
			}
		}
		if !found {
			trailingGapHi = gapInfinitySentinel
		}
	}
	trailingGap := lock.ResourceForGap(table, prev, trailingGapHi)
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), trailingGap, lock.LockS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		return err
	}

	return nil
}

// LockForInsert acquires an exclusive gap lock for inserting a new row.
// Finds the surrounding gap (prev, next] and locks it exclusively,
// then acquires X on the row itself.
func (m *Manager) LockForInsert(txn *Transaction, table, key string) error {
	tableRes := lock.ResourceForTable(table)
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockIX, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		return err
	}

	t, ok := m.Catalog.Lookup(table)
	if !ok {
		return types.NewTableNotFoundError(table)
	}
	keys := t.Keys()
	sortedKeys := make([]string, len(keys))
	for i, k := range keys {
		sortedKeys[i] = string(k)
	}
	sort.Strings(sortedKeys)

	// Find prev and next keys surrounding the insertion point
	var prev, next string
	for _, k := range sortedKeys {
		if k < key {
			prev = k
		} else if k > key && next == "" {
			next = k
			break
		}
	}
	if next == "" {
		next = gapInfinitySentinel
	}

	// M-10: gap locking requires printable-ASCII keys. A key containing
	// 0xFF would collide with the infinity sentinel and produce an
	// incorrect gap placement.
	if !isGapSafeKey(key) {
		return &types.TxnError{Code: types.ErrTxnAborted, Message: "gap-lock key contains non-printable bytes"}
	}

	// Lock the gap (prev, next] exclusively — prevents concurrent range scans from adding rows here
	gapRes := lock.ResourceForGap(table, prev, next)
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), gapRes, lock.LockX, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		return err
	}

	// Lock the new row exclusively
	rowRes := lock.ResourceForRow(table, key)
	if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), rowRes, lock.LockX, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
		return err
	}

	return nil
}

// TwoPLScan scans a table under 2PL with isolation-appropriate locking.
// For Serializable: acquires gap locks on the full table range (phantom prevention).
// For RepeatableRead: acquires S on the entire table.
// For ReadCommitted: no persistent scan lock.
func (m *Manager) TwoPLScan(txn *Transaction, table string, filter func(storage.Row) bool) ([]storage.Row, error) {
	switch txn.Isolation {
	case Serializable:
		if err := m.LockForRangeScan(txn, table, "", ""); err != nil {
			return nil, err
		}
	case RepeatableRead:
		tableRes := lock.ResourceForTable(table)
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			return nil, err
		}
	case ReadCommitted:
		// Per-statement table IS lock, released conceptually after statement
		tableRes := lock.ResourceForTable(table)
		if err := m.LockAcq.AcquireLockCtx(txn.requestCtxOrBackground(), uint64(txn.ID), tableRes, lock.LockIS, txn.LocksHeld, txn.LockTimeout, txn.AbortCh); err != nil {
			return nil, err
		}
	}

	t, ok := m.Catalog.Lookup(table)
	if !ok {
		return nil, types.NewTableNotFoundError(table)
	}
	return t.Scan(filter), nil
}
