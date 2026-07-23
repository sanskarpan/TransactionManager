package mvcc

// CheckWriteConflict returns an error if a concurrent transaction has written this row.
// "Concurrent" means started after our snapshot Xmin and was active when we began.
// snap is a SnapshotView (implemented by txn.Snapshot without needing import).
func CheckWriteConflict(txnID TxnID, chain *VersionChain, snap SnapshotView, mgr TxnStatusChecker) error {
	chain.mu.RLock()
	defer chain.mu.RUnlock()
	return checkWriteConflictLocked(txnID, chain, snap, mgr)
}

// CheckWriteConflictNoLock is the same as CheckWriteConflict but assumes the
// caller already holds the chain's write lock (e.g. MVCCWrite, which must
// perform the conflict check and the mutation atomically under one write
// lock). Calling this without holding the lock is a data race.
//
// C-07: previously MVCCWrite inlined a head-only check that missed concurrent
// committed writers sitting deeper in the chain behind an aborted-txn head
// version (IsActive==false, IsCommitted==false at the head, so the head-only
// check reported no conflict while a deeper concurrent committed version was
// the real conflict). Centralizing the full-chain walk here ensures the
// manager and any future caller use the same semantics as CheckWriteConflict.
func CheckWriteConflictNoLock(txnID TxnID, chain *VersionChain, snap SnapshotView, mgr TxnStatusChecker) error {
	return checkWriteConflictLocked(txnID, chain, snap, mgr)
}

func checkWriteConflictLocked(txnID TxnID, chain *VersionChain, snap SnapshotView, mgr TxnStatusChecker) error {
	for v := chain.head; v != nil; v = v.Prev {
		if v.XMin == txnID {
			continue // our own write
		}
		// Is this writer still active?
		if mgr.IsActive(v.XMin) {
			return &WriteConflictError{ConflictingTxn: v.XMin}
		}
		// Was this writer active when we took our snapshot and has now committed?
		if snap.ContainsID(v.XMin) && mgr.IsCommitted(v.XMin) {
			return &WriteConflictError{ConflictingTxn: v.XMin}
		}
	}
	return nil
}

// WriteConflictError is returned when a concurrent transaction's committed
// write conflicts with the current writer's snapshot.
type WriteConflictError struct {
	ConflictingTxn TxnID
}

func (e *WriteConflictError) Error() string {
	return "write-write conflict with concurrent transaction"
}
