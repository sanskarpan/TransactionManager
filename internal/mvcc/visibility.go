package mvcc

// TxnStatusChecker abstracts the txn manager for status checks (avoids circular import)
type TxnStatusChecker interface {
	IsCommitted(id TxnID) bool
	IsAborted(id TxnID) bool
	IsActive(id TxnID) bool
}

// SnapshotView is satisfied by txn.Snapshot (avoids circular import mvcc→txn)
type SnapshotView interface {
	// ContainsID reports whether txnID was active when the snapshot was taken
	ContainsID(id uint64) bool
	// GetXmin returns the lowest active txn ID at snapshot time
	GetXmin() uint64
	// GetXmax returns the next txn ID at snapshot time (exclusive upper bound)
	GetXmax() uint64
}

// IsVisible reports whether version v is visible to transaction txnID
// using the given snapshot snap.
func IsVisible(v *Version, txnID TxnID, snap SnapshotView, mgr TxnStatusChecker) bool {
	xmin := v.XMin
	xmax := v.XMax

	// Step 1: Is the creator (xmin) visible?
	selfCreated := (xmin == txnID)

	// Committed before our snapshot: xmin is below the snapshot's Xmin, so the
	// creator was not active when the snapshot was taken. It must therefore be
	// either committed or aborted. We must explicitly exclude aborted txns
	// (C-04): an aborted txn whose ID is below Xmin still has versions in the
	// chain until applyMVCCUndo runs, and a snapshot taken in the window after
	// `delete(m.txns, id)` + `m.aborted[id]=...` (both under m.mu, so IsAborted
	// is reliable here) but before undo would otherwise treat the aborted
	// version as committed-before-snapshot → dirty read of rolled-back data.
	committedBeforeSnapshot := (xmin < snap.GetXmin() && !mgr.IsAborted(xmin))

	// Committed during the window (between Xmin and Xmax) but not active
	committedDuringWindow := (xmin >= snap.GetXmin() &&
		xmin < snap.GetXmax() &&
		mgr.IsCommitted(xmin) &&
		!snap.ContainsID(xmin))

	xminVisible := selfCreated || committedBeforeSnapshot || committedDuringWindow
	if !xminVisible {
		return false
	}

	// Step 2: Is the deletion (xmax) invisible to us?
	if xmax == 0 {
		return true // not deleted
	}

	// Deleted by our own transaction
	if xmax == txnID {
		return false
	}

	// xmax was active when we took our snapshot → deletion not committed
	if snap.ContainsID(xmax) {
		return true
	}

	// xmax started after our snapshot → we don't see its deletion
	if xmax >= snap.GetXmax() {
		return true
	}

	// xmax has aborted → deletion rolled back
	if mgr.IsAborted(xmax) {
		return true
	}

	// xmax committed before our snapshot → row is deleted
	return false
}
