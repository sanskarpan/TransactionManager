package mvcc

import (
	"sync"
	"sync/atomic"
	"time"
)

// Vacuum periodically prunes versions from MVCC chains that are no longer
// visible to any active transaction (i.e. superseded by a committed
// deleter whose ID is below the oldest-active horizon). It also reaps
// empty chains so the store's sync.Map does not grow without bound over
// long-running workloads touching distinct keys.
type Vacuum struct {
	store  *Store
	txnMgr interface {
		OldestActiveTxnID() TxnID
		IsCommitted(TxnID) bool
		// PruneHistory is called on every vacuum tick to bound the
		// committed/aborted history maps. CT-02: previously the vacuum
		// never invoked this and the history maps grew without bound.
		PruneHistory() int
	}
	interval time.Duration
	stop     chan struct{}
	once     sync.Once

	// startMu guards the `started` flag so Run is idempotent without consuming
	// the `once` used by Stop (C-02: previously Run and Stop shared the same
	// sync.Once, so Stop became a no-op and the goroutine leaked forever).
	startMu sync.Mutex
	started bool

	// done is closed when the background loop exits, so tests and Shutdown can
	// wait for a real drain rather than racing the goroutine.
	done chan struct{}

	// Stats
	pruned  atomic.Int64
	runs    atomic.Int64
	lastRun int64 // unix nano
}

// NewVacuum constructs a Vacuum that will tick every `interval` once Run is
// called.
func NewVacuum(store *Store, txnMgr interface {
	OldestActiveTxnID() TxnID
	IsCommitted(TxnID) bool
	PruneHistory() int
}, interval time.Duration) *Vacuum {
	return &Vacuum{
		store:    store,
		txnMgr:   txnMgr,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Run starts the background vacuum loop. Idempotent: safe to call multiple
// times; only the first call starts a goroutine.
//
// The start guard is separate from the sync.Once used by Stop (C-02):
// previously Run consumed the shared once and Stop's once.Do became a no-op,
// so the goroutine could never be cancelled and leaked permanently.
func (v *Vacuum) Run() {
	v.startMu.Lock()
	defer v.startMu.Unlock()
	if v.started {
		return
	}
	v.started = true
	go func() {
		defer close(v.done)
		ticker := time.NewTicker(v.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				horizon := v.txnMgr.OldestActiveTxnID()
				v.pruneVersions(horizon)
			case <-v.stop:
				return
			}
		}
	}()
}

// Stop signals the background loop to exit. Idempotent: safe to call from
// multiple goroutines or invoke more than once (e.g. graceful-shutdown +
// test-cleanup both calling it). Previously this panicked with a
// close-of-closed-channel on the second call.
func (v *Vacuum) Stop() {
	v.once.Do(func() { close(v.stop) })
}

// Done returns a channel that is closed when the background vacuum loop has
// exited. Allows Shutdown and tests to wait for a real drain.
func (v *Vacuum) Done() <-chan struct{} { return v.done }

// RunOnce performs one synchronous prune pass. Used by the manual
// /api/mvcc/vacuum endpoint.
func (v *Vacuum) RunOnce() {
	horizon := v.txnMgr.OldestActiveTxnID()
	v.pruneVersions(horizon)
}

func (v *Vacuum) pruneVersions(horizon TxnID) {
	v.runs.Add(1)
	atomic.StoreInt64(&v.lastRun, time.Now().UnixNano())

	var pruned int64
	v.store.ForEachChain(func(chain *VersionChain) {
		before := len(chain.All())
		chain.Prune(func(ver *Version) bool {
			return ver.XMax != 0 &&
				ver.XMax <= horizon &&
				v.txnMgr.IsCommitted(ver.XMax)
		})
		after := len(chain.All())
		pruned += int64(before - after)
	})
	v.pruned.Add(pruned)

	// Reap empty chains so the store's sync.Map does not grow without bound.
	// H-03: previously chains were never deleted even after every version
	// was pruned, causing a memory leak proportional to the cardinality of
	// touched keys.
	v.store.ReapEmpty()

	// CT-02: bound the committed/aborted history maps in the txn manager.
	// Previously this was defined but never called; under sustained load
	// the maps grew ~28 GB/day at 10k committed txns/sec.
	v.txnMgr.PruneHistory()
}

// VacuumStats is a point-in-time snapshot of vacuum activity, returned
// by Vacuum.Stats. TotalChains / TotalVersions reflect the store's
// current shape; Pruned / Runs / LastRunAt are cumulative since startup.
type VacuumStats struct {
	TotalChains   int
	TotalVersions int
	Pruned        int64
	Runs          int64
	LastRunAt     time.Time
}

// Stats returns a snapshot of vacuum statistics.
func (v *Vacuum) Stats() VacuumStats {
	var totalChains, totalVersions int
	v.store.ForEachChain(func(chain *VersionChain) {
		totalChains++
		totalVersions += len(chain.All())
	})
	return VacuumStats{
		TotalChains:   totalChains,
		TotalVersions: totalVersions,
		Pruned:        v.pruned.Load(),
		Runs:          v.runs.Load(),
		LastRunAt:     time.Unix(0, atomic.LoadInt64(&v.lastRun)),
	}
}
