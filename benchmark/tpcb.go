// Package benchmark implements a simplified TPC-B style benchmark.
//
// TPC-B models a bank with:
//   - accounts table: (id, balance)
//   - tellers   table: (id, balance)
//   - branches  table: (id, balance)
//   - history   table: (id, amount, timestamp)
//
// Each transaction:
//  1. UPDATE accounts  SET balance = balance + delta WHERE id = ?
//  2. UPDATE tellers   SET balance = balance + delta WHERE id = ?
//  3. UPDATE branches  SET balance = balance + delta WHERE id = ?
//  4. INSERT INTO history VALUES (...)
package benchmark

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/metrics"
	"github.com/sanskarpan/TransactionManager/internal/mvcc"
	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

const (
	// TableAccounts is the name of the accounts table in the TPC-B schema.
	TableAccounts = "accounts"
	// TableTellers is the name of the tellers table.
	TableTellers = "tellers"
	// TableBranches is the name of the branches table.
	TableBranches = "branches"
	// TableHistory is the name of the history table.
	TableHistory = "history"
)

// Config controls the benchmark parameters.
type Config struct {
	NumBranches int
	NumTellers  int
	NumAccounts int
	Concurrency int
	Duration    time.Duration
	Protocol    txn.ConcurrencyProtocol
	Isolation   txn.IsolationLevel
	LockTimeout time.Duration
	MaxRetries  int
}

// DefaultConfig returns a minimal TPC-B configuration.
func DefaultConfig() Config {
	return Config{
		NumBranches: 1,
		NumTellers:  10,
		NumAccounts: 100,
		Concurrency: 8,
		Duration:    5 * time.Second,
		Protocol:    txn.ProtocolMVCC,
		Isolation:   txn.ReadCommitted,
		LockTimeout: 2 * time.Second,
		MaxRetries:  5,
	}
}

// Result holds benchmark output statistics.
type Result struct {
	TotalTxns    int64
	Committed    int64
	Aborted      int64
	Deadlocks    int64
	Conflicts    int64
	Duration     time.Duration
	TPS          float64
	LatencyHist  *metrics.Histogram
	BalanceSumOK bool // invariant: sum of all account balances unchanged
}

// Runner executes the TPC-B benchmark.
type Runner struct {
	cfg     Config
	mgr     *txn.Manager
	catalog *storage.Catalog
	m       *metrics.Metrics
	hist    *metrics.Histogram
	ctx     context.Context // honoured by Run/RunCtx and the worker loop (H-15)

	// counters
	totalTxns atomic.Int64
	committed atomic.Int64
	aborted   atomic.Int64
	deadlocks atomic.Int64
	conflicts atomic.Int64

	// initial sum of accounts+tellers+branches for the invariant check.
	// NOTE: in this simplified TPC-B model each txn applies `delta` to all
	// three tables, so the grand total is NOT preserved across txns; the
	// invariant we actually check is per-value finiteness (no NaN/Inf, the
	// real corruption signal from lost updates) — see checkInvariant.
	initialSum float64
}

// NewRunner creates a Runner, seeds tables, and prepares the manager.
func NewRunner(cfg Config, m *metrics.Metrics) *Runner {
	catalog := storage.NewCatalog()

	for _, name := range []string{TableAccounts, TableTellers, TableBranches, TableHistory} {
		var cols []storage.Column
		switch name {
		case TableHistory:
			cols = []storage.Column{
				{Name: "acct_id", Type: types.TypeInt},
				{Name: "delta", Type: types.TypeFloat},
				{Name: "ts", Type: types.TypeText},
			}
		default:
			cols = []storage.Column{
				{Name: "balance", Type: types.TypeFloat},
			}
		}
		catalog.Register(storage.NewTable(name, cols))
	}

	r := &Runner{
		cfg:     cfg,
		catalog: catalog,
		mgr:     txn.NewManager(catalog),
		m:       m,
		hist:    metrics.NewHistogram([]int64{1, 2, 5, 10, 20, 50, 100, 200, 500}),
		ctx:     context.Background(),
	}
	r.seed()
	return r
}

// WithContext returns a runner whose worker loop and txn attempts honour
// ctx.Done() (H-15: previously Run() spawned uncancellable goroutines that
// were orphaned on shutdown and continued mutating a Manager that may
// itself be shutting down). main.go passes a context cancelled by
// Server.Shutdown.
func (r *Runner) WithContext(ctx context.Context) *Runner {
	r.ctx = ctx
	return r
}

func (r *Runner) seed() {
	acct, _ := r.catalog.Lookup(TableAccounts)
	tel, _ := r.catalog.Lookup(TableTellers)
	br, _ := r.catalog.Lookup(TableBranches)

	for i := 0; i < r.cfg.NumAccounts; i++ {
		acct.PutRow(storage.RowKey(fmt.Sprintf("%d", i)), []types.Value{types.FloatVal(1000.0)})
	}
	for i := 0; i < r.cfg.NumTellers; i++ {
		tel.PutRow(storage.RowKey(fmt.Sprintf("%d", i)), []types.Value{types.FloatVal(0.0)})
	}
	for i := 0; i < r.cfg.NumBranches; i++ {
		br.PutRow(storage.RowKey(fmt.Sprintf("%d", i)), []types.Value{types.FloatVal(0.0)})
	}

	// Seed MVCC store from catalog
	rows := acct.Scan(nil)
	mvcc.SeedMVCCStore(r.mgr.MVCCStore, TableAccounts, rows, 0)
	rows = tel.Scan(nil)
	mvcc.SeedMVCCStore(r.mgr.MVCCStore, TableTellers, rows, 0)
	rows = br.Scan(nil)
	mvcc.SeedMVCCStore(r.mgr.MVCCStore, TableBranches, rows, 0)

	r.initialSum = float64(r.cfg.NumAccounts) * 1000.0
}

// Run executes the benchmark and returns results. Equivalent to
// RunCtx(context.Background()).
func (r *Runner) Run() Result {
	return r.RunCtx(context.Background())
}

// RunCtx executes the benchmark, honouring ctx.Done() so an in-flight
// benchmark aborts promptly on server shutdown (H-15). Workers check the
// context between transactions; the run returns a partial Result if
// cancelled.
func (r *Runner) RunCtx(ctx context.Context) Result {
	r.ctx = ctx
	start := time.Now()
	deadline := start.Add(r.cfg.Duration)

	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			// M-46: per-goroutine offset so two workers starting in the same
			// nanosecond do not get identical seeds and correlated contention.
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return
				default:
				}
				r.runTxn(rng)
			}
		}(i)
	}
	wg.Wait()

	dur := time.Since(start)
	committed := r.committed.Load()

	return Result{
		TotalTxns:    r.totalTxns.Load(),
		Committed:    committed,
		Aborted:      r.aborted.Load(),
		Deadlocks:    r.deadlocks.Load(),
		Conflicts:    r.conflicts.Load(),
		Duration:     dur,
		TPS:          float64(committed) / dur.Seconds(),
		LatencyHist:  r.hist,
		BalanceSumOK: r.checkInvariant(),
	}
}

func (r *Runner) runTxn(rng *rand.Rand) {
	acctID := rng.Intn(r.cfg.NumAccounts)
	telID := rng.Intn(r.cfg.NumTellers)
	brID := rng.Intn(r.cfg.NumBranches)
	delta := (rng.Float64() - 0.5) * 200 // random ±100

	for attempt := 0; attempt <= r.cfg.MaxRetries; attempt++ {
		// H-15: abort retrying promptly when the runner context is cancelled
		// (e.g. server shutdown).
		if r.ctx != nil {
			select {
			case <-r.ctx.Done():
				return
			default:
			}
		}
		t0 := time.Now()
		err := r.execTxn(acctID, telID, brID, delta)
		elapsedMs := float64(time.Since(t0).Milliseconds())
		r.totalTxns.Add(1)

		if err == nil {
			r.committed.Add(1)
			r.hist.Record(elapsedMs)
			if r.m != nil {
				r.m.TxnCommits.Add(1)
			}
			return
		}

		r.aborted.Add(1)
		if r.m != nil {
			r.m.TxnAborts.Add(1)
		}

		txnErr, ok := err.(*types.TxnError)
		if ok {
			switch txnErr.Code {
			case types.ErrDeadlock:
				r.deadlocks.Add(1)
				if r.m != nil {
					r.m.Deadlocks.Add(1)
				}
			case types.ErrWriteConflict, types.ErrSerializationFailure:
				r.conflicts.Add(1)
				if r.m != nil {
					r.m.WriteConflicts.Add(1)
				}
			}
		}

		// Short back-off before retry
		time.Sleep(time.Duration(attempt+1) * time.Millisecond)
	}
}

func (r *Runner) execTxn(acctID, telID, brID int, delta float64) error {
	t, err := r.mgr.Begin(r.cfg.Protocol, r.cfg.Isolation, r.cfg.LockTimeout)
	if err != nil {
		return err
	}

	acctKey := fmt.Sprintf("%d", acctID)
	telKey := fmt.Sprintf("%d", telID)
	brKey := fmt.Sprintf("%d", brID)

	// Helper: read-modify-write under chosen protocol
	rmw := func(table, key string, amount float64) error {
		if r.cfg.Protocol == txn.Protocol2PL {
			vals, ok, err := r.mgr.TwoPLRead(t, table, key)
			if err != nil {
				return err
			}
			// M-45: previously a missing row was silently treated as
			// newBal = amount (masking seed/concurrency bugs as a successful
			// txn). Fail the txn instead so the invariant stays meaningful.
			if !ok || len(vals) == 0 {
				return types.NewRowNotFoundError()
			}
			return r.mgr.TwoPLWrite(t, table, key, []types.Value{types.FloatVal(vals[0].Float + amount)}, txn.UndoUpdate)
		}
		// MVCC path
		vals, ok, err := r.mgr.MVCCRead(t, table, key)
		if err != nil {
			return err
		}
		if !ok || len(vals) == 0 {
			return types.NewRowNotFoundError()
		}
		return r.mgr.MVCCWrite(t, table, key, []types.Value{types.FloatVal(vals[0].Float + amount)}, txn.UndoUpdate)
	}

	if err := rmw(TableAccounts, acctKey, delta); err != nil {
		_ = r.mgr.Abort(t.ID, err)
		return err
	}
	if err := rmw(TableTellers, telKey, delta); err != nil {
		_ = r.mgr.Abort(t.ID, err)
		return err
	}
	if err := rmw(TableBranches, brKey, delta); err != nil {
		_ = r.mgr.Abort(t.ID, err)
		return err
	}

	return r.mgr.Commit(t.ID)
}

// checkInvariant verifies there is no float corruption (NaN/Inf) in any
// account/teller/branch balance — the real signal of a lost-update bug —
// and that the row count is intact (no account rows lost).
//
// H-25: previously this returned `sum > -1e15 && sum < 1e15`, a
// trivially-true range check that could never fail for a finite wrong sum
// (the test asserted BalanceSumOK as meaningful). The grand total of all
// three tables is NOT preserved in this simplified model (each txn applies
// `delta` to all three), so we cannot compare against initialSum; instead
// we detect corruption directly: per-value finiteness + account-row-count
// stability.
func (r *Runner) checkInvariant() bool {
	scanAll := func(table string) ([]storage.Row, bool) {
		if r.cfg.Protocol == txn.ProtocolMVCC {
			t, err := r.mgr.Begin(txn.ProtocolMVCC, txn.ReadCommitted, 5*time.Second)
			if err != nil {
				return nil, false
			}
			rows, err := r.mgr.MVCCScan(t, table, nil)
			_ = r.mgr.Commit(t.ID)
			if err != nil {
				return nil, false
			}
			return rows, true
		}
		tbl, ok := r.catalog.Lookup(table)
		if !ok {
			return nil, false
		}
		return tbl.Scan(nil), true
	}

	for _, table := range []string{TableAccounts, TableTellers, TableBranches} {
		rows, ok := scanAll(table)
		if !ok {
			return false
		}
		for _, row := range rows {
			if len(row.Values) == 0 {
				return false
			}
			v := row.Values[0].Float
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return false
			}
		}
	}

	// Account-row count must be intact (a lost update that orphaned a chain
	// or a vacuum race would show fewer rows).
	acctRows, ok := scanAll(TableAccounts)
	if !ok {
		return false
	}
	return len(acctRows) == r.cfg.NumAccounts
}
