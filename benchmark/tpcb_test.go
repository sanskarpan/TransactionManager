package benchmark

import (
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTPCB_BalanceInvariant_MVCC_RepeatableRead(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.Duration = 2 * time.Second
	cfg.Protocol = txn.ProtocolMVCC
	cfg.Isolation = txn.RepeatableRead
	cfg.NumAccounts = 20
	cfg.MaxRetries = 5

	runner := NewRunner(cfg, nil)
	result := runner.Run()

	t.Logf("TPC-B MVCC RR: committed=%d aborted=%d conflicts=%d TPS=%.0f",
		result.Committed, result.Aborted, result.Conflicts, result.TPS)

	require.Greater(t, result.Committed, int64(0), "at least some transactions must commit")
	assert.True(t, result.BalanceSumOK, "balance invariant must hold after concurrent run")
}

func TestTPCB_BalanceInvariant_MVCC_Serializable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.Duration = 2 * time.Second
	cfg.Protocol = txn.ProtocolMVCC
	cfg.Isolation = txn.Serializable
	cfg.NumAccounts = 20
	cfg.MaxRetries = 5

	runner := NewRunner(cfg, nil)
	result := runner.Run()

	t.Logf("TPC-B MVCC Serializable: committed=%d aborted=%d conflicts=%d TPS=%.0f",
		result.Committed, result.Aborted, result.Conflicts, result.TPS)

	require.Greater(t, result.Committed, int64(0), "at least some transactions must commit")
	assert.True(t, result.BalanceSumOK, "balance invariant must hold after concurrent run")
}

func TestTPCB_BalanceInvariant_2PL_ReadCommitted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.Duration = 2 * time.Second
	cfg.Protocol = txn.Protocol2PL
	cfg.Isolation = txn.ReadCommitted
	cfg.NumAccounts = 20
	cfg.MaxRetries = 5

	runner := NewRunner(cfg, nil)
	result := runner.Run()

	t.Logf("TPC-B 2PL RC: committed=%d aborted=%d deadlocks=%d TPS=%.0f",
		result.Committed, result.Aborted, result.Deadlocks, result.TPS)

	require.Greater(t, result.Committed, int64(0), "at least some transactions must commit")
	assert.True(t, result.BalanceSumOK, "balance invariant must hold")
}

func TestTPCB_AbortRate_Serializable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Concurrency = 8
	cfg.Duration = 3 * time.Second
	cfg.Protocol = txn.ProtocolMVCC
	cfg.Isolation = txn.Serializable
	cfg.NumAccounts = 100
	cfg.MaxRetries = 3

	runner := NewRunner(cfg, nil)
	result := runner.Run()

	t.Logf("TPC-B Serializable abort rate: %.1f%% (committed=%d aborted=%d)",
		float64(result.Aborted)/float64(result.TotalTxns)*100,
		result.Committed, result.Aborted)

	assert.True(t, result.BalanceSumOK, "balance invariant must hold even at high concurrency")
}
