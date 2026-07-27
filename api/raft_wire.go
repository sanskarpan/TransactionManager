package api

import (
	"github.com/sanskarpan/TransactionManager/internal/raft"
)

// RaftAdapter wraps *raft.Node and exposes the RaftProposer interface to handlers.
type RaftAdapter struct {
	node *raft.Node
}

// NewRaftAdapter creates a new RaftAdapter wrapping the given Raft node.
func NewRaftAdapter(node *raft.Node) *RaftAdapter { return &RaftAdapter{node: node} }

// IsLeader reports whether this node is the current Raft leader.
func (a *RaftAdapter) IsLeader() bool { return a.node.IsLeader() }

// LeaderAddr returns the current known leader's node ID as a string address hint.
func (a *RaftAdapter) LeaderAddr() string { return string(a.node.LeaderID()) }

// ProposeBegin proposes a CmdBegin entry to the Raft cluster.
func (a *RaftAdapter) ProposeBegin(txnID uint64, proto, iso uint8) error {
	res := a.node.Propose(raft.Command{Type: raft.CmdBegin, TxnID: txnID, Proto: proto, Iso: iso})
	return res.Err
}

// ProposeWrite proposes a CmdWrite entry to the Raft cluster.
func (a *RaftAdapter) ProposeWrite(txnID uint64, table, key string, op uint8, before, after []byte) error {
	res := a.node.Propose(raft.Command{
		Type:   raft.CmdWrite,
		TxnID:  txnID,
		Table:  table,
		RowKey: key,
		Op:     op,
		Before: before,
		After:  after,
	})
	return res.Err
}

// ProposeCommit proposes a CmdCommit entry to the Raft cluster.
func (a *RaftAdapter) ProposeCommit(txnID uint64) error {
	res := a.node.Propose(raft.Command{Type: raft.CmdCommit, TxnID: txnID})
	return res.Err
}

// ProposeAbort proposes a CmdAbort entry to the Raft cluster.
func (a *RaftAdapter) ProposeAbort(txnID uint64) error {
	res := a.node.Propose(raft.Command{Type: raft.CmdAbort, TxnID: txnID})
	return res.Err
}
