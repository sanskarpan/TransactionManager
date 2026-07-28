package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"testing"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// noopFSM is an FSM that does nothing — suitable for election tests.
type noopFSM struct{}

func (n *noopFSM) Apply(_ Entry) error       { return nil }
func (n *noopFSM) Snapshot() ([]byte, error) { return []byte{}, nil }
func (n *noopFSM) Restore(_ []byte) error    { return nil }

// TestRaftLog_RoundTrip verifies that 5 entries survive a close/reopen cycle.
func TestRaftLog_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	l, err := NewRaftLog(dir)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}

	var entries []Entry
	for i := 1; i <= 5; i++ {
		entries = append(entries, Entry{
			Index: uint64(i),
			Term:  1,
			Command: Command{
				Type:  CmdNoop,
				TxnID: uint64(i),
			},
		})
	}
	if err := l.Append(entries...); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen.
	l2, err := NewRaftLog(dir)
	if err != nil {
		t.Fatalf("reopen NewRaftLog: %v", err)
	}
	defer l2.Close()

	if got := l2.LastIndex(); got != 5 {
		t.Errorf("LastIndex after reopen = %d, want 5", got)
	}
	for i := 1; i <= 5; i++ {
		e, ok := l2.Get(uint64(i))
		if !ok {
			t.Errorf("Get(%d) not found", i)
			continue
		}
		if e.Command.TxnID != uint64(i) {
			t.Errorf("entry %d: TxnID = %d, want %d", i, e.Command.TxnID, i)
		}
	}
}

// TestRaftLog_TruncateAfter verifies that truncation works correctly.
func TestRaftLog_TruncateAfter(t *testing.T) {
	dir := t.TempDir()
	l, err := NewRaftLog(dir)
	if err != nil {
		t.Fatalf("NewRaftLog: %v", err)
	}
	defer l.Close()

	for i := 1; i <= 5; i++ {
		if err := l.Append(Entry{Index: uint64(i), Term: 1, Command: Command{Type: CmdNoop}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := l.TruncateAfter(3); err != nil {
		t.Fatalf("TruncateAfter(3): %v", err)
	}

	if got := l.LastIndex(); got != 3 {
		t.Errorf("LastIndex after truncate = %d, want 3", got)
	}
	if _, ok := l.Get(4); ok {
		t.Error("entry 4 still exists after truncate")
	}
	if _, ok := l.Get(5); ok {
		t.Error("entry 5 still exists after truncate")
	}
	if _, ok := l.Get(3); !ok {
		t.Error("entry 3 missing after truncate")
	}
}

// TestNodeElection_SingleNode verifies that a single-node cluster becomes leader.
func TestNodeElection_SingleNode(t *testing.T) {
	dir := t.TempDir()
	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, &noopFSM{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	// Wait up to 1s for leadership.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !node.IsLeader() {
		t.Error("single-node cluster did not become leader within 1s")
	}
}

// TestNodeProposal_SingleNode verifies that a single node can propose and commit an entry.
func TestNodeProposal_SingleNode(t *testing.T) {
	dir := t.TempDir()
	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, &noopFSM{})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	// Wait for leadership.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("node did not become leader")
	}

	result := node.Propose(Command{Type: CmdNoop})
	if result.Err != nil {
		t.Errorf("Propose returned error: %v", result.Err)
	}
	if result.Index == 0 {
		t.Error("Propose returned zero index")
	}
}

// TestTransport_Encode verifies that a RequestVote frame can be encoded and decoded.
func TestTransport_Encode(t *testing.T) {
	args := RequestVoteArgs{
		Term:         42,
		CandidateID:  "node1",
		LastLogIndex: 10,
		LastLogTerm:  5,
	}

	frame, err := encodeFrame(MsgRequestVote, args)
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}

	if len(frame) < 5 {
		t.Fatalf("frame too short: %d", len(frame))
	}
	if frame[0] != MsgRequestVote {
		t.Errorf("msgType = %d, want %d", frame[0], MsgRequestVote)
	}

	// Decode the body.
	body := frame[5:]
	var decoded RequestVoteArgs
	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Term != args.Term {
		t.Errorf("Term = %d, want %d", decoded.Term, args.Term)
	}
	if decoded.CandidateID != args.CandidateID {
		t.Errorf("CandidateID = %q, want %q", decoded.CandidateID, args.CandidateID)
	}
	if decoded.LastLogIndex != args.LastLogIndex {
		t.Errorf("LastLogIndex = %d, want %d", decoded.LastLogIndex, args.LastLogIndex)
	}
}

// TestRaft3NodeCluster tests leader election and proposal in a 3-node cluster.
func TestRaft3NodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 3-node cluster test in short mode")
	}

	// Create transports on loopback with OS-assigned ports.
	addrs := make([]string, 3)
	transports := make([]*TCPTransport, 3)
	for i := range transports {
		tr, err := NewTCPTransport("127.0.0.1:0")
		if err != nil {
			t.Fatalf("transport %d: %v", i, err)
		}
		transports[i] = tr
		addrs[i] = tr.listener.Addr().String()
	}
	defer func() {
		for _, tr := range transports {
			_ = tr.Close()
		}
	}()

	ids := []NodeID{"node1", "node2", "node3"}
	nodes := make([]*Node, 3)
	for i := range nodes {
		peers := make(map[NodeID]string)
		for j, id := range ids {
			if j != i {
				peers[id] = addrs[j]
			}
		}
		dir, err := os.MkdirTemp("", "raft-cluster-*")
		if err != nil {
			t.Fatalf("tempdir: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })

		node, err := NewNode(ids[i], peers, dir, transports[i], &noopFSM{})
		if err != nil {
			t.Fatalf("NewNode %d: %v", i, err)
		}
		nodes[i] = node
	}

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	// Wait for leader election (up to 2s).
	var leader *Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.IsLeader() {
				leader = node
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected within 2s")
	}

	// Propose 5 CmdNoop entries.
	for i := 0; i < 5; i++ {
		result := leader.Propose(Command{Type: CmdNoop})
		if result.Err != nil {
			t.Errorf("proposal %d failed: %v", i, result.Err)
		}
	}

	// Allow time for commit propagation.
	time.Sleep(200 * time.Millisecond)

	// Verify commitIndex >= 5 on all nodes (no-op + 5 proposals = at least 6 entries).
	for i, node := range nodes {
		if ci := node.commitIndex.Load(); ci < 5 {
			t.Errorf("node %d commitIndex = %d, want >= 5", i, ci)
		}
	}
}

// TestRaftMode_SingleNode_Propose starts a single-node cluster using
// TxnManagerFSM and verifies that proposing CmdBegin causes the FSM to apply
// BeginWithID so the transaction is visible in the manager.
func TestRaftMode_SingleNode_Propose(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-single-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	catalog := storage.NewCatalog()
	mgr := txn.NewManager(catalog)
	fsm := NewTxnManagerFSM(mgr)

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, fsm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	// Wait for leadership (single-node cluster converges quickly).
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("single-node cluster did not become leader within 1s")
	}

	// Allocate a txn ID and propose CmdBegin.
	txnID := mgr.AllocateTxnID()
	cmd := Command{
		Type:  CmdBegin,
		TxnID: uint64(txnID),
		Proto: 0, // 2PL
		Iso:   0, // ReadCommitted
	}
	result := node.Propose(cmd)
	if result.Err != nil {
		t.Fatalf("Propose CmdBegin failed: %v", result.Err)
	}
	if result.Index == 0 {
		t.Error("Propose returned zero index")
	}

	// The FSM should have applied BeginWithID, so the txn is now active.
	activeTxn := mgr.GetActiveTxn(txnID)
	if activeTxn == nil {
		t.Fatalf("expected active txn %d after Propose(CmdBegin), got nil", txnID)
	}
	if activeTxn.ID != txnID {
		t.Errorf("txn ID mismatch: got %d, want %d", activeTxn.ID, txnID)
	}

	// Propose CmdAbort to clean up.
	abortResult := node.Propose(Command{Type: CmdAbort, TxnID: uint64(txnID)})
	if abortResult.Err != nil {
		t.Errorf("Propose CmdAbort failed: %v", abortResult.Err)
	}
}

// TestRaftMode_CmdWrite_Commit verifies the full write path through the FSM:
// CmdBegin → CmdWrite (insert) → CmdCommit all applied without error.
func TestRaftMode_CmdWrite_Commit(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-write-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	catalog := storage.NewCatalog()
	tbl := storage.NewTable("accounts", []storage.Column{
		{Name: "balance", Type: types.TypeInt},
	})
	catalog.Register(tbl)
	mgr := txn.NewManager(catalog)
	fsm := NewTxnManagerFSM(mgr)

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, fsm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("single-node cluster did not become leader within 1s")
	}

	// CmdBegin
	txnID := mgr.AllocateTxnID()
	if res := node.Propose(Command{Type: CmdBegin, TxnID: uint64(txnID), Proto: 0, Iso: 0}); res.Err != nil {
		t.Fatalf("CmdBegin failed: %v", res.Err)
	}

	// CmdWrite (insert) — non-nil values encode correctly
	vals := []types.Value{{Type: types.TypeInt, Int: 1000}}
	after := types.EncodeValues(vals)
	writeRes := node.Propose(Command{
		Type:   CmdWrite,
		TxnID:  uint64(txnID),
		Table:  "accounts",
		RowKey: "alice",
		Op:     uint8(txn.UndoInsert),
		After:  after,
	})
	if writeRes.Err != nil {
		t.Fatalf("CmdWrite failed: %v", writeRes.Err)
	}

	// CmdCommit
	if res := node.Propose(Command{Type: CmdCommit, TxnID: uint64(txnID)}); res.Err != nil {
		t.Fatalf("CmdCommit failed: %v", res.Err)
	}

	// Verify the committed data is visible in the table.
	row, found := tbl.GetRow(storage.RowKey("alice"))
	if !found {
		t.Fatal("row 'alice' not found in table after commit")
	}
	if len(row) == 0 || row[0].Int != 1000 {
		t.Errorf("row value mismatch: got %v, want [{Int:1000}]", row)
	}
}

// TestRaftMode_CmdWrite_Delete verifies that CmdWrite with nil After (delete)
// does not crash the FSM with "buffer too short". Regression test for the
// DecodeValues(nil) bug where handleDelete passed nil values to raftWrite.
func TestRaftMode_CmdWrite_Delete(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-delete-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	catalog := storage.NewCatalog()
	tbl := storage.NewTable("accounts", []storage.Column{
		{Name: "balance", Type: types.TypeInt},
	})
	catalog.Register(tbl)
	mgr := txn.NewManager(catalog)
	fsm := NewTxnManagerFSM(mgr)

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, fsm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("single-node cluster did not become leader within 1s")
	}

	// Seed a row directly so we have something to delete.
	tbl.PutRow(storage.RowKey("bob"), []types.Value{{Type: types.TypeInt, Int: 500}})

	// CmdBegin
	txnID := mgr.AllocateTxnID()
	if res := node.Propose(Command{Type: CmdBegin, TxnID: uint64(txnID), Proto: 0, Iso: 0}); res.Err != nil {
		t.Fatalf("CmdBegin failed: %v", res.Err)
	}

	// CmdWrite (delete) — After is nil, must not crash with "buffer too short".
	deleteRes := node.Propose(Command{
		Type:   CmdWrite,
		TxnID:  uint64(txnID),
		Table:  "accounts",
		RowKey: "bob",
		Op:     uint8(txn.UndoDelete),
		After:  nil, // delete: no after-image
	})
	if deleteRes.Err != nil {
		t.Fatalf("CmdWrite (delete) failed: %v", deleteRes.Err)
	}

	// CmdCommit
	if res := node.Propose(Command{Type: CmdCommit, TxnID: uint64(txnID)}); res.Err != nil {
		t.Fatalf("CmdCommit failed: %v", res.Err)
	}
}

// TestRaftMode_FSMError_Propagated verifies that FSM errors are propagated
// back to the Propose caller (not silently discarded) so the HTTP handler
// receives the error instead of a false 200 OK.
func TestRaftMode_FSMError_Propagated(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft-fsmerr-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	transport, err := NewTCPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatalf("transport: %v", err)
	}
	defer transport.Close()

	catalog := storage.NewCatalog()
	mgr := txn.NewManager(catalog)
	fsm := NewTxnManagerFSM(mgr)

	node, err := NewNode("node1", map[NodeID]string{}, dir, transport, fsm)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if node.IsLeader() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("single-node cluster did not become leader within 1s")
	}

	// Propose CmdWrite for a txn that does not exist — FSM returns an error.
	res := node.Propose(Command{
		Type:   CmdWrite,
		TxnID:  9999,
		Table:  "accounts",
		RowKey: "x",
		Op:     uint8(txn.UndoInsert),
		After:  types.EncodeValues([]types.Value{{Type: types.TypeInt, Int: 1}}),
	})
	if res.Err == nil {
		t.Fatal("expected FSM error for write on non-existent txn, got nil")
	}
}
