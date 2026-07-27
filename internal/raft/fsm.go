package raft

import (
	"fmt"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// FSM is the interface that node.applyLoop calls for each committed entry.
type FSM interface {
	Apply(entry Entry) error
}

// TxnManagerFSM applies committed Raft entries to txn.Manager.
type TxnManagerFSM struct {
	mgr *txn.Manager
}

// NewTxnManagerFSM creates a new TxnManagerFSM.
func NewTxnManagerFSM(mgr *txn.Manager) *TxnManagerFSM {
	return &TxnManagerFSM{mgr: mgr}
}

// Apply applies a committed Raft entry to the transaction manager.
func (f *TxnManagerFSM) Apply(e Entry) error {
	switch e.Command.Type {
	case CmdNoop:
		return nil

	case CmdBegin:
		proto := txn.Protocol2PL
		if e.Command.Proto == 1 {
			proto = txn.ProtocolMVCC
		}
		iso := txn.IsolationLevel(e.Command.Iso)
		_, err := f.mgr.BeginWithID(txn.ID(e.Command.TxnID), proto, iso, 30*time.Second)
		return err

	case CmdWrite:
		t := f.mgr.GetActiveTxn(txn.ID(e.Command.TxnID))
		if t == nil {
			return fmt.Errorf("raft fsm: txn %d not found for write", e.Command.TxnID)
		}
		vals, err := types.DecodeValues(e.Command.After)
		if err != nil {
			return err
		}
		proto := t.Protocol
		if proto == txn.Protocol2PL {
			return f.mgr.TwoPLWrite(t, e.Command.Table, e.Command.RowKey,
				vals, txn.UndoOpType(e.Command.Op))
		}
		return f.mgr.MVCCWrite(t, e.Command.Table, e.Command.RowKey,
			vals, txn.UndoOpType(e.Command.Op))

	case CmdCommit:
		return f.mgr.Commit(txn.ID(e.Command.TxnID))

	case CmdAbort:
		return f.mgr.Abort(txn.ID(e.Command.TxnID), nil)
	}

	return nil
}
