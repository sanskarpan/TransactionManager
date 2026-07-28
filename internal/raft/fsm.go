package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// FSM is the interface that node.applyLoop calls for each committed entry.
type FSM interface {
	Apply(entry Entry) error
	// Snapshot serializes FSM state to bytes for InstallSnapshot.
	Snapshot() ([]byte, error)
	// Restore replaces FSM state from a snapshot.
	Restore(data []byte) error
}

// snapshotRow holds a single row in a snapshot payload.
type snapshotRow struct {
	Key    string
	Values []byte // types.EncodeValues output
}

// snapshotPayload is the gob-encoded body of a Raft snapshot.
type snapshotPayload struct {
	TableRows map[string][]snapshotRow
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
		// Guard against nil After (delete operations pass no after-image).
		// DecodeValues requires at least 2 bytes; calling it with nil/empty
		// bytes causes a "buffer too short" error and breaks all Raft-path
		// deletes. Use nil vals directly when After is absent.
		var vals []types.Value
		if len(e.Command.After) > 0 {
			var err error
			vals, err = types.DecodeValues(e.Command.After)
			if err != nil {
				return err
			}
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

// Snapshot serializes all catalog table rows to a gob-encoded payload.
func (f *TxnManagerFSM) Snapshot() ([]byte, error) {
	payload := snapshotPayload{TableRows: make(map[string][]snapshotRow)}
	for _, name := range f.mgr.Catalog.List() {
		tbl, ok := f.mgr.Catalog.Lookup(name)
		if !ok {
			continue
		}
		rows := tbl.Scan(nil)
		srows := make([]snapshotRow, 0, len(rows))
		for _, r := range rows {
			enc := types.EncodeValues(r.Values)
			srows = append(srows, snapshotRow{Key: string(r.Key), Values: enc})
		}
		payload.TableRows[name] = srows
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return nil, fmt.Errorf("raft fsm snapshot encode: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces catalog table rows from a snapshot payload.
func (f *TxnManagerFSM) Restore(data []byte) error {
	var payload snapshotPayload
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&payload); err != nil {
		return fmt.Errorf("raft fsm snapshot decode: %w", err)
	}
	for name, srows := range payload.TableRows {
		tbl, ok := f.mgr.Catalog.Lookup(name)
		if !ok {
			continue
		}
		for _, sr := range srows {
			vals, err := types.DecodeValues(sr.Values)
			if err != nil {
				continue
			}
			tbl.PutRow(storage.RowKey(sr.Key), vals)
		}
	}
	return nil
}
