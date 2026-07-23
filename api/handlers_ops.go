package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/sanskarpan/TransactionManager/internal/storage"
	"github.com/sanskarpan/TransactionManager/internal/txn"
	"github.com/sanskarpan/TransactionManager/internal/types"
)

// RowOpRequest is the JSON body for read/write/scan operations.
type RowOpRequest struct {
	Table  string        `json:"table"`
	Key    string        `json:"key"`
	Values []types.Value `json:"values,omitempty"`
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	var req RowOpRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	var values []types.Value
	var found bool

	// H-08: tie in-flight lock acquisitions to the client connection so
	// a client disconnect cancels the wait rather than pinning locks.
	s.txnMgr.WithRequestCtx(r.Context(), id, func() {
		if t.Protocol == txn.ProtocolMVCC {
			values, found, err = s.txnMgr.MVCCRead(t, req.Table, req.Key)
		} else {
			values, found, err = s.txnMgr.TwoPLRead(t, req.Table, req.Key)
		}
	})

	if err != nil {
		s.logger.Error("read failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	if s.m != nil {
		s.m.ReadsTotal.Add(1)
	}

	okJSON(w, map[string]interface{}{
		"found":  found,
		"values": values,
	})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	var req RowOpRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	// CT-12/CT-13: validate row shape against the table's column
	// declarations before any write touches storage. The previous code
	// accepted any-length value slices and any-type values, so a single
	// value could be written to a 4-column table, silently corrupting
	// the row (returns "ok" with 1 value, but the row is now 1-col).
	if tbl, ok := s.txnMgr.Catalog.Lookup(req.Table); ok {
		if err := validateRowShape(tbl, req.Values); err != nil {
			// CT-22: route through errWrite (which uses the generic
			// ErrInvalidRowShape → "row shape does not match table
			// declaration" message) rather than errResponse with the
			// raw err.Error() (which leaks column names, counts, and
			// type codes). AGENTS.md mandates sanitized errors via
			// errWrite.
			errWrite(w, err)
			return
		}
	} else {
		errResponse(w, http.StatusNotFound, "table not found")
		return
	}

	s.txnMgr.WithRequestCtx(r.Context(), id, func() {
		if t.Protocol == txn.ProtocolMVCC {
			err = s.txnMgr.MVCCWrite(t, req.Table, req.Key, req.Values, txn.UndoUpdate)
		} else {
			err = s.txnMgr.TwoPLWrite(t, req.Table, req.Key, req.Values, txn.UndoUpdate)
		}
	})

	if err != nil {
		if s.m != nil {
			var txnErr *types.TxnError
			if errors.As(err, &txnErr) && txnErr.Code == types.ErrWriteConflict {
				s.m.WriteConflicts.Add(1)
			}
		}
		s.logger.Error("write failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	if s.m != nil {
		s.m.WritesTotal.Add(1)
	}

	okJSON(w, map[string]interface{}{"ok": true})
}

func (s *Server) handleInsert(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	var req RowOpRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	// CT-12/CT-13: validate row shape against the table's column
	// declarations before any insert touches storage. See handleWrite
	// (CT-22: route through errWrite for sanitized output).
	if tbl, ok := s.txnMgr.Catalog.Lookup(req.Table); ok {
		if err := validateRowShape(tbl, req.Values); err != nil {
			errWrite(w, err)
			return
		}
	} else {
		errResponse(w, http.StatusNotFound, "table not found")
		return
	}

	s.txnMgr.WithRequestCtx(r.Context(), id, func() {
		if t.Protocol == txn.ProtocolMVCC {
			err = s.txnMgr.MVCCWrite(t, req.Table, req.Key, req.Values, txn.UndoInsert)
		} else {
			err = s.txnMgr.TwoPLWrite(t, req.Table, req.Key, req.Values, txn.UndoInsert)
		}
	})

	if err != nil {
		s.logger.Error("insert failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	if s.m != nil {
		s.m.WritesTotal.Add(1)
	}
	okJSON(w, map[string]interface{}{"ok": true})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	var req RowOpRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	s.txnMgr.WithRequestCtx(r.Context(), id, func() {
		if t.Protocol == txn.ProtocolMVCC {
			err = s.txnMgr.MVCCWrite(t, req.Table, req.Key, nil, txn.UndoDelete)
		} else {
			err = s.txnMgr.TwoPLWrite(t, req.Table, req.Key, nil, txn.UndoDelete)
		}
	})

	if err != nil {
		s.logger.Error("delete failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	if s.m != nil {
		s.m.WritesTotal.Add(1)
	}
	okJSON(w, map[string]interface{}{"ok": true})
}

// ScanRequest for POST /api/txn/{id}/scan.
type ScanRequest struct {
	Table string `json:"table"`
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	var req ScanRequest
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	var rows []storage.Row
	var scanErr error
	s.txnMgr.WithRequestCtx(r.Context(), id, func() {
		if t.Protocol == txn.ProtocolMVCC {
			rows, scanErr = s.txnMgr.MVCCScan(t, req.Table, nil)
		} else {
			// 2PL scan via catalog (TwoPLScan handles gap locks; the
			// raw catalog.Scan path is retained for parity but does
			// not acquire gap locks).
			rows, scanErr = s.txnMgr.TwoPLScan(t, req.Table, nil)
		}
	})
	err = scanErr

	if err != nil {
		s.logger.Error("scan failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	if s.m != nil {
		s.m.ScansTotal.Add(1)
	}

	okJSON(w, map[string]interface{}{
		"rows":  rows,
		"count": len(rows),
	})
}

// validateRowShape rejects writes/inserts whose value slice does not
// match the table's column declarations (CT-12: row-length mismatch,
// CT-13: type mismatch). Returns nil for a valid row.
//
// Both the storage layer and the API now refuse to admit a structurally
// invalid row, so a buggy client cannot silently corrupt the catalog.
func validateRowShape(tbl *storage.Table, values []types.Value) error {
	if len(values) != len(tbl.Columns) {
		return types.NewRowShapeError(
			fmt.Sprintf("table %q has %d columns, got %d values", tbl.Name, len(tbl.Columns), len(values)),
		)
	}
	for i, col := range tbl.Columns {
		v := values[i]
		if v.Type != col.Type {
			return types.NewRowShapeError(
				fmt.Sprintf("type mismatch at column %d (%s): expected type %d, got type %d", i, col.Name, col.Type, v.Type),
			)
		}
	}
	return nil
}
