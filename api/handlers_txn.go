package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/sanskarpan/TransactionManager/internal/txn"
)

// raftLeaderCheck returns true and writes a 503 response when a Raft node is
// configured and this node is not the leader. Handlers should return immediately
// when this returns true.
func (s *Server) raftLeaderCheck(w http.ResponseWriter) bool {
	if s.Raft == nil {
		return false // Raft not enabled; all nodes serve writes directly.
	}
	if s.Raft.IsLeader() {
		return false // We are the leader; proceed.
	}
	leaderAddr := s.Raft.LeaderAddr()
	w.Header().Set("Content-Type", "application/json")
	if leaderAddr != "" {
		w.Header().Set("X-Raft-Leader", leaderAddr)
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":"not leader","leader":"` + leaderAddr + `"}`))
	return true
}

// BeginRequest is the JSON body for POST /api/txn/begin.
//
// LockTimeoutMs is in milliseconds; 0 (or omitted) selects the server
// default (DefaultLockTimeout). The unit is fixed as ms rather than
// seconds to align with Prometheus-style histogram buckets and to avoid
// the previous C-03 bug where the field name (`lockTimeoutMs`) and the
// frontend's `lockTimeoutSec` disagreed, causing the timeout to silently
// default or, once reconciled, be interpreted as 30 ms.
type BeginRequest struct {
	Protocol      string `json:"protocol"`
	Isolation     string `json:"isolation"`
	LockTimeoutMs int    `json:"lockTimeoutMs"`
}

// BeginResponse is the JSON response for POST /api/txn/begin.
type BeginResponse struct {
	ID        uint64 `json:"id"`
	Protocol  string `json:"protocol"`
	Isolation string `json:"isolation"`
	BeginAt   string `json:"beginAt"`
}

func (s *Server) handleBegin(w http.ResponseWriter, r *http.Request) {
	if s.raftLeaderCheck(w) {
		return
	}

	var req BeginRequest
	// H-07 + C-06: previously compared err.Error() != "EOF" by string;
	// now routed through decodeJSON which handles io.EOF, MaxBytesError,
	// and generic JSON errors with the right status (400 / 413).
	if status, msg := decodeJSON(r, &req, true); status != 0 {
		errResponse(w, status, msg)
		return
	}

	proto, err := protocolFromString(req.Protocol)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	iso, err := isolationFromString(req.Isolation)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	timeout := time.Duration(req.LockTimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = DefaultLockTimeout
	}
	// Reject nonsensical timeouts to fail closed (C-03: also ensures the
	// frontend cannot accidentally pin a transaction indefinitely).
	if timeout < 0 || timeout > MaxLockTimeout {
		errResponse(w, http.StatusBadRequest, "lockTimeoutMs out of range (0.."+strconv.Itoa(int(MaxLockTimeout/time.Millisecond))+")")
		return
	}

	// Early-exit hint before acquiring the manager lock; the real atomic
	// cap enforcement is inside Manager.Begin under m.mu.
	if s.txnMgr.ActiveCount() >= MaxActiveTxns {
		errResponse(w, http.StatusServiceUnavailable, "too many active transactions")
		return
	}

	// Raft path: allocate an ID on the leader, propose CmdBegin, and let the
	// FSM apply BeginWithID on all nodes (including this leader). The handler
	// can safely return the pre-allocated ID because Propose blocks until the
	// entry is committed and applied by the local FSM, so GetActiveTxn is
	// guaranteed to find the transaction after a successful ProposeBegin.
	if s.Raft != nil {
		txnID := s.txnMgr.AllocateTxnID()
		var protoU8, isoU8 uint8
		if proto == txn.ProtocolMVCC {
			protoU8 = 1
		}
		isoU8 = uint8(iso)
		if err := s.Raft.ProposeBegin(uint64(txnID), protoU8, isoU8); err != nil {
			s.logger.Error("raft propose begin failed", "err", err, "req_id", reqIDFromCtx(r))
			errWrite(w, err)
			return
		}
		// Propose blocks until the FSM has applied BeginWithID, so the txn
		// is guaranteed to be active here. A nil result would indicate an
		// FSM/apply-loop bug rather than a race.
		t := s.txnMgr.GetActiveTxn(txnID)
		if t == nil {
			s.logger.Error("raft begin: txn not active after successful propose", "txn_id", uint64(txnID), "req_id", reqIDFromCtx(r))
			errResponse(w, http.StatusInternalServerError, "internal error")
			return
		}
		s.sseBus.Publish(SSEEvent{Event: "txn_begin", Data: map[string]interface{}{
			"id":       uint64(txnID),
			"protocol": req.Protocol,
		}})
		okJSON(w, BeginResponse{
			ID:        uint64(txnID),
			Protocol:  req.Protocol,
			Isolation: req.Isolation,
			BeginAt:   t.BeginAt.Format(time.RFC3339Nano),
		})
		return
	}

	t, err := s.txnMgr.Begin(proto, iso, timeout)
	if err != nil {
		s.logger.Error("txn op failed", "err", err, "req_id", reqIDFromCtx(r))
		// ErrTooManyTransactions fires when concurrent goroutines race past the
		// pre-check above. Map it to 503 so the client gets the same status.
		if errors.Is(err, txn.ErrTooManyTransactions) {
			errResponse(w, http.StatusServiceUnavailable, "too many active transactions")
			return
		}
		errWrite(w, err)
		return
	}

	// H-19: do NOT bump TxnBegins here — the mgr.OnBegin callback (wired in
	// main.go) is the single source of truth. Bumping here double-counted
	// every transactional op, and the smoke tests masked it by asserting
	// GreaterOrEqual rather than exact values.

	s.sseBus.Publish(SSEEvent{Event: "txn_begin", Data: map[string]interface{}{
		"id":       uint64(t.ID),
		"protocol": req.Protocol,
	}})

	okJSON(w, BeginResponse{
		ID:        uint64(t.ID),
		Protocol:  req.Protocol,
		Isolation: req.Isolation,
		BeginAt:   t.BeginAt.Format(time.RFC3339Nano),
	})
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	if s.raftLeaderCheck(w) {
		return
	}

	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}

	// Raft path: propose CmdCommit and let the FSM apply mgr.Commit.
	if s.Raft != nil {
		if err := s.Raft.ProposeCommit(uint64(id)); err != nil {
			s.logger.Error("raft propose commit failed", "err", err, "req_id", reqIDFromCtx(r))
			errWrite(w, err)
			return
		}
		s.sseBus.Publish(SSEEvent{Event: "txn_commit", Data: map[string]interface{}{"id": uint64(id)}})
		okJSON(w, map[string]interface{}{"id": uint64(id), "status": "committed"})
		return
	}

	if err := s.txnMgr.Commit(id); err != nil {
		// CT-10: SSIAborts is now classified in the mgr.OnAbort callback
		// (the single source of truth), so it is correctly counted for
		// every system-initiated SSI abort, not just the handleCommit
		// path. H-19: do not bump TxnAborts here — OnAbort is the single
		// source of truth for real aborts. A commit-of-unknown-id (404)
		// or commit-of-aborted txn is not a fresh transactional abort and
		// must not inflate the abort counter.
		s.logger.Error("txn op failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	// H-19: TxnCommits is bumped by the mgr.OnCommit callback only.
	s.sseBus.Publish(SSEEvent{Event: "txn_commit", Data: map[string]interface{}{"id": uint64(id)}})
	okJSON(w, map[string]interface{}{"id": uint64(id), "status": "committed"})
}

func (s *Server) handleAbort(w http.ResponseWriter, r *http.Request) {
	if s.raftLeaderCheck(w) {
		return
	}

	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}

	// Raft path: propose CmdAbort and let the FSM apply mgr.Abort.
	if s.Raft != nil {
		if err := s.Raft.ProposeAbort(uint64(id)); err != nil {
			s.logger.Error("raft propose abort failed", "err", err, "req_id", reqIDFromCtx(r))
			errWrite(w, err)
			return
		}
		s.sseBus.Publish(SSEEvent{Event: "txn_abort", Data: map[string]interface{}{"id": uint64(id)}})
		okJSON(w, map[string]interface{}{"id": uint64(id), "status": "aborted"})
		return
	}

	if err := s.txnMgr.Abort(id, nil); err != nil {
		s.logger.Error("txn op failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}

	// H-19: TxnAborts is bumped by the mgr.OnAbort callback only.
	s.sseBus.Publish(SSEEvent{Event: "txn_abort", Data: map[string]interface{}{"id": uint64(id)}})
	okJSON(w, map[string]interface{}{"id": uint64(id), "status": "aborted"})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}

	t, ok := s.txnMgr.GetTxn(id)
	if !ok {
		if s.txnMgr.IsCommitted(id) {
			okJSON(w, map[string]interface{}{"id": uint64(id), "status": "committed"})
			return
		}
		if s.txnMgr.IsAborted(id) {
			okJSON(w, map[string]interface{}{"id": uint64(id), "status": "aborted"})
			return
		}
		errResponse(w, http.StatusNotFound, "transaction not found")
		return
	}

	status := "active"
	switch t.GetStatus() {
	case txn.TxnCommitted:
		status = "committed"
	case txn.TxnAborted:
		status = "aborted"
	}

	okJSON(w, map[string]interface{}{
		"id":      uint64(id),
		"status":  status,
		"beginAt": t.BeginAt.Format(time.RFC3339Nano),
	})
}

func (s *Server) handleActive(w http.ResponseWriter, _ *http.Request) {
	active := s.txnMgr.ActiveTxns()
	result := make([]map[string]interface{}, 0, len(active))
	for _, t := range active {
		result = append(result, map[string]interface{}{
			"id":      uint64(t.ID),
			"beginAt": t.BeginAt.Format(time.RFC3339Nano),
		})
	}
	okJSON(w, result)
}

// SavepointRequest is used for savepoint create and rollback operations.
type SavepointRequest struct {
	Name string `json:"name"`
}

func (s *Server) handleSavepoint(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	var req SavepointRequest
	if status, msg := decodeJSON(r, &req, false); status != 0 {
		errResponse(w, status, msg)
		return
	}
	if req.Name == "" {
		errResponse(w, http.StatusBadRequest, "missing savepoint name")
		return
	}
	if err := s.txnMgr.CreateSavepoint(id, req.Name); err != nil {
		s.logger.Error("savepoint failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}
	okJSON(w, map[string]interface{}{"name": req.Name, "created": true})
}

func (s *Server) handleRollbackTo(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	var req SavepointRequest
	if status, msg := decodeJSON(r, &req, false); status != 0 {
		errResponse(w, status, msg)
		return
	}
	if req.Name == "" {
		errResponse(w, http.StatusBadRequest, "missing savepoint name")
		return
	}
	if err := s.txnMgr.RollbackToSavepoint(id, req.Name); err != nil {
		s.logger.Error("rollback-to failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}
	okJSON(w, map[string]interface{}{"name": req.Name, "rolledBack": true})
}

func (s *Server) handleReleaseSavepoint(w http.ResponseWriter, r *http.Request) {
	id, err := parseTxnID(r)
	if err != nil {
		errResponse(w, http.StatusBadRequest, "invalid request")
		return
	}
	name := chi.URLParam(r, "name")
	if name == "" {
		errResponse(w, http.StatusBadRequest, "missing savepoint name")
		return
	}
	if err := s.txnMgr.ReleaseSavepoint(id, name); err != nil {
		s.logger.Error("txn op failed", "err", err, "req_id", reqIDFromCtx(r))
		errWrite(w, err)
		return
	}
	okJSON(w, map[string]interface{}{"name": name, "released": true})
}
