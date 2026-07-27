package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/sanskarpan/TransactionManager/internal/lock"
)

// handleLocks returns all currently active lock queues.
func (s *Server) handleLocks(w http.ResponseWriter, _ *http.Request) {
	lt := s.txnMgr.LockTable
	if lt == nil {
		okJSON(w, []interface{}{})
		return
	}
	snaps := lt.Snapshot()

	type reqSnap struct {
		TxnID     uint64 `json:"txnId"`
		Mode      string `json:"mode"`
		Status    string `json:"status"`
		RequestAt string `json:"requestAt"`
		GrantedAt string `json:"grantedAt,omitempty"`
	}
	type queueSnap struct {
		Resource string    `json:"resource"`
		Granted  []reqSnap `json:"granted"`
		Waiting  []reqSnap `json:"waiting"`
	}

	result := make([]queueSnap, 0, len(snaps))
	for _, snap := range snaps {
		qs := queueSnap{Resource: snap.Resource.String()}
		for _, g := range snap.Granted {
			entry := reqSnap{
				TxnID:     g.TxnID,
				Mode:      g.Mode.String(),
				Status:    g.Status.String(),
				RequestAt: g.RequestAt.Format(time.RFC3339),
			}
			if !g.GrantedAt.IsZero() {
				entry.GrantedAt = g.GrantedAt.Format(time.RFC3339)
			}
			qs.Granted = append(qs.Granted, entry)
		}
		for _, w := range snap.Waiting {
			qs.Waiting = append(qs.Waiting, reqSnap{
				TxnID:     w.TxnID,
				Mode:      w.Mode.String(),
				Status:    w.Status.String(),
				RequestAt: w.RequestAt.Format(time.RFC3339),
			})
		}
		result = append(result, qs)
	}
	okJSON(w, result)
}

// handleLockByResource returns the lock queue for a specific table/key.
func (s *Server) handleLockByResource(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	key := chi.URLParam(r, "key")
	lt := s.txnMgr.LockTable
	if lt == nil {
		okJSON(w, map[string]interface{}{"resource": fmt.Sprintf("row:%s:%s", table, key), "granted": []interface{}{}, "waiting": []interface{}{}})
		return
	}
	res := lock.ResourceForRow(table, key)
	q, ok := lt.Get(res)
	if !ok {
		okJSON(w, map[string]interface{}{"resource": res.String(), "granted": []interface{}{}, "waiting": []interface{}{}})
		return
	}
	snap := q.Snapshot()
	type reqSnap struct {
		TxnID     uint64 `json:"txnId"`
		Mode      string `json:"mode"`
		Status    string `json:"status"`
		RequestAt string `json:"requestAt"`
		GrantedAt string `json:"grantedAt,omitempty"`
	}
	granted := make([]reqSnap, 0, len(snap.Granted))
	for _, g := range snap.Granted {
		entry := reqSnap{TxnID: g.TxnID, Mode: g.Mode.String(), Status: g.Status.String(), RequestAt: g.RequestAt.Format(time.RFC3339)}
		if !g.GrantedAt.IsZero() {
			entry.GrantedAt = g.GrantedAt.Format(time.RFC3339)
		}
		granted = append(granted, entry)
	}
	waiting := make([]reqSnap, 0, len(snap.Waiting))
	for _, w2 := range snap.Waiting {
		waiting = append(waiting, reqSnap{TxnID: w2.TxnID, Mode: w2.Mode.String(), Status: w2.Status.String(), RequestAt: w2.RequestAt.Format(time.RFC3339)})
	}
	okJSON(w, map[string]interface{}{"resource": snap.Resource.String(), "granted": granted, "waiting": waiting})
}

// handleSSEWFG streams the wait-for graph as SSE events (500ms tick).
func (s *Server) handleSSEWFG(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// CT-04: track the WFG SSE handler on sseWg so Server.Shutdown can
	// drain it explicitly in addition to /sse/events handlers.
	s.sseWg.Add(1)
	defer s.sseWg.Done()

	// M-31: was hardcoded 500ms; now references the named constant.
	ticker := time.NewTicker(WFGStreamTickInterval)
	defer ticker.Stop()

	reqID := reqIDFromCtx(r)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.sseCtx.Done():
			return
		case <-ticker.C:
			if s.wfg == nil {
				continue
			}
			nodes := s.wfg.Nodes()
			edges := s.wfg.Edges()
			type edge struct {
				From uint64 `json:"from"`
				To   uint64 `json:"to"`
			}
			edgeList := make([]edge, len(edges))
			for i, e := range edges {
				edgeList[i] = edge{From: e[0], To: e[1]}
			}
			nodeList := make([]uint64, len(nodes))
			copy(nodeList, nodes)
			data, err := json.Marshal(map[string]interface{}{"nodes": nodeList, "edges": edgeList})
			if err != nil {
				// L-63: was silently dropped.
				s.logger.Warn("sse wfg marshal failed", "err", err, "req_id", reqID)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				s.logger.Warn("sse wfg write failed", "err", err, "req_id", reqID)
				return
			}
			flusher.Flush()
		}
	}
}

// handleReset aborts all active transactions and reseeds the database.
func (s *Server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.txnMgr.Reset()
	okJSON(w, map[string]interface{}{"ok": true, "message": "database reset and reseeded"})
}

// handleDeadlocks returns the recent deadlock history.
func (s *Server) handleDeadlocks(w http.ResponseWriter, _ *http.Request) {
	if s.dlHistory == nil {
		okJSON(w, []interface{}{})
		return
	}
	records := s.dlHistory.All()
	type deadlockEntry struct {
		ID           string   `json:"id"`
		DetectedAt   string   `json:"detectedAt"`
		Cycle        []uint64 `json:"cycle"`
		Victim       uint64   `json:"victim"`
		VictimReason string   `json:"victimReason"`
	}
	result := make([]deadlockEntry, 0, len(records))
	for _, rec := range records {
		cycle := make([]uint64, len(rec.Cycle))
		copy(cycle, rec.Cycle)
		result = append(result, deadlockEntry{
			ID:           rec.ID,
			DetectedAt:   rec.DetectedAt.String(),
			Cycle:        cycle,
			Victim:       rec.Victim,
			VictimReason: rec.VictimReason,
		})
	}
	okJSON(w, result)
}

// handleWFG returns the current wait-for graph as an edge list.
func (s *Server) handleWFG(w http.ResponseWriter, _ *http.Request) {
	if s.wfg == nil {
		okJSON(w, map[string]interface{}{"nodes": []interface{}{}, "edges": []interface{}{}})
		return
	}
	nodes := s.wfg.Nodes()
	edges := s.wfg.Edges()

	type edge struct {
		From uint64 `json:"from"`
		To   uint64 `json:"to"`
	}
	edgeList := make([]edge, len(edges))
	for i, e := range edges {
		edgeList[i] = edge{From: e[0], To: e[1]}
	}
	nodeList := make([]uint64, len(nodes))
	copy(nodeList, nodes)
	okJSON(w, map[string]interface{}{"nodes": nodeList, "edges": edgeList})
}

// handleVersionChain returns the version chain for a specific row.
func (s *Server) handleVersionChain(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	key := chi.URLParam(r, "key")

	if s.txnMgr.MVCCStore == nil {
		errResponse(w, http.StatusServiceUnavailable, "mvcc store not initialized")
		return
	}

	chain, ok := s.txnMgr.MVCCStore.GetChain(table, key)
	if !ok {
		errResponse(w, http.StatusNotFound, "version chain not found")
		return
	}

	versions := chain.All()
	type versionEntry struct {
		XMin uint64        `json:"xmin"`
		XMax uint64        `json:"xmax"`
		Data []interface{} `json:"data"`
	}
	result := make([]versionEntry, len(versions))
	for i, v := range versions {
		data := make([]interface{}, len(v.Data))
		for j, val := range v.Data {
			data[j] = val.String()
		}
		result[i] = versionEntry{XMin: v.XMin, XMax: v.XMax, Data: data}
	}
	okJSON(w, map[string]interface{}{"table": table, "key": key, "versions": result})
}

// handleMVCCStats returns vacuum and MVCC statistics.
func (s *Server) handleMVCCStats(w http.ResponseWriter, _ *http.Request) {
	if s.vacuum == nil {
		okJSON(w, map[string]interface{}{"note": "vacuum not configured"})
		return
	}
	stats := s.vacuum.Stats()
	okJSON(w, map[string]interface{}{
		"totalChains":   stats.TotalChains,
		"totalVersions": stats.TotalVersions,
		"pruned":        stats.Pruned,
		"vacuumRuns":    stats.Runs,
		"lastRunAt":     stats.LastRunAt.String(),
	})
}

// handleVacuum triggers a manual vacuum run.
func (s *Server) handleVacuum(w http.ResponseWriter, _ *http.Request) {
	if s.vacuum == nil {
		errResponse(w, http.StatusServiceUnavailable, "vacuum not configured")
		return
	}
	s.vacuum.RunOnce()
	if s.m != nil {
		s.m.VacuumRuns.Add(1)
	}
	okJSON(w, map[string]interface{}{"ok": true, "message": "vacuum completed"})
}

// handleMetrics returns a snapshot of all metrics.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	if s.m == nil {
		okJSON(w, map[string]interface{}{})
		return
	}
	okJSON(w, s.m.Snapshot())
}
