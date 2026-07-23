// Package metrics provides atomic counters and a latency histogram for
// observing the transaction manager at runtime.
package metrics

import (
	"sync/atomic"
)

// Metrics holds all atomic counters for the transaction manager. Every
// field is an atomic.Int64 so concurrent incs are lock-free.
type Metrics struct {
	TxnBegins       atomic.Int64
	TxnCommits      atomic.Int64
	TxnAborts       atomic.Int64
	Deadlocks       atomic.Int64
	LockTimeouts    atomic.Int64
	WriteConflicts  atomic.Int64
	SSIAborts       atomic.Int64
	ReadsTotal      atomic.Int64
	WritesTotal     atomic.Int64
	ScansTotal      atomic.Int64
	VersionsCreated atomic.Int64
	VersionsPruned  atomic.Int64
	VacuumRuns      atomic.Int64

	// HTTPRequestsByClass tracks response counts by status class (1xx, 2xx,
	// 3xx, 4xx, 5xx) so operators can detect 5xx spikes and 4xx abuse waves
	// via /api/metrics (M-33: previously the latency middleware discarded the
	// status code). Indexed as status/100, with 0..5 supported.
	HTTPRequestsByClass [6]atomic.Int64
}

// Snapshot is a point-in-time copy of all metrics values.
type Snapshot struct {
	TxnBegins       int64 `json:"txnBegins"`
	TxnCommits      int64 `json:"txnCommits"`
	TxnAborts       int64 `json:"txnAborts"`
	Deadlocks       int64 `json:"deadlocks"`
	LockTimeouts    int64 `json:"lockTimeouts"`
	WriteConflicts  int64 `json:"writeConflicts"`
	SSIAborts       int64 `json:"ssiAborts"`
	ReadsTotal      int64 `json:"readsTotal"`
	WritesTotal     int64 `json:"writesTotal"`
	ScansTotal      int64 `json:"scansTotal"`
	VersionsCreated int64 `json:"versionsCreated"`
	VersionsPruned  int64 `json:"versionsPruned"`
	VacuumRuns      int64 `json:"vacuumRuns"`
	HTTP1xx         int64 `json:"http1xx"`
	HTTP2xx         int64 `json:"http2xx"`
	HTTP3xx         int64 `json:"http3xx"`
	HTTP4xx         int64 `json:"http4xx"`
	HTTP5xx         int64 `json:"http5xx"`
}

// Snapshot returns a consistent point-in-time copy of all metrics.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		TxnBegins:       m.TxnBegins.Load(),
		TxnCommits:      m.TxnCommits.Load(),
		TxnAborts:       m.TxnAborts.Load(),
		Deadlocks:       m.Deadlocks.Load(),
		LockTimeouts:    m.LockTimeouts.Load(),
		WriteConflicts:  m.WriteConflicts.Load(),
		SSIAborts:       m.SSIAborts.Load(),
		ReadsTotal:      m.ReadsTotal.Load(),
		WritesTotal:     m.WritesTotal.Load(),
		ScansTotal:      m.ScansTotal.Load(),
		VersionsCreated: m.VersionsCreated.Load(),
		VersionsPruned:  m.VersionsPruned.Load(),
		VacuumRuns:      m.VacuumRuns.Load(),
		HTTP1xx:         m.HTTPRequestsByClass[1].Load(),
		HTTP2xx:         m.HTTPRequestsByClass[2].Load(),
		HTTP3xx:         m.HTTPRequestsByClass[3].Load(),
		HTTP4xx:         m.HTTPRequestsByClass[4].Load(),
		HTTP5xx:         m.HTTPRequestsByClass[5].Load(),
	}
}

// RecordHTTPStatus bumps the status-class counter for the given status code
// (e.g. 503 → class 5). Classes are clamped to [0,5].
func (m *Metrics) RecordHTTPStatus(status int) {
	class := status / 100
	if class < 0 {
		class = 0
	}
	if class > 5 {
		class = 5
	}
	m.HTTPRequestsByClass[class].Add(1)
}

// Histogram tracks latency distribution in fixed buckets (milliseconds).
type Histogram struct {
	buckets []int64        // upper bounds in ms
	counts  []atomic.Int64 // len(buckets)+1 — last bucket catches overflow
	total   atomic.Int64
	sumUs   atomic.Int64 // sum of durations in microseconds
}

// NewHistogram creates a Histogram with the given upper-bound bucket boundaries (ms).
func NewHistogram(buckets []int64) *Histogram {
	h := &Histogram{buckets: buckets}
	h.counts = make([]atomic.Int64, len(buckets)+1)
	return h
}

// Record adds a duration (in milliseconds) to the histogram.
func (h *Histogram) Record(durationMs float64) {
	h.total.Add(1)
	h.sumUs.Add(int64(durationMs * 1000))
	for i, b := range h.buckets {
		if int64(durationMs) <= b {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(h.buckets)].Add(1)
}

// Mean returns the mean duration in milliseconds.
func (h *Histogram) Mean() float64 {
	t := h.total.Load()
	if t == 0 {
		return 0
	}
	return float64(h.sumUs.Load()) / float64(t) / 1000.0
}

// P50 returns the 50th-percentile latency in milliseconds (estimated).
func (h *Histogram) P50() float64 { return h.percentile(50) }

// P95 returns the 95th-percentile latency in milliseconds (estimated).
func (h *Histogram) P95() float64 { return h.percentile(95) }

// P99 returns the 99th-percentile latency in milliseconds (estimated).
func (h *Histogram) P99() float64 { return h.percentile(99) }

func (h *Histogram) percentile(p float64) float64 {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	target := int64(float64(total) * p / 100.0)
	var cum int64
	for i, b := range h.buckets {
		cum += h.counts[i].Load()
		if cum >= target {
			return float64(b)
		}
	}
	if len(h.buckets) > 0 {
		return float64(h.buckets[len(h.buckets)-1]) * 2
	}
	return 0
}
