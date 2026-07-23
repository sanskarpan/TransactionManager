package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistogram_Mean_Empty(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	assert.Equal(t, float64(0), h.Mean())
}

func TestHistogram_Mean_WithValues(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	h.Record(20)
	h.Record(30)
	mean := h.Mean()
	assert.Greater(t, mean, 0.0)
}

func TestHistogram_P50(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	h.Record(30)
	h.Record(40)
	p50 := h.P50()
	assert.Equal(t, float64(50), p50)
}

func TestHistogram_P50_AllInFirst(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	h.Record(5)
	p50 := h.P50()
	assert.Equal(t, float64(10), p50)
}

func TestHistogram_Mean_ZeroTotal(t *testing.T) {
	h := NewHistogram([]int64{10})
	assert.Equal(t, float64(0), h.Mean())
}

func TestHistogram_Percentile_EmptyNoBuckets(t *testing.T) {
	h := NewHistogram(nil)
	assert.Equal(t, float64(0), h.P50())
	assert.Equal(t, float64(0), h.P95())
}

func TestHistogram_P50_SecondBucket(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	for i := 0; i < 100; i++ {
		h.Record(5)
	}
	p50 := h.P50()
	assert.Equal(t, float64(10), p50)
}

func TestHistogram_P95(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	for i := 0; i < 100; i++ {
		h.Record(5)
	}
	p95 := h.P95()
	assert.Equal(t, float64(10), p95)
}

func TestHistogram_P99_Empty(t *testing.T) {
	h := NewHistogram(nil)
	assert.Equal(t, float64(0), h.P99())
}

func TestHistogram_P99_WithBuckets(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})
	for i := 0; i < 100; i++ {
		h.Record(200)
	}
	p99 := h.P99()
	assert.Equal(t, float64(200), p99)
}

func TestHistogram_Record_Distributes(t *testing.T) {
	h := NewHistogram([]int64{10, 50, 100})

	h.Record(5)
	assert.Equal(t, int64(1), h.counts[0].Load())

	h.Record(30)
	assert.Equal(t, int64(1), h.counts[1].Load())

	h.Record(75)
	assert.Equal(t, int64(1), h.counts[2].Load())

	h.Record(200)
	assert.Equal(t, int64(1), h.counts[3].Load())
}

func TestMetrics_RecordHTTPStatus(t *testing.T) {
	m := &Metrics{}
	m.RecordHTTPStatus(200)
	assert.Equal(t, int64(1), m.HTTPRequestsByClass[2].Load())

	m2 := &Metrics{}
	m2.RecordHTTPStatus(404)
	assert.Equal(t, int64(1), m2.HTTPRequestsByClass[4].Load())

	m3 := &Metrics{}
	m3.RecordHTTPStatus(500)
	assert.Equal(t, int64(1), m3.HTTPRequestsByClass[5].Load())

	m4 := &Metrics{}
	m4.RecordHTTPStatus(-1)
	assert.Equal(t, int64(1), m4.HTTPRequestsByClass[0].Load())

	m5 := &Metrics{}
	m5.RecordHTTPStatus(999)
	assert.Equal(t, int64(1), m5.HTTPRequestsByClass[5].Load())
}

func TestMetrics_Snapshot(t *testing.T) {
	m := &Metrics{}
	m.TxnBegins.Add(10)
	m.TxnCommits.Add(7)
	m.TxnAborts.Add(3)

	snap := m.Snapshot()
	assert.Equal(t, int64(10), snap.TxnBegins)
	assert.Equal(t, int64(7), snap.TxnCommits)
	assert.Equal(t, int64(3), snap.TxnAborts)
}
