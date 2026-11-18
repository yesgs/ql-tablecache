package tablecache

import (
	"sync/atomic"
	"time"
)

// Metrics records runtime statistics for observability. All fields are safe
// for concurrent reads. Zero values indicate the metric has not been recorded yet.
type Metrics struct {
	// RefreshSuccess counts successful refresh cycles that loaded and published data.
	RefreshSuccess atomic.Int64

	// RefreshFailure counts failed refresh attempts. Transient failures that leave
	// the local snapshot intact still increment this counter.
	RefreshFailure atomic.Int64

	// LastRefreshTime is the Unix timestamp (in seconds) of the most recent successful
	// loader execution. Use this to detect stale data in time-sensitive applications.
	LastRefreshTime atomic.Int64

	// LastRefreshDuration is the duration in milliseconds of the most recent refresh cycle,
	// from acquiring the lease to publishing the snapshot.
	LastRefreshDuration atomic.Int64

	// SnapshotVersion is the SHA-256 hex digest of the current snapshot.
	// Empty string before the first snapshot is installed.
	SnapshotVersion atomic.Pointer[string]

	// SnapshotBytes is the serialized size of the current snapshot in bytes.
	SnapshotBytes atomic.Int64

	// SnapshotRows is the number of rows in the current snapshot.
	SnapshotRows atomic.Int64

	// PollAttempts counts Redis polling operations.
	PollAttempts atomic.Int64

	// PollErrors counts failed Redis polling operations.
	PollErrors atomic.Int64
}

// GetMetrics returns a snapshot of current metrics. The returned struct is
// a point-in-time copy and safe to read without synchronization.
func (m *Metrics) GetMetrics() MetricsSnapshot {
	version := ""
	if v := m.SnapshotVersion.Load(); v != nil {
		version = *v
	}
	return MetricsSnapshot{
		RefreshSuccess:      m.RefreshSuccess.Load(),
		RefreshFailure:      m.RefreshFailure.Load(),
		LastRefreshTime:     time.Unix(m.LastRefreshTime.Load(), 0),
		LastRefreshDuration: time.Duration(m.LastRefreshDuration.Load()) * time.Millisecond,
		SnapshotVersion:     version,
		SnapshotBytes:       m.SnapshotBytes.Load(),
		SnapshotRows:        m.SnapshotRows.Load(),
		PollAttempts:        m.PollAttempts.Load(),
		PollErrors:          m.PollErrors.Load(),
	}
}

// MetricsSnapshot is a point-in-time copy of runtime metrics.
type MetricsSnapshot struct {
	RefreshSuccess      int64
	RefreshFailure      int64
	LastRefreshTime     time.Time
	LastRefreshDuration time.Duration
	SnapshotVersion     string
	SnapshotBytes       int64
	SnapshotRows        int64
	PollAttempts        int64
	PollErrors          int64
}
