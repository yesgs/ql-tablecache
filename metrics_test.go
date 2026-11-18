package tablecache

import (
	"testing"
	"time"
)

func TestMetricsGetMetrics(t *testing.T) {
	var m Metrics
	m.RefreshSuccess.Store(10)
	m.RefreshFailure.Store(2)
	m.LastRefreshTime.Store(time.Unix(1000000, 0).Unix())
	m.LastRefreshDuration.Store(500)
	version := "abc123"
	m.SnapshotVersion.Store(&version)
	m.SnapshotBytes.Store(1024)
	m.SnapshotRows.Store(50)
	m.PollAttempts.Store(100)
	m.PollErrors.Store(5)

	snapshot := m.GetMetrics()
	if snapshot.RefreshSuccess != 10 {
		t.Errorf("expected RefreshSuccess=10, got %d", snapshot.RefreshSuccess)
	}
	if snapshot.RefreshFailure != 2 {
		t.Errorf("expected RefreshFailure=2, got %d", snapshot.RefreshFailure)
	}
	if snapshot.LastRefreshTime.Unix() != 1000000 {
		t.Errorf("expected LastRefreshTime=1000000, got %d", snapshot.LastRefreshTime.Unix())
	}
	if snapshot.LastRefreshDuration != 500*time.Millisecond {
		t.Errorf("expected LastRefreshDuration=500ms, got %v", snapshot.LastRefreshDuration)
	}
	if snapshot.SnapshotVersion != "abc123" {
		t.Errorf("expected SnapshotVersion=abc123, got %s", snapshot.SnapshotVersion)
	}
	if snapshot.SnapshotBytes != 1024 {
		t.Errorf("expected SnapshotBytes=1024, got %d", snapshot.SnapshotBytes)
	}
	if snapshot.SnapshotRows != 50 {
		t.Errorf("expected SnapshotRows=50, got %d", snapshot.SnapshotRows)
	}
	if snapshot.PollAttempts != 100 {
		t.Errorf("expected PollAttempts=100, got %d", snapshot.PollAttempts)
	}
	if snapshot.PollErrors != 5 {
		t.Errorf("expected PollErrors=5, got %d", snapshot.PollErrors)
	}
}

func TestMetricsGetMetricsEmptyVersion(t *testing.T) {
	var m Metrics
	snapshot := m.GetMetrics()
	if snapshot.SnapshotVersion != "" {
		t.Errorf("expected empty SnapshotVersion, got %s", snapshot.SnapshotVersion)
	}
}
