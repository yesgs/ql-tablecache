package tablecache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestForceRefresh(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()

	var calls int
	load := func(context.Context) (map[int]string, error) {
		calls++
		return map[int]string{calls: "value"}, nil
	}

	c := makeCache(t, r, load)
	mustStep(t, c)
	if calls != 1 {
		t.Fatalf("expected 1 load, got %d", calls)
	}

	// Normal poll should not trigger refresh (still in cooldown)
	if err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected no additional load during cooldown, got %d", calls)
	}

	// ForceRefresh clears the gate
	if err := c.ForceRefresh(ctx); err != nil {
		t.Fatal(err)
	}

	// Next poll should acquire lease and refresh
	if err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected force refresh to trigger load, got %d calls", calls)
	}

	if v, ok := c.Get(2); !ok || v != "value" {
		t.Fatalf("expected updated snapshot with key 2, got %v, %t", v, ok)
	}
}

func TestForceRefreshNonBlocking(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	c := makeCache(t, r, func(context.Context) (map[int]string, error) {
		return map[int]string{1: "a"}, nil
	})
	mustStep(t, c)

	// Multiple ForceRefresh calls should not block
	for i := 0; i < 10; i++ {
		if err := c.ForceRefresh(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Channel should have at most 1 pending signal
	select {
	case <-c.force:
		// Expected: one pending
	default:
		t.Fatal("expected at least one pending force signal")
	}

	// Should not have more
	select {
	case <-c.force:
		t.Fatal("channel should be buffered at 1")
	default:
		// Expected: no more signals
	}
}

func TestWarnBytes(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()

	var warnCalled bool
	var warnErr error

	c, err := New(r, Options{
		Namespace: "test:warn:v1",
		MaxBytes:  1000,
		WarnBytes: 100,
		OnWarn: func(err error) {
			warnCalled = true
			warnErr = err
		},
	}, func(context.Context) (map[int]string, error) {
		// Generate ~150 bytes
		return map[int]string{1: strings.Repeat("x", 50), 2: strings.Repeat("y", 50)}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.step(ctx); err != nil {
		t.Fatal(err)
	}

	if !warnCalled {
		t.Fatal("expected OnWarn to be called")
	}

	if warnErr == nil || !strings.Contains(warnErr.Error(), "exceeds WarnBytes") {
		t.Fatalf("expected warning about WarnBytes, got %v", warnErr)
	}

	// Verify snapshot was still published despite warning
	if c.Snapshot().Len() != 2 {
		t.Fatal("snapshot should be published despite warning")
	}
}

func TestWarnBytesValidation(t *testing.T) {
	r := testRedis(t)

	_, err := New(r, Options{
		Namespace: "test:invalid",
		MaxBytes:  100,
		WarnBytes: 100, // Equal to MaxBytes
	}, func(context.Context) (map[int]string, error) {
		return map[int]string{}, nil
	})

	if err == nil || !strings.Contains(err.Error(), "WarnBytes must be less than MaxBytes") {
		t.Fatalf("expected validation error, got %v", err)
	}

	_, err = New(r, Options{
		Namespace: "test:invalid2",
		MaxBytes:  100,
		WarnBytes: 150, // Greater than MaxBytes
	}, func(context.Context) (map[int]string, error) {
		return map[int]string{}, nil
	})

	if err == nil || !strings.Contains(err.Error(), "WarnBytes must be less than MaxBytes") {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestMetricsUpdated(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()

	generation := 1
	c := makeCache(t, r, func(context.Context) (map[int]string, error) {
		result := make(map[int]string)
		for i := 0; i < generation*10; i++ {
			result[i] = "value"
		}
		generation++
		return result, nil
	})

	// First refresh
	mustStep(t, c)

	m := c.Metrics().GetMetrics()
	if m.RefreshSuccess != 1 {
		t.Errorf("expected RefreshSuccess=1, got %d", m.RefreshSuccess)
	}
	if m.RefreshFailure != 0 {
		t.Errorf("expected RefreshFailure=0, got %d", m.RefreshFailure)
	}
	if m.SnapshotRows != 10 {
		t.Errorf("expected SnapshotRows=10, got %d", m.SnapshotRows)
	}
	if m.SnapshotBytes == 0 {
		t.Error("expected SnapshotBytes > 0")
	}
	if m.SnapshotVersion == "" {
		t.Error("expected non-empty SnapshotVersion")
	}
	if m.LastRefreshTime.IsZero() {
		t.Error("expected non-zero LastRefreshTime")
	}
	if m.PollAttempts == 0 {
		t.Error("expected PollAttempts > 0")
	}

	// Second refresh with more data
	nextRound(t, c, r)
	mustStep(t, c)

	m2 := c.Metrics().GetMetrics()
	if m2.RefreshSuccess != 2 {
		t.Errorf("expected RefreshSuccess=2, got %d", m2.RefreshSuccess)
	}
	if m2.SnapshotRows != 20 {
		t.Errorf("expected SnapshotRows=20, got %d", m2.SnapshotRows)
	}
	if m2.SnapshotBytes <= m.SnapshotBytes {
		t.Error("expected SnapshotBytes to increase")
	}
	if m2.SnapshotVersion == m.SnapshotVersion {
		t.Error("expected SnapshotVersion to change")
	}

	// Failed refresh
	nextRound(t, c, r)
	c.loader = func(context.Context) (map[int]string, error) {
		return nil, errors.New("simulated failure")
	}
	if err := c.step(ctx); err == nil {
		t.Fatal("expected step to fail")
	}

	m3 := c.Metrics().GetMetrics()
	if m3.RefreshFailure != 1 {
		t.Errorf("expected RefreshFailure=1, got %d", m3.RefreshFailure)
	}
	// Snapshot should remain unchanged
	if m3.SnapshotRows != m2.SnapshotRows {
		t.Error("expected SnapshotRows to remain unchanged after failure")
	}
}

func TestMetricsPollErrors(t *testing.T) {
	r := testRedis(t)
	c := makeCache(t, r, func(context.Context) (map[int]string, error) {
		return map[int]string{1: "a"}, nil
	})

	// Close Redis to cause poll errors
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = c.step(ctx)

	m := c.Metrics().GetMetrics()
	if m.PollErrors == 0 {
		t.Error("expected PollErrors > 0 after Redis closed")
	}
}

func TestIsReady(t *testing.T) {
	c := new(Cache[int, string])
	if c.IsReady() {
		t.Fatal("should not be ready initially")
	}
	c.current.Store(&Snapshot[int, string]{rows: map[int]string{}})
	if !c.IsReady() {
		t.Fatal("should be ready after snapshot installed")
	}
}

func TestClearLocalSnapshot(t *testing.T) {
	c := new(Cache[int, string])

	// Clear when empty
	if c.ClearLocalSnapshot() {
		t.Fatal("should not clear when empty")
	}
	if c.IsReady() {
		t.Fatal("should not be ready")
	}

	// Install snapshot
	c.current.Store(&Snapshot[int, string]{rows: map[int]string{1: "a"}})
	if !c.IsReady() {
		t.Fatal("should be ready")
	}

	// Clear when not empty
	if !c.ClearLocalSnapshot() {
		t.Fatal("should clear when not empty")
	}
	if c.IsReady() {
		t.Fatal("should not be ready after clear")
	}

	// Second clear should return false
	if c.ClearLocalSnapshot() {
		t.Fatal("second clear should return false")
	}
}

func TestSnapshotInfo(t *testing.T) {
	r := testRedis(t)

	c := makeCache(t, r, func(context.Context) (map[int]string, error) {
		return map[int]string{1: "a", 2: "b"}, nil
	})

	// Before ready
	info := c.SnapshotInfo()
	if info.Ready {
		t.Fatal("should not be ready initially")
	}
	if info.Version != "" {
		t.Fatal("should have empty version before ready")
	}
	if info.Rows != 0 {
		t.Fatal("should have 0 rows before ready")
	}

	// After refresh
	mustStep(t, c)
	info = c.SnapshotInfo()
	if !info.Ready {
		t.Fatal("should be ready after refresh")
	}
	if info.Version == "" {
		t.Fatal("should have non-empty version after refresh")
	}
	if info.Rows != 2 {
		t.Fatalf("expected 2 rows, got %d", info.Rows)
	}
	if info.Bytes == 0 {
		t.Fatal("expected non-zero bytes")
	}
	if info.StalenessSeconds < 0 {
		t.Fatal("staleness should be non-negative")
	}
	if info.StalenessSeconds > 1 {
		t.Logf("warning: staleness %d seconds is higher than expected", info.StalenessSeconds)
	}
}

func TestJitterInRun(t *testing.T) {


	r := testRedis(t)
	c, err := New(r, Options{
		Namespace:    "test:jitter:v1",
		PollInterval: 50 * time.Millisecond,
	}, func(context.Context) (map[int]string, error) {
		return map[int]string{1: "a"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	<-ctx.Done()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	// Should have polled multiple times
	m := c.Metrics().GetMetrics()
	if m.PollAttempts < 2 {
		t.Errorf("expected at least 2 poll attempts with jitter, got %d", m.PollAttempts)
	}
}
