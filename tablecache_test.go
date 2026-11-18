package tablecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Each integration test uses its own ephemeral Redis process on loopback.
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server executable required for this integration test")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "--bind", "127.0.0.1", "--port", port, "--save", "", "--appendonly", "no", "--dir", t.TempDir())
	cmd.Stdout, cmd.Stderr = io.Discard, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	r := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if err := r.Ping(ctx).Err(); err == nil {
			return r
		}
		if err := ctx.Err(); err != nil {
			t.Fatal("Redis startup:", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func makeCache(t *testing.T, r *redis.Client, load Loader[int, string]) *Cache[int, string] {
	t.Helper()
	c, err := New(r, Options{Namespace: "test:v1", PollInterval: 12 * time.Millisecond,
		RefreshInterval: time.Hour, RetryInterval: time.Minute}, load)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func mustStep(t *testing.T, c *Cache[int, string]) {
	t.Helper()
	if err := c.step(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func nextRound(t *testing.T, c *Cache[int, string], r *redis.Client) {
	t.Helper()
	if err := r.Del(context.Background(), c.store.keys[1]).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestFiftyInstancesOneLoad(t *testing.T) {
	r := testRedis(t)
	var calls atomic.Int32
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer func() { cancel(); wg.Wait() }()
	load := func(context.Context) (map[int]string, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		return map[int]string{7: "live"}, nil
	}
	const n = 50
	caches := make([]*Cache[int, string], n)
	for i := range caches {
		c := makeCache(t, r, load)
		caches[i] = c
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	for _, c := range caches {
		if err := c.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		if v, ok := c.Get(7); !ok || v != "live" {
			t.Fatalf("read %q, %t", v, ok)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times", calls.Load())
	}
	if err := caches[0].Run(ctx); err == nil {
		t.Fatal("duplicate Run accepted")
	}
}

func TestChangesDeletesEmptyAndOutage(t *testing.T) {
	r := testRedis(t)
	input := map[int]string{1: "a", 2: "b"}
	var loadErr error
	var calls int
	load := func(context.Context) (map[int]string, error) { calls++; return input, loadErr }
	leader, follower := makeCache(t, r, load), makeCache(t, r, load)
	mustStep(t, leader)
	mustStep(t, follower)
	old := follower.Snapshot()
	nextRound(t, leader, r)
	mustStep(t, leader)
	mustStep(t, follower)
	if old != follower.Snapshot() {
		t.Fatal("unchanged snapshot rebuilt")
	}
	input = map[int]string{2: "B", 3: "c"}
	nextRound(t, leader, r)
	mustStep(t, leader)
	mustStep(t, follower)
	if _, ok := follower.Get(1); ok {
		t.Fatal("deleted row survived")
	}
	if v, _ := follower.Get(2); v != "B" {
		t.Fatal(v)
	}
	if v, _ := old.Get(2); v != "b" {
		t.Fatal("previous snapshot changed")
	}

	loadErr = errors.New("source offline")
	nextRound(t, leader, r)
	if err := leader.step(context.Background()); !errors.Is(err, loadErr) {
		t.Fatal(err)
	}
	prevCalls := calls
	cold := makeCache(t, r, load)
	mustStep(t, cold)
	for i := 0; i < 3; i++ {
		mustStep(t, follower)
	}
	if v, _ := cold.Get(3); v != "c" || calls != prevCalls {
		t.Fatal("backoff or startup from Redis failed")
	}
	loadErr = nil
	input = map[int]string{}
	nextRound(t, leader, r)
	mustStep(t, leader)
	mustStep(t, follower)
	if follower.Snapshot().Len() != 0 || follower.Snapshot().Version() == "" {
		t.Fatal("empty snapshot not published")
	}
	stable := follower.Snapshot()
	_ = r.Close()
	if err := follower.step(context.Background()); err == nil || follower.Snapshot() != stable || calls != prevCalls+1 {
		t.Fatal("Redis failure changed local state or triggered source load")
	}
	unready := makeCache(t, r, load)
	if err := unready.step(context.Background()); err == nil {
		t.Fatal("cold start unexpectedly succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if !errors.Is(unready.WaitReady(ctx), context.DeadlineExceeded) {
		t.Fatal("cold instance became ready")
	}
}

func TestExpiredOwnerCannotOverwrite(t *testing.T) {
	r := testRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	a := makeCache(t, r, func(context.Context) (map[int]string, error) {
		close(entered)
		<-resume
		return map[int]string{1: "stale"}, nil
	})
	b := makeCache(t, r, func(context.Context) (map[int]string, error) { return map[int]string{1: "fresh"}, nil })
	done := make(chan error, 1)
	go func() { done <- a.step(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not start")
	}
	// End the real Redis lease before A's source query completes.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.PExpire(ctx, a.store.keys[1], time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	for {
		n, err := r.Exists(ctx, a.store.keys[1]).Result()
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mustStep(t, b)
	close(resume)
	if err := <-done; err == nil {
		t.Fatal("expired owner published")
	}
	if a.Snapshot() != nil {
		t.Fatal("expired owner installed unpublished data")
	}
	mustStep(t, a)
	if v, _ := a.Get(1); v != "fresh" {
		t.Fatal(v)
	}
}

func TestInvalidLoadKeepsPublishedSnapshot(t *testing.T) {
	r := testRedis(t)
	data := map[int]string{1: "a"}
	c := makeCache(t, r, func(context.Context) (map[int]string, error) { return data, nil })
	mustStep(t, c)
	old := c.Snapshot()
	for _, input := range []map[int]string{nil, {2: "xxxxxxxxxxxxxxxxxxxxx"}} {
		c.opts.MaxBytes = 16
		data = input
		nextRound(t, c, r)
		if err := c.step(context.Background()); err == nil || c.Snapshot() != old {
			t.Fatal("invalid load changed snapshot")
		}
	}
}

func TestConcurrentReadsDuringRefresh(t *testing.T) {
	r := testRedis(t)
	generation := 0
	c := makeCache(t, r, func(context.Context) (map[int]string, error) {
		generation++
		label := fmt.Sprint(generation)
		return map[int]string{1: label, 2: label}, nil
	})
	mustStep(t, c)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); wg.Wait() }()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				s := c.Snapshot()
				a, _ := s.Get(1)
				b, _ := s.Get(2)
				if a != b {
					t.Error("mixed generations")
					return
				}
			}
		}()
	}
	for i := 0; i < 15; i++ {
		nextRound(t, c, r)
		mustStep(t, c)
	}
}

func TestGet(t *testing.T) {
	c := new(Cache[int, string])
	if _, ok := c.Get(7); ok {
		t.Fatal("unexpected hit")
	}
	c.current.Store(&Snapshot[int, string]{rows: map[int]string{7: "x"}})
	if n := testing.AllocsPerRun(1000, func() {
		v, ok := c.Get(7)
		if !ok || v != "x" {
			panic("lookup failed")
		}
	}); n != 0 {
		t.Fatalf("Get allocated %v times", n)
	}
}

func TestNewRequiresRedisDeadlines(t *testing.T) {
	load := func(context.Context) (map[int]string, error) { return map[int]string{}, nil }
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })
	cluster := redis.NewClusterClient(&redis.ClusterOptions{})
	t.Cleanup(func() { _ = cluster.Close() })
	ring := redis.NewRing(&redis.RingOptions{})
	t.Cleanup(func() { _ = ring.Close() })
	noSocketDeadline := redis.NewClient(&redis.Options{ContextTimeoutEnabled: true, ReadTimeout: -2})
	t.Cleanup(func() { _ = noSocketDeadline.Close() })
	for _, tc := range []struct {
		name    string
		client  redis.Scripter
		message string
	}{
		{"client", client, "ContextTimeoutEnabled"},
		{"cluster", cluster, "ContextTimeoutEnabled"},
		{"ring", ring, "ContextTimeoutEnabled"},
		{"disabled deadline", noSocketDeadline, "socket deadlines"},
		{"nil client", (*redis.Client)(nil), "Redis client is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.client, Options{Namespace: "deadlines"}, load); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("expected %q, got %v", tc.message, err)
			}
		})
	}
}

func TestPollHonorsRedisTimeout(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	c, err := New(r, Options{Namespace: "redis-timeout", RedisTimeout: 30 * time.Millisecond}, func(context.Context) (map[int]string, error) {
		return map[int]string{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustStep(t, c)
	if err := r.ClientPause(ctx, 500*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = c.store.poll(ctx, c.Snapshot().Version(), c.opts.MaxBytes)
	if elapsed := time.Since(start); err == nil || elapsed >= 400*time.Millisecond {
		t.Fatalf("expected poll to time out while Redis was paused, elapsed %s, error %v", elapsed, err)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	c := new(Cache[int, int])
	rows := make(map[int]int, 65536)
	for i := 0; i < 65536; i++ {
		rows[i] = i
	}
	c.current.Store(&Snapshot[int, int]{rows: rows})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		k := 0
		for pb.Next() {
			if v, ok := c.Get(k); !ok || v != k {
				b.Fatal(k)
			}
			k = (k + 7919) & 65535
		}
	})
}

func TestRunRefreshesAfterCooldown(t *testing.T) {
	r := testRedis(t)
	var source atomic.Int32
	source.Store(1)
	opts := Options{Namespace: "rollover", PollInterval: 8 * time.Millisecond, RefreshInterval: 60 * time.Millisecond}
	load := func(context.Context) (map[int]int32, error) { return map[int]int32{1: source.Load()}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	var caches []*Cache[int, int32]
	for i := 0; i < 3; i++ {
		c, err := New(r, opts, load)
		if err != nil {
			t.Fatal(err)
		}
		caches = append(caches, c)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Run(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	for _, c := range caches {
		if err := c.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
	}
	source.Store(2)
	for _, c := range caches {
		for {
			if v, _ := c.Get(1); v == 2 {
				break
			}
			if err := ctx.Err(); err != nil {
				t.Fatal("update not visible:", err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}
