package tablecache

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestColdCacheCanReadWhenRedisRejectsWrites(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	warm := makeCache(t, r, func(context.Context) (map[int]string, error) {
		return map[int]string{1: "stable"}, nil
	})
	mustStep(t, warm)
	nextRound(t, warm, r)
	if err := r.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.ConfigSet(ctx, "maxmemory-policy", "noeviction").Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.ConfigSet(ctx, "maxmemory", "1").Err(); err != nil {
		t.Fatal(err)
	}
	cold := makeCache(t, r, func(context.Context) (map[int]string, error) {
		t.Fatal("loader ran without a lease")
		return nil, nil
	})
	if err := cold.step(ctx); err == nil || !strings.Contains(err.Error(), "acquire refresh lease: OOM") {
		t.Fatalf("expected a reported lease write failure, got %v", err)
	}
	readyCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := cold.WaitReady(readyCtx); err != nil {
		t.Fatal(err)
	}
	if value, ok := cold.Get(1); !ok || value != "stable" {
		t.Fatalf("read %q, %t", value, ok)
	}
}

func TestRefreshRepairsMissingAndCorruptBody(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	load := func(context.Context) (map[int]string, error) { return map[int]string{1: "stable"}, nil }
	warm := makeCache(t, r, load)
	mustStep(t, warm)
	stable := warm.Snapshot()
	key := warm.store.keys[0]
	for _, tc := range []struct {
		name string
		edit func() error
	}{
		{"missing", func() error { return r.HDel(ctx, key, "body").Err() }},
		{"same-length corruption", func() error { return r.HSet(ctx, key, "body", `{"1":"broken"}`).Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.edit(); err != nil {
				t.Fatal(err)
			}
			nextRound(t, warm, r)
			mustStep(t, warm)
			if warm.Snapshot() != stable {
				t.Fatal("repair rebuilt an unchanged local snapshot")
			}
			cold := makeCache(t, r, load)
			mustStep(t, cold)
			if value, ok := cold.Get(1); !ok || value != "stable" || cold.Snapshot().Version() != stable.Version() {
				t.Fatalf("did not load repaired snapshot: %q, %t", value, ok)
			}
		})
	}
}

var decodeStarted, resumeDecode chan struct{}

type blockingValue string

func (v *blockingValue) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "cached" {
		close(decodeStarted)
		<-resumeDecode
	}
	*v = blockingValue(s)
	return nil
}

func TestSlowSnapshotDecodeDoesNotHoldLease(t *testing.T) {
	r := testRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seed := makeCache(t, r, func(context.Context) (map[int]string, error) { return map[int]string{1: "cached"}, nil })
	mustStep(t, seed)
	nextRound(t, seed, r)

	decodeStarted, resumeDecode = make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(resumeDecode) })
	loads := 0
	slow, err := New(r, seed.opts, func(context.Context) (map[int]blockingValue, error) {
		loads++
		return map[int]blockingValue{1: "stale"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var stepErr error
	go func() {
		defer close(done)
		stepErr = slow.step(ctx)
	}()
	defer func() { resume(); <-done }()

	select {
	case <-decodeStarted:
	case <-ctx.Done():
		t.Fatal("snapshot decoding did not start")
	}
	fast := makeCache(t, r, func(context.Context) (map[int]string, error) { return map[int]string{1: "fresh"}, nil })
	mustStep(t, fast)
	if value, _ := fast.Get(1); value != "fresh" {
		t.Fatalf("slow decoder blocked the refresh, read %q", value)
	}
	resume()
	<-done
	if stepErr != nil || loads != 0 {
		t.Fatalf("slow cache ran its loader %d times, error %v", loads, stepErr)
	}
	if err := slow.step(ctx); err != nil {
		t.Fatal(err)
	}
	if value, _ := slow.Get(1); value != "fresh" {
		t.Fatalf("slow cache did not converge, read %q", value)
	}
}

type delayedAcquireClient struct {
	redis.Scripter
	afterAcquire func()
}

func (c delayedAcquireClient) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.Scripter.EvalSha(ctx, sha, keys, args...)
	if sha == acquireScript.Hash() {
		if won, err := cmd.Int64(); err == nil && won == 1 {
			c.afterAcquire()
		}
	}
	return cmd
}

func TestExpiredAcquireResponseDoesNotStartLoader(t *testing.T) {
	r := testRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := acquireScript.Load(ctx, r).Err(); err != nil {
		t.Fatal(err)
	}
	opts := Options{Namespace: "expired-acquire", LoadTimeout: 50 * time.Millisecond, RedisTimeout: 100 * time.Millisecond}
	c, err := New(delayedAcquireClient{r, func() {
		time.Sleep(opts.LoadTimeout + 2*opts.RedisTimeout + 10*time.Millisecond)
	}}, opts, func(context.Context) (map[int]string, error) {
		t.Fatal("loader ran after the lease expired")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.step(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected lease deadline, got %v", err)
	}
}

func TestPollLeavesOversizedBodyInRedis(t *testing.T) {
	r := testRedis(t)
	load := func(context.Context) (map[int]string, error) { return map[int]string{1: strings.Repeat("x", 300)}, nil }
	warm := makeCache(t, r, load)
	mustStep(t, warm)
	cold := makeCache(t, r, load)
	cold.opts.MaxBytes = 100
	ctx := context.Background()
	remote, err := cold.store.poll(ctx, "", cold.opts.MaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !remote.oversized || remote.body != "" {
		t.Fatal("poll returned an oversized body")
	}
	if err := cold.step(ctx); err == nil || !strings.Contains(err.Error(), "exceeds MaxBytes") || cold.Snapshot() != nil {
		t.Fatalf("expected the oversized snapshot to be rejected, got %v", err)
	}
}

type customKey string

func (k *customKey) UnmarshalText(text []byte) error {
	switch string(text) {
	case "left":
		*k = "right"
	case "right":
		*k = "left"
	default:
		*k = customKey(strings.ToLower(string(text)))
	}
	return nil
}

func TestRefreshRejectsLossyKeys(t *testing.T) {
	r := testRedis(t)
	ctx := context.Background()
	input := map[customKey]string{"a": "stable"}
	c, err := New(r, Options{Namespace: "custom-key"}, func(context.Context) (map[customKey]string, error) {
		return input, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.step(ctx); err != nil {
		t.Fatal(err)
	}
	old := c.Snapshot()
	for _, keys := range []map[customKey]string{
		{"A": "renamed", "a": "collision"},
		{"left": "first", "right": "second"},
	} {
		input = keys
		if err := r.Del(ctx, c.store.keys[1]).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.step(ctx); !errors.Is(err, errSourceKeyRoundTrip) {
			t.Fatalf("expected key validation to fail, got %v", err)
		}
		version, err := r.HGet(ctx, c.store.keys[0], "version").Result()
		if err != nil || version != old.Version() || c.Snapshot() != old {
			t.Fatalf("lossy keys changed the snapshot, Redis version %q, error %v", version, err)
		}
	}
}
