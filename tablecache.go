// Package tablecache distributes periodically refreshed tables to process-local,
// read-only snapshots. One loader per dataset normally runs in each refresh round.
package tablecache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Key admits integer and string types that preserve each key in a JSON round-trip.
type Key interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

// Loader returns the complete dataset. A non-nil empty map is a valid snapshot.
// It must honor ctx and finish constructing the map before returning it.
type Loader[K Key, V any] func(ctx context.Context) (map[K]V, error)

// Options are copied at construction. Zero durations and MaxBytes use defaults.
type Options struct {
	Namespace       string        // Unique to environment, dataset and schema version.
	RefreshInterval time.Duration // Cooldown after a successful load. Default 1 minute.
	PollInterval    time.Duration // Redis polling, with 20% jitter. Default 2 seconds.
	LoadTimeout     time.Duration // Budget for the source read and encoding. Default 15 seconds.
	RedisTimeout    time.Duration // Budget per Redis operation. The client must honor context deadlines. Default 2 seconds.
	RetryInterval   time.Duration // Shared cooldown after a source failure. Default 5 seconds.
	MaxBytes        int           // Maximum encoded snapshot size. Default 256 MiB.
	WarnBytes       int           // Soft limit for snapshot size. OnWarn is called when exceeded. Default 0 (disabled).
	OnError         func(error)   // Called in the worker; should return promptly.
	OnWarn          func(error)   // Called when snapshot size exceeds WarnBytes. Optional.
}

// Snapshot is immutable. Treat reference fields inside returned values as read-only.
// Multiple calls on the same Snapshot see a single generation. A nil Snapshot is empty.
type Snapshot[K Key, V any] struct {
	version string
	rows    map[K]V
}

func (s *Snapshot[K, V]) Get(key K) (V, bool) {
	if s == nil {
		var zero V
		return zero, false
	}
	v, ok := s.rows[key]
	return v, ok
}

func (s *Snapshot[K, V]) Len() int {
	if s == nil {
		return 0
	}
	return len(s.rows)
}

// Version is the SHA-256 of the serialized rows, or "" before the first snapshot.
func (s *Snapshot[K, V]) Version() string {
	if s == nil {
		return ""
	}
	return s.version
}

type Cache[K Key, V any] struct {
	current atomic.Pointer[Snapshot[K, V]]
	running atomic.Bool
	ready   chan struct{}
	force   chan struct{} // Signals a forced refresh request
	opts    Options
	loader  Loader[K, V]
	store   redisStore
	metrics Metrics
}

// New constructs a cache. The Redis client must honor context deadlines.
// With go-redis, enable ContextTimeoutEnabled and leave socket deadlines enabled.
func New[K Key, V any](client redis.Scripter, opts Options, loader Loader[K, V]) (*Cache[K, V], error) {
	if client == nil || loader == nil {
		return nil, errors.New("tablecache: Redis client and loader are required")
	}
	if err := validateRedisClient(client); err != nil {
		return nil, err
	}
	if strings.TrimSpace(opts.Namespace) == "" || strings.ContainsAny(opts.Namespace, "{}") {
		return nil, errors.New("tablecache: namespace must be nonempty and cannot contain braces")
	}
	for _, d := range []struct {
		value    *time.Duration
		fallback time.Duration
	}{
		{&opts.RefreshInterval, time.Minute}, {&opts.PollInterval, 2 * time.Second},
		{&opts.LoadTimeout, 15 * time.Second}, {&opts.RedisTimeout, 2 * time.Second},
		{&opts.RetryInterval, 5 * time.Second},
	} {
		if *d.value == 0 {
			*d.value = d.fallback
		}
		if *d.value < time.Millisecond {
			return nil, errors.New("tablecache: durations must be at least 1ms")
		}
	}
	if opts.RedisTimeout > (time.Duration(math.MaxInt64)-opts.LoadTimeout)/2 {
		return nil, errors.New("tablecache: LoadTimeout and RedisTimeout overflow the refresh lease duration")
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 256 << 20
	}
	if opts.MaxBytes < 2 {
		return nil, errors.New("tablecache: MaxBytes must be at least 2")
	}
	if opts.WarnBytes > 0 && opts.WarnBytes >= opts.MaxBytes {
		return nil, errors.New("tablecache: WarnBytes must be less than MaxBytes")
	}
	base := "tablecache:{" + opts.Namespace + "}"
	store := redisStore{client: client, keys: []string{base + ":snapshot", base + ":gate"}, timeout: opts.RedisTimeout}
	return &Cache[K, V]{ready: make(chan struct{}), force: make(chan struct{}, 1), opts: opts, loader: loader, store: store}, nil
}

// Get performs a process-local lookup. It returns ok=false until the cache is ready.
func (c *Cache[K, V]) Get(key K) (V, bool) { return c.current.Load().Get(key) }

func (c *Cache[K, V]) Snapshot() *Snapshot[K, V] { return c.current.Load() }

// Metrics returns a pointer to the cache's runtime metrics. Safe for concurrent access.
func (c *Cache[K, V]) Metrics() *Metrics { return &c.metrics }

// ForceRefresh triggers an immediate refresh attempt by clearing the Redis gate.
// The next polling cycle (within PollInterval) will attempt to acquire the lease.
// This is a hint, not a guarantee: if another instance is already refreshing,
// this instance will wait for that refresh to complete. Returns an error if
// Redis is unavailable. Safe to call concurrently.
func (c *Cache[K, V]) ForceRefresh(ctx context.Context) error {
	select {
	case c.force <- struct{}{}:
	default:
		// Already pending
	}
	return c.store.clearGate(ctx)
}

// WaitReady waits until a complete snapshot, including a valid empty one, is installed.
// An existing shared snapshot can satisfy readiness even while the source is down.
func (c *Cache[K, V]) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run refreshes until cancellation, returning nil on normal shutdown. Start one
// goroutine per Cache. Transient failures go to OnError and leave local reads available.
func (c *Cache[K, V]) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("tablecache: Run already active")
	}
	defer c.running.Store(false)
	for ctx.Err() == nil {
		if err := c.step(ctx); err != nil && ctx.Err() == nil && c.opts.OnError != nil {
			c.opts.OnError(fmt.Errorf("tablecache %q: %w", c.opts.Namespace, err))
		}
		p := c.opts.PollInterval
		timer := time.NewTimer(jitter(p, 20))
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-c.force:
			timer.Stop()
			// Immediate retry on force refresh
		case <-timer.C:
		}
	}
	return nil
}

// jitter returns base duration with ±percent random variation.
// For example, jitter(100ms, 20) returns a duration between 80ms and 120ms.
func jitter(base time.Duration, percent int) time.Duration {
	if percent <= 0 || percent >= 100 {
		return base
	}
	delta := base * time.Duration(percent) / 100
	return base - delta + rand.N(2*delta+1)
}

func (c *Cache[K, V]) install(s *Snapshot[K, V]) {
	if old := c.current.Swap(s); old == nil {
		close(c.ready)
	}
}
