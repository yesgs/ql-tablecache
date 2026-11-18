package tablecache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (c *Cache[K, V]) decode(version string, body []byte) (*Snapshot[K, V], error) {
	var rows map[K]V
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, errors.New("snapshot must be a JSON object")
	}
	return &Snapshot[K, V]{version: version, rows: rows}, nil
}

func (c *Cache[K, V]) accept(remote pollResult) error {
	if remote.oversized || len(remote.body) > c.opts.MaxBytes {
		return errors.New("Redis snapshot exceeds MaxBytes")
	}
	body := []byte(remote.body)
	if digest(body) != remote.version {
		return errors.New("Redis snapshot checksum mismatch")
	}
	s, err := c.decode(remote.version, body)
	if err == nil {
		c.install(s)
	}
	return err
}

func (c *Cache[K, V]) step(ctx context.Context) error {
	c.metrics.PollAttempts.Add(1)
	local := c.Snapshot().Version()
	remote, err := c.store.poll(ctx, local, c.opts.MaxBytes)
	if err != nil {
		c.metrics.PollErrors.Add(1)
		return fmt.Errorf("poll: %w", err)
	}

	var readErr error
	if remote.version != "" && remote.version != local {
		readErr = c.accept(remote)
	}
	if !remote.canAcquire || ctx.Err() != nil {
		return readErr
	}

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.Join(readErr, err)
	}
	token := hex.EncodeToString(nonce[:])
	// Lease TTL = LoadTimeout + 2*RedisTimeout (one for acquire, one for publish)
	ttl := (c.opts.LoadTimeout + 2*c.opts.RedisTimeout).Truncate(time.Millisecond)
	// Start the deadline before Redis grants the lease and after local decoding.
	leaseDeadline := time.Now().Add(ttl)
	won, err := c.store.acquire(ctx, token, ttl)
	if err != nil {
		return errors.Join(readErr, fmt.Errorf("acquire refresh lease: %w", err))
	}
	if !won {
		return readErr
	}
	leaseCtx, cancel := context.WithDeadline(ctx, leaseDeadline)
	defer cancel()
	known := remote.version
	if readErr != nil {
		known = "" // Repair invalid Redis data on a successful source read.
	}
	refreshStart := time.Now()
	err = c.refresh(leaseCtx, token, known, leaseDeadline.Add(-c.opts.RedisTimeout))
	if err != nil {
		c.metrics.RefreshFailure.Add(1)
		if ctx.Err() == nil {
			// A lost response can leave publication ambiguous. Back off only if
			// this token still owns the gate on the active Redis primary.
			err = errors.Join(err, c.store.backoff(ctx, token, c.opts.RetryInterval))
		}
	} else {
		c.metrics.RefreshSuccess.Add(1)
		c.metrics.LastRefreshTime.Store(time.Now().Unix())
		c.metrics.LastRefreshDuration.Store(time.Since(refreshStart).Milliseconds())
	}
	return errors.Join(readErr, err)
}

func (c *Cache[K, V]) refresh(ctx context.Context, token, known string, loadDeadline time.Time) error {
	if timeout := time.Now().Add(c.opts.LoadTimeout); timeout.Before(loadDeadline) {
		loadDeadline = timeout
	}
	loadCtx, cancel := context.WithDeadline(ctx, loadDeadline)
	defer cancel()
	if err := loadCtx.Err(); err != nil {
		return fmt.Errorf("load: no time left in refresh lease: %w", err)
	}
	rows, err := c.loader(loadCtx)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	if rows == nil {
		return errors.New("loader returned a nil map; return an empty map for an empty dataset")
	}
	if err := loadCtx.Err(); err != nil {
		return err
	}
	body, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if len(body) > c.opts.MaxBytes {
		return fmt.Errorf("encoded snapshot has %d bytes; MaxBytes is %d", len(body), c.opts.MaxBytes)
	}
	if c.opts.WarnBytes > 0 && len(body) > c.opts.WarnBytes && c.opts.OnWarn != nil {
		c.opts.OnWarn(fmt.Errorf("snapshot size %d bytes exceeds WarnBytes %d", len(body), c.opts.WarnBytes))
	}
	version := digest(body)
	current := c.Snapshot()
	next := current
	if current.Version() != version {
		// Give the publisher the same representation and ownership as followers.
		next, err = c.decode(version, body)
		if err != nil {
			return fmt.Errorf("decode source snapshot: %w", err)
		}
	}
	if err := validateSourceKeys(rows, next.rows); err != nil {
		return err
	}
	if err := loadCtx.Err(); err != nil {
		return err
	}

	result, err := c.store.publish(ctx, token, version, body, c.opts.RefreshInterval, known == version)
	if err == nil && result == -1 {
		result, err = c.store.publish(ctx, token, version, body, c.opts.RefreshInterval, false)
	}
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	if result != 1 {
		return fmt.Errorf(
			"lease lost before publication; "+
				"this usually means LoadTimeout is too short for your query. "+
				"Current: %v. Try increasing it or optimize the database query. "+
				"See TROUBLESHOOTING.md section 7 'Lease expiration'",
			c.opts.LoadTimeout,
		)
	}
	if next != current {
		c.install(next)
	}
	// Update metrics after successful installation
	c.metrics.snapshotVersion.Store(version)
	c.metrics.SnapshotBytes.Store(int64(len(body)))
	c.metrics.SnapshotRows.Store(int64(len(next.rows)))
	return nil
}

var errSourceKeyRoundTrip = errors.New("source snapshot keys must survive a JSON round-trip")

func validateSourceKeys[K Key, V any](source, decoded map[K]V) error {
	if len(source) != len(decoded) {
		return errSourceKeyRoundTrip
	}
	var sample K
	_, customMarshal := any(sample).(encoding.TextMarshaler)
	_, customUnmarshal := any(&sample).(encoding.TextUnmarshaler)
	for key := range source {
		if _, ok := decoded[key]; !ok {
			return errSourceKeyRoundTrip
		}
		if customMarshal || customUnmarshal {
			// A custom codec can swap two keys without changing the key set.
			// Round-trip one key at a time to verify its mapping.
			encoded, err := json.Marshal(map[K]struct{}{key: {}})
			if err != nil {
				return errors.Join(errSourceKeyRoundTrip, err)
			}
			var roundTrip map[K]struct{}
			if err := json.Unmarshal(encoded, &roundTrip); err != nil {
				return errors.Join(errSourceKeyRoundTrip, err)
			}
			if _, ok := roundTrip[key]; !ok {
				return errSourceKeyRoundTrip
			}
		}
	}
	return nil
}
