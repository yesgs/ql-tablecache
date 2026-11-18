package tablecache

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Both keys share a cluster hash slot. Polling has no writes so a cold cache
// can read an existing snapshot even when Redis refuses writes.
var pollScript = redis.NewScript(`
local version = redis.call('HGET', KEYS[1], 'version') or ''
local body = ''
local oversized = 0
if version ~= '' and version ~= ARGV[1] then
    if redis.call('HSTRLEN', KEYS[1], 'body') > tonumber(ARGV[2]) then
        oversized = 1
    else
        body = redis.call('HGET', KEYS[1], 'body') or ''
    end
end
return {version, body, redis.call('EXISTS', KEYS[2]), oversized}`)

var acquireScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then return 1 end
if current then return 0 end
redis.call('PSETEX', KEYS[1], ARGV[2], ARGV[1])
return 1`)

// Redis uses its built-in SHA-1 to check the existing body against the source
// without transferring it. Snapshot versions and follower checks still use SHA-256.
var publishScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
if ARGV[3] ~= '' then
    redis.call('HSET', KEYS[1], 'version', ARGV[2], 'body', ARGV[3])
else
    if redis.call('HGET', KEYS[1], 'version') ~= ARGV[2] or
        redis.call('HSTRLEN', KEYS[1], 'body') ~= tonumber(ARGV[6]) then return -1 end
    if redis.sha1hex(redis.call('HGET', KEYS[1], 'body')) ~= ARGV[5] then return -1 end
end
redis.call('PSETEX', KEYS[2], ARGV[4], 'cooldown')
return 1`)

var backoffScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PSETEX', KEYS[1], ARGV[2], 'backoff')
end
return 1`)

type redisStore struct {
	client  redis.Scripter
	keys    []string
	timeout time.Duration
}

type pollResult struct {
	version, body string
	canAcquire    bool
	oversized     bool
}

func validateRedisClient(client redis.Scripter) error {
	switch c := client.(type) {
	case *redis.Client:
		if c != nil {
			opts := c.Options()
			return validateRedisDeadlines(opts.ContextTimeoutEnabled, opts.ReadTimeout >= 0 && opts.WriteTimeout >= 0)
		}
	case *redis.ClusterClient:
		if c != nil {
			opts := c.Options()
			return validateRedisDeadlines(opts.ContextTimeoutEnabled, opts.ReadTimeout >= 0 && opts.WriteTimeout >= 0)
		}
	case *redis.Ring:
		if c != nil {
			opts := c.Options()
			// Ring options have not converted the -1 sentinel to zero yet.
			return validateRedisDeadlines(opts.ContextTimeoutEnabled, opts.ReadTimeout >= -1 && opts.WriteTimeout >= -1)
		}
	default:
		return nil
	}
	return errors.New("tablecache: Redis client is required")
}

func validateRedisDeadlines(contextEnabled, socketEnabled bool) error {
	if !contextEnabled {
		return errors.New("tablecache: enable ContextTimeoutEnabled on the Redis client")
	}
	if !socketEnabled {
		return errors.New("tablecache: Redis client socket deadlines must be enabled")
	}
	return nil
}

func (s redisStore) poll(ctx context.Context, local string, maxBytes int) (pollResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	vals, err := pollScript.Run(ctx, s.client, s.keys, local, maxBytes).Slice()
	if err != nil {
		return pollResult{}, err
	}
	if len(vals) != 4 {
		return pollResult{}, errors.New("invalid Redis poll response length")
	}
	version, ok1 := vals[0].(string)
	body, ok2 := vals[1].(string)
	gateExists, ok3 := vals[2].(int64)
	oversized, ok4 := vals[3].(int64)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return pollResult{}, errors.New("invalid Redis poll response types")
	}
	return pollResult{version: version, body: body, canAcquire: gateExists == 0, oversized: oversized == 1}, nil
}

func (s redisStore) acquire(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	won, err := acquireScript.Run(ctx, s.client, s.keys[1:], token, ttl.Milliseconds()).Int64()
	return won == 1, err
}

func (s redisStore) publish(ctx context.Context, token, version string, body []byte, interval time.Duration, reuse bool) (int64, error) {
	payload := body
	var checksum string
	if reuse {
		payload = nil
		sum := sha1.Sum(body)
		checksum = hex.EncodeToString(sum[:])
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return publishScript.Run(ctx, s.client, s.keys, token, version, payload, interval.Milliseconds(), checksum, len(body)).Int64()
}

func (s redisStore) backoff(ctx context.Context, token string, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return backoffScript.Run(ctx, s.client, s.keys[1:], token, interval.Milliseconds()).Err()
}
