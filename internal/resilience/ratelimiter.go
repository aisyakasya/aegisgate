package resilience

import (
	"sync"
	"sync/atomic"
	"time"
)

// BucketState represents a snapshot of a token bucket for telemetry and UI inspection.
type BucketState struct {
	Key             string    `json:"key"`
	AvailableTokens float64   `json:"available_tokens"`
	Capacity        float64   `json:"capacity"`
	RefillPerSec    float64   `json:"refill_per_sec"`
	AllowedCount    uint64    `json:"allowed_count"`
	RejectedCount   uint64    `json:"rejected_count"`
	LastAccess      time.Time `json:"last_access"`
}

type bucket struct {
	mu            sync.Mutex
	tokens        float64
	capacity      float64
	refillPerSec  float64
	lastRefill    time.Time
	allowedCount  uint64
	rejectedCount uint64
}

// TokenBucketLimiter implements a thread-safe per-key Token Bucket rate limiter
// with continuous sub-millisecond replenishment.
type TokenBucketLimiter struct {
	mu           sync.RWMutex
	enabled      atomic.Bool
	defaultCap   float64
	defaultRate  float64
	buckets      map[string]*bucket
	totalAllowed atomic.Uint64
	totalDenied  atomic.Uint64
}

// NewTokenBucketLimiter creates a rate limiter with the specified burst capacity and refill rate (tokens/sec).
func NewTokenBucketLimiter(capacity, refillPerSec float64, enabled bool) *TokenBucketLimiter {
	if capacity <= 0 {
		capacity = 50
	}
	if refillPerSec <= 0 {
		refillPerSec = 20
	}
	l := &TokenBucketLimiter{
		defaultCap:  capacity,
		defaultRate: refillPerSec,
		buckets:     make(map[string]*bucket, 64),
	}
	l.enabled.Store(enabled)
	return l
}

// Allow attempts to consume 1.0 token from the bucket identified by key.
// Returns (allowed, remainingTokens, retryAfterDuration).
func (l *TokenBucketLimiter) Allow(key string) (bool, float64, time.Duration) {
	if !l.enabled.Load() {
		l.totalAllowed.Add(1)
		return true, l.defaultCap, 0
	}

	b := l.getOrCreateBucket(key)
	now := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.refillPerSec
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.lastRefill = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		b.allowedCount++
		l.totalAllowed.Add(1)
		return true, b.tokens, 0
	}

	b.rejectedCount++
	l.totalDenied.Add(1)
	deficit := 1.0 - b.tokens
	waitSec := deficit / b.refillPerSec
	return false, b.tokens, time.Duration(waitSec * float64(time.Second))
}

// UpdateConfig dynamically updates rate-limiter parameters at runtime.
func (l *TokenBucketLimiter) UpdateConfig(capacity, refillPerSec float64, enabled bool) {
	l.enabled.Store(enabled)
	l.mu.Lock()
	defer l.mu.Unlock()
	if capacity > 0 {
		l.defaultCap = capacity
	}
	if refillPerSec > 0 {
		l.defaultRate = refillPerSec
	}
	for _, b := range l.buckets {
		b.mu.Lock()
		b.capacity = l.defaultCap
		b.refillPerSec = l.defaultRate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.mu.Unlock()
	}
}

// Snapshot returns the current configuration and per-bucket telemetry.
func (l *TokenBucketLimiter) Snapshot() (enabled bool, capacity, refillPerSec float64, allowed, denied uint64, states []BucketState) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	now := time.Now()
	states = make([]BucketState, 0, len(l.buckets))
	for k, b := range l.buckets {
		b.mu.Lock()
		elapsed := now.Sub(b.lastRefill).Seconds()
		curr := b.tokens + (elapsed * b.refillPerSec)
		if curr > b.capacity {
			curr = b.capacity
		}
		states = append(states, BucketState{
			Key:             k,
			AvailableTokens: curr,
			Capacity:        b.capacity,
			RefillPerSec:    b.refillPerSec,
			AllowedCount:    b.allowedCount,
			RejectedCount:   b.rejectedCount,
			LastAccess:      b.lastRefill,
		})
		b.mu.Unlock()
	}

	return l.enabled.Load(), l.defaultCap, l.defaultRate, l.totalAllowed.Load(), l.totalDenied.Load(), states
}

func (l *TokenBucketLimiter) getOrCreateBucket(key string) *bucket {
	l.mu.RLock()
	b, ok := l.buckets[key]
	l.mu.RUnlock()
	if ok {
		return b
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, exists := l.buckets[key]; exists {
		return existing
	}
	b = &bucket{
		tokens:       l.defaultCap,
		capacity:     l.defaultCap,
		refillPerSec: l.defaultRate,
		lastRefill:   time.Now(),
	}
	l.buckets[key] = b
	return b
}
