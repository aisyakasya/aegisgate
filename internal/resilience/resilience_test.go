package resilience

import (
	"errors"
	"testing"
	"time"
)

func TestTokenBucketLimiterBurstAndRefill(t *testing.T) {
	// Capacity of 3 tokens, refill rate of 20 tokens/sec (1 token every 50ms)
	limiter := NewTokenBucketLimiter(3, 20, true)

	for i := 0; i < 3; i++ {
		allowed, _, _ := limiter.Allow("client_ip_1")
		if !allowed {
			t.Fatalf("expected burst request %d to be allowed", i)
		}
	}

	// 4th immediate request should be rate-limited
	allowed, _, retryAfter := limiter.Allow("client_ip_1")
	if allowed {
		t.Fatalf("expected 4th request exceeding burst to be rejected")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected positive retryAfter duration, got %v", retryAfter)
	}

	// Wait 65ms so at least 1 token replenishes
	time.Sleep(65 * time.Millisecond)
	allowedAfterWait, _, _ := limiter.Allow("client_ip_1")
	if !allowedAfterWait {
		t.Fatalf("expected request after token replenishment to succeed")
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	reg := NewCircuitBreakerRegistry(BreakerConfig{
		FailureThreshold:  3,
		SuccessThreshold:  2,
		OpenCooldown:      60 * time.Millisecond,
		MaxHalfOpenProbes: 1,
	})

	route := "POST:/v1/embeddings"

	// Trip breaker with 3 consecutive failures
	for i := 0; i < 3; i++ {
		if _, err := reg.Acquire(route); err != nil {
			t.Fatalf("unexpected error before threshold on attempt %d: %v", i, err)
		}
		state := reg.RecordOutcome(route, false)
		if i == 2 && state != StateOpen {
			t.Fatalf("expected state OPEN after 3rd failure, got %s", state)
		}
	}

	// Subsequent call while OPEN must short-circuit with ErrCircuitOpen
	if _, err := reg.Acquire(route); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen while circuit is OPEN, got %v", err)
	}

	// Wait for cooldown to expire -> transitions to HALF_OPEN
	time.Sleep(75 * time.Millisecond)

	state, err := reg.Acquire(route)
	if err != nil || state != StateHalfOpen {
		t.Fatalf("expected HALF_OPEN probe allowed after cooldown, got state=%s err=%v", state, err)
	}

	// Concurrent second probe while first is in-flight must be rejected
	if _, err := reg.Acquire(route); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected concurrent probe beyond MaxHalfOpenProbes to be rejected")
	}

	// Record 2 consecutive probe successes -> transitions back to CLOSED
	reg.RecordOutcome(route, true)
	if _, err := reg.Acquire(route); err != nil {
		t.Fatalf("expected second HALF_OPEN probe allowed: %v", err)
	}
	finalState := reg.RecordOutcome(route, true)
	if finalState != StateClosed {
		t.Fatalf("expected circuit to close after 2 successful probes, got %s", finalState)
	}
}
