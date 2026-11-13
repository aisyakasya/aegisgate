package resilience

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned when a route's circuit breaker is in the OPEN state
// or when HALF_OPEN probe concurrency is exhausted.
var ErrCircuitOpen = errors.New("resilience: circuit breaker is open")

// State represents the finite-state-machine status of a CircuitBreaker.
type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

// BreakerConfig defines the thresholds for tripping and recovering a CircuitBreaker.
type BreakerConfig struct {
	FailureThreshold  int           `json:"failure_threshold"`
	SuccessThreshold  int           `json:"success_threshold"`
	OpenCooldown      time.Duration `json:"open_cooldown_ns"`
	MaxHalfOpenProbes int           `json:"max_half_open_probes"`
}

// DefaultBreakerConfig returns sensible defaults for upstream service protection.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureThreshold:  5,
		SuccessThreshold:  2,
		OpenCooldown:      10 * time.Second,
		MaxHalfOpenProbes: 1,
	}
}

// BreakerSnapshot describes the live health of a single route's circuit breaker.
type BreakerSnapshot struct {
	Route               string    `json:"route"`
	State               State     `json:"state"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	ConsecutiveSuccess  int       `json:"consecutive_success"`
	TotalFailures       uint64    `json:"total_failures"`
	TotalSuccesses      uint64    `json:"total_successes"`
	TotalShortCircuited uint64    `json:"total_short_circuited"`
	OpenedAt            time.Time `json:"opened_at,omitempty"`
	CooldownRemainingMs int64     `json:"cooldown_remaining_ms"`
}

type routeBreaker struct {
	mu                  sync.Mutex
	route               string
	state               State
	consecutiveFailures int
	consecutiveSuccess  int
	inFlightProbes      int
	totalFailures       uint64
	totalSuccesses      uint64
	totalShortCircuited uint64
	openedAt            time.Time
}

// CircuitBreakerRegistry manages per-route state machines (`CLOSED` -> `OPEN` -> `HALF_OPEN` -> `CLOSED`).
type CircuitBreakerRegistry struct {
	mu       sync.RWMutex
	cfg      BreakerConfig
	breakers map[string]*routeBreaker
}

// NewCircuitBreakerRegistry initializes a registry with the provided configuration.
func NewCircuitBreakerRegistry(cfg BreakerConfig) *CircuitBreakerRegistry {
	if cfg.FailureThreshold <= 0 {
		cfg = DefaultBreakerConfig()
	}
	return &CircuitBreakerRegistry{
		cfg:      cfg,
		breakers: make(map[string]*routeBreaker, 32),
	}
}

// Acquire checks whether a request to route is permitted to proceed.
// If permitted, returns the current State and nil error; otherwise returns ErrCircuitOpen.
func (r *CircuitBreakerRegistry) Acquire(route string) (State, error) {
	b := r.getOrCreate(route)
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	switch b.state {
	case StateClosed:
		return StateClosed, nil

	case StateOpen:
		if now.Sub(b.openedAt) >= r.cfg.OpenCooldown {
			b.state = StateHalfOpen
			b.consecutiveSuccess = 0
			b.inFlightProbes = 1
			return StateHalfOpen, nil
		}
		b.totalShortCircuited++
		return StateOpen, ErrCircuitOpen

	case StateHalfOpen:
		if b.inFlightProbes >= r.cfg.MaxHalfOpenProbes {
			b.totalShortCircuited++
			return StateHalfOpen, ErrCircuitOpen
		}
		b.inFlightProbes++
		return StateHalfOpen, nil
	}
	return StateClosed, nil
}

// RecordOutcome updates the circuit breaker state machine based on whether the upstream call succeeded.
func (r *CircuitBreakerRegistry) RecordOutcome(route string, success bool) State {
	b := r.getOrCreate(route)
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateHalfOpen && b.inFlightProbes > 0 {
		b.inFlightProbes--
	}

	if success {
		b.totalSuccesses++
		b.consecutiveFailures = 0
		if b.state == StateHalfOpen {
			b.consecutiveSuccess++
			if b.consecutiveSuccess >= r.cfg.SuccessThreshold {
				b.state = StateClosed
				b.consecutiveSuccess = 0
				b.inFlightProbes = 0
			}
		}
		return b.state
	}

	// Failure path
	b.totalFailures++
	b.consecutiveSuccess = 0
	b.consecutiveFailures++

	if b.state == StateHalfOpen || b.consecutiveFailures >= r.cfg.FailureThreshold {
		b.state = StateOpen
		b.openedAt = time.Now()
		b.inFlightProbes = 0
	}
	return b.state
}

// ForceState allows manual operator override of a route's circuit breaker (for testing or chaos drills).
func (r *CircuitBreakerRegistry) ForceState(route string, target State) BreakerSnapshot {
	b := r.getOrCreate(route)
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = target
	b.consecutiveFailures = 0
	b.consecutiveSuccess = 0
	b.inFlightProbes = 0
	if target == StateOpen {
		b.openedAt = time.Now()
	}
	return r.snapshotLocked(b, time.Now())
}

// Snapshots returns the live state of all registered route circuit breakers.
func (r *CircuitBreakerRegistry) Snapshots() []BreakerSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	out := make([]BreakerSnapshot, 0, len(r.breakers))
	for _, b := range r.breakers {
		b.mu.Lock()
		out = append(out, r.snapshotLocked(b, now))
		b.mu.Unlock()
	}
	return out
}

func (r *CircuitBreakerRegistry) snapshotLocked(b *routeBreaker, now time.Time) BreakerSnapshot {
	var remMs int64
	if b.state == StateOpen {
		rem := r.cfg.OpenCooldown - now.Sub(b.openedAt)
		if rem > 0 {
			remMs = rem.Milliseconds()
		}
	}
	return BreakerSnapshot{
		Route:               b.route,
		State:               b.state,
		ConsecutiveFailures: b.consecutiveFailures,
		ConsecutiveSuccess:  b.consecutiveSuccess,
		TotalFailures:       b.totalFailures,
		TotalSuccesses:      b.totalSuccesses,
		TotalShortCircuited: b.totalShortCircuited,
		OpenedAt:            b.openedAt,
		CooldownRemainingMs: remMs,
	}
}

func (r *CircuitBreakerRegistry) getOrCreate(route string) *routeBreaker {
	r.mu.RLock()
	b, ok := r.breakers[route]
	r.mu.RUnlock()
	if ok {
		return b
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, exists := r.breakers[route]; exists {
		return existing
	}
	b = &routeBreaker{
		route: route,
		state: StateClosed,
	}
	r.breakers[route] = b
	return b
}
