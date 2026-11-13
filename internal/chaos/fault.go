package chaos

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Rule defines a programmable chaos experiment or fault injection policy applied to matching requests.
type Rule struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	Enabled         bool    `json:"enabled"`
	Method          string  `json:"method"`            // "" matches all HTTP methods
	PathPrefix      string  `json:"path_prefix"`       // e.g. "/v1/chat/completions"
	Probability     float64 `json:"probability"`       // 0.0 to 1.0
	FixedLatencyMs  int     `json:"fixed_latency_ms"`  // deterministic delay
	JitterLatencyMs int     `json:"jitter_latency_ms"` // uniform random additional delay [0, JitterLatencyMs]
	FaultStatusCode int     `json:"fault_status_code"` // 0 means do not abort HTTP status; e.g. 429, 502, 503
	FaultBody       string  `json:"fault_body,omitempty"`
	CorruptPayload  bool    `json:"corrupt_payload"`   // if true, mutates response bytes to test client parser resilience
	TriggeredCount  uint64  `json:"triggered_count"`
}

// Decision describes the fault actions to apply to a specific in-flight request.
type Decision struct {
	Matched      bool
	RuleID       string
	RuleName     string
	SummaryLabel string
	SleepDelay   time.Duration
	AbortStatus  int
	AbortBody    []byte
	Corrupt      bool
}

// Engine manages active fault-injection rules and evaluates incoming traffic against them.
type Engine struct {
	mu    sync.RWMutex
	rules map[string]*Rule
	order []string
	rng   *rand.Rand
}

// NewEngine creates a Chaos Engine pre-populated with realistic default fault templates (disabled by default).
func NewEngine() *Engine {
	e := &Engine{
		rules: make(map[string]*Rule),
		order: make([]string, 0),
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	defaultRules := []Rule{
		{
			ID:              "chaos_llm_rate_limit",
			Name:            "Simulate Upstream HTTP 429 Rate Limit",
			Enabled:         false,
			Method:          "POST",
			PathPrefix:      "/v1/chat/completions",
			Probability:     0.25,
			FixedLatencyMs:  40,
			JitterLatencyMs: 60,
			FaultStatusCode: 429,
			FaultBody:       `{"error":{"code":"rate_limit_exceeded","message":"Chaos Engine: simulated token quota exhaustion","retry_after_ms":1500}}`,
		},
		{
			ID:              "chaos_payment_latency_spike",
			Name:            "Tail-Latency P99 Spike (+450ms Jitter)",
			Enabled:         false,
			Method:          "",
			PathPrefix:      "/v1/payments",
			Probability:     0.35,
			FixedLatencyMs:  250,
			JitterLatencyMs: 200,
			FaultStatusCode: 0,
		},
		{
			ID:              "chaos_gateway_503",
			Name:            "Upstream 503 Service Unavailable Outage",
			Enabled:         false,
			Method:          "",
			PathPrefix:      "/v1/",
			Probability:     0.15,
			FixedLatencyMs:  15,
			FaultStatusCode: 503,
			FaultBody:       `{"error":{"code":"upstream_unavailable","message":"Chaos Engine: synthetic 503 partition fault"}}`,
		},
	}

	for _, r := range defaultRules {
		rc := r
		e.rules[rc.ID] = &rc
		e.order = append(e.order, rc.ID)
	}
	return e
}

// Evaluate checks all enabled rules in order and returns the first triggered fault Decision.
func (e *Engine) Evaluate(method, path string) Decision {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, id := range e.order {
		r, ok := e.rules[id]
		if !ok || !r.Enabled {
			continue
		}
		if r.Method != "" && !strings.EqualFold(r.Method, method) {
			continue
		}
		if r.PathPrefix != "" && !strings.HasPrefix(path, r.PathPrefix) {
			continue
		}
		if r.Probability <= 0 {
			continue
		}
		if r.Probability < 1.0 && e.rng.Float64() > r.Probability {
			continue
		}

		r.TriggeredCount++
		totalDelayMs := r.FixedLatencyMs
		if r.JitterLatencyMs > 0 {
			totalDelayMs += e.rng.Intn(r.JitterLatencyMs + 1)
		}

		parts := make([]string, 0, 3)
		if totalDelayMs > 0 {
			parts = append(parts, fmt.Sprintf("delay:%dms", totalDelayMs))
		}
		if r.FaultStatusCode > 0 {
			parts = append(parts, fmt.Sprintf("status:%d", r.FaultStatusCode))
		}
		if r.CorruptPayload {
			parts = append(parts, "corrupt_body")
		}

		body := r.FaultBody
		if body == "" && r.FaultStatusCode > 0 {
			body = fmt.Sprintf(`{"error":"chaos_fault_injected","rule_id":%q,"status":%d}`, r.ID, r.FaultStatusCode)
		}

		return Decision{
			Matched:      true,
			RuleID:       r.ID,
			RuleName:     r.Name,
			SummaryLabel: strings.Join(parts, "+"),
			SleepDelay:   time.Duration(totalDelayMs) * time.Millisecond,
			AbortStatus:  r.FaultStatusCode,
			AbortBody:    []byte(body),
			Corrupt:      r.CorruptPayload,
		}
	}

	return Decision{Matched: false}
}

// UpsertRule adds or updates a fault injection rule.
func (e *Engine) UpsertRule(rule Rule) Rule {
	e.mu.Lock()
	defer e.mu.Unlock()

	if rule.ID == "" {
		rule.ID = fmt.Sprintf("chaos_%d", time.Now().UnixNano())
	}
	if rule.Probability < 0 {
		rule.Probability = 0
	}
	if rule.Probability > 1 {
		rule.Probability = 1
	}

	if existing, exists := e.rules[rule.ID]; exists {
		rule.TriggeredCount = existing.TriggeredCount
		*existing = rule
		return *existing
	}

	copied := rule
	e.rules[copied.ID] = &copied
	e.order = append(e.order, copied.ID)
	return copied
}

// DeleteRule removes a rule by ID.
func (e *Engine) DeleteRule(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.rules[id]; !exists {
		return false
	}
	delete(e.rules, id)
	filtered := make([]string, 0, len(e.order))
	for _, item := range e.order {
		if item != id {
			filtered = append(filtered, item)
		}
	}
	e.order = filtered
	return true
}

// ListRules returns a snapshot of all configured rules.
func (e *Engine) ListRules() []Rule {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]Rule, 0, len(e.order))
	for _, id := range e.order {
		if r, ok := e.rules[id]; ok {
			out = append(out, *r)
		}
	}
	return out
}
