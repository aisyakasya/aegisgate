package chaos

import (
	"testing"
)

func TestChaosEngineRuleEvaluationAndCRUD(t *testing.T) {
	eng := NewEngine()

	// Default rules are disabled initially
	dec := eng.Evaluate("POST", "/v1/chat/completions")
	if dec.Matched {
		t.Fatalf("expected no match when default rules are disabled")
	}

	// Enable a 100% probability fault rule
	created := eng.UpsertRule(Rule{
		ID:              "custom_fault_1",
		Name:            "Deterministic 502 Fault",
		Enabled:         true,
		Method:          "POST",
		PathPrefix:      "/v1/orders",
		Probability:     1.0,
		FixedLatencyMs:  50,
		JitterLatencyMs: 25,
		FaultStatusCode: 502,
	})

	if created.ID != "custom_fault_1" {
		t.Fatalf("unexpected rule ID: %s", created.ID)
	}

	// Non-matching method should not trigger
	if d := eng.Evaluate("GET", "/v1/orders"); d.Matched {
		t.Fatalf("expected GET not to match POST rule")
	}

	// Matching request must trigger
	d := eng.Evaluate("POST", "/v1/orders/create")
	if !d.Matched {
		t.Fatalf("expected POST /v1/orders/create to match")
	}
	if d.AbortStatus != 502 {
		t.Fatalf("expected AbortStatus=502, got %d", d.AbortStatus)
	}
	if d.SleepDelay.Milliseconds() < 50 || d.SleepDelay.Milliseconds() > 75 {
		t.Fatalf("expected SleepDelay in [50ms, 75ms], got %v", d.SleepDelay)
	}

	rules := eng.ListRules()
	var found bool
	for _, r := range rules {
		if r.ID == "custom_fault_1" && r.TriggeredCount == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected TriggeredCount=1 on custom_fault_1")
	}

	if !eng.DeleteRule("custom_fault_1") {
		t.Fatalf("expected DeleteRule to return true")
	}
}
