package diff

import (
	"testing"
)

func TestAnalyzerDetectsBreakingAndNonBreakingDrift(t *testing.T) {
	analyzer := NewAnalyzer(DefaultOptions())

	baselineJSON := []byte(`{
		"request_id": "550e8400-e29b-41d4-a716-446655440000",
		"created_at": "2026-11-13T10:00:00Z",
		"customer": {
			"id": "cust_991",
			"tier": "enterprise",
			"credits": 1500
		},
		"items": [
			{"sku": "gpu-h100", "price": 2.50}
		]
	}`)

	// Candidate removes "credits" (BREAKING), changes "price" from number to string (BREAKING),
	// adds "region" (NON_BREAKING), and rotates "request_id" + "created_at" (EPHEMERAL_IGNORED).
	candidateJSON := []byte(`{
		"request_id": "771e8400-e29b-41d4-a716-999955441111",
		"created_at": "2026-11-13T12:45:10Z",
		"customer": {
			"id": "cust_991",
			"tier": "enterprise",
			"region": "us-east-1"
		},
		"items": [
			{"sku": "gpu-h100", "price": "$2.50"}
		]
	}`)

	report := analyzer.Compare("tr_base", "tr_cand", 200, 200, baselineJSON, candidateJSON, 18.0, 22.0)

	if report.Compatible {
		t.Fatalf("expected Compatible=false due to breaking schema drift")
	}
	if report.BreakingCount != 2 {
		t.Fatalf("expected 2 breaking changes (removed field + type mismatch), got %d: %+v", report.BreakingCount, report.Changes)
	}
	if report.NonBreakingCount != 1 {
		t.Fatalf("expected 1 non-breaking change (added field), got %d: %+v", report.NonBreakingCount, report.Changes)
	}
	if report.IgnoredEphemeral != 2 {
		t.Fatalf("expected 2 ignored ephemeral fields (request_id, created_at), got %d", report.IgnoredEphemeral)
	}
}

func TestAnalyzerStatusAndLatencyRegression(t *testing.T) {
	analyzer := NewAnalyzer(DefaultOptions())
	body := []byte(`{"ok":true}`)

	report := analyzer.Compare("tr_1", "tr_2", 200, 502, body, body, 25.0, 290.0)
	if report.Compatible {
		t.Fatalf("expected HTTP 200 -> 502 to be incompatible")
	}
	if report.BreakingCount != 1 {
		t.Fatalf("expected 1 breaking change for 502 status, got %d", report.BreakingCount)
	}
	if report.NonBreakingCount != 1 {
		t.Fatalf("expected 1 non-breaking change for latency regression, got %d", report.NonBreakingCount)
	}
}
