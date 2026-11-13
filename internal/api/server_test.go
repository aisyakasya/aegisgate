package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/chaos"
	"github.com/aegisgate/aegisgate/internal/diff"
	"github.com/aegisgate/aegisgate/internal/proxy"
	"github.com/aegisgate/aegisgate/internal/resilience"
	"github.com/aegisgate/aegisgate/internal/storage"
)

func TestControlPlaneStatusSimulateAndTraceInspection(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Simulate-Schema-Drift") == "true" {
			// Emit drifted contract (amount is string instead of number, currency removed)
			_, _ = w.Write([]byte(`{"status":"authorized","amount":"$125.00"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"authorized","amount":12500,"currency":"usd"}`))
	}))
	defer upstream.Close()

	tmpDir := t.TempDir()
	casStore, _ := cas.NewStore(tmpDir, 64)
	walStore, _ := storage.NewWALStore(tmpDir, 500, 10*time.Millisecond)
	defer walStore.Close()

	differ := diff.NewAnalyzer(diff.DefaultOptions())
	limiter := resilience.NewTokenBucketLimiter(100, 50, true)
	breakers := resilience.NewCircuitBreakerRegistry(resilience.DefaultBreakerConfig())
	chaosEng := chaos.NewEngine()

	eng, err := proxy.NewEngine(
		proxy.Config{TargetURL: upstream.URL, InitialMode: storage.ModeRecord},
		casStore, walStore, differ, limiter, breakers, chaosEng,
	)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	cp := NewControlPlane(":8080", "", eng, casStore, walStore, differ, limiter, breakers, chaosEng)
	srv := httptest.NewServer(cp.Handler())
	defer srv.Close()

	// 1. Trigger synthetic traffic batch via /api/v1/simulate
	simResp, err := http.Post(srv.URL+"/api/v1/simulate", "application/json", nil)
	if err != nil || simResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/v1/simulate failed: status=%v err=%v", simResp.StatusCode, err)
	}
	_ = simResp.Body.Close()

	// 2. Verify /api/v1/traces returns the 5 simulated traces including detected schema drift
	trResp, err := http.Get(srv.URL + "/api/v1/traces?only_drift=true")
	if err != nil {
		t.Fatalf("GET /api/v1/traces failed: %v", err)
	}
	defer trResp.Body.Close()

	var trPayload struct {
		Count  int                   `json:"count"`
		Traces []storage.TraceRecord `json:"traces"`
	}
	if err := json.NewDecoder(trResp.Body).Decode(&trPayload); err != nil {
		t.Fatalf("decode traces failed: %v", err)
	}
	if trPayload.Count == 0 {
		t.Fatalf("expected at least 1 trace with detected schema drift")
	}

	// 3. Switch mode to REPLAY via /api/v1/mode
	modeBody := bytes.NewBufferString(`{"mode":"REPLAY"}`)
	modeResp, err := http.Post(srv.URL+"/api/v1/mode", "application/json", modeBody)
	if err != nil || modeResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/v1/mode failed: %v", err)
	}
	_ = modeResp.Body.Close()

	if eng.Mode() != storage.ModeReplay {
		t.Fatalf("expected engine mode REPLAY, got %s", eng.Mode())
	}
}
