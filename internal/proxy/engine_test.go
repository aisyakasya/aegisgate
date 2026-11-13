package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/chaos"
	"github.com/aegisgate/aegisgate/internal/diff"
	"github.com/aegisgate/aegisgate/internal/resilience"
	"github.com/aegisgate/aegisgate/internal/storage"
)

func TestCanonicalFingerprintKeyOrderInvariance(t *testing.T) {
	bodyA := []byte(`{"model":"gpt-4o","temperature":0.2,"request_id":"req_111","messages":[{"role":"user","content":"hello"}]}`)
	bodyB := []byte(`{"messages":[{"content":"hello","role":"user"}],"request_id":"req_999","temperature":0.2,"model":"gpt-4o"}`)

	fpA := ComputeFingerprint("post", "/v1/chat/completions/", "b=2&a=1&_t=123", bodyA)
	fpB := ComputeFingerprint("POST", "/v1/chat/completions", "a=1&b=2&_t=999", bodyB)

	if fpA != fpB {
		t.Fatalf("expected identical canonical fingerprints for reordered JSON & query params, got %s vs %s", fpA, fpB)
	}
}

func TestProxyEngineRecordReplayAndShadowDrift(t *testing.T) {
	var upstreamHits atomic.Int32
	var returnDriftedSchema atomic.Bool

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if returnDriftedSchema.Load() {
			// Introduce a breaking change: "amount" changed from number to string, "currency" removed
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"pi_100","amount":"$49.00"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"pi_100","amount":4900,"currency":"usd"}`))
	}))
	defer upstream.Close()

	tmpDir := t.TempDir()
	casStore, _ := cas.NewStore(tmpDir, 64)
	walStore, _ := storage.NewWALStore(tmpDir, 500, 10*time.Millisecond)
	defer walStore.Close()

	eng, err := NewEngine(
		Config{TargetURL: upstream.URL, InitialMode: storage.ModeRecord},
		casStore,
		walStore,
		diff.NewAnalyzer(diff.DefaultOptions()),
		resilience.NewTokenBucketLimiter(100, 50, true),
		resilience.NewCircuitBreakerRegistry(resilience.DefaultBreakerConfig()),
		chaos.NewEngine(),
	)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}

	gateway := httptest.NewServer(eng)
	defer gateway.Close()

	// 1. RECORD mode: hits upstream and stores golden trace
	reqPayload := []byte(`{"customer":"cust_1","amount":4900}`)
	resp1, err := http.Post(gateway.URL+"/v1/charges", "application/json", bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("RECORD request failed: %v", err)
	}
	body1, _ := io.ReadAll(resp1.Body)
	_ = resp1.Body.Close()

	if resp1.StatusCode != http.StatusOK || upstreamHits.Load() != 1 {
		t.Fatalf("expected status 200 and upstreamHits=1, got status=%d hits=%d", resp1.StatusCode, upstreamHits.Load())
	}

	// 2. Switch to REPLAY mode: must serve identical payload with 0 new upstream hits!
	eng.SetMode(storage.ModeReplay)
	resp2, err := http.Post(gateway.URL+"/v1/charges", "application/json", bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("REPLAY request failed: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	if upstreamHits.Load() != 1 {
		t.Fatalf("expected REPLAY mode not to hit upstream (hits=%d)", upstreamHits.Load())
	}
	if resp2.Header.Get("X-AegisGate-Replay-Hit") != "true" {
		t.Fatalf("expected X-AegisGate-Replay-Hit=true header")
	}
	if !bytes.Equal(body1, body2) {
		t.Fatalf("expected replayed payload to match golden payload")
	}

	// 3. Switch to SHADOW_DIFF mode while upstream introduces a breaking contract change
	eng.SetMode(storage.ModeShadowDiff)
	returnDriftedSchema.Store(true)

	resp3, err := http.Post(gateway.URL+"/v1/charges", "application/json", bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("SHADOW_DIFF request failed: %v", err)
	}
	_ = resp3.Body.Close()

	if resp3.Header.Get("X-AegisGate-Contract-Drift") != "BREAKING" {
		t.Fatalf("expected X-AegisGate-Contract-Drift=BREAKING header, got %q", resp3.Header.Get("X-AegisGate-Contract-Drift"))
	}
}
