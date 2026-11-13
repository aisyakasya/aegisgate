package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/chaos"
	"github.com/aegisgate/aegisgate/internal/diff"
	"github.com/aegisgate/aegisgate/internal/proxy"
	"github.com/aegisgate/aegisgate/internal/resilience"
	"github.com/aegisgate/aegisgate/internal/storage"
)

// ControlPlane exposes the REST API, SSE telemetry stream, traffic simulator, and static Web UI.
type ControlPlane struct {
	proxyAddr string
	uiDistDir string
	engine    *proxy.Engine
	casStore  *cas.Store
	walStore  *storage.WALStore
	differ    *diff.Analyzer
	limiter   *resilience.TokenBucketLimiter
	breakers  *resilience.CircuitBreakerRegistry
	chaosEng  *chaos.Engine
	startedAt time.Time
}

// NewControlPlane constructs the Control Plane server.
func NewControlPlane(
	proxyAddr string,
	uiDistDir string,
	engine *proxy.Engine,
	casStore *cas.Store,
	walStore *storage.WALStore,
	differ *diff.Analyzer,
	limiter *resilience.TokenBucketLimiter,
	breakers *resilience.CircuitBreakerRegistry,
	chaosEng *chaos.Engine,
) *ControlPlane {
	return &ControlPlane{
		proxyAddr: proxyAddr,
		uiDistDir: uiDistDir,
		engine:    engine,
		casStore:  casStore,
		walStore:  walStore,
		differ:    differ,
		limiter:   limiter,
		breakers:  breakers,
		chaosEng:  chaosEng,
		startedAt: time.Now().UTC(),
	}
}

// Handler returns the multiplexed HTTP handler with CORS middleware.
func (cp *ControlPlane) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/status", cp.handleStatus)
	mux.HandleFunc("/api/v1/mode", cp.handleMode)
	mux.HandleFunc("/api/v1/traces", cp.handleTraces)
	mux.HandleFunc("/api/v1/traces/", cp.handleTraceDetail)
	mux.HandleFunc("/api/v1/diff", cp.handleDiff)
	mux.HandleFunc("/api/v1/chaos/rules", cp.handleChaosRules)
	mux.HandleFunc("/api/v1/chaos/rules/", cp.handleChaosRuleDelete)
	mux.HandleFunc("/api/v1/circuit-breakers/force", cp.handleCircuitForce)
	mux.HandleFunc("/api/v1/ratelimit/config", cp.handleRateLimitConfig)
	mux.HandleFunc("/api/v1/simulate", cp.handleSimulateTraffic)
	mux.HandleFunc("/api/v1/stream", cp.handleSSEStream)

	// Serve built React Web Inspector SPA if present
	mux.HandleFunc("/", cp.handleStaticUI)

	return withCORS(mux)
}

func (cp *ControlPlane) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	rlEnabled, rlCap, rlRate, rlAllowed, rlDenied, rlBuckets := cp.limiter.Snapshot()

	resp := map[string]any{
		"uptime_sec":       int64(time.Since(cp.startedAt).Seconds()),
		"mode":             cp.engine.Mode(),
		"target_url":       cp.engine.TargetURL(),
		"proxy_listen":     cp.proxyAddr,
		"metrics":          cp.walStore.Metrics(),
		"cas":              cp.casStore.Stats(),
		"circuit_breakers": cp.breakers.Snapshots(),
		"chaos_rules":      cp.chaosEng.ListRules(),
		"rate_limiter": map[string]any{
			"enabled":        rlEnabled,
			"capacity":       rlCap,
			"refill_per_sec": rlRate,
			"total_allowed":  rlAllowed,
			"total_denied":   rlDenied,
			"buckets":        rlBuckets,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (cp *ControlPlane) handleMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Mode storage.ProxyMode `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json payload")
		return
	}

	switch req.Mode {
	case storage.ModeRecord, storage.ModeReplay, storage.ModeShadowDiff, storage.ModePassthrough:
		cp.engine.SetMode(req.Mode)
		writeJSON(w, http.StatusOK, map[string]any{"mode": cp.engine.Mode()})
	default:
		writeJSONError(w, http.StatusBadRequest, "unsupported proxy mode")
	}
}

func (cp *ControlPlane) handleTraces(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	filter := storage.QueryFilter{
		Method:     q.Get("method"),
		PathPrefix: q.Get("path_prefix"),
		Mode:       q.Get("mode"),
		OnlyChaos:  q.Get("only_chaos") == "true",
		OnlyDrift:  q.Get("only_drift") == "true",
		Limit:      limit,
	}
	records := cp.walStore.Query(filter)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":  len(records),
		"traces": records,
	})
}

func (cp *ControlPlane) handleTraceDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	traceID := strings.TrimPrefix(r.URL.Path, "/api/v1/traces/")
	if traceID == "" {
		writeJSONError(w, http.StatusBadRequest, "missing trace id")
		return
	}

	rec, err := cp.walStore.GetByID(traceID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "trace not found")
		return
	}

	reqBytes, _ := cp.casStore.Get(rec.RequestBodyHash)
	respBytes, _ := cp.casStore.Get(rec.ResponseHash)

	var diffReport *diff.Report
	var goldenBodyStr string
	var goldenTraceID string

	if golden, err := cp.walStore.FindGoldenByFingerprint(rec.Fingerprint); err == nil {
		goldenTraceID = golden.ID
		if gBytes, err := cp.casStore.Get(golden.ResponseHash); err == nil {
			goldenBodyStr = string(gBytes)
			rep := cp.differ.Compare(
				golden.ID, rec.ID,
				golden.StatusCode, rec.StatusCode,
				gBytes, respBytes,
				golden.LatencyMs, rec.LatencyMs,
			)
			diffReport = &rep
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"trace":            rec,
		"request_body":     string(reqBytes),
		"response_body":    string(respBytes),
		"golden_trace_id":  goldenTraceID,
		"golden_body":      goldenBodyStr,
		"contract_diff":    diffReport,
		"reproducible_curl": buildCurlCommand(cp.proxyAddr, rec, reqBytes),
	})
}

func (cp *ControlPlane) handleDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		BaselineID  string `json:"baseline_id"`
		CandidateID string `json:"candidate_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid json body")
		return
	}

	baseRec, err := cp.walStore.GetByID(req.BaselineID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "baseline trace not found")
		return
	}
	candRec, err := cp.walStore.GetByID(req.CandidateID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "candidate trace not found")
		return
	}

	baseBody, _ := cp.casStore.Get(baseRec.ResponseHash)
	candBody, _ := cp.casStore.Get(candRec.ResponseHash)

	rep := cp.differ.Compare(
		baseRec.ID, candRec.ID,
		baseRec.StatusCode, candRec.StatusCode,
		baseBody, candBody,
		baseRec.LatencyMs, candRec.LatencyMs,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"report":         rep,
		"baseline_body":  string(baseBody),
		"candidate_body": string(candBody),
	})
}

func (cp *ControlPlane) handleChaosRules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"rules": cp.chaosEng.ListRules()})
	case http.MethodPost:
		var rule chaos.Rule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid rule json")
			return
		}
		saved := cp.chaosEng.UpsertRule(rule)
		writeJSON(w, http.StatusOK, saved)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (cp *ControlPlane) handleChaosRuleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	ruleID := strings.TrimPrefix(r.URL.Path, "/api/v1/chaos/rules/")
	if !cp.chaosEng.DeleteRule(ruleID) {
		writeJSONError(w, http.StatusNotFound, "rule not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": ruleID})
}

func (cp *ControlPlane) handleCircuitForce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Route string           `json:"route"`
		State resilience.State `json:"state"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Route == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	snap := cp.breakers.ForceState(req.Route, req.State)
	writeJSON(w, http.StatusOK, snap)
}

func (cp *ControlPlane) handleRateLimitConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Enabled      bool    `json:"enabled"`
		Capacity     float64 `json:"capacity"`
		RefillPerSec float64 `json:"refill_per_sec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid payload")
		return
	}
	cp.limiter.UpdateConfig(req.Capacity, req.RefillPerSec, req.Enabled)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleSimulateTraffic executes a realistic batch of requests directly against the proxy engine
// so operators can immediately inspect Golden Recording, CAS Deduplication, Replay, and Contract Drift.
func (cp *ControlPlane) handleSimulateTraffic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	samples := []struct {
		method  string
		path    string
		headers map[string]string
		body    string
	}{
		{
			method: "POST",
			path:   "/v1/chat/completions",
			body:   `{"model":"claude-3-7-sonnet","temperature":0.1,"messages":[{"role":"user","content":"Analyze p99 tail latency in raft consensus"}]}`,
		},
		{
			method: "POST",
			path:   "/v1/payments/authorize",
			body:   `{"customer_id":"cust_enterprise_88","amount":12500,"currency":"usd","idempotency_key":"idem_901"}`,
		},
		{
			method: "POST",
			path:   "/v1/embeddings",
			body:   `{"model":"text-embedding-3-large","input":"deterministic replay and content-addressable storage"}`,
		},
		{
			method:  "POST",
			path:    "/v1/payments/authorize",
			headers: map[string]string{"X-Simulate-Schema-Drift": "true"},
			body:    `{"customer_id":"cust_enterprise_88","amount":12500,"currency":"usd","idempotency_key":"idem_901"}`,
		},
		{
			method: "GET",
			path:   "/v1/models?tier=production",
			body:   ``,
		},
	}

	results := make([]int, 0, len(samples))
	for _, s := range samples {
		req, err := http.NewRequest(s.method, "http://localhost"+s.path, bytes.NewReader([]byte(s.body)))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range s.headers {
			req.Header.Set(k, v)
		}
		rec := &inMemoryResponseWriter{header: make(http.Header), statusCode: http.StatusOK}
		cp.engine.ServeHTTP(rec, req)
		results = append(results, rec.statusCode)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"simulated_requests": len(results),
		"statuses":           results,
	})
}

func (cp *ControlPlane) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub := cp.engine.Subscribe()
	defer cp.engine.Unsubscribe(sub)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case rec := <-sub:
			data, err := json.Marshal(rec)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: trace\ndata: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			_, _ = fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

func (cp *ControlPlane) handleStaticUI(w http.ResponseWriter, r *http.Request) {
	if cp.uiDistDir != "" {
		cleanPath := filepath.Clean(r.URL.Path)
		if cleanPath == "/" {
			cleanPath = "/index.html"
		}
		fullPath := filepath.Join(cp.uiDistDir, cleanPath)
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			http.ServeFile(w, r, fullPath)
			return
		}
		indexFile := filepath.Join(cp.uiDistDir, "index.html")
		if _, err := os.Stat(indexFile); err == nil {
			http.ServeFile(w, r, indexFile)
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service": "AegisGate Control Plane API",
		"status":  "healthy",
		"docs":    "/api/v1/status",
	})
}

type inMemoryResponseWriter struct {
	header     http.Header
	body       bytes.Buffer
	statusCode int
}

func (w *inMemoryResponseWriter) Header() http.Header         { return w.header }
func (w *inMemoryResponseWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *inMemoryResponseWriter) WriteHeader(code int)        { w.statusCode = code }

func buildCurlCommand(proxyAddr string, rec storage.TraceRecord, body []byte) string {
	host := proxyAddr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}
	urlStr := fmt.Sprintf("http://%s%s", host, rec.Path)
	if rec.Query != "" {
		urlStr += "?" + rec.Query
	}
	cmd := fmt.Sprintf("curl -i -X %s %q \\\n  -H \"Content-Type: application/json\"", rec.Method, urlStr)
	if len(body) > 0 {
		escaped := strings.ReplaceAll(string(body), "'", "'\\''")
		cmd += fmt.Sprintf(" \\\n  -d '%s'", escaped)
	}
	return cmd
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Simulate-Schema-Drift")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

var _ io.Writer = (*inMemoryResponseWriter)(nil)
