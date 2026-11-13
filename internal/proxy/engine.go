package proxy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/chaos"
	"github.com/aegisgate/aegisgate/internal/diff"
	"github.com/aegisgate/aegisgate/internal/resilience"
	"github.com/aegisgate/aegisgate/internal/storage"
)

// Config holds the runtime parameters for the AegisGate Proxy Engine.
type Config struct {
	TargetURL    string
	InitialMode  storage.ProxyMode
	ReplayFallback bool // If true, REPLAY cache misses fall back to live upstream and record
	MaxBodyBytes int64
}

// Engine coordinates reverse proxying, deterministic replay, shadow AST diffing,
// chaos fault injection, rate limiting, and circuit breaking.
type Engine struct {
	mu             sync.RWMutex
	targetURL      *url.URL
	mode           storage.ProxyMode
	replayFallback bool
	maxBodyBytes   int64
	httpClient     *http.Client

	casStore   *cas.Store
	walStore   *storage.WALStore
	differ     *diff.Analyzer
	limiter    *resilience.TokenBucketLimiter
	breakers   *resilience.CircuitBreakerRegistry
	chaosEng   *chaos.Engine

	subsMu      sync.RWMutex
	subscribers map[chan storage.TraceRecord]struct{}
	seqCounter  atomic.Uint64
}

// NewEngine constructs a ready-to-serve Proxy Engine wired to all storage and resilience subsystems.
func NewEngine(
	cfg Config,
	casStore *cas.Store,
	walStore *storage.WALStore,
	differ *diff.Analyzer,
	limiter *resilience.TokenBucketLimiter,
	breakers *resilience.CircuitBreakerRegistry,
	chaosEng *chaos.Engine,
) (*Engine, error) {
	parsedURL, err := url.Parse(cfg.TargetURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return nil, fmt.Errorf("proxy: invalid target upstream URL %q", cfg.TargetURL)
	}
	if cfg.InitialMode == "" {
		cfg.InitialMode = storage.ModeRecord
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 10 * 1024 * 1024 // 10 MiB default payload cap
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          128,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &Engine{
		targetURL:      parsedURL,
		mode:           cfg.InitialMode,
		replayFallback: cfg.ReplayFallback,
		maxBodyBytes:   cfg.MaxBodyBytes,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
		casStore:    casStore,
		walStore:    walStore,
		differ:      differ,
		limiter:     limiter,
		breakers:    breakers,
		chaosEng:    chaosEng,
		subscribers: make(map[chan storage.TraceRecord]struct{}),
	}, nil
}

// Mode returns the current operational proxy mode.
func (e *Engine) Mode() storage.ProxyMode {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.mode
}

// SetMode dynamically switches the proxy mode at runtime (`RECORD`, `REPLAY`, `SHADOW_DIFF`, `PASSTHROUGH`).
func (e *Engine) SetMode(m storage.ProxyMode) {
	e.mu.Lock()
	e.mode = m
	e.mu.Unlock()
}

// TargetURL returns the currently configured upstream target URL string.
func (e *Engine) TargetURL() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.targetURL.String()
}

// SetTargetURL updates the upstream target server URL at runtime.
func (e *Engine) SetTargetURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid target URL: %q", rawURL)
	}
	e.mu.Lock()
	e.targetURL = u
	e.mu.Unlock()
	return nil
}

// Subscribe registers a channel to receive live TraceRecords as traffic flows through the gateway.
func (e *Engine) Subscribe() chan storage.TraceRecord {
	ch := make(chan storage.TraceRecord, 64)
	e.subsMu.Lock()
	e.subscribers[ch] = struct{}{}
	e.subsMu.Unlock()
	return ch
}

// Unsubscribe removes and closes a subscriber channel.
func (e *Engine) Unsubscribe(ch chan storage.TraceRecord) {
	e.subsMu.Lock()
	if _, ok := e.subscribers[ch]; ok {
		delete(e.subscribers, ch)
		close(ch)
	}
	e.subsMu.Unlock()
}

// ServeHTTP implements http.Handler for intercepting, replaying, and proxying API traffic.
func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTotal := time.Now()
	traceID := e.nextTraceID()
	currentMode := e.Mode()

	// Read and buffer request body up to MaxBodyBytes
	var reqBody []byte
	if r.Body != nil {
		limited := io.LimitReader(r.Body, e.maxBodyBytes)
		reqBody, _ = io.ReadAll(limited)
		_ = r.Body.Close()
	}

	reqHash, _ := e.casStore.Put(reqBody)
	fingerprint := ComputeFingerprint(r.Method, r.URL.Path, r.URL.RawQuery, reqBody)
	clientIP := extractClientIP(r)
	routeKey := r.Method + ":" + r.URL.Path

	// 1. Check Token Bucket Rate Limiter
	allowed, remainingTokens, retryAfter := e.limiter.Allow(clientIP)
	w.Header().Set("X-AegisGate-Trace-Id", traceID)
	w.Header().Set("X-AegisGate-Fingerprint", fingerprint)
	w.Header().Set("X-AegisGate-Mode", string(currentMode))
	w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%.1f", remainingTokens))

	if !allowed {
		errPayload := []byte(fmt.Sprintf(`{"error":{"code":"rate_limit_exceeded","message":"Token bucket exhausted for client %s","retry_after_ms":%d}}`, clientIP, retryAfter.Milliseconds()))
		respHash, _ := e.casStore.Put(errPayload)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retryAfter.Seconds())+1))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write(errPayload)

		totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
		e.recordAndBroadcast(storage.TraceRecord{
			ID:              traceID,
			Timestamp:       startTotal.UTC(),
			Mode:            currentMode,
			Method:          r.Method,
			Path:            r.URL.Path,
			Query:           r.URL.RawQuery,
			Fingerprint:     fingerprint,
			StatusCode:      http.StatusTooManyRequests,
			LatencyMs:       totalMs,
			UpstreamMs:      0,
			ProxyOverheadMs: totalMs,
			RequestHeaders:  cloneHeaders(r.Header),
			ResponseHeaders: cloneHeaders(w.Header()),
			RequestBodyHash: reqHash,
			RequestSize:     len(reqBody),
			ResponseHash:    respHash,
			ResponseSize:    len(errPayload),
			RateLimited:     true,
			ClientIP:        clientIP,
		})
		return
	}

	// 2. Evaluate Chaos & Fault Injection Rules
	chaosDecision := e.chaosEng.Evaluate(r.Method, r.URL.Path)
	if chaosDecision.Matched && chaosDecision.SleepDelay > 0 {
		time.Sleep(chaosDecision.SleepDelay)
	}
	if chaosDecision.Matched && chaosDecision.AbortStatus > 0 {
		respHash, _ := e.casStore.Put(chaosDecision.AbortBody)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-AegisGate-Chaos", chaosDecision.SummaryLabel)
		w.WriteHeader(chaosDecision.AbortStatus)
		_, _ = w.Write(chaosDecision.AbortBody)

		// Also feed simulated 5xx faults into the Circuit Breaker so operators can test breaker tripping!
		cState := resilience.StateClosed
		if chaosDecision.AbortStatus >= 500 {
			cState = e.breakers.RecordOutcome(routeKey, false)
		}

		totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
		e.recordAndBroadcast(storage.TraceRecord{
			ID:              traceID,
			Timestamp:       startTotal.UTC(),
			Mode:            currentMode,
			Method:          r.Method,
			Path:            r.URL.Path,
			Query:           r.URL.RawQuery,
			Fingerprint:     fingerprint,
			StatusCode:      chaosDecision.AbortStatus,
			LatencyMs:       totalMs,
			UpstreamMs:      float64(chaosDecision.SleepDelay.Microseconds()) / 1000.0,
			ProxyOverheadMs: 0.15,
			RequestHeaders:  cloneHeaders(r.Header),
			ResponseHeaders: cloneHeaders(w.Header()),
			RequestBodyHash: reqHash,
			RequestSize:     len(reqBody),
			ResponseHash:    respHash,
			ResponseSize:    len(chaosDecision.AbortBody),
			ChaosInjected:   chaosDecision.SummaryLabel,
			CircuitState:    string(cState),
			ClientIP:        clientIP,
		})
		return
	}

	// 3. Deterministic REPLAY Mode Fast-Path (0ms Upstream Latency)
	if currentMode == storage.ModeReplay {
		if golden, err := e.walStore.FindGoldenByFingerprint(fingerprint); err == nil {
			if cachedBody, err := e.casStore.Get(golden.ResponseHash); err == nil {
				for k, vals := range golden.ResponseHeaders {
					if isHopByHopHeader(k) {
						continue
					}
					for _, v := range vals {
						w.Header().Add(k, v)
					}
				}
				w.Header().Set("X-AegisGate-Replay-Hit", "true")
				w.Header().Set("X-AegisGate-Golden-Trace", golden.ID)
				w.WriteHeader(golden.StatusCode)
				_, _ = w.Write(cachedBody)

				totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
				e.recordAndBroadcast(storage.TraceRecord{
					ID:              traceID,
					Timestamp:       startTotal.UTC(),
					Mode:            storage.ModeReplay,
					Method:          r.Method,
					Path:            r.URL.Path,
					Query:           r.URL.RawQuery,
					Fingerprint:     fingerprint,
					StatusCode:      golden.StatusCode,
					LatencyMs:       totalMs,
					UpstreamMs:      0,
					ProxyOverheadMs: totalMs,
					RequestHeaders:  cloneHeaders(r.Header),
					ResponseHeaders: cloneHeaders(w.Header()),
					RequestBodyHash: reqHash,
					RequestSize:     len(reqBody),
					ResponseHash:    golden.ResponseHash,
					ResponseSize:    len(cachedBody),
					ChaosInjected:   chaosDecision.SummaryLabel,
					CircuitState:    string(resilience.StateClosed),
					ClientIP:        clientIP,
				})
				return
			}
		}

		// Cache miss in REPLAY mode
		if !e.replayFallback {
			missBody := []byte(fmt.Sprintf(`{"error":{"code":"replay_cache_miss","message":"No deterministic golden trace recorded for fingerprint %s","method":%q,"path":%q}}`, fingerprint, r.Method, r.URL.Path))
			respHash, _ := e.casStore.Put(missBody)
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-AegisGate-Replay-Hit", "false")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write(missBody)

			totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
			e.recordAndBroadcast(storage.TraceRecord{
				ID:              traceID,
				Timestamp:       startTotal.UTC(),
				Mode:            storage.ModeReplay,
				Method:          r.Method,
				Path:            r.URL.Path,
				Query:           r.URL.RawQuery,
				Fingerprint:     fingerprint,
				StatusCode:      http.StatusNotFound,
				LatencyMs:       totalMs,
				UpstreamMs:      0,
				ProxyOverheadMs: totalMs,
				RequestHeaders:  cloneHeaders(r.Header),
				ResponseHeaders: cloneHeaders(w.Header()),
				RequestBodyHash: reqHash,
				RequestSize:     len(reqBody),
				ResponseHash:    respHash,
				ResponseSize:    len(missBody),
				ClientIP:        clientIP,
			})
			return
		}
	}

	// 4. Check Route Circuit Breaker before forwarding to Upstream
	cState, err := e.breakers.Acquire(routeKey)
	if errors.Is(err, resilience.ErrCircuitOpen) {
		cbPayload := []byte(fmt.Sprintf(`{"error":{"code":"circuit_breaker_open","message":"Circuit breaker is %s for route %s","route":%q}}`, cState, routeKey, routeKey))
		respHash, _ := e.casStore.Put(cbPayload)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-AegisGate-Circuit", string(cState))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(cbPayload)

		totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
		e.recordAndBroadcast(storage.TraceRecord{
			ID:              traceID,
			Timestamp:       startTotal.UTC(),
			Mode:            currentMode,
			Method:          r.Method,
			Path:            r.URL.Path,
			Query:           r.URL.RawQuery,
			Fingerprint:     fingerprint,
			StatusCode:      http.StatusServiceUnavailable,
			LatencyMs:       totalMs,
			UpstreamMs:      0,
			ProxyOverheadMs: totalMs,
			RequestHeaders:  cloneHeaders(r.Header),
			ResponseHeaders: cloneHeaders(w.Header()),
			RequestBodyHash: reqHash,
			RequestSize:     len(reqBody),
			ResponseHash:    respHash,
			ResponseSize:    len(cbPayload),
			CircuitState:    string(cState),
			ClientIP:        clientIP,
		})
		return
	}

	// 5. Forward Request to Target Upstream
	target := e.getTargetURL()
	upstreamURL := *target
	upstreamURL.Path = singleJoiningSlash(target.Path, r.URL.Path)
	upstreamURL.RawQuery = r.URL.RawQuery

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL.String(), bytes.NewReader(reqBody))
	if err != nil {
		http.Error(w, `{"error":"invalid_upstream_request"}`, http.StatusInternalServerError)
		return
	}
	copyHeaders(outReq.Header, r.Header)
	outReq.Header.Set("X-Forwarded-For", clientIP)
	outReq.Header.Set("X-AegisGate-Trace-Id", traceID)

	upstreamStart := time.Now()
	resp, err := e.httpClient.Do(outReq)
	upstreamMs := float64(time.Since(upstreamStart).Microseconds()) / 1000.0

	if err != nil {
		newState := e.breakers.RecordOutcome(routeKey, false)
		errBody := []byte(fmt.Sprintf(`{"error":{"code":"upstream_connection_failed","message":%q}}`, err.Error()))
		respHash, _ := e.casStore.Put(errBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write(errBody)

		totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
		e.recordAndBroadcast(storage.TraceRecord{
			ID:              traceID,
			Timestamp:       startTotal.UTC(),
			Mode:            currentMode,
			Method:          r.Method,
			Path:            r.URL.Path,
			Query:           r.URL.RawQuery,
			Fingerprint:     fingerprint,
			StatusCode:      http.StatusBadGateway,
			LatencyMs:       totalMs,
			UpstreamMs:      upstreamMs,
			ProxyOverheadMs: maxFloat(0.05, totalMs-upstreamMs),
			RequestHeaders:  cloneHeaders(r.Header),
			ResponseHeaders: cloneHeaders(w.Header()),
			RequestBodyHash: reqHash,
			RequestSize:     len(reqBody),
			ResponseHash:    respHash,
			ResponseSize:    len(errBody),
			ChaosInjected:   chaosDecision.SummaryLabel,
			CircuitState:    string(newState),
			ClientIP:        clientIP,
		})
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, e.maxBodyBytes))
	if chaosDecision.Matched && chaosDecision.Corrupt && len(respBody) > 4 {
		// Deterministic payload corruption to test client parser error handling
		respBody = append([]byte("<<CORRUPTED_BY_AEGISGATE_CHAOS>>"), respBody[4:]...)
	}

	respHash, _ := e.casStore.Put(respBody)
	isHealthy := resp.StatusCode < 500
	newState := e.breakers.RecordOutcome(routeKey, isHealthy)

	// 6. Perform Live Contract AST Diffing against Golden Trace (if one exists)
	var diffBreaking, diffNonBreaking int
	if currentMode == storage.ModeShadowDiff || currentMode == storage.ModeRecord {
		if golden, err := e.walStore.FindGoldenByFingerprint(fingerprint); err == nil && golden.ID != traceID {
			if goldenBody, err := e.casStore.Get(golden.ResponseHash); err == nil {
				rep := e.differ.Compare(
					golden.ID, traceID,
					golden.StatusCode, resp.StatusCode,
					goldenBody, respBody,
					golden.LatencyMs, upstreamMs,
				)
				diffBreaking = rep.BreakingCount
				diffNonBreaking = rep.NonBreakingCount
				if diffBreaking > 0 {
					w.Header().Set("X-AegisGate-Contract-Drift", "BREAKING")
				} else if diffNonBreaking > 0 {
					w.Header().Set("X-AegisGate-Contract-Drift", "NON_BREAKING")
				}
			}
		}
	}

	copyHeaders(w.Header(), resp.Header)
	w.Header().Set("X-AegisGate-Circuit", string(newState))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)

	totalMs := float64(time.Since(startTotal).Microseconds()) / 1000.0
	overheadMs := maxFloat(0.05, totalMs-upstreamMs-float64(chaosDecision.SleepDelay.Microseconds())/1000.0)

	e.recordAndBroadcast(storage.TraceRecord{
		ID:              traceID,
		Timestamp:       startTotal.UTC(),
		Mode:            currentMode,
		Method:          r.Method,
		Path:            r.URL.Path,
		Query:           r.URL.RawQuery,
		Fingerprint:     fingerprint,
		StatusCode:      resp.StatusCode,
		LatencyMs:       round2(totalMs),
		UpstreamMs:      round2(upstreamMs),
		ProxyOverheadMs: round2(overheadMs),
		RequestHeaders:  cloneHeaders(r.Header),
		ResponseHeaders: cloneHeaders(resp.Header),
		RequestBodyHash: reqHash,
		RequestSize:     len(reqBody),
		ResponseHash:    respHash,
		ResponseSize:    len(respBody),
		ChaosInjected:   chaosDecision.SummaryLabel,
		CircuitState:    string(newState),
		DiffBreaking:    diffBreaking,
		DiffNonBreaking: diffNonBreaking,
		ClientIP:        clientIP,
	})
}

func (e *Engine) recordAndBroadcast(rec storage.TraceRecord) {
	_ = e.walStore.Enqueue(rec)

	e.subsMu.RLock()
	for ch := range e.subscribers {
		select {
		case ch <- rec:
		default:
			// Drop slow consumer frame without blocking proxy hot path
		}
	}
	e.subsMu.RUnlock()
}

func (e *Engine) getTargetURL() *url.URL {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.targetURL
}

func (e *Engine) nextTraceID() string {
	seq := e.seqCounter.Add(1)
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("tr_%d_%s", seq, hex.EncodeToString(b[:]))
}

func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || host == "" {
		return "127.0.0.1"
	}
	return host
}

func copyHeaders(dst, src http.Header) {
	for k, vals := range src {
		if isHopByHopHeader(k) {
			continue
		}
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

func cloneHeaders(h http.Header) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, v := range h {
		copied := make([]string, len(v))
		copy(copied, v)
		out[k] = copied
	}
	return out
}

func isHopByHopHeader(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length":
		return true
	default:
		return false
	}
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}
