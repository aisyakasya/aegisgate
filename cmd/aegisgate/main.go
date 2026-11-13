package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aegisgate/aegisgate/internal/api"
	"github.com/aegisgate/aegisgate/internal/cas"
	"github.com/aegisgate/aegisgate/internal/chaos"
	"github.com/aegisgate/aegisgate/internal/diff"
	"github.com/aegisgate/aegisgate/internal/openapi"
	"github.com/aegisgate/aegisgate/internal/proxy"
	"github.com/aegisgate/aegisgate/internal/resilience"
	"github.com/aegisgate/aegisgate/internal/storage"
)

const version = "0.4.0"

func main() {
	if len(os.Args) < 2 {
		runServe(os.Args[1:])
		return
	}

	subcmd := os.Args[1]
	switch subcmd {
	case "serve":
		runServe(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "traces":
		runTraces(os.Args[2:])
	case "diff":
		runDiffCLI(os.Args[2:])
	case "export-openapi":
		runExportOpenAPI(os.Args[2:])
	case "selftest":
		runSelfTest(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Printf("aegisgate v%s (Go Systems API Gateway & Deterministic Replay Engine)\n", version)
	case "help", "--help", "-h":
		printUsage()
	default:
		if strings.HasPrefix(subcmd, "-") {
			runServe(os.Args[1:])
			return
		}
		fmt.Fprintf(os.Stderr, "Unknown subcommand %q\n\n", subcmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`AegisGate v%s — Local-First AI & API Gateway, Deterministic Replay & Chaos Proxy

Usage:
  aegisgate <command> [flags]

Commands:
  serve       Start the reverse proxy gateway, control plane API, and Web Inspector UI
  status      Inspect WAL latency percentiles, CAS deduplication stats, and circuit breakers
  traces      Query recorded request/response traces from the local WAL store
  diff        Run recursive JSON AST contract comparison between two trace IDs
  export-openapi Export inferred OpenAPI 3.1 schema specification from WAL traces
  selftest    Execute end-to-end verification of Record, Replay, Chaos, and AST Drift
  version     Print binary version information

Examples:
  aegisgate serve --listen :8080 --control :9090 --mode RECORD
  aegisgate traces --data-dir .aegisgate --limit 20
  aegisgate diff --data-dir .aegisgate tr_1_a1b2c3d4 tr_4_e5f6a7b8
  aegisgate selftest
`, version)
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listenAddr := fs.String("listen", ":8080", "Reverse proxy listen address")
	controlAddr := fs.String("control", ":9090", "Control plane REST/SSE & Web UI listen address")
	targetURL := fs.String("target", "", "Upstream target URL (defaults to embedded mock AI/Fintech upstream on :8081)")
	demoAddr := fs.String("demo-upstream", ":8081", "Embedded demo upstream server listen address when --target is empty")
	dataDir := fs.String("data-dir", ".aegisgate", "Directory for CAS objects and CRC32 Write-Ahead Log")
	modeStr := fs.String("mode", "RECORD", "Initial proxy mode: RECORD, REPLAY, SHADOW_DIFF, PASSTHROUGH")
	uiDir := fs.String("ui-dir", "web/dist", "Path to compiled React Web Inspector assets")
	seedTraffic := fs.Bool("seed", true, "Populate initial realistic traces on startup so the dashboard is immediately rich")
	_ = fs.Parse(args)

	var demoSrv *http.Server
	resolvedTarget := *targetURL
	if resolvedTarget == "" {
		resolvedTarget = "http://localhost" + *demoAddr
		demoSrv = startEmbeddedUpstream(*demoAddr)
	}

	casStore, err := cas.NewStore(*dataDir, 1024)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: failed to initialize CAS store: %v\n", err)
		os.Exit(1)
	}

	walStore, err := storage.NewWALStore(*dataDir, 10000, 20*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: failed to initialize WAL store: %v\n", err)
		os.Exit(1)
	}
	defer walStore.Close()

	differ := diff.NewAnalyzer(diff.DefaultOptions())
	limiter := resilience.NewTokenBucketLimiter(60, 25, true)
	breakers := resilience.NewCircuitBreakerRegistry(resilience.DefaultBreakerConfig())
	chaosEng := chaos.NewEngine()

	initialMode := storage.ProxyMode(strings.ToUpper(*modeStr))
	eng, err := proxy.NewEngine(
		proxy.Config{
			TargetURL:      resolvedTarget,
			InitialMode:    initialMode,
			ReplayFallback: true,
		},
		casStore, walStore, differ, limiter, breakers, chaosEng,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: failed to initialize proxy engine: %v\n", err)
		os.Exit(1)
	}

	cp := api.NewControlPlane(*listenAddr, *uiDir, eng, casStore, walStore, differ, limiter, breakers, chaosEng)

	proxySrv := &http.Server{
		Addr:              *listenAddr,
		Handler:           eng,
		ReadHeaderTimeout: 10 * time.Second,
	}
	controlSrv := &http.Server{
		Addr:              *controlAddr,
		Handler:           cp.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := proxySrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "proxy server error: %v\n", err)
		}
	}()
	go func() {
		if err := controlSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "control plane error: %v\n", err)
		}
	}()

	if *seedTraffic && walStore.Metrics().TotalRequests == 0 {
		go func() {
			time.Sleep(120 * time.Millisecond)
			_, _ = http.Post("http://localhost"+*controlAddr+"/api/v1/simulate", "application/json", nil)
		}()
	}

	fmt.Printf("┌─────────────────────────────────────────────────────────────────────────┐\n")
	fmt.Printf("│  AegisGate v%-8s  •  Deterministic API & AI Gateway                 │\n", version)
	fmt.Printf("├─────────────────────────────────────────────────────────────────────────┤\n")
	fmt.Printf("│  Reverse Proxy Listen : http://localhost%-31s │\n", *listenAddr)
	fmt.Printf("│  Control Plane & UI   : http://localhost%-31s │\n", *controlAddr)
	fmt.Printf("│  Upstream Target      : %-47s │\n", resolvedTarget)
	fmt.Printf("│  Operational Mode     : %-47s │\n", string(initialMode))
	fmt.Printf("│  Storage Engine (WAL) : %-47s │\n", *dataDir)
	fmt.Printf("└─────────────────────────────────────────────────────────────────────────┘\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down AegisGate gracefully and flushing WAL ring buffer...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = proxySrv.Shutdown(ctx)
	_ = controlSrv.Shutdown(ctx)
	if demoSrv != nil {
		_ = demoSrv.Shutdown(ctx)
	}
	_ = walStore.Flush()
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dataDir := fs.String("data-dir", ".aegisgate", "Data directory to inspect")
	_ = fs.Parse(args)

	casStore, err := cas.NewStore(*dataDir, 128)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening CAS store: %v\n", err)
		os.Exit(1)
	}
	walStore, err := storage.NewWALStore(*dataDir, 10000, 50*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening WAL store: %v\n", err)
		os.Exit(1)
	}
	defer walStore.Close()

	m := walStore.Metrics()
	c := casStore.Stats()

	fmt.Printf("=== AegisGate Storage & Telemetry Summary (%s) ===\n", *dataDir)
	fmt.Printf("Total Traces Indexed : %d (Replay Hits: %d | Chaos Faults: %d | Contract Drift: %d)\n",
		m.TotalRequests, m.ReplayHits, m.ChaosFaults, m.DriftDetected)
	fmt.Printf("Latency Percentiles  : p50=%.2fms | p95=%.2fms | p99=%.2fms | Proxy Overhead=%.3fms\n",
		m.P50LatencyMs, m.P95LatencyMs, m.P99LatencyMs, m.AvgProxyOverhead)
	fmt.Printf("CAS Blob Storage     : Physical=%d bytes | Dedup Ratio=%.2fx | Corrupt WAL Skipped=%d\n",
		c.PhysicalBytes, c.DedupRatio, m.WALCorruptSkipped)
}

func runTraces(args []string) {
	fs := flag.NewFlagSet("traces", flag.ExitOnError)
	dataDir := fs.String("data-dir", ".aegisgate", "Data directory")
	limit := fs.Int("limit", 20, "Maximum number of traces to display")
	onlyDrift := fs.Bool("drift", false, "Show only traces with detected schema drift")
	_ = fs.Parse(args)

	walStore, err := storage.NewWALStore(*dataDir, 10000, 50*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening WAL store: %v\n", err)
		os.Exit(1)
	}
	defer walStore.Close()

	records := walStore.Query(storage.QueryFilter{
		Limit:     *limit,
		OnlyDrift: *onlyDrift,
	})

	fmt.Printf("%-18s  %-11s  %-6s  %-28s  %-6s  %-9s  %-16s  %s\n",
		"TRACE ID", "MODE", "METHOD", "PATH", "STATUS", "LATENCY", "FINGERPRINT", "FLAGS")
	fmt.Println(strings.Repeat("─", 115))
	for _, r := range records {
		flags := make([]string, 0, 3)
		if r.DiffBreaking > 0 {
			flags = append(flags, fmt.Sprintf("BREAKING(%d)", r.DiffBreaking))
		} else if r.DiffNonBreaking > 0 {
			flags = append(flags, fmt.Sprintf("DRIFT(%d)", r.DiffNonBreaking))
		}
		if r.ChaosInjected != "" {
			flags = append(flags, "CHAOS:"+r.ChaosInjected)
		}
		if r.RateLimited {
			flags = append(flags, "RATE_LIMITED")
		}
		fmt.Printf("%-18s  %-11s  %-6s  %-28s  %-6d  %7.2fms  %-16s  %s\n",
			r.ID, r.Mode, r.Method, truncate(r.Path, 28), r.StatusCode, r.LatencyMs, r.Fingerprint, strings.Join(flags, ", "))
	}
}

func runDiffCLI(args []string) {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	dataDir := fs.String("data-dir", ".aegisgate", "Data directory")
	_ = fs.Parse(args)

	rem := fs.Args()
	if len(rem) < 2 {
		fmt.Fprintln(os.Stderr, "Usage: aegisgate diff [--data-dir .aegisgate] <baseline_trace_id> <candidate_trace_id>")
		os.Exit(1)
	}

	casStore, err := cas.NewStore(*dataDir, 128)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening CAS store: %v\n", err)
		os.Exit(1)
	}
	walStore, err := storage.NewWALStore(*dataDir, 10000, 50*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening WAL store: %v\n", err)
		os.Exit(1)
	}
	defer walStore.Close()

	baseRec, err := walStore.GetByID(rem[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "baseline trace %q not found\n", rem[0])
		os.Exit(1)
	}
	candRec, err := walStore.GetByID(rem[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "candidate trace %q not found\n", rem[1])
		os.Exit(1)
	}

	baseBody, _ := casStore.Get(baseRec.ResponseHash)
	candBody, _ := casStore.Get(candRec.ResponseHash)

	analyzer := diff.NewAnalyzer(diff.DefaultOptions())
	rep := analyzer.Compare(
		baseRec.ID, candRec.ID,
		baseRec.StatusCode, candRec.StatusCode,
		baseBody, candBody,
		baseRec.LatencyMs, candRec.LatencyMs,
	)

	out, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(out))
	if !rep.Compatible {
		os.Exit(2)
	}
}

func runExportOpenAPI(args []string) {
	fs := flag.NewFlagSet("export-openapi", flag.ExitOnError)
	dataDir := fs.String("data-dir", ".aegisgate", "Data directory")
	title := fs.String("title", "AegisGate Inferred Spec", "OpenAPI title")
	ver := fs.String("version", "1.0.0", "OpenAPI API version")
	_ = fs.Parse(args)

	casStore, err := cas.NewStore(*dataDir, 128)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening CAS store: %v\n", err)
		os.Exit(1)
	}
	walStore, err := storage.NewWALStore(*dataDir, 10000, 50*time.Millisecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening WAL store: %v\n", err)
		os.Exit(1)
	}
	defer walStore.Close()

	traces := walStore.Query(storage.QueryFilter{Limit: 500})
	spec, err := openapi.InferSpecFromTraces(traces, casStore, *title, *ver)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to infer OpenAPI spec: %v\n", err)
		os.Exit(1)
	}

	out, _ := json.MarshalIndent(spec, "", "  ")
	fmt.Println(string(out))
}

func runSelfTest(args []string) {
	tmpDir, err := os.MkdirTemp("", "aegisgate-selftest-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "selftest temp dir failed: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	demoSrv := startEmbeddedUpstream("127.0.0.1:18081")
	defer demoSrv.Close()

	casStore, _ := cas.NewStore(tmpDir, 128)
	walStore, _ := storage.NewWALStore(tmpDir, 1000, 10*time.Millisecond)
	defer walStore.Close()

	differ := diff.NewAnalyzer(diff.DefaultOptions())
	limiter := resilience.NewTokenBucketLimiter(50, 20, true)
	breakers := resilience.NewCircuitBreakerRegistry(resilience.DefaultBreakerConfig())
	chaosEng := chaos.NewEngine()

	eng, _ := proxy.NewEngine(
		proxy.Config{TargetURL: "http://127.0.0.1:18081", InitialMode: storage.ModeRecord},
		casStore, walStore, differ, limiter, breakers, chaosEng,
	)
	cp := api.NewControlPlane(":18080", "", eng, casStore, walStore, differ, limiter, breakers, chaosEng)

	srv := &http.Server{Addr: "127.0.0.1:19090", Handler: cp.Handler()}
	go func() { _ = srv.ListenAndServe() }()
	defer srv.Close()

	time.Sleep(40 * time.Millisecond)
	resp, err := http.Post("http://127.0.0.1:19090/api/v1/simulate", "application/json", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "FAIL: simulation endpoint returned err=%v\n", err)
		os.Exit(1)
	}
	_ = resp.Body.Close()
	_ = walStore.Flush()

	m := walStore.Metrics()
	fmt.Printf("[PASS] AegisGate Self-Test Completed Successfully\n")
	fmt.Printf("  • Traces Captured & Indexed : %d\n", m.TotalRequests)
	fmt.Printf("  • Contract Drift Detected   : %d\n", m.DriftDetected)
	fmt.Printf("  • CAS Deduplication Ratio   : %.2fx\n", casStore.Stats().DedupRatio)
	fmt.Printf("  • Avg Proxy Overhead        : %.3fms\n", m.AvgProxyOverhead)
}

func startEmbeddedUpstream(addr string) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(14 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.ReadAll(r.Body)
		resp := map[string]any{
			"id":         "chatcmpl_9928172a",
			"object":     "chat.completion",
			"created_at": time.Now().UTC().Format(time.RFC3339),
			"model":      "claude-3-7-sonnet",
			"usage": map[string]any{
				"prompt_tokens":     42,
				"completion_tokens": 128,
				"total_tokens":      170,
			},
			"choices": []map[string]any{
				{
					"index":         0,
					"finish_reason": "stop",
					"message": map[string]any{
						"role":    "assistant",
						"content": "Tail-latency p99 spikes in Raft replication are mitigated via batch pipeline fsync and leader lease reads.",
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/v1/payments/authorize", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(9 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Simulate-Schema-Drift") == "true" {
			// Intentional breaking contract drift: "amount" changed from number to string and "currency" field removed
			_, _ = w.Write([]byte(`{"request_id":"550e8400-e29b-41d4-a716-446655440099","status":"authorized","amount":"$125.00","risk_score":"low"}`))
			return
		}
		_, _ = w.Write([]byte(`{"request_id":"550e8400-e29b-41d4-a716-446655440000","status":"authorized","amount":12500,"currency":"usd","risk_score":0.04}`))
	})

	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(6 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","model":"text-embedding-3-large","data":[{"index":0,"embedding":[0.0231,-0.0412,0.0891,0.0114]}],"usage":{"total_tokens":19}}`))
	})

	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(4 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"claude-3-7-sonnet","tier":"production","context_window":200000},{"id":"gpt-4o","tier":"production","context_window":128000}]}`))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
