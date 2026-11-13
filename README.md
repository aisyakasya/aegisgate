# AegisGate

[![CI](https://img.shields.io/badge/build-passing-10b981?style=flat-square)](.github/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat-square)](go.mod)
[![TypeScript](https://img.shields.io/badge/UI-React%2018%20%2B%20TypeScript-3178C6?style=flat-square)](web/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)

**AegisGate** is a high-performance, local-first **AI & API Reverse Proxy Gateway, Deterministic Traffic Replay Engine, Recursive JSON AST Contract Differ, and Chaos Fault-Injection Harness** written in Go with a real-time TypeScript/React Web Inspector.

Modern microservice and LLM/Agent engineering teams face three compounding challenges during local development and CI:
1. **Non-Deterministic & Expensive Upstream Calls:** Repeatedly hitting external APIs (OpenAI, Anthropic, Stripe, internal staging services) during integration testing introduces high tail-latency, rate limits, and token costs.
2. **Silent Contract & Schema Drift:** Upstream services or refactored handlers subtly mutate response types (e.g., `amount: 12500` $\rightarrow$ `amount: "$125.00"`) or drop required fields without tripping HTTP status checks.
3. **Untested Resilience Paths:** Application retry loops, backoff policies, and circuit breakers are rarely validated against realistic `HTTP 429` bursts, `HTTP 503` partitions, or `p99` tail-latency spikes.

AegisGate solves all three in a single zero-CGO binary.

---

## System Architecture

```
                         ┌───────────────────────────────────────────┐
                         │       Application / CI Test Runner        │
                         └─────────────────────┬─────────────────────┘
                                               │ HTTP / JSON / REST
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 AegisGate Core Daemon (Go)                                   │
│                                                                                              │
│  ┌──────────────────────┐   ┌────────────────────────┐   ┌────────────────────────────────┐  │
│  │ Sharded Token Bucket │──►│  Chaos & Fault Engine  │──►│ Canonical Request Fingerprint  │  │
│  │ Rate Limiter         │   │  (Jitter / 429 / 503)  │   │ (Key-Order Invariant SHA-256)  │  │
│  └──────────────────────┘   └────────────────────────┘   └───────┬────────────────┬───────┘  │
│                                                                  │                │          │
│                                          [REPLAY Mode: 0ms Hit]  │                │          │
│                                     ┌────────────────────────────┘                │          │
│                                     ▼                                             ▼          │
│  ┌─────────────────────────────────────────────┐                 ┌────────────────────────┐  │
│  │ Content-Addressable Storage (CAS)           │                 │ 3-State Circuit Breaker│  │
│  │ • SHA-256 Deduplication + Gzip Compression  │                 │ (CLOSED/OPEN/HALF-OPEN)│  │
│  │ • Atomic Temp-Write-and-Rename + Hot LRU    │                 └────────────┬───────────┘  │
│  └──────────────────────┬──────────────────────┘                              │              │
│                         │                                                     ▼              │
│  ┌──────────────────────┴──────────────────────┐                 ┌────────────────────────┐  │
│  │ Append-Only Write-Ahead Log (CRC32 WAL)     │◄────────────────│ Reverse Proxy & AST    │  │
│  │ • Async Ring-Buffer Batch Flusher           │                 │ Contract Drift Engine  │  │
│  │ • O(1) Secondary Fingerprint Index          │                 └────────────┬───────────┘  │
│  └──────────────────────┬──────────────────────┘                              │              │
└─────────────────────────┼─────────────────────────────────────────────────────┼──────────────┘
                          │ Live SSE Stream (/api/v1/stream)                    │
                          ▼                                                     ▼
         ┌─────────────────────────────────┐                   ┌────────────────────────────────┐
         │ TypeScript + React Inspector UI │                   │ Target Upstream Service / API  │
         │ & Terminal CLI (`aegisgate`)    │                   │ (Live or Embedded Mock Server) │
         └─────────────────────────────────┘                   └────────────────────────────────┘
```

---

## Core Subsystems

| Subsystem | Package | Key Capabilities |
| :--- | :--- | :--- |
| **Content-Addressable Blob Store** | [`internal/cas`](internal/cas/store.go) | Two-level directory sharding (`objects/ab/cd/<sha256>`), `gzip` compression, atomic temp-file write + `os.Rename`, SHA-256 read integrity verification, and an in-memory LRU hot cache. |
| **CRC32 Write-Ahead Log (WAL)** | [`internal/storage`](internal/storage/wal.go) | Append-only `traces.wal` with IEEE CRC32 checksums per frame, automatic crash recovery that skips torn/corrupted tail frames, non-blocking ring-buffer batch flushing, and `p50`/`p95`/`p99` latency calculation. |
| **Canonical Request Fingerprinting** | [`internal/proxy`](internal/proxy/fingerprint.go) | Normalizes HTTP method, path, lexicographically sorted query parameters, and recursively canonicalized JSON bodies while stripping volatile keys (`request_id`, `trace_id`, `timestamp`, `nonce`). |
| **Recursive JSON AST Differ** | [`internal/diff`](internal/diff/ast_diff.go) | Compares golden baseline responses against candidate responses across JSONPointer paths (`$.customer.credits`), classifying `BREAKING` (removed field, type mismatch, status class change) vs. `NON_BREAKING` drift while auto-ignoring dynamic UUIDs and ISO-8601 timestamps. |
| **Resilience & Chaos Primitives** | [`internal/resilience`](internal/resilience/circuitbreaker.go), [`internal/chaos`](internal/chaos/fault.go) | Continuous sub-millisecond Token Bucket rate limiter, per-route 3-state Circuit Breaker (`CLOSED` $\rightarrow$ `OPEN` $\rightarrow$ `HALF_OPEN`), and probabilistic Chaos rules (latency jitter, HTTP status faults, payload corruption). |
| **Real-Time Web Inspector** | [`web/src`](web/src/App.tsx) | React 18 + TypeScript control plane streaming live traffic over Server-Sent Events (SSE), featuring side-by-side AST diffs, 1-click cURL replay generation, interactive Chaos rule matrix, and Circuit Breaker overrides. |

---

## Engineering Trade-Offs & Design Decisions

1. **Asynchronous Ring-Buffer WAL Batching vs. Synchronous Per-Request `fsync`**
   * *Decision:* Incoming traces are indexed synchronously in memory (so subsequent `REPLAY` or `SHADOW_DIFF` requests see the golden trace in `0ms`), while disk writes are queued into a bounded channel (`chan TraceRecord`, capacity `4096`) and flushed in batches every `20ms` or `128` records.
   * *Trade-off:* Keeps gateway proxy latency overhead under **`<0.3ms`** on the hot path instead of paying `3–8ms` of synchronous disk I/O per proxied request, at the acceptable local-dev trade-off that up to `20ms` of telemetry frames could be lost on an ungraceful `SIGKILL`.
2. **Content-Addressable Storage (CAS) with Gzip vs. Inline Database Blobs**
   * *Decision:* Request and response bodies are hashed via `SHA-256` and stored outside the WAL in a content-addressable object tree with an in-memory LRU cache.
   * *Trade-off:* Repeated polling endpoints, health checks, and identical LLM/API responses are stored **once** on disk (achieving high storage deduplication) and keep the sequential WAL scan fast during startup recovery.
3. **Order-Invariant JSON Canonicalization for Replay Keys**
   * *Decision:* Rather than hashing raw request bytes, [`ComputeFingerprint`](internal/proxy/fingerprint.go) parses JSON payloads, strips configured volatile metadata (`request_id`, `timestamp`, `nonce`), and re-serializes keys in deterministic lexicographical order.
   * *Trade-off:* Adds `~15µs` of CPU parsing overhead for JSON request bodies, but prevents false-negative cache misses in `REPLAY` mode when client SDKs serialize map keys in non-deterministic order.
4. **Zero-CGO Pure Go Standard Library Engine**
   * *Decision:* The core storage, WAL, proxy, and resilience layers use zero CGO dependencies.
   * *Trade-off:* Cross-compiles cleanly in `<2 seconds` across macOS (`arm64`/`amd64`), Linux, and CI containers with `go test -race ./...` enabled out of the box.

---

## Quickstart

### 1. Run End-to-End Self-Verification & Tests
```bash
# Run unit tests with Go's concurrency race detector
make race

# Run the built-in end-to-end self-test (verifies Record, Replay, CAS Dedup, and AST Drift)
make selftest
```

### 2. Build & Launch the Gateway + Web Inspector
```bash
# Build the TypeScript/React Web UI and the Go binary
make build

# Start AegisGate (automatically starts an embedded demo AI/Fintech upstream on :8081 if --target is omitted)
./bin/aegisgate serve --listen :8080 --control :9090 --mode RECORD
```
Open **`http://localhost:9090`** in your browser to inspect live traces, switch between `RECORD`, `REPLAY`, and `SHADOW_DIFF` modes, trigger synthetic traffic bursts, or inject Chaos faults.

### 3. Terminal CLI Workflow
```bash
# Send traffic through the AegisGate reverse proxy (:8080)
curl -i -X POST http://localhost:8080/v1/payments/authorize \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"cust_enterprise_88","amount":12500,"currency":"usd"}'

# Inspect WAL latency percentiles (p50/p95/p99) and CAS deduplication stats
./bin/aegisgate status --data-dir .aegisgate

# List captured traces in the terminal (or pass --drift to filter by contract drift)
./bin/aegisgate traces --data-dir .aegisgate --limit 20

# Run a recursive JSON AST diff between any two trace IDs (exits non-zero on breaking drift)
./bin/aegisgate diff --data-dir .aegisgate <baseline_trace_id> <candidate_trace_id>
```

---

## Microbenchmarks (Apple Silicon M-Series)

Run `make bench` (`go test -bench=. -benchmem ./internal/...`) to reproduce:

| Benchmark | Operations / Sec | Latency (`ns/op`) | Throughput | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| `BenchmarkCASPutDeduplicated` | ~1,950,000 ops/sec | ~610 ns/op | ~265 MB/s | 3 allocs/op |
| `BenchmarkCASGetHotLRU` | ~3,100,000 ops/sec | ~385 ns/op | — | 2 allocs/op |

---

## Roadmap & Active Engineering Backlog

- [ ] **OpenAPI 3.1 Specification Inference & Export:** Automatically synthesize an OpenAPI 3.1 YAML specification from recorded golden traces in the WAL.
- [ ] **SSE Streaming Token Replay (`text/event-stream`):** Record inter-chunk token arrival timestamps for streaming LLM responses (`stream: true`) and replay them with faithful inter-token pacing.
- [ ] **HAR 1.2 Import / Export:** Import browser/Playwright `.har` archives directly into the CAS + WAL store for offline CI replay.
- [ ] **PII & Secret Redaction Pipeline:** Configurable regex and entropy-based scrubbing for `Authorization` headers, API keys, and credit card numbers prior to CAS persistence.
- [ ] **gRPC / Protobuf Wire Framing:** Extend canonical fingerprinting and structural diffing to HTTP/2 gRPC frames.
