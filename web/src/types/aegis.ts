export type ProxyMode = 'RECORD' | 'REPLAY' | 'SHADOW_DIFF' | 'PASSTHROUGH';

export interface TraceRecord {
  id: string;
  timestamp: string;
  mode: ProxyMode;
  method: string;
  path: string;
  query?: string;
  fingerprint: string;
  status_code: number;
  latency_ms: number;
  upstream_ms: number;
  proxy_overhead_ms: number;
  request_headers?: Record<string, string[]>;
  response_headers?: Record<string, string[]>;
  request_body_hash: string;
  request_size: number;
  response_hash: string;
  response_size: number;
  chaos_injected?: string;
  circuit_state?: string;
  rate_limited?: boolean;
  diff_breaking?: number;
  diff_non_breaking?: number;
  client_ip?: string;
}

export interface LatencyMetrics {
  total_requests: number;
  replay_hits: number;
  chaos_faults: number;
  rate_limited_count: number;
  drift_detected: number;
  error_rate_pct: number;
  p50_latency_ms: number;
  p95_latency_ms: number;
  p99_latency_ms: number;
  avg_proxy_overhead_ms: number;
  wal_flushes: number;
  wal_corrupt_skipped: number;
}

export interface CASStats {
  total_puts: number;
  deduplicated_hits: number;
  cache_hits: number;
  cache_misses: number;
  logical_bytes: number;
  physical_bytes: number;
  dedup_ratio: number;
}

export interface BreakerSnapshot {
  route: string;
  state: 'CLOSED' | 'OPEN' | 'HALF_OPEN';
  consecutive_failures: number;
  consecutive_success: number;
  total_failures: number;
  total_successes: number;
  total_short_circuited: number;
  opened_at?: string;
  cooldown_remaining_ms: number;
}

export interface ChaosRule {
  id: string;
  name: string;
  enabled: boolean;
  method: string;
  path_prefix: string;
  probability: number;
  fixed_latency_ms: number;
  jitter_latency_ms: number;
  fault_status_code: number;
  fault_body?: string;
  corrupt_payload: boolean;
  triggered_count: number;
}

export interface BucketState {
  key: string;
  available_tokens: number;
  capacity: number;
  refill_per_sec: number;
  allowed_count: number;
  rejected_count: number;
  last_access: string;
}

export interface GatewayStatus {
  uptime_sec: number;
  mode: ProxyMode;
  target_url: string;
  proxy_listen: string;
  metrics: LatencyMetrics;
  cas: CASStats;
  circuit_breakers: BreakerSnapshot[];
  chaos_rules: ChaosRule[];
  rate_limiter: {
    enabled: boolean;
    capacity: number;
    refill_per_sec: number;
    total_allowed: number;
    total_denied: number;
    buckets: BucketState[];
  };
}

export interface ChangeEntry {
  path: string;
  kind: string;
  severity: 'BREAKING' | 'NON_BREAKING' | 'INFO';
  baseline?: string;
  candidate?: string;
  message: string;
}

export interface ContractDiffReport {
  baseline_trace_id: string;
  candidate_trace_id: string;
  compatible: boolean;
  breaking_count: number;
  non_breaking_count: number;
  ignored_ephemeral: number;
  changes: ChangeEntry[];
}

export interface TraceDetailResponse {
  trace: TraceRecord;
  request_body: string;
  response_body: string;
  golden_trace_id: string;
  golden_body: string;
  contract_diff?: ContractDiffReport;
  reproducible_curl: string;
}
