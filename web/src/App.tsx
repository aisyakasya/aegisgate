import React, { useCallback, useEffect, useState } from 'react';
import {
  Activity,
  Flame,
  Layers,
  Play,
  RefreshCw,
  Shield,
  Zap,
} from 'lucide-react';
import {
  fetchGatewayStatus,
  fetchTraceDetail,
  fetchTraces,
  forceCircuitState,
  saveChaosRule,
  setGatewayMode,
  triggerTrafficSimulation,
} from './api/client';
import {
  ChaosRule,
  GatewayStatus,
  ProxyMode,
  TraceDetailResponse,
  TraceRecord,
} from './types/aegis';
import { TraceInspector } from './components/TraceInspector';
import { ChaosMatrix } from './components/ChaosMatrix';
import { ResiliencePanel } from './components/ResiliencePanel';

const MODES: ProxyMode[] = ['RECORD', 'REPLAY', 'SHADOW_DIFF', 'PASSTHROUGH'];

export const App: React.FC = () => {
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [traces, setTraces] = useState<TraceRecord[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [traceDetail, setTraceDetail] = useState<TraceDetailResponse | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [activeTab, setActiveTab] = useState<'stream' | 'chaos' | 'resilience'>('stream');
  const [onlyDrift, setOnlyDrift] = useState(false);

  const refreshAll = useCallback(async () => {
    try {
      const [st, tr] = await Promise.all([
        fetchGatewayStatus(),
        fetchTraces({ onlyDrift }),
      ]);
      setStatus(st);
      setTraces(tr);
      if (!selectedId && tr.length > 0) {
        setSelectedId(tr[0].id);
      }
    } catch (err) {
      console.error('Failed to refresh gateway telemetry:', err);
    }
  }, [onlyDrift, selectedId]);

  useEffect(() => {
    refreshAll();
    const interval = setInterval(refreshAll, 2500);
    return () => clearInterval(interval);
  }, [refreshAll]);

  useEffect(() => {
    const es = new EventSource('/api/v1/stream');
    es.addEventListener('trace', (evt) => {
      try {
        const rec = JSON.parse((evt as MessageEvent).data) as TraceRecord;
        setTraces((prev) => [rec, ...prev.slice(0, 119)]);
      } catch {
        // ignore malformed frame
      }
    });
    return () => es.close();
  }, []);

  useEffect(() => {
    if (!selectedId) return;
    let cancelled = false;
    setLoadingDetail(true);
    fetchTraceDetail(selectedId)
      .then((d) => {
        if (!cancelled) setTraceDetail(d);
      })
      .catch(console.error)
      .finally(() => {
        if (!cancelled) setLoadingDetail(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedId]);

  const handleModeSwitch = async (mode: ProxyMode) => {
    await setGatewayMode(mode);
    await refreshAll();
  };

  const handleSimulate = async () => {
    await triggerTrafficSimulation();
    await refreshAll();
  };

  const handleToggleRule = async (rule: ChaosRule) => {
    await saveChaosRule(rule);
    await refreshAll();
  };

  const handleForceBreaker = async (
    route: string,
    state: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
  ) => {
    await forceCircuitState(route, state);
    await refreshAll();
  };

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-badge">
            <Zap size={20} />
          </div>
          <div>
            <div style={{ fontWeight: 800, fontSize: '1.05rem', letterSpacing: '-0.02em' }}>
              AegisGate <span style={{ color: 'var(--accent-cyan)', fontWeight: 600 }}>v0.4</span>
            </div>
            <div style={{ fontSize: '0.73rem', color: 'var(--text-secondary)' }}>
              Deterministic Replay, AST Contract Drift &amp; Chaos Gateway •{' '}
              <span className="mono">{status?.target_url ?? 'connecting...'}</span>
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.85rem' }}>
          <div className="mode-switcher">
            {MODES.map((m) => (
              <button
                key={m}
                className={`mode-btn mono ${status?.mode === m ? 'active' : ''}`}
                onClick={() => handleModeSwitch(m)}
              >
                {m}
              </button>
            ))}
          </div>

          <button className="btn btn-primary" onClick={handleSimulate}>
            <Play size={14} />
            Simulate Traffic Burst
          </button>

          <button className="btn" onClick={refreshAll}>
            <RefreshCw size={14} />
          </button>
        </div>
      </header>

      <section className="metrics-strip">
        <div className="metric-card">
          <div className="metric-label">Total Intercepted Traces</div>
          <div className="metric-value mono">{status?.metrics.total_requests ?? 0}</div>
          <div className="metric-sub mono">
            Replay Hits: {status?.metrics.replay_hits ?? 0} • WAL Flushes:{' '}
            {status?.metrics.wal_flushes ?? 0}
          </div>
        </div>

        <div className="metric-card">
          <div className="metric-label">Latency Distribution (p50 / p95 / p99)</div>
          <div className="metric-value mono">
            {status?.metrics.p50_latency_ms ?? 0} / {status?.metrics.p95_latency_ms ?? 0} /{' '}
            {status?.metrics.p99_latency_ms ?? 0}ms
          </div>
          <div className="metric-sub mono">
            Avg Gateway Overhead: {status?.metrics.avg_proxy_overhead_ms ?? 0}ms
          </div>
        </div>

        <div className="metric-card">
          <div className="metric-label">CAS Storage Deduplication</div>
          <div className="metric-value mono">
            {status?.cas.dedup_ratio ? `${status.cas.dedup_ratio.toFixed(2)}x` : '1.00x'}
          </div>
          <div className="metric-sub mono">
            Dedup Hits: {status?.cas.deduplicated_hits ?? 0} • Cache Hits:{' '}
            {status?.cas.cache_hits ?? 0}
          </div>
        </div>

        <div className="metric-card">
          <div className="metric-label">AST Contract Drift</div>
          <div
            className="metric-value mono"
            style={{
              color:
                (status?.metrics.drift_detected ?? 0) > 0
                  ? '#fb7185'
                  : 'var(--accent-emerald)',
            }}
          >
            {status?.metrics.drift_detected ?? 0} traces
          </div>
          <div className="metric-sub">Recursive JSON schema &amp; status checks</div>
        </div>

        <div className="metric-card">
          <div className="metric-label">Chaos Faults &amp; Rate Limits</div>
          <div className="metric-value mono">
            {status?.metrics.chaos_faults ?? 0} / {status?.metrics.rate_limited_count ?? 0}
          </div>
          <div className="metric-sub mono">Error Rate: {status?.metrics.error_rate_pct ?? 0}%</div>
        </div>
      </section>

      <nav className="nav-tabs">
        <button
          className={`nav-tab ${activeTab === 'stream' ? 'active' : ''}`}
          onClick={() => setActiveTab('stream')}
        >
          <Layers size={15} />
          Live Traffic Stream &amp; AST Diff Inspector
        </button>
        <button
          className={`nav-tab ${activeTab === 'chaos' ? 'active' : ''}`}
          onClick={() => setActiveTab('chaos')}
        >
          <Flame size={15} />
          Chaos &amp; Fault Injection Matrix
        </button>
        <button
          className={`nav-tab ${activeTab === 'resilience' ? 'active' : ''}`}
          onClick={() => setActiveTab('resilience')}
        >
          <Shield size={15} />
          Circuit Breakers &amp; Token Bucket Rate Limiter
        </button>
      </nav>

      {activeTab === 'stream' && (
        <main className="workspace-split">
          <div className="panel">
            <div className="panel-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontWeight: 700 }}>
                <Activity size={16} color="#06b6d4" />
                Intercepted API &amp; LLM Transactions
              </div>
              <label
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.4rem',
                  fontSize: '0.76rem',
                  color: 'var(--text-secondary)',
                  cursor: 'pointer',
                }}
              >
                <input
                  type="checkbox"
                  checked={onlyDrift}
                  onChange={(e) => setOnlyDrift(e.target.checked)}
                />
                Only Contract Drift
              </label>
            </div>

            <div style={{ overflowY: 'auto', maxHeight: 'calc(100vh - 300px)' }}>
              <table className="trace-table">
                <thead>
                  <tr>
                    <th>Mode</th>
                    <th>Endpoint</th>
                    <th>Status</th>
                    <th>Latency</th>
                    <th>Fingerprint</th>
                    <th>Telemetry Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {traces.map((t) => (
                    <tr
                      key={t.id}
                      className={`trace-row ${selectedId === t.id ? 'selected' : ''}`}
                      onClick={() => setSelectedId(t.id)}
                    >
                      <td>
                        <span className="badge badge-info mono">{t.mode}</span>
                      </td>
                      <td className="mono">
                        <strong style={{ color: '#38bdf8' }}>{t.method}</strong> {t.path}
                      </td>
                      <td>
                        <span
                          className={`badge ${
                            t.status_code >= 500
                              ? 'badge-err'
                              : t.status_code >= 400
                              ? 'badge-warn'
                              : 'badge-ok'
                          } mono`}
                        >
                          {t.status_code}
                        </span>
                      </td>
                      <td className="mono">{t.latency_ms}ms</td>
                      <td className="mono" style={{ color: 'var(--text-secondary)' }}>
                        {t.fingerprint.slice(0, 13)}
                      </td>
                      <td>
                        <div style={{ display: 'flex', gap: '0.3rem', flexWrap: 'wrap' }}>
                          {(t.diff_breaking ?? 0) > 0 && (
                            <span className="badge badge-err mono">
                              BREAKING ({t.diff_breaking})
                            </span>
                          )}
                          {(t.diff_non_breaking ?? 0) > 0 && (t.diff_breaking ?? 0) === 0 && (
                            <span className="badge badge-warn mono">
                              DRIFT ({t.diff_non_breaking})
                            </span>
                          )}
                          {t.chaos_injected && (
                            <span className="badge badge-warn mono">CHAOS</span>
                          )}
                          {t.rate_limited && (
                            <span className="badge badge-err mono">429 LIMIT</span>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <TraceInspector detail={traceDetail} loading={loadingDetail} />
        </main>
      )}

      {activeTab === 'chaos' && (
        <ChaosMatrix
          rules={status?.chaos_rules ?? []}
          onToggleRule={handleToggleRule}
          onCreateRule={handleToggleRule}
        />
      )}

      {activeTab === 'resilience' && status && (
        <ResiliencePanel
          breakers={status.circuit_breakers}
          rateLimiter={status.rate_limiter}
          onForceBreaker={handleForceBreaker}
        />
      )}
    </div>
  );
};
