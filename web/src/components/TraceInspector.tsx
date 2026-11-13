import React, { useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Copy,
  FileDiff,
  Hash,
  Terminal,
} from 'lucide-react';
import { TraceDetailResponse } from '../types/aegis';

interface TraceInspectorProps {
  detail: TraceDetailResponse | null;
  loading: boolean;
}

export const TraceInspector: React.FC<TraceInspectorProps> = ({ detail, loading }) => {
  const [subTab, setSubTab] = useState<'diff' | 'payloads' | 'curl'>('diff');
  const [copied, setCopied] = useState(false);

  if (loading) {
    return (
      <div className="panel" style={{ padding: '2rem', color: 'var(--text-secondary)' }}>
        Loading trace AST &amp; CAS blobs...
      </div>
    );
  }

  if (!detail) {
    return (
      <div className="panel" style={{ padding: '2rem', color: 'var(--text-secondary)' }}>
        Select a captured trace from the stream to inspect its SHA-256 CAS payload, canonical
        fingerprint, and recursive JSON AST contract diff.
      </div>
    );
  }

  const { trace, contract_diff } = detail;

  const formatPrettyJSON = (raw: string): string => {
    if (!raw) return '<empty payload>';
    try {
      return JSON.stringify(JSON.parse(raw), null, 2);
    } catch {
      return raw;
    }
  };

  const handleCopyCurl = () => {
    navigator.clipboard.writeText(detail.reproducible_curl);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="panel">
      <div className="panel-header">
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <span className="badge badge-info mono">{trace.method}</span>
            <span className="mono" style={{ fontWeight: 700, fontSize: '0.9rem' }}>
              {trace.path}
            </span>
            <span
              className={`badge ${
                trace.status_code >= 500
                  ? 'badge-err'
                  : trace.status_code >= 400
                  ? 'badge-warn'
                  : 'badge-ok'
              } mono`}
            >
              HTTP {trace.status_code}
            </span>
          </div>
          <div
            className="mono"
            style={{ fontSize: '0.72rem', color: 'var(--text-muted)', marginTop: '0.3rem' }}
          >
            ID: {trace.id} • Fingerprint: {trace.fingerprint} • Overhead: {trace.proxy_overhead_ms}ms
          </div>
        </div>

        <div className="mode-switcher">
          <button
            className={`mode-btn ${subTab === 'diff' ? 'active' : ''}`}
            onClick={() => setSubTab('diff')}
          >
            <FileDiff size={13} style={{ marginRight: 4, verticalAlign: 'middle' }} />
            AST Diff ({contract_diff?.changes.length ?? 0})
          </button>
          <button
            className={`mode-btn ${subTab === 'payloads' ? 'active' : ''}`}
            onClick={() => setSubTab('payloads')}
          >
            <Hash size={13} style={{ marginRight: 4, verticalAlign: 'middle' }} />
            CAS Blobs
          </button>
          <button
            className={`mode-btn ${subTab === 'curl' ? 'active' : ''}`}
            onClick={() => setSubTab('curl')}
          >
            <Terminal size={13} style={{ marginRight: 4, verticalAlign: 'middle' }} />
            cURL Replay
          </button>
        </div>
      </div>

      <div style={{ padding: '1rem', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
        {subTab === 'diff' && (
          <>
            {contract_diff ? (
              <div
                style={{
                  padding: '0.75rem 1rem',
                  borderRadius: 8,
                  border: '1px solid',
                  borderColor: contract_diff.compatible
                    ? 'rgba(16, 185, 129, 0.35)'
                    : 'rgba(244, 63, 94, 0.45)',
                  background: contract_diff.compatible
                    ? 'rgba(16, 185, 129, 0.08)'
                    : 'rgba(244, 63, 94, 0.08)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                  {contract_diff.compatible ? (
                    <CheckCircle2 size={18} color="#34d399" />
                  ) : (
                    <AlertTriangle size={18} color="#fb7185" />
                  )}
                  <div>
                    <div style={{ fontWeight: 700, fontSize: '0.84rem' }}>
                      {contract_diff.compatible
                        ? 'Contract Compatible with Golden Baseline'
                        : `Breaking Schema Drift Detected (${contract_diff.breaking_count} breaking)`}
                    </div>
                    <div style={{ fontSize: '0.73rem', color: 'var(--text-secondary)' }}>
                      Golden Trace: <span className="mono">{detail.golden_trace_id}</span> •{' '}
                      {contract_diff.ignored_ephemeral} volatile UUID/timestamp fields auto-ignored
                    </div>
                  </div>
                </div>
                <span
                  className={`badge ${contract_diff.compatible ? 'badge-ok' : 'badge-err'} mono`}
                >
                  {contract_diff.compatible ? 'PASS' : 'BREAKING'}
                </span>
              </div>
            ) : (
              <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)' }}>
                No golden trace recorded for fingerprint <span className="mono">{trace.fingerprint}</span> yet.
              </div>
            )}

            {contract_diff && contract_diff.changes.length > 0 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {contract_diff.changes.map((c, idx) => (
                  <div
                    key={idx}
                    style={{
                      padding: '0.65rem 0.85rem',
                      background: 'rgba(15, 23, 42, 0.8)',
                      border: '1px solid var(--border-subtle)',
                      borderRadius: 7,
                      fontSize: '0.78rem',
                    }}
                  >
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        marginBottom: '0.25rem',
                      }}
                    >
                      <span className="mono" style={{ fontWeight: 700, color: '#38bdf8' }}>
                        {c.path}
                      </span>
                      <span
                        className={`badge ${
                          c.severity === 'BREAKING' ? 'badge-err' : 'badge-warn'
                        } mono`}
                      >
                        {c.severity} • {c.kind}
                      </span>
                    </div>
                    <div style={{ color: 'var(--text-secondary)', marginBottom: '0.3rem' }}>
                      {c.message}
                    </div>
                    <div className="mono" style={{ fontSize: '0.72rem', color: 'var(--text-muted)' }}>
                      Baseline: <span style={{ color: '#34d399' }}>{c.baseline || 'N/A'}</span> →
                      Candidate: <span style={{ color: '#fb7185' }}>{c.candidate || 'N/A'}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
              <div>
                <div className="metric-label">Golden Baseline Payload ({detail.golden_trace_id || 'N/A'})</div>
                <pre className="code-block mono">{formatPrettyJSON(detail.golden_body)}</pre>
              </div>
              <div>
                <div className="metric-label">Captured Response Payload ({trace.id})</div>
                <pre className="code-block mono">{formatPrettyJSON(detail.response_body)}</pre>
              </div>
            </div>
          </>
        )}

        {subTab === 'payloads' && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
            <div>
              <div className="metric-label">
                Request Payload • SHA-256: <span className="mono">{trace.request_body_hash.slice(0, 24)}...</span> ({trace.request_size} B)
              </div>
              <pre className="code-block mono">{formatPrettyJSON(detail.request_body)}</pre>
            </div>
            <div>
              <div className="metric-label">
                Response Payload • SHA-256: <span className="mono">{trace.response_hash.slice(0, 24)}...</span> ({trace.response_size} B)
              </div>
              <pre className="code-block mono">{formatPrettyJSON(detail.response_body)}</pre>
            </div>
          </div>
        )}

        {subTab === 'curl' && (
          <div>
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: '0.5rem',
              }}
            >
              <span className="metric-label">Deterministic Reproducible cURL Command</span>
              <button className="btn" onClick={handleCopyCurl}>
                <Copy size={13} />
                {copied ? 'Copied!' : 'Copy Command'}
              </button>
            </div>
            <pre className="code-block mono">{detail.reproducible_curl}</pre>
          </div>
        )}
      </div>
    </div>
  );
};
