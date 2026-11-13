import React, { useState } from 'react';
import { Flame, Plus, Power } from 'lucide-react';
import { ChaosRule } from '../types/aegis';

interface ChaosMatrixProps {
  rules: ChaosRule[];
  onToggleRule: (rule: ChaosRule) => Promise<void>;
  onCreateRule: (rule: ChaosRule) => Promise<void>;
}

export const ChaosMatrix: React.FC<ChaosMatrixProps> = ({
  rules,
  onToggleRule,
  onCreateRule,
}) => {
  const [name, setName] = useState('');
  const [pathPrefix, setPathPrefix] = useState('/v1/chat/completions');
  const [method, setMethod] = useState('POST');
  const [probability, setProbability] = useState(0.5);
  const [latencyMs, setLatencyMs] = useState(180);
  const [statusCode, setStatusCode] = useState(503);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    await onCreateRule({
      id: `chaos_${Date.now()}`,
      name: name.trim(),
      enabled: true,
      method,
      path_prefix: pathPrefix,
      probability,
      fixed_latency_ms: latencyMs,
      jitter_latency_ms: 50,
      fault_status_code: statusCode,
      corrupt_payload: false,
      triggered_count: 0,
    });
    setName('');
  };

  return (
    <div className="workspace-split">
      <div className="panel">
        <div className="panel-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontWeight: 700 }}>
            <Flame size={16} color="#f43f5e" />
            Active Fault Injection &amp; Chaos Rules
          </div>
          <span className="badge badge-warn mono">
            {rules.filter((r) => r.enabled).length} ENABLED
          </span>
        </div>

        <table className="trace-table">
          <thead>
            <tr>
              <th>Rule Name</th>
              <th>Target Route</th>
              <th>Fault Profile</th>
              <th>Prob</th>
              <th>Triggered</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {rules.map((r) => (
              <tr key={r.id} className="trace-row">
                <td style={{ fontWeight: 600 }}>{r.name}</td>
                <td className="mono">
                  {r.method || 'ANY'} {r.path_prefix}
                </td>
                <td className="mono">
                  +{r.fixed_latency_ms}ms (±{r.jitter_latency_ms}ms){' '}
                  {r.fault_status_code > 0 ? `• HTTP ${r.fault_status_code}` : ''}
                </td>
                <td className="mono">{Math.round(r.probability * 100)}%</td>
                <td className="mono">{r.triggered_count}x</td>
                <td>
                  <button
                    className={`btn ${r.enabled ? 'btn-primary' : ''}`}
                    onClick={() => onToggleRule({ ...r, enabled: !r.enabled })}
                  >
                    <Power size={12} />
                    {r.enabled ? 'Enabled' : 'Disabled'}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <div className="panel-header">
          <div style={{ fontWeight: 700 }}>Create Custom Chaos Experiment</div>
        </div>
        <form
          onSubmit={handleAdd}
          style={{ padding: '1rem', display: 'flex', flexDirection: 'column', gap: '0.85rem' }}
        >
          <div>
            <div className="metric-label">Experiment Name</div>
            <input
              type="text"
              placeholder="e.g. Stripe Webhook 502 Bad Gateway Partition"
              value={name}
              onChange={(e) => setName(e.target.value)}
              style={{
                width: '100%',
                padding: '0.55rem',
                borderRadius: 6,
                border: '1px solid var(--border-highlight)',
                background: '#060911',
                color: 'white',
              }}
            />
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '0.75rem' }}>
            <div>
              <div className="metric-label">HTTP Method</div>
              <select
                value={method}
                onChange={(e) => setMethod(e.target.value)}
                style={{
                  width: '100%',
                  padding: '0.55rem',
                  borderRadius: 6,
                  border: '1px solid var(--border-highlight)',
                  background: '#060911',
                  color: 'white',
                }}
              >
                <option value="POST">POST</option>
                <option value="GET">GET</option>
                <option value="">ANY</option>
              </select>
            </div>
            <div>
              <div className="metric-label">Path Prefix</div>
              <input
                type="text"
                value={pathPrefix}
                onChange={(e) => setPathPrefix(e.target.value)}
                className="mono"
                style={{
                  width: '100%',
                  padding: '0.55rem',
                  borderRadius: 6,
                  border: '1px solid var(--border-highlight)',
                  background: '#060911',
                  color: 'white',
                }}
              />
            </div>
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '0.75rem' }}>
            <div>
              <div className="metric-label">Probability (0-1)</div>
              <input
                type="number"
                step="0.05"
                min="0.05"
                max="1.0"
                value={probability}
                onChange={(e) => setProbability(Number(e.target.value))}
                style={{
                  width: '100%',
                  padding: '0.55rem',
                  borderRadius: 6,
                  border: '1px solid var(--border-highlight)',
                  background: '#060911',
                  color: 'white',
                }}
              />
            </div>
            <div>
              <div className="metric-label">Fixed Delay (ms)</div>
              <input
                type="number"
                value={latencyMs}
                onChange={(e) => setLatencyMs(Number(e.target.value))}
                style={{
                  width: '100%',
                  padding: '0.55rem',
                  borderRadius: 6,
                  border: '1px solid var(--border-highlight)',
                  background: '#060911',
                  color: 'white',
                }}
              />
            </div>
            <div>
              <div className="metric-label">Fault HTTP Status</div>
              <input
                type="number"
                value={statusCode}
                onChange={(e) => setStatusCode(Number(e.target.value))}
                style={{
                  width: '100%',
                  padding: '0.55rem',
                  borderRadius: 6,
                  border: '1px solid var(--border-highlight)',
                  background: '#060911',
                  color: 'white',
                }}
              />
            </div>
          </div>

          <button type="submit" className="btn btn-primary" style={{ alignSelf: 'flex-start' }}>
            <Plus size={14} />
            Register &amp; Enable Chaos Rule
          </button>
        </form>
      </div>
    </div>
  );
};
