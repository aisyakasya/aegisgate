import React from 'react';
import { Activity, ShieldAlert } from 'lucide-react';
import { BreakerSnapshot, BucketState } from '../types/aegis';

interface ResiliencePanelProps {
  breakers: BreakerSnapshot[];
  rateLimiter: {
    enabled: boolean;
    capacity: number;
    refill_per_sec: number;
    total_allowed: number;
    total_denied: number;
    buckets: BucketState[];
  };
  onForceBreaker: (route: string, state: 'CLOSED' | 'OPEN' | 'HALF_OPEN') => Promise<void>;
}

export const ResiliencePanel: React.FC<ResiliencePanelProps> = ({
  breakers,
  rateLimiter,
  onForceBreaker,
}) => {
  return (
    <div className="workspace-split">
      <div className="panel">
        <div className="panel-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontWeight: 700 }}>
            <ShieldAlert size={16} color="#06b6d4" />
            Per-Route Circuit Breaker State Machines
          </div>
        </div>
        <table className="trace-table">
          <thead>
            <tr>
              <th>Route Key</th>
              <th>FSM State</th>
              <th>Success / Fail</th>
              <th>Short-Circuited</th>
              <th>Operator Override</th>
            </tr>
          </thead>
          <tbody>
            {breakers.map((b) => (
              <tr key={b.route} className="trace-row">
                <td className="mono" style={{ fontWeight: 600 }}>
                  {b.route}
                </td>
                <td>
                  <span
                    className={`badge ${
                      b.state === 'CLOSED'
                        ? 'badge-ok'
                        : b.state === 'HALF_OPEN'
                        ? 'badge-warn'
                        : 'badge-err'
                    } mono`}
                  >
                    {b.state}
                  </span>
                </td>
                <td className="mono">
                  {b.total_successes} ok / {b.total_failures} err
                </td>
                <td className="mono">{b.total_short_circuited}</td>
                <td style={{ display: 'flex', gap: '0.35rem' }}>
                  <button
                    className="btn"
                    onClick={() => onForceBreaker(b.route, 'CLOSED')}
                    style={{ padding: '0.25rem 0.5rem', fontSize: '0.7rem' }}
                  >
                    CLOSE
                  </button>
                  <button
                    className="btn"
                    onClick={() => onForceBreaker(b.route, 'HALF_OPEN')}
                    style={{ padding: '0.25rem 0.5rem', fontSize: '0.7rem' }}
                  >
                    HALF-OPEN
                  </button>
                  <button
                    className="btn"
                    onClick={() => onForceBreaker(b.route, 'OPEN')}
                    style={{ padding: '0.25rem 0.5rem', fontSize: '0.7rem' }}
                  >
                    TRIP OPEN
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="panel">
        <div className="panel-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontWeight: 700 }}>
            <Activity size={16} color="#10b981" />
            Sharded Token-Bucket Rate Limiter
          </div>
          <span className="badge badge-ok mono">
            {rateLimiter.capacity} burst / {rateLimiter.refill_per_sec} tok/s
          </span>
        </div>
        <table className="trace-table">
          <thead>
            <tr>
              <th>Client Bucket Key</th>
              <th>Available Tokens</th>
              <th>Allowed</th>
              <th>Rejected (429)</th>
            </tr>
          </thead>
          <tbody>
            {rateLimiter.buckets.map((b) => (
              <tr key={b.key} className="trace-row">
                <td className="mono">{b.key}</td>
                <td className="mono">
                  {b.available_tokens.toFixed(1)} / {b.capacity}
                </td>
                <td className="mono" style={{ color: '#34d399' }}>
                  {b.allowed_count}
                </td>
                <td className="mono" style={{ color: b.rejected_count > 0 ? '#fb7185' : undefined }}>
                  {b.rejected_count}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
