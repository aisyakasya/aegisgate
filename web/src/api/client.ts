import {
  ChaosRule,
  GatewayStatus,
  ProxyMode,
  TraceDetailResponse,
  TraceRecord,
} from '../types/aegis';

const API_BASE = '/api/v1';

export async function fetchGatewayStatus(): Promise<GatewayStatus> {
  const res = await fetch(`${API_BASE}/status`);
  if (!res.ok) throw new Error(`Status request failed: ${res.status}`);
  return res.json();
}

export async function fetchTraces(params?: {
  onlyDrift?: boolean;
  onlyChaos?: boolean;
  mode?: string;
}): Promise<TraceRecord[]> {
  const q = new URLSearchParams();
  q.set('limit', '120');
  if (params?.onlyDrift) q.set('only_drift', 'true');
  if (params?.onlyChaos) q.set('only_chaos', 'true');
  if (params?.mode) q.set('mode', params.mode);

  const res = await fetch(`${API_BASE}/traces?${q.toString()}`);
  if (!res.ok) throw new Error(`Traces request failed: ${res.status}`);
  const data = (await res.json()) as { traces: TraceRecord[] };
  return data.traces ?? [];
}

export async function fetchTraceDetail(traceId: string): Promise<TraceDetailResponse> {
  const res = await fetch(`${API_BASE}/traces/${encodeURIComponent(traceId)}`);
  if (!res.ok) throw new Error(`Trace detail failed: ${res.status}`);
  return res.json();
}

export async function setGatewayMode(mode: ProxyMode): Promise<ProxyMode> {
  const res = await fetch(`${API_BASE}/mode`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mode }),
  });
  if (!res.ok) throw new Error(`Failed to switch mode: ${res.status}`);
  const data = (await res.json()) as { mode: ProxyMode };
  return data.mode;
}

export async function triggerTrafficSimulation(): Promise<void> {
  const res = await fetch(`${API_BASE}/simulate`, { method: 'POST' });
  if (!res.ok) throw new Error(`Simulation failed: ${res.status}`);
}

export async function saveChaosRule(rule: ChaosRule): Promise<ChaosRule> {
  const res = await fetch(`${API_BASE}/chaos/rules`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(rule),
  });
  if (!res.ok) throw new Error(`Failed to update chaos rule: ${res.status}`);
  return res.json();
}

export async function forceCircuitState(
  route: string,
  state: 'CLOSED' | 'OPEN' | 'HALF_OPEN'
): Promise<void> {
  const res = await fetch(`${API_BASE}/circuit-breakers/force`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ route, state }),
  });
  if (!res.ok) throw new Error(`Failed to force circuit state: ${res.status}`);
}
