import type {
  ActorInfo,
  HealthResponse,
  IncidentDetail,
  IncidentSummary,
  SkillOut,
} from './types';

export const ROLE_TOKENS = {
  VIEWER: 'viewer-demo-token',
  SRE_APPROVER: 'sre-approver-demo-token',
} as const;

async function parseJsonOrThrow<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
  return resp.json() as Promise<T>;
}

function authHeaders(token?: string): Record<string, string> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  return headers;
}

export async function fetchHealth(): Promise<HealthResponse> {
  const resp = await fetch('/api/v1/health');
  return parseJsonOrThrow<HealthResponse>(resp);
}

export async function fetchCurrentActor(token: string): Promise<ActorInfo> {
  const resp = await fetch('/api/v1/auth/me', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return parseJsonOrThrow<ActorInfo>(resp);
}

export async function fetchSkills(): Promise<SkillOut[]> {
  const resp = await fetch('/api/v1/skills');
  return parseJsonOrThrow<SkillOut[]>(resp);
}

export async function fetchIncidents(): Promise<IncidentSummary[]> {
  const resp = await fetch('/api/v1/incidents');
  return parseJsonOrThrow<IncidentSummary[]>(resp);
}

export async function fetchIncidentDetail(incidentId: string): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}`);
  return parseJsonOrThrow<IncidentDetail>(resp);
}

export async function injectFault(faultType: string): Promise<IncidentSummary> {
  const resp = await fetch('/api/v1/incidents/inject', {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ fault_type: faultType }),
  });
  return parseJsonOrThrow<IncidentSummary>(resp);
}

export async function approveIncident(
  incidentId: string,
  operator: string,
  token: string,
  comment?: string
): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}/approve`, {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify({ operator, comment }),
  });
  return parseJsonOrThrow<IncidentDetail>(resp);
}

export async function rejectIncident(
  incidentId: string,
  operator: string,
  token: string,
  comment?: string
): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}/reject`, {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify({ operator, comment }),
  });
  return parseJsonOrThrow<IncidentDetail>(resp);
}
