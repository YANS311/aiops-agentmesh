import type {
  HealthResponse,
  IncidentDetail,
  IncidentSummary,
  SkillOut,
} from './types';

async function parseJsonOrThrow<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${text}`);
  }
  return resp.json() as Promise<T>;
}

export async function fetchHealth(): Promise<HealthResponse> {
  const resp = await fetch('/api/v1/health');
  return parseJsonOrThrow<HealthResponse>(resp);
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
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fault_type: faultType }),
  });
  return parseJsonOrThrow<IncidentSummary>(resp);
}

export async function approveIncident(
  incidentId: string,
  operator: string,
  comment?: string
): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ operator, comment }),
  });
  return parseJsonOrThrow<IncidentDetail>(resp);
}

export async function rejectIncident(
  incidentId: string,
  operator: string,
  comment?: string
): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ operator, comment }),
  });
  return parseJsonOrThrow<IncidentDetail>(resp);
}
