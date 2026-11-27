import type {
  A2APeerDiscoverResponse,
  A2APeerOut,
  ActorInfo,
  HealthResponse,
  IncidentDetail,
  IncidentSummary,
  NLParseResponse,
  SkillOut,
  TraceEvent,
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

function authHeaders(token: string, jsonContent = true): Record<string, string> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
  };
  if (jsonContent) {
    headers['Content-Type'] = 'application/json';
  }
  return headers;
}

export async function fetchHealth(): Promise<HealthResponse> {
  const resp = await fetch('/api/v1/health');
  return parseJsonOrThrow<HealthResponse>(resp);
}

export async function fetchCurrentActor(token: string): Promise<ActorInfo> {
  const resp = await fetch('/api/v1/auth/me', {
    headers: authHeaders(token, false),
  });
  return parseJsonOrThrow<ActorInfo>(resp);
}

export async function fetchSkills(token: string): Promise<SkillOut[]> {
  const resp = await fetch('/api/v1/skills', {
    headers: authHeaders(token, false),
  });
  return parseJsonOrThrow<SkillOut[]>(resp);
}

export async function fetchIncidents(token: string): Promise<IncidentSummary[]> {
  const resp = await fetch('/api/v1/incidents', {
    headers: authHeaders(token, false),
  });
  return parseJsonOrThrow<IncidentSummary[]>(resp);
}

export async function fetchIncidentDetail(
  incidentId: string,
  token: string
): Promise<IncidentDetail> {
  const resp = await fetch(`/api/v1/incidents/${incidentId}`, {
    headers: authHeaders(token, false),
  });
  return parseJsonOrThrow<IncidentDetail>(resp);
}

export function subscribeIncidentEvents(
  incidentId: string,
  token: string,
  onTrace: (ev: TraceEvent) => void,
  onComplete: () => void,
  onError?: (err: Error) => void
): () => void {
  const controller = new AbortController();

  (async () => {
    try {
      const resp = await fetch(`/api/v1/incidents/${incidentId}/events`, {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'text/event-stream',
        },
        signal: controller.signal,
      });
      if (!resp.ok || !resp.body) {
        throw new Error(`SSE HTTP ${resp.status}`);
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buffer = '';

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        let boundary = buffer.indexOf('\n\n');
        while (boundary !== -1) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);

          let eventName = 'message';
          const dataLines: string[] = [];
          for (const rawLine of frame.split(/\r?\n/)) {
            if (!rawLine || rawLine.startsWith(':')) continue;
            if (rawLine.startsWith('event:')) {
              eventName = rawLine.slice(6).trim();
            } else if (rawLine.startsWith('data:')) {
              dataLines.push(rawLine.slice(5).trim());
            }
          }

          if (dataLines.length > 0) {
            const payloadStr = dataLines.join('\n');
            if (eventName === 'trace') {
              onTrace(JSON.parse(payloadStr) as TraceEvent);
            } else if (eventName === 'complete') {
              onComplete();
              return;
            }
          }
          boundary = buffer.indexOf('\n\n');
        }
      }
    } catch (err) {
      if (controller.signal.aborted) return;
      if (onError) {
        onError(err instanceof Error ? err : new Error(String(err)));
      }
    }
  })();

  return () => controller.abort();
}

export async function injectFault(faultType: string, token: string): Promise<IncidentSummary> {
  const resp = await fetch('/api/v1/incidents/inject', {
    method: 'POST',
    headers: authHeaders(token),
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

export async function fetchA2APeers(token: string): Promise<A2APeerOut[]> {
  const resp = await fetch('/api/v1/a2a/peers', {
    headers: authHeaders(token, false),
  });
  return parseJsonOrThrow<A2APeerOut[]>(resp);
}

export async function discoverA2APeer(
  baseUrl: string,
  token: string
): Promise<A2APeerDiscoverResponse> {
  const resp = await fetch('/api/v1/a2a/peers/discover', {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify({ base_url: baseUrl }),
  });
  return parseJsonOrThrow<A2APeerDiscoverResponse>(resp);
}

export async function registerA2APeer(
  baseUrl: string,
  token: string
): Promise<A2APeerOut> {
  const resp = await fetch('/api/v1/a2a/peers', {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify({ base_url: baseUrl, enabled: true }),
  });
  return parseJsonOrThrow<A2APeerOut>(resp);
}

export async function deleteA2APeer(peerId: string, token: string): Promise<void> {
  const resp = await fetch(`/api/v1/a2a/peers/${peerId}`, {
    method: 'DELETE',
    headers: authHeaders(token, false),
  });
  await parseJsonOrThrow<{ status: string }>(resp);
}

export async function parseNLIntent(query: string, token: string): Promise<NLParseResponse> {
  const resp = await fetch('/api/v1/incidents/nl/parse', {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify({ query }),
  });
  return parseJsonOrThrow<NLParseResponse>(resp);
}

export async function confirmNLIntent(
  draft: NLParseResponse,
  token: string
): Promise<IncidentSummary> {
  const resp = await fetch('/api/v1/incidents/nl/confirm', {
    method: 'POST',
    headers: authHeaders(token),
    body: JSON.stringify(draft),
  });
  return parseJsonOrThrow<IncidentSummary>(resp);
}
