# AgentMesh AIOps — Phase 1 Execution Plan

## 1. Architecture & Locked Tech Stack

- **Repository**: Standalone Git repository at `d:\Projects\aiops-agentmesh` (`main` branch).
- **Backend**: Python 3.12 + FastAPI + SQLAlchemy 2.0 (async `asyncpg`) + PostgreSQL 16 + Redis 7 (`redis.asyncio`).
- **Frontend**: Vite + React + TypeScript + Tailwind CSS (`@tailwindcss/vite`).
- **Design Influences (from Hermes Agent, adapted to distributed control plane)**:
  1. **Declarative Runbook Skills (`backend/skills/*.yaml`)**: SOPs defining fault signatures, read-only diagnostic tools, and guarded remediation actions.
  2. **Trajectory Archival (`incident_trajectories`)**: Structured JSONB postmortem archive capturing root cause, evidence, human approval audit, executed command, and ordered trace summary.

### Local Ports & Services
- PostgreSQL 16 (`postgres:16-alpine`): `127.0.0.1:5436` (`aiops_mesh` + `aiops_mesh_test`)
- Redis 7 (`redis:7-alpine`): `127.0.0.1:6386` (db `0` runtime, db `1` test)
- FastAPI Backend: `127.0.0.1:8016`
- Vite React Frontend: `127.0.0.1:5176` (proxies `/api` -> `http://127.0.0.1:8016`)

## 2. Multi-Agent State Machine & Fail-Closed Protocol

```text
OPEN
  -> (COORDINATOR matches Runbook Skill, writes blackboard, fans out read-only tasks)
TRIAGING -> DIAGNOSING
  -> (LOG_DIAGNOSTICS + DB_DIAGNOSTICS execute READ_ONLY tools via PolicyGuard [AUTO-ALLOWED])
  -> (REMEDIATION synthesizes root cause & proposes HIGH_RISK_WRITE plan; creates PENDING approval + BLOCKED_APPROVAL executor task)
PENDING_APPROVAL
  -> [Direct /execute without APPROVED status] => FAIL-CLOSED HTTP 403 + persists EXECUTION_BLOCKED_FAIL_CLOSED trace
  -> [Human /reject] => REJECTED
  -> [Human /approve] => EXECUTING_REMEDIATION -> (EXECUTOR verifies APPROVED + runs tool -> archives Trajectory) -> RESOLVED
```

### PolicyGuard Rules (Fail-Closed)
- `READ_ONLY` tools may auto-execute ONLY for `LOG_DIAGNOSTICS` / `DB_DIAGNOSTICS`.
- `HIGH_RISK_WRITE` tools MUST be denied unless `agent_role == "EXECUTOR"`, `incident.status == "EXECUTING_REMEDIATION"`, and `remediation_approvals.status == "APPROVED"`.
- Unknown tools or mismatched states fail closed with explicit audit traces.

## 3. PostgreSQL 16 Data Model

- `incidents`: Incident lifecycle, `alert_payload` (`JSONB` + `GIN`), `blackboard` (`JSONB` + `GIN`), `status`, `root_cause_summary`.
- `agent_tasks`: Distributed task queue claimed atomically via `SELECT ... FOR UPDATE SKIP LOCKED` with composite index `(status, step_order, created_at)`.
- `remediation_approvals`: Human-in-the-loop approval record (`PENDING` -> `APPROVED` / `REJECTED` -> `EXECUTED`).
- `trace_events`: Monotonically ordered (`BigInteger` PK) SSE/audit log with `payload` (`JSONB` + `GIN`).
- `incident_trajectories`: Archived postmortem trajectories with `trajectory_doc` (`JSONB` + `GIN`).

## 4. Phase 1 Milestones & Hard Acceptance Gates

1. **Step 1**: Repo init, `PLAN.md`, `docker-compose.yml` (PG16 + Redis 7 healthy) + `git commit`.
2. **Step 2**: PG16 schema, GIN indexes, `FOR UPDATE SKIP LOCKED` queue, Redis bus, `pytest` green (0 skipped) + `git commit`.
3. **Step 3**: Runbook Skills, `MockLLMProvider`, `PolicyGuard`, Orchestrator, FastAPI + race-free SSE, `pytest` green (0 skipped) + `git commit`.
4. **Step 4**: Vite + React + TS + Tailwind console (`npm run build` clean + `pytest` green) + `git commit`.
5. **Step 5**: Live Playwright MCP E2E verification (`Inject Fault` -> SSE/Trace & `PENDING_APPROVAL` -> `Approve & Execute` -> `RESOLVED` & Trajectory rendered; 0 console errors, 0 HTTP 500s).

## 5. Deferred to Phase 2 (Out of Scope for Phase 1)
- Prometheus & Grafana telemetry endpoints/containers
- Monte Carlo Shapley attribution scoring
- Multi-service external chaos engineering daemons
