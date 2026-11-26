# AgentMesh AIOps Control Plane (Phase 1 MVP)

Production-grade multi-agent AIOps troubleshooting control plane built with **Python 3.12 + FastAPI + PostgreSQL 16 + Redis 7** and **Vite + React + TypeScript + Tailwind CSS**.

## Architecture Highlights (Phase 1)

- **PostgreSQL 16 Persistence & Queueing**:
  - `JSONB` columns (`alert_payload`, `blackboard`, `trace_events.payload`, `trajectory_doc`) backed by `GIN` indexes.
  - Concurrent task queue claimed atomically via `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Hermes-Inspired Extensions**:
  - **Declarative Runbook Skills** (`backend/skills/*.yaml`): SOP definitions mapping fault types to `READ_ONLY` diagnostic tools and `HIGH_RISK_WRITE` remediations.
  - **Trajectory Archival** (`incident_trajectories`): Post-resolution structured JSONB archive of evidence, approval audit, execution output, and trace timeline.
- **Fail-Closed Human Approval Gate (`PolicyGuard`)**:
  - `READ_ONLY` diagnostic agents (`LOG_DIAGNOSTICS`, `DB_DIAGNOSTICS`) execute automatically.
  - `HIGH_RISK_WRITE` operations always suspend in `PENDING_APPROVAL`; direct `/execute` without an `APPROVED` record returns HTTP `403` (`APPROVAL_REQUIRED_FAIL_CLOSED`) and records an audit trace event.
- **Race-Free Live SSE Streaming**:
  - Subscribes to Redis Pub/Sub (`incident:{id}:events`) prior to reading PG16 `trace_events` history, deduplicating by monotonic `trace_events.id` and terminating cleanly on `event: complete`.

## Local Services & Ports

- **PostgreSQL 16 (`postgres:16-alpine`)**: `127.0.0.1:5436` (`aiops_mesh` & `aiops_mesh_test`)
- **Redis 7 (`redis:7-alpine`)**: `127.0.0.1:6386`
- **FastAPI Backend**: `http://127.0.0.1:8016`
- **Vite React Console**: `http://127.0.0.1:5176` (proxies `/api` -> `http://127.0.0.1:8016`)

## Quickstart

```bash
# 1. Start PostgreSQL 16 + Redis 7
docker compose up -d --wait

# 2. Run Backend Tests (real PG16 + Redis, 0 SQLite)
cd backend
.\.venv\Scripts\pytest.exe

# 3. Start FastAPI Control Plane
.\.venv\Scripts\uvicorn.exe app.main:app --host 127.0.0.1 --port 8016

# 4. Start Frontend Console (new terminal)
cd frontend
npm run dev
```
