# AgentMesh AIOps Control Plane (Phase 1 MVP)

Production-grade multi-agent AIOps troubleshooting control plane built with **Python 3.12 + FastAPI + PostgreSQL 16 + Redis 7** and **Vite + React + TypeScript + Tailwind CSS**.

## Architecture Highlights (Phase 1)

- **PostgreSQL 16 Transactional Task Dispatch**:
  - `JSONB` columns (`alert_payload`, `blackboard`, `trace_events.payload`, `trajectory_doc`) backed by `GIN` indexes.
  - Concurrent task queue claimed atomically via `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Redis 7 Streams (Consumer Group + `XACK` + `XAUTOCLAIM`)**:
  - **Real-Time Incident Event Stream** (`stream:incident:{id}:events`): SSE subscribers create an ephemeral consumer group (`cg:sse:*`), read via `XREADGROUP`, deduplicate against PG16 `trace_events.id`, call `XACK` on each frame, and clean up via `XGROUP DESTROY`.
  - **Trajectory Archival Consumer** (`stream:aiops:events` + `cg:trajectory-archiver`): consumes `HIGH_RISK_EXECUTED` events, archives `IncidentTrajectory` and transitions `Incident` to `RESOLVED` in PG16, calls `XACK` only after PG16 commit, and recovers unacknowledged PEL entries via `XAUTOCLAIM`.
- **Hermes-Inspired Extensions**:
  - **Declarative Runbook Skills** (`backend/skills/*.yaml`): SOP definitions mapping fault types to `READ_ONLY` diagnostic tools and `HIGH_RISK_WRITE` remediations.
  - **Trajectory Archival** (`incident_trajectories`): Post-resolution structured JSONB archive of evidence, approval audit, execution output, `stream_consumption` metadata, and trace timeline.
- **Fail-Closed Human Approval Gate (`PolicyGuard`)**:
  - `READ_ONLY` diagnostic agents (`LOG_DIAGNOSTICS`, `DB_DIAGNOSTICS`) execute automatically.
  - `HIGH_RISK_WRITE` operations always suspend in `PENDING_APPROVAL`; direct `/execute` without an `APPROVED` record returns HTTP `403` (`APPROVAL_REQUIRED_FAIL_CLOSED`) and records an audit trace event.

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

# 5. Run Concurrency & Reliability Load Benchmark (LLM_PROVIDER=mock, real PG16 + Redis 7)
cd backend
.\.venv\Scripts\python.exe benchmarks\run_load_benchmark.py --tiers 10 30 50 --workers 4 --archivers 2
```

## Concurrency & Reliability Benchmark (Local PG16 + Redis 7, `LLM_PROVIDER=mock`)

Full root-cause analysis & report: [docs/BENCHMARK.md](docs/BENCHMARK.md) (4 concurrent `SKIP LOCKED` Workers + 2 Redis Stream Archivers):

| Concurrency | Baseline Req/s | Optimized Req/s | Throughput Gain | Baseline P99 (ms) | Optimized P99 (ms) | P99 Reduction | Success | DupTasks | Redis `XPENDING` |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 5.02 | **7.15** | **1.42x (+42.4%)** | 1,953.89 | **1,347.68** | **-31.0%** | 100.00% | **0** | **0** |
| **30** | 3.22 | **8.20** | **2.55x (+154.7%)** | 9,237.87 | **3,574.30** | **-61.3%** | 100.00% | **0** | **0** |
| **50** | 2.35 | **8.57** | **3.65x (+264.7%)** | 21,158.80 | **5,685.34** | **-73.1%** | 100.00% | **0** | **0** |
