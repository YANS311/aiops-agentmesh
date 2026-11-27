# AgentMesh AIOps Control Plane

[![CI](https://github.com/YANS311/aiops-agentmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/YANS311/aiops-agentmesh/actions/workflows/ci.yml)
[![CD](https://github.com/YANS311/aiops-agentmesh/actions/workflows/cd.yml/badge.svg)](https://github.com/YANS311/aiops-agentmesh/actions/workflows/cd.yml)

Production-grade multi-agent AIOps troubleshooting control plane built with **Python 3.12 + FastAPI + PostgreSQL 16 + Redis 7 + Alembic + Prometheus** and **Vite + React + TypeScript + Tailwind CSS + Nginx**.

> **Automated CI/CD Pipeline**:
> - **CI** ([.github/workflows/ci.yml](.github/workflows/ci.yml)): Triggered on every `push` / `pull_request` to `main` with two parallel jobs — `backend-ci` (real `postgres:16` + `redis:7` service containers, `LLM_PROVIDER=mock`, `alembic upgrade head` on an empty DB, and full 36-test `pytest -v`) and `frontend-ci` (Node 20 `npm ci` + `npm run build` with `tsc -b`, plus `docker compose -f docker-compose.prod.yml config -q`).
> - **CD** ([.github/workflows/cd.yml](.github/workflows/cd.yml)): Triggered on `v*` release tags to build and push multi-service container images (`backend` & `frontend`) to **GitHub Container Registry (GHCR)** and publish the **GitHub Release** with `docker-compose.prod.yml`.

## Architecture Highlights

- **PostgreSQL 16 Transactional Task Dispatch & Lease Watchdog**:
  - `JSONB` columns (`alert_payload`, `blackboard`, `trace_events.payload`, `trajectory_doc`) backed by `GIN` indexes, version-managed via **Alembic** (`backend/alembic/versions/0001_initial_schema.py`).
  - Concurrent task queue claimed atomically via `SELECT ... FOR UPDATE SKIP LOCKED` with time-bound worker leases (`lease_owner`, `lease_expires_at`, `attempt_count`, `max_attempts`), periodic heartbeat renewal, fencing checks, and background `LeaseWatchdog` (requeues zombie tasks or dead-letters after `max_attempts`).
- **Transactional Outbox (`outbox_events`) + Redis 7 Streams (`Consumer Group + XACK + XAUTOCLAIM`)**:
  - All `TraceEvent` rows atomically write `outbox_events` in the same PG16 transaction (no bare `XADD` outside DB transactions); `OutboxRelay` relays pending batches via `FOR UPDATE SKIP LOCKED` + advisory lock with exponential backoff, guaranteeing zero event loss across transient Redis outages.
  - **Real-Time Incident Event Stream** (`stream:incident:{id}:events`): SSE subscribers create an ephemeral consumer group (`cg:sse:*`), read via `XREADGROUP`, deduplicate against PG16 `trace_events.id`, call `XACK` on each frame, and clean up via `XGROUP DESTROY`.
  - **Trajectory Archival Consumer** (`stream:aiops:events` + `cg:trajectory-archiver`): consumes `HIGH_RISK_EXECUTED` events, archives `IncidentTrajectory` and transitions `Incident` to `RESOLVED` in PG16, calls `XACK` only after PG16 commit, and recovers unacknowledged PEL entries via `XAUTOCLAIM`.
- **RBAC & Fail-Closed Human Approval Gate (`PolicyGuard`)**:
  - Bearer API Token middleware distinguishes `VIEWER` (read-only + fault injection) and `SRE_APPROVER` (`/approve`, `/reject`, `/execute`). Unauthenticated requests return HTTP `401` (`AUTH_REQUIRED`), `VIEWER` attempts return HTTP `403` (`RBAC_FORBIDDEN`), and real `actor_id` / `actor_role` are persisted to `remediation_approvals` and `trace_events`.
  - `READ_ONLY` diagnostic agents (`LOG_DIAGNOSTICS`, `DB_DIAGNOSTICS`) execute automatically; `HIGH_RISK_WRITE` operations always suspend in `PENDING_APPROVAL`, and direct `/execute` without an `APPROVED` record returns HTTP `403` (`APPROVAL_REQUIRED_FAIL_CLOSED`).
- **Observability & Graceful Shutdown**:
  - **Prometheus `/metrics`**: exposes `aiops_incidents_total`, `aiops_task_duration_seconds`, `aiops_fail_closed_blocks_total`, `aiops_outbox_pending_events`, and `aiops_dead_letter_tasks_total`.
  - **JSON Structured Logging**: unified JSON log formatter automatically carrying `incident_id`, `task_id`, `actor_id`, and `actor_role`.
  - **Graceful Shutdown**: FastAPI `lifespan` stops claiming new tasks, drains in-flight worker tasks and flushes pending `outbox_events` (bounded by `SHUTDOWN_TIMEOUT_SECONDS`) before closing Redis and PG16 connection pools.
- **Standard A2A (Agent-to-Agent) Protocol & Academy Guidance Integration (`Phase 6.1`)**:
  - Exposes `GET /.well-known/agent.json` and `GET /api/v1/a2a/peers` via `backend/app/a2a/` (`AgentCard`, `A2ATaskRequest`, `A2ATaskResponse`, async `A2AClient` with card caching & timeout circuit breaking).
  - Includes standalone lightweight `adapters/guidance_a2a_adapter/` (`ADAPTER_MODE=live|mock`), exposing read-only skills `diagnose_neo4j_cypher` & `diagnose_rag_pipeline` (strictly rejecting undeclared/write requests with `state="REJECTED"`).
  - Runbooks `guidance_neo4j_slow_cypher.yaml` and `guidance_rag_retrieval_degradation.yaml` delegate read-only diagnosis via `delegate_a2a_diagnosis`, archive A2A artifacts + traces (`A2A_TASK_DELEGATED` / `A2A_TASK_COMPLETED` / `A2A_TASK_FAILED`), and gate all remediations behind `SRE_APPROVER`.
- **Decoupled 6-Container Production Architecture, Live Probes & Alertmanager Storm Dedup (`Phase 5`)**:
  - **Process Decoupling (`EMBEDDED_WORKERS=false`)**: FastAPI `backend-api` serves only HTTP/SSE traffic; `backend-worker` (`python -m app.entrypoints.worker`) runs `AgentWorker` + `LeaseWatchdog` (horizontally scalable via `--scale backend-worker=N`); `backend-relay` (`python -m app.entrypoints.relay`) runs `OutboxRelay` + `TrajectoryStreamConsumer`, each with independent healthchecks and `SIGTERM` graceful drain.
  - **Live Read-Only Probes (`TOOL_EXECUTION_MODE=live|mock`)**: In `live` mode, diagnostic tools execute real queries against PostgreSQL 16 (`pg_stat_activity`, `pg_locks`) and Redis 7 (`INFO memory`, `INFO stats`, `SLOWLOG GET`), while all `HIGH_RISK_WRITE` tools remain strictly Fail-Closed.
  - **Prometheus Alertmanager Webhook (`POST /api/v1/webhooks/alertmanager`)**: Ingests standard Alertmanager v4 payloads with concurrency-safe active-fingerprint deduplication (`pg_advisory_xact_lock` + partial unique index `uq_incidents_active_fingerprint` via Alembic `0002_alert_dedup_fields`), atomically incrementing `dedup_count` and refreshing `last_triggered_at` during alert storms.
- **Multi-Agent Parallel Diagnosis, Weighted Consensus Arbiter & Exact Shapley Attribution (`Phase 4`)**:
  - Dispatches 3 specialist diagnostic roles (`DB_AGENT`, `CACHE_AGENT`, `LOG_AGENT`) in parallel via `asyncio.gather`, each emitting a structured hypothesis (`root_cause_label`, `confidence`, `evidence_summary`).
  - `ConsensusArbiter` (`backend/app/agents/consensus.py`) aggregates weighted hypotheses: reaching consensus (`consensus_confidence >= 0.60` and `disagreement_score <= 0.40`) advances to `PENDING_APPROVAL`; severe conflict or low confidence triggers **Fail-Closed** (`NEEDS_HUMAN_REVIEW` + `CONSENSUS_CONFLICT_ALERT`, strictly preventing high-risk remediation generation).
  - `backend/app/agents/shapley.py` computes **exact Shapley values** over all $2^3 = 8$ coalitions using noisy-OR confidence gain $v(S) = 1 - \prod_{i \in S}(1 - p_i)$, satisfying Efficiency ($\sum \phi_i = v(N) - v(\emptyset)$), Symmetry, and Null-Player axioms, archived into `incident_trajectories.trajectory_doc` JSONB and rendered in the React Console.
- **Hermes-Inspired Extensions**:
  - **Declarative Runbook Skills** (`backend/skills/*.yaml`): SOP definitions mapping fault types to `READ_ONLY` diagnostic tools and `HIGH_RISK_WRITE` remediations.
  - **Trajectory Archival** (`incident_trajectories`): Post-resolution structured JSONB archive of evidence, approval audit, execution output, `stream_consumption` metadata, and trace timeline.

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

# 6. Run 12-Case Real DeepSeek Adversarial & Fault-Injection Eval (uses local .env)
.\.venv\Scripts\python.exe benchmarks\run_llm_eval.py
```

## Production Deployment (`docker-compose.prod.yml`)

One-command **6-container decoupled production deployment** (**`postgres` + `redis` + `backend-api` + `backend-worker` + `backend-relay` + `frontend-nginx`**):

```bash
# Build & launch 6-container production stack (backend-api auto-runs `alembic upgrade head`)
LLM_PROVIDER=mock TOOL_EXECUTION_MODE=live docker compose -f docker-compose.prod.yml up -d --build --wait

# Scale concurrent SKIP LOCKED workers horizontally (optional)
docker compose -f docker-compose.prod.yml up -d --scale backend-worker=2

# Console entrypoint (Nginx -> React SPA + /api reverse proxy + unbuffered SSE)
# http://127.0.0.1:8088/

# Trigger standard Prometheus Alertmanager webhook
curl -X POST http://127.0.0.1:8088/api/v1/webhooks/alertmanager \
  -H "Content-Type: application/json" \
  -d '{"version":"4","status":"firing","alerts":[{"status":"firing","fingerprint":"fp-demo-001","labels":{"alertname":"PG16PoolExhausted","severity":"critical","service":"payment-core","fault_type":"db_connection_pool_exhausted"},"annotations":{"summary":"PG16 pool saturated"}}]}'

# Prometheus metrics endpoint
curl http://127.0.0.1:8088/metrics

# Tear down stack and volumes
docker compose -f docker-compose.prod.yml down -v
```

### Key Production Features

- **Alembic Schema Migrations**: Production containers run with `AUTO_INIT_DB=false` and apply versioned migrations via `alembic upgrade head` (`backend/alembic/versions/0001_initial_schema.py`, covering all 6 tables, 4 JSONB GIN indexes, and 3 partial B-tree indexes).
- **RBAC Bearer Tokens** (override via env vars in production):
  - `VIEWER` (`RBAC_VIEWER_TOKEN=viewer-demo-token`): read-only inspection & fault injection; `/approve`, `/reject`, `/execute` return HTTP `403` (`RBAC_FORBIDDEN`).
  - `SRE_APPROVER` (`RBAC_APPROVER_TOKEN=sre-approver-demo-token`): full approval & execution privileges; real `actor_id` and `actor_role` are persisted to `remediation_approvals` and `trace_events`. Unauthenticated requests return HTTP `401` (`AUTH_REQUIRED`).
- **Nginx SSE Proxy (`frontend/nginx.conf`)**: Explicitly disables proxy buffering (`proxy_buffering off; proxy_cache off; chunked_transfer_encoding off;`) on `/api/v1/incidents/<id>/events` to guarantee zero-latency SSE event delivery.
- **Observability**: `GET /metrics` exposes Prometheus text metrics (`aiops_incidents_total`, `aiops_task_duration_seconds`, `aiops_fail_closed_blocks_total`, `aiops_outbox_pending_events`, `aiops_dead_letter_tasks_total`), and backend logs emit structured JSON with `incident_id`, `task_id`, `actor_id`, and `actor_role`.

## Concurrency & Reliability Benchmark (Local PG16 + Redis 7, `LLM_PROVIDER=mock`)

Full root-cause analysis & report: [docs/BENCHMARK.md](docs/BENCHMARK.md) (4 concurrent `SKIP LOCKED` Workers + 2 Redis Stream Archivers):

| Concurrency | Baseline Req/s | Optimized Req/s | Throughput Gain | Baseline P99 (ms) | Optimized P99 (ms) | P99 Reduction | Success | DupTasks | Redis `XPENDING` |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 5.02 | **7.15** | **1.42x (+42.4%)** | 1,953.89 | **1,347.68** | **-31.0%** | 100.00% | **0** | **0** |
| **30** | 3.22 | **8.20** | **2.55x (+154.7%)** | 9,237.87 | **3,574.30** | **-61.3%** | 100.00% | **0** | **0** |
| **50** | 2.35 | **8.57** | **3.65x (+264.7%)** | 21,158.80 | **5,685.34** | **-73.1%** | 100.00% | **0** | **0** |

## Real DeepSeek Adversarial & Fault-Injection Evaluation (12 Cases)

Full report: [docs/EVAL_REPORT.md](docs/EVAL_REPORT.md) | Raw JSON: [docs/eval_results_deepseek.json](docs/eval_results_deepseek.json) (`deepseek-chat`, 6 normal + 3 prompt-injection + 3 boundary cases):

| Root Cause Hit Rate | Pydantic JSON Pass Rate | Fail-Closed Block Rate | Avg / P95 Inference Latency | Total Token Usage | Redis `XPENDING` |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **11/11 (100.00%)** | **44/44 (100.00%)** | **12/12 (100.00%)** | **2,047.48 ms / 3,361.36 ms** | **23,227** (13,807 in / 9,420 out) | **0** |
