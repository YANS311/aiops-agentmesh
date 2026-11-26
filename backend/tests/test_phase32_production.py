"""Phase 3.2 production tests: RBAC (401/403/200 + audit), Prometheus /metrics, JSON logging, Alembic upgrade head."""

import asyncio
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
from app.main import create_app
from app.observability.logging import JsonLogFormatter, bind_log_context


@pytest.mark.asyncio
async def test_rbac_auth_401_403_and_approver_actor_audit_persistence(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
    )

    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}

    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # /auth/me verification
            assert (await client.get("/api/v1/auth/me")).status_code == 401
            me_viewer = await client.get("/api/v1/auth/me", headers=viewer_headers)
            assert me_viewer.status_code == 200
            assert me_viewer.json() == {"actor_id": "viewer-demo-01", "role": "VIEWER"}

            me_approver = await client.get("/api/v1/auth/me", headers=approver_headers)
            assert me_approver.status_code == 200
            assert me_approver.json() == {"actor_id": "sre-lead-01", "role": "SRE_APPROVER"}

            # Inject fault and wait for PENDING_APPROVAL
            inj = await client.post(
                "/api/v1/incidents/inject",
                json={"fault_type": "db_connection_pool_exhausted"},
            )
            assert inj.status_code == 201
            incident_id = inj.json()["id"]

            for _ in range(50):
                detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)

            # 1. Unauthenticated requests -> 401
            for path in ("approve", "reject", "execute"):
                resp = await client.post(
                    f"/api/v1/incidents/{incident_id}/{path}",
                    json={"comment": "no-auth"} if path != "execute" else None,
                )
                assert resp.status_code == 401
                assert resp.json()["detail"]["code"] == "AUTH_REQUIRED"

            # 2. VIEWER role -> 403 RBAC_FORBIDDEN
            for path in ("approve", "reject", "execute"):
                resp = await client.post(
                    f"/api/v1/incidents/{incident_id}/{path}",
                    headers=viewer_headers,
                    json={"comment": "viewer-attempt"} if path != "execute" else None,
                )
                assert resp.status_code == 403
                assert resp.json()["detail"]["code"] == "RBAC_FORBIDDEN"

            # 3. SRE_APPROVER -> 200 and persists real actor_id & role (ignores spoofed body operator)
            ok_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/approve",
                headers=approver_headers,
                json={"operator": "spoofed-name", "comment": "RBAC verified"},
            )
            assert ok_resp.status_code == 200
            approval = ok_resp.json()["approval"]
            assert approval["approved_by"] == "sre-lead-01"
            assert approval["approved_by_role"] == "SRE_APPROVER"

            # Wait for RESOLVED and verify audit trace persisted actor_id & actor_role
            for _ in range(50):
                detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                if detail["status"] == "RESOLVED":
                    break
                await asyncio.sleep(0.05)
            assert detail["status"] == "RESOLVED"

            traces = (await client.get(f"/api/v1/incidents/{incident_id}/traces")).json()
            granted = [t for t in traces if t["event_type"] == "APPROVAL_GRANTED"]
            assert len(granted) == 1
            assert granted[0]["payload"]["actor_id"] == "sre-lead-01"
            assert granted[0]["payload"]["actor_role"] == "SRE_APPROVER"

            # 4. Prometheus /metrics endpoint format & required metrics
            metrics_resp = await client.get("/metrics")
            assert metrics_resp.status_code == 200
            assert "text/plain" in metrics_resp.headers["content-type"]
            body = metrics_resp.text
            for metric_name in (
                "aiops_incidents_total",
                "aiops_task_duration_seconds",
                "aiops_fail_closed_blocks_total",
                "aiops_outbox_pending_events",
                "aiops_dead_letter_tasks_total",
            ):
                assert metric_name in body, f"Missing Prometheus metric: {metric_name}"


def test_json_structured_log_formatter_carries_context_fields() -> None:
    formatter = JsonLogFormatter()
    record = logging.LogRecord(
        name="app.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Approval executed",
        args=(),
        exc_info=None,
    )
    with bind_log_context(
        incident_id="inc-123",
        task_id="task-456",
        actor_id="sre-lead-01",
        actor_role="SRE_APPROVER",
    ):
        rendered = formatter.format(record)

    parsed = json.loads(rendered)
    assert parsed["message"] == "Approval executed"
    assert parsed["incident_id"] == "inc-123"
    assert parsed["task_id"] == "task-456"
    assert parsed["actor_id"] == "sre-lead-01"
    assert parsed["actor_role"] == "SRE_APPROVER"


@pytest.mark.asyncio
async def test_alembic_upgrade_head_on_brand_new_empty_pg16_database() -> None:
    """Create a fresh empty PG16 DB, run `alembic upgrade head`, verify all 6 tables & indexes."""
    settings = get_settings()
    base_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    admin_url = base_url.rsplit("/", 1)[0] + "/postgres"
    fresh_db_name = "aiops_mesh_alembic_test"
    fresh_db_url = base_url.rsplit("/", 1)[0] + f"/{fresh_db_name}"

    admin_engine = create_async_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        async with admin_engine.connect() as conn:
            await conn.execute(text(f"DROP DATABASE IF EXISTS {fresh_db_name} WITH (FORCE)"))
            await conn.execute(text(f"CREATE DATABASE {fresh_db_name}"))

        backend_dir = Path(__file__).resolve().parents[1]
        env = os.environ.copy()
        env["DATABASE_URL"] = fresh_db_url
        proc = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=str(backend_dir),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, f"alembic upgrade head failed:\n{proc.stdout}\n{proc.stderr}"

        fresh_engine = create_async_engine(fresh_db_url)
        try:
            async with fresh_engine.connect() as conn:
                tables = {
                    row[0]
                    for row in (
                        await conn.execute(
                            text(
                                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                            )
                        )
                    ).all()
                }
                assert {
                    "incidents",
                    "agent_tasks",
                    "remediation_approvals",
                    "trace_events",
                    "incident_trajectories",
                    "outbox_events",
                    "alembic_version",
                }.issubset(tables)

                indexes = {
                    row[0]: row[1]
                    for row in (
                        await conn.execute(
                            text(
                                "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'"
                            )
                        )
                    ).all()
                }
                for gin_idx in (
                    "ix_incidents_alert_payload_gin",
                    "ix_incidents_blackboard_gin",
                    "ix_trace_events_payload_gin",
                    "ix_incident_trajectories_doc_gin",
                ):
                    assert gin_idx in indexes and "using gin" in indexes[gin_idx].lower()

                for partial_idx in (
                    "ix_agent_tasks_queued_partial",
                    "ix_agent_tasks_running_lease",
                    "ix_outbox_events_pending",
                ):
                    assert partial_idx in indexes and "where" in indexes[partial_idx].lower()
        finally:
            await fresh_engine.dispose()
    finally:
        async with admin_engine.connect() as conn:
            await conn.execute(text(f"DROP DATABASE IF EXISTS {fresh_db_name} WITH (FORCE)"))
        await admin_engine.dispose()
