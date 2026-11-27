"""Phase 5 tests: Alertmanager webhook & 10x concurrent dedup, live PG16/Redis7 probes, decoupled API/Worker/Relay E2E."""

from __future__ import annotations

import asyncio
import os

from asgi_lifespan import LifespanManager
import httpx
import pytest
import redis.asyncio as aioredis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.tools import ApprovalRequiredError, PolicyGuard, ToolRegistry
from app.config import get_settings
from app.db.models import Incident, OutboxEvent
from app.entrypoints.relay import run_relay_service
from app.entrypoints.worker import run_worker_service
from app.main import create_app


@pytest.mark.asyncio
async def test_alertmanager_webhook_and_concurrent_storm_deduplication(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """① 10 concurrent identical-fingerprint Alertmanager webhooks MUST create 1 Incident with dedup_count=10."""
    _, session_factory = pg_engine_and_factory
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
    )

    payload = {
        "version": "4",
        "status": "firing",
        "receiver": "aiops-agentmesh",
        "alerts": [
            {
                "status": "firing",
                "fingerprint": "fp-storm-pg16-001",
                "labels": {
                    "alertname": "PG16ConnectionPoolExhausted",
                    "severity": "critical",
                    "service": "payment-core",
                    "fault_type": "db_connection_pool_exhausted",
                },
                "annotations": {
                    "summary": "[P0] PostgreSQL 16 Connection Pool Exhausted on payment-core",
                },
            }
        ],
    }

    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            responses = await asyncio.gather(
                *(client.post("/api/v1/webhooks/alertmanager", json=payload) for _ in range(10))
            )

            assert all(r.status_code == 200 for r in responses)
            items = [r.json()["results"][0] for r in responses]
            incident_ids = {item["incident_id"] for item in items}
            assert len(incident_ids) == 1

            actions = [item["action"] for item in items]
            assert actions.count("created") == 1
            assert actions.count("deduplicated") == 9
            assert max(item["dedup_count"] for item in items) == 10

            # Resolved alert status should be ignored cleanly
            resolved_resp = await client.post(
                "/api/v1/webhooks/alertmanager",
                json={
                    "version": "4",
                    "status": "resolved",
                    "alerts": [{"status": "resolved", "fingerprint": "fp-storm-pg16-001"}],
                },
            )
            assert resolved_resp.status_code == 200
            assert resolved_resp.json()["results"][0]["action"] == "ignored_resolved"

    async with session_factory() as sess:
        total_incidents = (await sess.execute(select(func.count(Incident.id)))).scalar_one()
        inc = (await sess.execute(select(Incident))).scalar_one()
    assert total_incidents == 1
    assert inc.alert_fingerprint == "fp-storm-pg16-001"
    assert inc.dedup_count == 10
    assert inc.last_triggered_at is not None


@pytest.mark.asyncio
async def test_live_read_only_probes_query_real_pg16_and_redis7(
    db_session: AsyncSession,
    redis_client: aioredis.Redis,
) -> None:
    """② TOOL_EXECUTION_MODE=live runs real SQL on PG16 and real INFO/SLOWLOG on Redis 7 while keeping writes Fail-Closed."""
    registry = ToolRegistry(execution_mode="live")
    guard = PolicyGuard(registry)

    incident = Incident(
        title="Live probe check",
        severity="P0",
        service_name="payment-core",
        fault_type="db_connection_pool_exhausted",
        status="DIAGNOSING",
        alert_payload={},
        blackboard={},
    )
    ctx = {"service_name": "payment-core", "db_session": db_session, "redis_client": redis_client}

    _, pg_act = await guard.execute_authorized_async(
        agent_role="DB_DIAGNOSTICS",
        tool_name="inspect_pg_stat_activity",
        incident=incident,
        context=ctx,
    )
    assert pg_act["mode"] == "live"
    assert pg_act["source"] == "pg16-live:pg_stat_activity"
    assert pg_act["max_connections"] > 0
    assert pg_act["total_connections"] >= 1
    assert isinstance(pg_act["suspect_pids"], list)

    _, pg_locks = await guard.execute_authorized_async(
        agent_role="DB_DIAGNOSTICS",
        tool_name="inspect_pg_locks",
        incident=incident,
        context=ctx,
    )
    assert pg_locks["mode"] == "live"
    assert pg_locks["source"] == "pg16-live:pg_locks"
    assert pg_locks["total_locks"] >= 0
    assert isinstance(pg_locks["blocking_pids"], list)

    # Touch Redis so stats/memory are active
    await redis_client.set("probe:health", "1")
    await redis_client.get("probe:health")
    _, redis_info = await guard.execute_authorized_async(
        agent_role="DB_DIAGNOSTICS",
        tool_name="inspect_redis_hotkeys",
        incident=incident,
        context=ctx,
    )
    assert redis_info["mode"] == "live"
    assert redis_info["source"] == "redis7-live:info_and_slowlog"
    assert redis_info["used_memory_bytes"] > 0
    assert isinstance(redis_info["slowlog_sample"], list)

    # High-risk write MUST still Fail-Closed without approval even in live mode
    incident.status = "PENDING_APPROVAL"
    with pytest.raises(ApprovalRequiredError):
        await guard.execute_authorized_async(
            agent_role="EXECUTOR",
            tool_name="terminate_idle_in_tx_and_recycle_pool",
            incident=incident,
            context=ctx,
            approval=None,
        )


@pytest.mark.asyncio
async def test_decoupled_api_worker_relay_end_to_end_closed_loop(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """③ Decoupled API (EMBEDDED_WORKERS=false) + standalone Worker + standalone Relay complete full closed loop."""
    _, session_factory = pg_engine_and_factory
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
        tool_execution_mode="live",
    )

    worker_stop = asyncio.Event()
    relay_stop = asyncio.Event()
    worker_task = asyncio.create_task(
        run_worker_service(
            settings=settings,
            database_url=test_db_url,
            redis_url=test_redis_url,
            worker_id="standalone-worker-test-1",
            stop_event=worker_stop,
        )
    )
    relay_task = asyncio.create_task(
        run_relay_service(
            settings=settings,
            database_url=test_db_url,
            redis_url=test_redis_url,
            consumer_name="standalone-relay-test-1",
            stop_event=relay_stop,
        )
    )

    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}
    try:
        async with LifespanManager(app) as manager:
            assert app.state.embedded_workers is False
            transport = httpx.ASGITransport(app=manager.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
                wh = await client.post(
                    "/api/v1/webhooks/alertmanager",
                    json={
                        "version": "4",
                        "status": "firing",
                        "alerts": [
                            {
                                "status": "firing",
                                "fingerprint": "fp-decoupled-e2e-001",
                                "labels": {
                                    "alertname": "PG16PoolExhausted",
                                    "severity": "P0",
                                    "service": "payment-core",
                                    "fault_type": "db_connection_pool_exhausted",
                                },
                                "annotations": {"summary": "Decoupled E2E Pool Exhausted"},
                            }
                        ],
                    },
                )
                assert wh.status_code == 200
                incident_id = wh.json()["results"][0]["incident_id"]

                # Wait for standalone worker to reach PENDING_APPROVAL
                detail = None
                for _ in range(80):
                    detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                    if detail["status"] == "PENDING_APPROVAL":
                        break
                    await asyncio.sleep(0.05)
                assert detail is not None
                assert detail["status"] == "PENDING_APPROVAL"
                assert detail["blackboard"]["consensus_report"]["status"] == "CONSENSUS_REACHED"

                # Approve as SRE_APPROVER -> standalone worker executes -> standalone relay archives Trajectory
                app_resp = await client.post(
                    f"/api/v1/incidents/{incident_id}/approve",
                    headers=approver_headers,
                    json={"comment": "Decoupled E2E approved"},
                )
                assert app_resp.status_code == 200

                for _ in range(80):
                    detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                    if detail["status"] == "RESOLVED":
                        break
                    await asyncio.sleep(0.05)
                assert detail["status"] == "RESOLVED"
                assert detail["trajectory"] is not None
                assert detail["trajectory"]["trajectory_doc"]["stream_consumption"]["consumer_name"] == (
                    "standalone-relay-test-1"
                )
    finally:
        worker_stop.set()
        relay_stop.set()
        worker_report, relay_report = await asyncio.gather(worker_task, relay_task)

    assert worker_report["worker_drained"] is True
    assert relay_report["outbox_pending_remaining"] == 0

    async with session_factory() as sess:
        pending_outbox = (
            await sess.execute(
                select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "PENDING")
            )
        ).scalar_one()
    assert pending_outbox == 0
