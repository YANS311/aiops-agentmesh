"""Phase 6 tests: Truthfulness isolation (LIVE vs MOCK), zero synthetic mixing, outcome state machine, and Post-Check."""

from __future__ import annotations

import os

from asgi_lifespan import LifespanManager
import httpx
import pytest
import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.semantics import EvidenceSource, ExecutionOutcome
from app.agents.tools import PolicyGuard, ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.models import AgentTask, Incident
from app.db.repositories import (
    get_incident_full,
    get_trajectory_by_incident,
    list_trace_events,
)
from app.main import create_app
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry


def _build_orchestrator(
    session_factory: async_sessionmaker[AsyncSession],
    redis_bus: RedisEventBus,
    *,
    tool_mode: str = "mock",
) -> tuple[AIOpsOrchestrator, AgentWorker]:
    settings = get_settings()
    consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name="phase6-archiver",
    )
    orch = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=SkillRegistry(settings.skills_dir),
        tool_registry=ToolRegistry(execution_mode=tool_mode),
        llm_provider=MockLLMProvider(),
        step_delay_ms=0,
        trajectory_consumer=consumer,
    )
    return orch, AgentWorker(orch, worker_id="phase6-worker", trajectory_consumer=consumer)


@pytest.mark.asyncio
async def test_live_mode_zero_synthetic_mixing_and_explicit_evidence_source(
    db_session: AsyncSession,
    redis_client: aioredis.Redis,
) -> None:
    """① TOOL_EXECUTION_MODE=live MUST tag all probes evidence_source=LIVE with ZERO fake logs/metrics."""
    live_guard = PolicyGuard(ToolRegistry(execution_mode="live"))
    mock_guard = PolicyGuard(ToolRegistry(execution_mode="mock"))

    incident = Incident(
        title="Truthfulness probe check",
        severity="P0",
        service_name="payment-core",
        fault_type="db_connection_pool_exhausted",
        status="DIAGNOSING",
        alert_payload={},
        blackboard={},
    )
    ctx = {"service_name": "payment-core", "db_session": db_session, "redis_client": redis_client}

    for tool_name in (
        "query_service_error_logs",
        "query_slow_sql_logs",
        "query_gateway_latency_logs",
    ):
        _, out = await live_guard.execute_authorized_async(
            agent_role="LOG_DIAGNOSTICS",
            tool_name=tool_name,
            incident=incident,
            context=ctx,
        )
        assert out["evidence_source"] == EvidenceSource.LIVE.value
        assert out["log_source_available"] is False
        assert out["entries"] == []
        # Strictly NO synthetic fake counts/signatures leaked into live mode!
        for forbidden_key in ("error_count_5m", "top_signature", "sample_trace_ids", "http_504_count_1m", "blocked_queries"):
            assert forbidden_key not in out

    for db_tool in ("inspect_pg_stat_activity", "inspect_pg_locks", "inspect_redis_hotkeys"):
        _, out = await live_guard.execute_authorized_async(
            agent_role="DB_DIAGNOSTICS",
            tool_name=db_tool,
            incident=incident,
            context=ctx,
        )
        assert out["evidence_source"] == EvidenceSource.LIVE.value
        assert out["mode"] == "live"

    # Mock mode MUST explicitly tag evidence_source=MOCK
    _, mock_log = await mock_guard.execute_authorized_async(
        agent_role="LOG_DIAGNOSTICS",
        tool_name="query_service_error_logs",
        incident=incident,
        context=ctx,
    )
    assert mock_log["evidence_source"] == EvidenceSource.MOCK.value


@pytest.mark.asyncio
async def test_simulated_remediation_never_marked_verified_and_increments_metrics(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """② Simulated remediation MUST be tagged SIMULATED (never VERIFIED) and increment /metrics."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
    )
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}

    async with LifespanManager(app):
        orch = app.state.orchestrator
        worker = AgentWorker(orch, worker_id="phase6-sim-worker", trajectory_consumer=app.state.trajectory_consumer)
        inc = await orch.inject_incident(fault_type="db_connection_pool_exhausted")
        await worker.run_until_quiescent()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            app_resp = await client.post(
                f"/api/v1/incidents/{inc.id}/approve",
                headers=approver_headers,
                json={"comment": "Simulated gate check"},
            )
            assert app_resp.status_code == 200
            await worker.run_until_quiescent()

            detail = (await client.get(f"/api/v1/incidents/{inc.id}")).json()
            assert detail["status"] == "RESOLVED"
            assert detail["approval"]["execution_result"]["execution_outcome"] == ExecutionOutcome.SIMULATED.value
            assert detail["approval"]["execution_result"]["simulated"] is True
            assert detail["trajectory"]["trajectory_doc"]["execution_outcome"] == ExecutionOutcome.SIMULATED.value

            traces = (await client.get(f"/api/v1/incidents/{inc.id}/traces")).json()
            assert any(t["event_type"] == "REMEDIATION_SIMULATED" for t in traces)
            assert not any(t["event_type"] == "POST_CHECK_VERIFIED" for t in traces)

            metrics_text = (await client.get("/metrics")).text
            assert 'aiops_remediation_outcomes_total{outcome="SIMULATED"}' in metrics_text


@pytest.mark.asyncio
async def test_independent_post_check_verified_and_unknown_reconcile_paths(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """③ Real execution + Post-Check pass -> VERIFIED; Post-Check timeout -> UNKNOWN (NEEDS_HUMAN_REVIEW, no auto-retry)."""
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus, tool_mode="live")

    # Case A: Real execution + independent Post-Check passes -> VERIFIED -> RESOLVED
    inc_verified = await orch.inject_incident(
        fault_type="db_connection_pool_exhausted",
        alert_payload={"_mock_real_exec_mode": True, "_mock_post_check_mode": "verified"},
    )
    await worker.run_until_quiescent()
    await orch.approve_remediation(inc_verified.id, approved_by="sre-lead-01")
    await worker.run_until_quiescent()

    async with session_factory() as sess:
        final_v = await get_incident_full(sess, inc_verified.id)
        traj_v = await get_trajectory_by_incident(sess, inc_verified.id)
        traces_v = await list_trace_events(sess, inc_verified.id)
    assert final_v is not None and final_v.status == "RESOLVED"
    assert final_v.approval is not None
    assert final_v.approval.execution_result is not None
    assert final_v.approval.execution_result["execution_outcome"] == ExecutionOutcome.VERIFIED.value
    assert traj_v is not None and traj_v.trajectory_doc["execution_outcome"] == ExecutionOutcome.VERIFIED.value
    assert any(t.event_type == "POST_CHECK_VERIFIED" for t in traces_v)

    # Case B: Real execution + independent Post-Check timeout -> UNKNOWN -> NEEDS_HUMAN_REVIEW & NO retry!
    inc_unknown = await orch.inject_incident(
        fault_type="db_connection_pool_exhausted",
        alert_payload={"_mock_real_exec_mode": True, "_mock_post_check_mode": "timeout"},
    )
    await worker.run_until_quiescent()
    await orch.approve_remediation(inc_unknown.id, approved_by="sre-lead-01")
    await worker.run_until_quiescent()

    async with session_factory() as sess:
        final_u = await get_incident_full(sess, inc_unknown.id)
        traces_u = await list_trace_events(sess, inc_unknown.id)
        exec_task = (
            await sess.execute(
                select(AgentTask).where(
                    AgentTask.incident_id == inc_unknown.id,
                    AgentTask.agent_role == "EXECUTOR",
                )
            )
        ).scalar_one()
    assert final_u is not None
    assert final_u.status == "NEEDS_HUMAN_REVIEW"
    assert final_u.approval is not None and final_u.approval.execution_result is not None
    assert final_u.approval.execution_result["execution_outcome"] == ExecutionOutcome.UNKNOWN.value
    assert exec_task.status == "FAILED"
    assert exec_task.attempt_count == exec_task.max_attempts == 1  # Strictly no auto-retry!
    assert any(t.event_type == "POST_CHECK_UNKNOWN_RECONCILE_REQUIRED" for t in traces_u)
    assert not any(t.event_type == "HIGH_RISK_EXECUTED" for t in traces_u)
