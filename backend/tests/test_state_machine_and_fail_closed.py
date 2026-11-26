import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import (
    AgentWorker,
    AIOpsOrchestrator,
    ConflictStateError,
    FailClosedBlockedError,
)
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.repositories import (
    get_incident_full,
    get_trajectory_by_incident,
    list_trace_events,
)
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry


def _build_orchestrator(
    session_factory: async_sessionmaker[AsyncSession],
    redis_bus: RedisEventBus,
) -> tuple[AIOpsOrchestrator, AgentWorker]:
    settings = get_settings()
    consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name="test-archiver",
    )
    orch = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=SkillRegistry(settings.skills_dir),
        tool_registry=ToolRegistry(),
        llm_provider=MockLLMProvider(),
        step_delay_ms=0,
        trajectory_consumer=consumer,
    )
    return orch, AgentWorker(orch, worker_id="test-worker", trajectory_consumer=consumer)


@pytest.mark.asyncio
async def test_full_state_machine_fail_closed_gate_and_trajectory_archival(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus)

    incident = await orch.inject_incident(fault_type="db_connection_pool_exhausted")
    assert incident.status == "OPEN"

    # Run Coordinator -> Log & DB Diagnostics -> Remediation -> stops at PENDING_APPROVAL
    processed = await worker.run_until_quiescent()
    assert processed == 4  # COORDINATOR, LOG_DIAGNOSTICS, DB_DIAGNOSTICS, REMEDIATION

    async with session_factory() as sess:
        mid = await get_incident_full(sess, incident.id)
        assert mid is not None
        assert mid.status == "PENDING_APPROVAL"
        assert mid.approval is not None
        assert mid.approval.status == "PENDING"
        assert mid.trajectory is None
        assert "log_diagnostics" in mid.blackboard["evidence"]
        assert "db_diagnostics" in mid.blackboard["evidence"]
        executor_tasks = [t for t in mid.tasks if t.agent_role == "EXECUTOR"]
        assert len(executor_tasks) == 1
        assert executor_tasks[0].status == "BLOCKED_APPROVAL"

    # Fail-Closed Gate: direct execution MUST fail closed and persist an audit trace!
    with pytest.raises(FailClosedBlockedError) as blocked:
        await orch.execute_remediation_direct(incident.id)
    assert "APPROVAL" in str(blocked.value).upper()

    async with session_factory() as sess:
        after_block = await get_incident_full(sess, incident.id)
        assert after_block is not None
        assert after_block.status == "PENDING_APPROVAL"
        assert after_block.approval is not None
        assert after_block.approval.status == "PENDING"
        traces = await list_trace_events(sess, incident.id)
        assert any(t.event_type == "EXECUTION_BLOCKED_FAIL_CLOSED" for t in traces)

    # Human approval -> Executor runs -> Trajectory archived -> RESOLVED
    approved = await orch.approve_remediation(
        incident.id, approved_by="sre-lead", comment="Verified idle-in-tx PIDs, safe to recycle"
    )
    assert approved.status == "EXECUTING_REMEDIATION"

    # Second concurrent/duplicate approval must be rejected cleanly with ConflictStateError
    with pytest.raises(ConflictStateError):
        await orch.approve_remediation(incident.id, approved_by="sre-duplicate")

    processed_after = await worker.run_until_quiescent()
    assert processed_after == 1  # EXECUTOR only

    async with session_factory() as sess:
        final_inc = await get_incident_full(sess, incident.id)
        assert final_inc is not None
        assert final_inc.status == "RESOLVED"
        assert final_inc.resolved_at is not None
        assert final_inc.approval is not None
        assert final_inc.approval.status == "EXECUTED"
        assert final_inc.approval.approved_by == "sre-lead"

        traj = await get_trajectory_by_incident(sess, incident.id)
        assert traj is not None
        assert traj.skill_id == "skill.pg16.pool_exhaustion"
        assert traj.trajectory_doc["approval_audit"]["approved_by"] == "sre-lead"
        assert traj.trajectory_doc["execution_result"]["status"] == "SUCCESS"
        assert len(traj.trajectory_doc["trace_steps"]) >= 8

        traces = await list_trace_events(sess, incident.id)
        event_types = [t.event_type for t in traces]
        assert "TOOL_CALL_AUTO_ALLOWED" in event_types
        assert "HIGH_RISK_EXECUTED" in event_types
        assert "TRAJECTORY_ARCHIVED" in event_types
        assert event_types[-1] == "INCIDENT_RESOLVED"


@pytest.mark.asyncio
async def test_mock_llm_fault_injection_blocks_rogue_diagnostic_write(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus)

    incident = await orch.inject_incident(
        fault_type="db_connection_pool_exhausted",
        alert_payload={"_mock_fault_mode": "unauthorized_diag_write"},
    )
    await worker.run_until_quiescent()

    async with session_factory() as sess:
        final_inc = await get_incident_full(sess, incident.id)
        assert final_inc is not None
        assert final_inc.status == "FAILED"
        assert final_inc.approval is None
        traces = await list_trace_events(sess, incident.id)
        assert any(t.event_type == "POLICY_VIOLATION_BLOCKED" for t in traces)
        assert not any(t.event_type == "HIGH_RISK_EXECUTED" for t in traces)


@pytest.mark.asyncio
async def test_mock_llm_malformed_output_fails_safely(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus)

    incident = await orch.inject_incident(
        fault_type="slow_sql_lock_contention",
        alert_payload={"_mock_fault_mode": "llm_malformed_output"},
    )
    await worker.run_until_quiescent()

    async with session_factory() as sess:
        final_inc = await get_incident_full(sess, incident.id)
        assert final_inc is not None
        assert final_inc.status == "FAILED"
        traces = await list_trace_events(sess, incident.id)
        assert any(t.event_type == "INCIDENT_FAILED" for t in traces)


@pytest.mark.asyncio
async def test_human_rejection_aborts_execution(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus)

    incident = await orch.inject_incident(fault_type="redis_cache_stampede")
    await worker.run_until_quiescent()

    rejected = await orch.reject_remediation(
        incident.id, rejected_by="oncall-eng", comment="Maintenance window frozen"
    )
    assert rejected.status == "REJECTED"
    assert rejected.approval is not None
    assert rejected.approval.status == "REJECTED"

    # Subsequent execution attempt must remain Fail-Closed
    with pytest.raises(FailClosedBlockedError):
        await orch.execute_remediation_direct(incident.id)
