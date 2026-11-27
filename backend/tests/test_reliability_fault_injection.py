"""Phase 3.1 fault-injection tests: worker crash / lease watchdog, Redis outage / outbox, graceful shutdown."""

import asyncio
import os
from typing import Any
from uuid import UUID

from asgi_lifespan import LifespanManager
import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.reliability import LeaseWatchdog, OutboxRelay
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.models import AgentTask, OutboxEvent
from app.db.repositories import (
    claim_next_queued_task,
    get_incident_full,
    get_trajectory_by_incident,
    list_trace_events,
)
from app.main import create_app
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    TRAJECTORY_CONSUMER_GROUP,
    RedisEventBus,
    incident_events_stream,
)
from app.skills.loader import SkillRegistry


def _build(
    session_factory: async_sessionmaker[AsyncSession],
    redis_bus: RedisEventBus,
    *,
    relay: OutboxRelay | None = None,
    lease_seconds: float = 30.0,
    step_delay_ms: int = 0,
    worker_id: str = "test-worker-b",
) -> tuple[AIOpsOrchestrator, AgentWorker, OutboxRelay]:
    settings = get_settings()
    relay = relay or OutboxRelay(
        session_factory=session_factory,
        redis_bus=redis_bus,
        base_backoff_seconds=0.01,
        max_backoff_seconds=0.05,
    )
    consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name="reliability-archiver",
        outbox_relay=relay,
    )
    orch = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=SkillRegistry(settings.skills_dir),
        tool_registry=ToolRegistry(),
        llm_provider=MockLLMProvider(),
        step_delay_ms=step_delay_ms,
        trajectory_consumer=consumer,
        outbox_relay=relay,
        lease_seconds=lease_seconds,
    )
    return orch, AgentWorker(orch, worker_id=worker_id, trajectory_consumer=consumer), relay


async def _tasks_by_role(
    session_factory: async_sessionmaker[AsyncSession], incident_id: UUID
) -> dict[str, list[AgentTask]]:
    async with session_factory() as sess:
        rows = (
            await sess.execute(select(AgentTask).where(AgentTask.incident_id == incident_id))
        ).scalars().all()
    grouped: dict[str, list[AgentTask]] = {}
    for row in rows:
        grouped.setdefault(row.agent_role, []).append(row)
    return grouped


class FlakyRedisEventBus(RedisEventBus):
    """Raises real redis-py ConnectionError on XADD to simulate a transient Redis outage."""

    def __init__(self, redis_client: Any, *, fail_times: int) -> None:
        super().__init__(redis_client)
        self.remaining_failures = fail_times
        self.injected_failures = 0

    async def publish_trace_event(
        self, incident_id: UUID | str, event_payload: dict[str, Any]
    ) -> tuple[str, str]:
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            self.injected_failures += 1
            raise RedisConnectionError(
                "Injected fault: Error 10061 connecting to 127.0.0.1:6386. Connection refused."
            )
        return await super().publish_trace_event(incident_id, event_payload)


@pytest.mark.asyncio
async def test_heartbeat_keeps_slow_live_worker_lease_alive(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """A live but slow worker must keep renewing its lease and never be reclaimed."""
    _, session_factory = pg_engine_and_factory
    # Lease 0.3s, but the step takes 0.7s: only the heartbeat keeps the lease alive.
    orch, _, _ = _build(session_factory, redis_bus, lease_seconds=0.3, step_delay_ms=700)
    watchdog = LeaseWatchdog(orch, interval_seconds=60)
    incident = await orch.inject_incident(fault_type="db_connection_pool_exhausted")

    async def aggressive_sweeper() -> int:
        reclaimed = 0
        for _ in range(9):
            await asyncio.sleep(0.1)
            requeued, dead = await watchdog.reclaim_expired_leases()
            reclaimed += requeued + dead
        return reclaimed

    did_work, reclaimed = await asyncio.gather(
        orch.process_one_task("live-slow-worker"), aggressive_sweeper()
    )
    assert did_work is True
    assert reclaimed == 0

    tasks = await _tasks_by_role(session_factory, incident.id)
    coordinator = tasks["COORDINATOR"][0]
    assert coordinator.status == "COMPLETED"
    assert coordinator.attempt_count == 1
    assert coordinator.lease_owner == "live-slow-worker"
    assert coordinator.lease_expires_at is None


@pytest.mark.asyncio
async def test_watchdog_reclaims_crashed_worker_and_peer_completes_with_fencing(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """Worker A claims and crashes (no heartbeat); Watchdog requeues; Worker B finishes; A is fenced."""
    _, session_factory = pg_engine_and_factory
    orch, worker_b, relay = _build(session_factory, redis_bus, worker_id="test-worker-b")
    watchdog = LeaseWatchdog(orch, interval_seconds=60)

    incident = await orch.inject_incident(fault_type="db_connection_pool_exhausted")

    # Worker A claims the COORDINATOR task with a short lease, then "crashes" (never executes/renews).
    async with session_factory() as sess:
        async with sess.begin():
            crashed = await claim_next_queued_task(sess, "worker-a-crashed", lease_seconds=0.2)
            assert crashed is not None
            crashed_task_id = crashed.id
            assert crashed.agent_role == "COORDINATOR"

    # While the zombie lease is live, nobody else can pick the task up.
    assert await worker_b.run_until_quiescent() == 0
    assert await watchdog.reclaim_expired_leases() == (0, 0)

    await asyncio.sleep(0.3)
    assert await watchdog.reclaim_expired_leases() == (1, 0)

    async with session_factory() as sess:
        requeued = (
            await sess.execute(select(AgentTask).where(AgentTask.id == crashed_task_id))
        ).scalar_one()
    assert requeued.status == "QUEUED"
    assert requeued.lease_owner is None
    assert requeued.lease_expires_at is None
    assert requeued.attempt_count == 1

    # Peer Worker B picks it up and drives the whole pipeline to the approval gate.
    assert await worker_b.run_until_quiescent() == 4

    async with session_factory() as sess:
        traces_before_zombie = await list_trace_events(sess, incident.id)

    # Zombie A wakes up and tries to commit its stale claim -> fenced, zero side effects.
    assert await orch.execute_claimed_task(crashed_task_id, "worker-a-crashed") is False

    async with session_factory() as sess:
        mid = await get_incident_full(sess, incident.id)
        traces = await list_trace_events(sess, incident.id)
    assert mid is not None
    assert mid.status == "PENDING_APPROVAL"
    assert len(traces) == len(traces_before_zombie)

    event_types = [t.event_type for t in traces]
    assert event_types.count("LEASE_EXPIRED_REQUEUED") == 1
    assert event_types.count("SKILL_MATCHED") == 1  # executed exactly once

    tasks = await _tasks_by_role(session_factory, incident.id)
    assert {role: len(rows) for role, rows in tasks.items()} == {
        "COORDINATOR": 1,
        "LOG_DIAGNOSTICS": 1,
        "DB_DIAGNOSTICS": 1,
        "REMEDIATION": 1,
        "EXECUTOR": 1,
    }
    coordinator = tasks["COORDINATOR"][0]
    assert coordinator.status == "COMPLETED"
    assert coordinator.attempt_count == 2
    assert coordinator.lease_owner == "test-worker-b"

    assert await relay.pending_count() == 0


@pytest.mark.asyncio
async def test_watchdog_dead_letters_task_after_max_attempts(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    orch, _, relay = _build(session_factory, redis_bus)
    watchdog = LeaseWatchdog(orch, interval_seconds=60)

    incident = await orch.inject_incident(fault_type="slow_sql_lock_contention")
    async with session_factory() as sess:
        async with sess.begin():
            await sess.execute(
                update(AgentTask)
                .where(AgentTask.incident_id == incident.id)
                .values(max_attempts=1)
            )

    async with session_factory() as sess:
        async with sess.begin():
            crashed = await claim_next_queued_task(sess, "worker-crash-loop", lease_seconds=0.1)
            assert crashed is not None
            task_id = crashed.id

    await asyncio.sleep(0.2)
    assert await watchdog.reclaim_expired_leases() == (0, 1)

    async with session_factory() as sess:
        task = (await sess.execute(select(AgentTask).where(AgentTask.id == task_id))).scalar_one()
        inc = await get_incident_full(sess, incident.id)
        traces = await list_trace_events(sess, incident.id)
    assert task.status == "FAILED"
    assert "Dead-lettered" in (task.error_message or "")
    assert inc is not None and inc.status == "FAILED"
    event_types = [t.event_type for t in traces]
    assert "TASK_DEAD_LETTERED" in event_types
    assert event_types[-1] == "INCIDENT_FAILED"

    # Dead-letter audit events are themselves delivered through the outbox.
    await relay.drain()
    assert await relay.pending_count() == 0
    entries = await redis_bus.redis.xrange(incident_events_stream(incident.id))
    streamed_types = [fields["event_type"] for _, fields in entries]
    assert "TASK_DEAD_LETTERED" in streamed_types
    assert streamed_types[-1] == "INCIDENT_FAILED"


@pytest.mark.asyncio
async def test_outbox_relay_survives_redis_outage_and_backfills_with_zero_loss(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    flaky_bus = FlakyRedisEventBus(redis_bus.redis, fail_times=10**6)  # Redis "down"
    relay = OutboxRelay(
        session_factory=session_factory,
        redis_bus=flaky_bus,
        base_backoff_seconds=0.01,
        max_backoff_seconds=0.05,
    )
    orch, worker, _ = _build(session_factory, redis_bus, relay=relay)

    incident = await orch.inject_incident(fault_type="redis_cache_stampede")
    # Business path must keep working while Redis Streams are unreachable.
    assert await worker.run_until_quiescent() == 4

    async with session_factory() as sess:
        mid = await get_incident_full(sess, incident.id)
        traces_mid = await list_trace_events(sess, incident.id)
        outbox_mid = (
            await sess.execute(
                select(OutboxEvent)
                .where(OutboxEvent.incident_id == incident.id)
                .order_by(OutboxEvent.id)
            )
        ).scalars().all()
    assert mid is not None and mid.status == "PENDING_APPROVAL"
    assert flaky_bus.injected_failures >= 1
    assert await redis_bus.redis.xlen(GLOBAL_EVENTS_STREAM) == 0  # nothing leaked through
    assert len(outbox_mid) == len(traces_mid)
    assert all(row.status == "PENDING" for row in outbox_mid)
    assert outbox_mid[0].retry_count >= 1
    assert "Injected fault" in (outbox_mid[0].last_error or "")

    # Redis recovers.
    flaky_bus.remaining_failures = 0
    await asyncio.sleep(0.1)  # let the retry backoff window pass

    await orch.approve_remediation(incident.id, approved_by="sre-outage-drill")
    assert await worker.run_until_quiescent() == 1
    assert await relay.flush(timeout=5.0) == 0

    async with session_factory() as sess:
        final = await get_incident_full(sess, incident.id)
        traces = await list_trace_events(sess, incident.id)
        trajectory = await get_trajectory_by_incident(sess, incident.id)
        outbox_final = (
            await sess.execute(
                select(OutboxEvent)
                .where(OutboxEvent.incident_id == incident.id)
                .order_by(OutboxEvent.id)
            )
        ).scalars().all()
    assert final is not None and final.status == "RESOLVED"
    assert trajectory is not None
    assert all(row.status == "PUBLISHED" and row.published_at is not None for row in outbox_final)
    assert len(outbox_final) == len(traces)

    # Zero loss, zero duplicates, strictly ordered per incident.
    entries = await redis_bus.redis.xrange(incident_events_stream(incident.id))
    streamed_trace_ids = [int(fields["trace_id"]) for _, fields in entries]
    assert streamed_trace_ids == [t.id for t in traces]
    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0


@pytest.mark.asyncio
async def test_graceful_shutdown_drains_inflight_tasks_and_outbox(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = pg_engine_and_factory
    settings = get_settings()
    app = create_app(
        settings=settings,
        database_url=os.environ.get("TEST_DATABASE_URL", settings.test_database_url),
        redis_url=os.environ.get("TEST_REDIS_URL", settings.test_redis_url),
        start_worker=True,
        enable_fault_injection=True,
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}

    async with LifespanManager(app, shutdown_timeout=30) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            for fault_type in (
                "db_connection_pool_exhausted",
                "slow_sql_lock_contention",
                "redis_cache_stampede",
            ):
                resp = await client.post(
                    "/api/v1/incidents/inject",
                    headers=viewer_headers,
                    json={"fault_type": fault_type},
                )
                assert resp.status_code == 201
            await asyncio.sleep(0.05)
    # Leaving the context triggers the lifespan graceful shutdown.

    report = app.state.shutdown_report
    assert report is not None
    assert report["worker_drained"] is True
    assert report["outbox_pending_remaining"] == 0

    async with session_factory() as sess:
        running = (
            await sess.execute(
                select(func.count(AgentTask.id)).where(AgentTask.status == "RUNNING")
            )
        ).scalar_one()
        pending_outbox = (
            await sess.execute(
                select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "PENDING")
            )
        ).scalar_one()
        completed = (
            await sess.execute(
                select(func.count(AgentTask.id)).where(AgentTask.status == "COMPLETED")
            )
        ).scalar_one()
    assert running == 0
    assert pending_outbox == 0
    assert completed >= 1
