import asyncio
import json
import os

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.repositories import get_incident_full, get_trajectory_by_incident
from app.main import create_app
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    TRAJECTORY_CONSUMER_GROUP,
    RedisEventBus,
    incident_events_stream,
)
from app.skills.loader import SkillRegistry


@pytest.mark.asyncio
async def test_redis_streams_xadd_xreadgroup_pending_and_xack(
    redis_bus: RedisEventBus,
) -> None:
    stream_key = "stream:test:unit"
    group_name = "cg:test-unit"
    await redis_bus.ensure_consumer_group(stream_key, group_name, start_id="0")

    eid = await redis_bus.redis.xadd(
        stream_key,
        {"data": json.dumps({"id": 1, "event_type": "ALERT_INJECTED"})},
    )
    entries = await redis_bus.read_group(stream_key, group_name, "c-alpha", count=10, block_ms=None)
    assert len(entries) == 1
    assert entries[0][0] == str(eid)
    assert await redis_bus.pending_count(stream_key, group_name) == 1

    acked = await redis_bus.ack(stream_key, group_name, str(eid))
    assert acked == 1
    assert await redis_bus.pending_count(stream_key, group_name) == 0


@pytest.mark.asyncio
async def test_trajectory_consumer_crash_pel_recovery_via_xautoclaim(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    _, session_factory = pg_engine_and_factory
    settings = get_settings()

    # Wire orchestrator/worker WITHOUT trajectory_consumer so we can simulate consumer crash mid-flight.
    orch = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=SkillRegistry(settings.skills_dir),
        tool_registry=ToolRegistry(),
        llm_provider=MockLLMProvider(),
        step_delay_ms=0,
        trajectory_consumer=None,
    )
    worker = AgentWorker(orch, worker_id="worker-crash-sim", trajectory_consumer=None)

    await redis_bus.ensure_consumer_group(
        GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP, start_id="0"
    )

    incident = await orch.inject_incident(fault_type="slow_sql_lock_contention")
    await worker.run_until_quiescent()
    await orch.approve_remediation(incident.id, approved_by="sre-pel-test")
    await worker.run_until_quiescent()

    # Simulate Consumer A reading from Consumer Group and crashing BEFORE committing / XACKing.
    crashed_reads = await redis_bus.read_group(
        GLOBAL_EVENTS_STREAM,
        TRAJECTORY_CONSUMER_GROUP,
        "crashed-consumer-a",
        count=50,
        block_ms=None,
    )
    assert len(crashed_reads) >= 1
    assert any(p.get("event_type") == "HIGH_RISK_EXECUTED" for _, p in crashed_reads)
    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == len(
        crashed_reads
    )

    async with session_factory() as sess:
        mid = await get_incident_full(sess, incident.id)
        assert mid is not None
        assert mid.status == "EXECUTING_REMEDIATION"
        assert mid.trajectory is None

    # Allow entries to age past min_idle_ms, then have survivor Consumer B reclaim via XAUTOCLAIM.
    await asyncio.sleep(0.05)
    recovery_consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name="recovery-consumer-b",
    )
    reclaimed = await recovery_consumer.reclaim_and_process_pending(min_idle_ms=10, count=50)
    assert reclaimed == len(crashed_reads)
    await recovery_consumer.drain_until_empty()

    # Verify PEL is completely drained (XPENDING == 0) and Trajectory was archived once!
    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0

    async with session_factory() as sess:
        final_inc = await get_incident_full(sess, incident.id)
        assert final_inc is not None
        assert final_inc.status == "RESOLVED"
        traj = await get_trajectory_by_incident(sess, incident.id)
        assert traj is not None
        assert traj.trajectory_doc["stream_consumption"]["stream"] == GLOBAL_EVENTS_STREAM
        assert (
            traj.trajectory_doc["stream_consumption"]["consumer_group"]
            == TRAJECTORY_CONSUMER_GROUP
        )
        assert (
            traj.trajectory_doc["stream_consumption"]["consumer_name"]
            == "recovery-consumer-b"
        )


@pytest.mark.asyncio
async def test_sse_consumer_group_acks_and_destroys_ephemeral_group(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
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

    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            inject_resp = await client.post(
                "/api/v1/incidents/inject",
                json={"fault_type": "redis_cache_stampede"},
            )
            assert inject_resp.status_code == 201
            incident_id = inject_resp.json()["id"]

            for _ in range(50):
                r = await client.get(f"/api/v1/incidents/{incident_id}")
                if r.json()["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)

            async def approve_soon() -> None:
                await asyncio.sleep(0.15)
                resp = await client.post(
                    f"/api/v1/incidents/{incident_id}/approve",
                    json={"operator": "sre-sse-ack"},
                )
                assert resp.status_code == 200

            async def read_sse() -> list[str]:
                seen_events: list[str] = []
                async with client.stream(
                    "GET", f"/api/v1/incidents/{incident_id}/events"
                ) as sse_resp:
                    assert sse_resp.status_code == 200
                    current_event = "message"
                    async for line in sse_resp.aiter_lines():
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            current_event = line.split(":", 1)[1].strip()
                        elif line.startswith("data:"):
                            seen_events.append(current_event)
                            if current_event == "complete":
                                return seen_events
                return seen_events

            sse_events, _ = await asyncio.gather(read_sse(), approve_soon())
            assert sse_events[-1] == "complete"

            # Wait briefly for generator finally + background consumer to finish final XACKs
            sse_groups: list[dict] = []
            pending = -1
            for _ in range(40):
                groups = await redis_bus.list_groups(incident_events_stream(incident_id))
                sse_groups = [
                    g for g in groups if str(g.get("name", "")).startswith("cg:sse:")
                ]
                pending = await redis_bus.pending_count(
                    GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP
                )
                if sse_groups == [] and pending == 0:
                    break
                await asyncio.sleep(0.05)

            assert sse_groups == []
            assert pending == 0
