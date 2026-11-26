import asyncio
import json
from uuid import uuid4

import pytest
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.db.models import AgentTask, Incident, IncidentTrajectory, TraceEvent
from app.db.repositories import (
    append_trace_event,
    claim_next_queued_task,
    list_incidents,
)
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    RedisEventBus,
    incident_events_stream,
)


@pytest.mark.asyncio
async def test_postgres16_version_and_gin_indexes(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, session_factory = pg_engine_and_factory
    async with session_factory() as session:
        ver_num = (
            await session.execute(text("SELECT current_setting('server_version_num')::int"))
        ).scalar_one()
        assert ver_num >= 160000, f"Expected PostgreSQL 16+, got {ver_num}"

        rows = (
            await session.execute(
                text(
                    """
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND indexname IN (
                        'ix_incidents_alert_payload_gin',
                        'ix_incidents_blackboard_gin',
                        'ix_trace_events_payload_gin',
                        'ix_incident_trajectories_doc_gin'
                      )
                    """
                )
            )
        ).all()
        found = {row[0]: row[1] for row in rows}
        assert set(found.keys()) == {
            "ix_incidents_alert_payload_gin",
            "ix_incidents_blackboard_gin",
            "ix_trace_events_payload_gin",
            "ix_incident_trajectories_doc_gin",
        }
        for name, ddl in found.items():
            assert "using gin" in ddl.lower(), f"Index {name} is not GIN: {ddl}"


@pytest.mark.asyncio
async def test_jsonb_containment_query_and_crud(db_session: AsyncSession) -> None:
    inc1 = Incident(
        title="Payment DB pool exhausted",
        severity="P0",
        service_name="payment-core",
        fault_type="db_connection_pool_exhausted",
        status="DIAGNOSING",
        alert_payload={"region": "cn-north-1", "cluster": "prod-pg-01", "active_conns": 200},
        blackboard={"matched_skill": {"skill_id": "skill.pg16.pool_exhaustion"}, "tags": ["pg16"]},
    )
    inc2 = Incident(
        title="Redis hotkey stampede",
        severity="P1",
        service_name="catalog-api",
        fault_type="redis_cache_stampede",
        status="OPEN",
        alert_payload={"region": "cn-north-1", "cluster": "prod-redis-02"},
        blackboard={"matched_skill": {"skill_id": "skill.redis.cache_stampede"}},
    )
    db_session.add_all([inc1, inc2])
    await db_session.commit()

    matched = await list_incidents(
        db_session,
        blackboard_contains={"matched_skill": {"skill_id": "skill.pg16.pool_exhaustion"}},
    )
    assert [m.id for m in matched] == [inc1.id]

    ev = await append_trace_event(
        db_session,
        incident_id=inc1.id,
        agent_role="COORDINATOR",
        event_type="SKILL_MATCHED",
        risk_tier="NONE",
        summary="Matched PG16 pool exhaustion runbook",
        payload={"skill_id": "skill.pg16.pool_exhaustion"},
    )
    traj = IncidentTrajectory(
        incident_id=inc1.id,
        skill_id="skill.pg16.pool_exhaustion",
        fault_type=inc1.fault_type,
        root_cause="Idle-in-transaction connections held pool slots",
        resolution_command="SELECT pg_terminate_backend(pid) ...",
        trajectory_doc={"steps": ["COORDINATOR", "LOG_DIAGNOSTICS"], "verified": True},
    )
    db_session.add(traj)
    await db_session.commit()

    assert ev.id >= 1
    row = (
        await db_session.execute(
            text(
                "SELECT count(*) FROM incident_trajectories "
                "WHERE trajectory_doc @> '{\"verified\": true}'::jsonb"
            )
        )
    ).scalar_one()
    assert row == 1


@pytest.mark.asyncio
async def test_for_update_skip_locked_non_blocking_and_concurrent_claim(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    _, session_factory = pg_engine_and_factory

    async with session_factory() as seed_sess:
        inc = Incident(
            title="Skip locked test",
            severity="P1",
            service_name="order-svc",
            fault_type="slow_sql_lock_contention",
            status="DIAGNOSING",
            alert_payload={},
            blackboard={},
        )
        seed_sess.add(inc)
        await seed_sess.flush()

        tasks = [
            AgentTask(
                incident_id=inc.id,
                agent_role=f"WORKER_{i}",
                step_order=10 + i,
                action_type="READ_ONLY_DIAGNOSE",
                status="QUEUED",
                input_payload={"idx": i},
            )
            for i in range(5)
        ]
        seed_sess.add_all(tasks)
        await seed_sess.commit()
        first_task_id = tasks[0].id
        second_task_id = tasks[1].id

    # Step A: Hold an uncommitted row lock on task 0 in session A,
    # verify session B immediately skips task 0 and claims task 1.
    async with session_factory() as sess_a:
        async with sess_a.begin():
            locked_first = await claim_next_queued_task(sess_a, "worker-a")
            assert locked_first is not None
            assert locked_first.id == first_task_id

            async with session_factory() as sess_b:
                async with sess_b.begin():
                    claimed_second = await asyncio.wait_for(
                        claim_next_queued_task(sess_b, "worker-b"),
                        timeout=2.0,
                    )
                    assert claimed_second is not None
                    assert claimed_second.id == second_task_id

    # Step B: Concurrent workers race to claim remaining 3 queued tasks -> zero duplicates.
    async def worker_claim(name: str) -> list[str]:
        claimed_ids: list[str] = []
        while True:
            async with session_factory() as sess:
                async with sess.begin():
                    task = await claim_next_queued_task(sess, name)
                    if task is None:
                        return claimed_ids
                    claimed_ids.append(str(task.id))

    results = await asyncio.gather(*(worker_claim(f"race-{i}") for i in range(4)))
    flat = [tid for group in results for tid in group]
    assert len(flat) == 3
    assert len(set(flat)) == 3


@pytest.mark.asyncio
async def test_redis_blackboard_and_streams_roundtrip(
    redis_bus: RedisEventBus,
) -> None:
    assert await redis_bus.ping() is True
    inc_id = uuid4()

    await redis_bus.set_blackboard_cache(inc_id, {"stage": "DIAGNOSING", "hits": 3})
    cached = await redis_bus.get_blackboard_cache(inc_id)
    assert cached == {"stage": "DIAGNOSING", "hits": 3}

    stream_key = incident_events_stream(inc_id)
    group_name = "cg:test-roundtrip"
    await redis_bus.ensure_consumer_group(stream_key, group_name, start_id="0")

    inc_entry_id, global_entry_id = await redis_bus.publish_trace_event(
        inc_id,
        {
            "id": 42,
            "incident_id": str(inc_id),
            "agent_role": "DB_DIAGNOSTICS",
            "event_type": "TOOL_RESULT",
            "risk_tier": "READ_ONLY",
            "summary": "ok",
        },
    )
    assert inc_entry_id
    assert global_entry_id

    entries = await redis_bus.read_group(stream_key, group_name, "c1", count=10, block_ms=None)
    assert len(entries) == 1
    entry_id, payload = entries[0]
    assert entry_id == inc_entry_id
    assert payload["id"] == 42
    assert await redis_bus.pending_count(stream_key, group_name) == 1

    acked = await redis_bus.ack(stream_key, group_name, entry_id)
    assert acked == 1
    assert await redis_bus.pending_count(stream_key, group_name) == 0
    await redis_bus.destroy_consumer_group(stream_key, group_name)
