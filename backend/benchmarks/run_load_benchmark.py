"""Async concurrency & reliability load benchmark for AgentMesh AIOps.

Strictly enforces LLM_PROVIDER=mock (zero external API calls / zero token cost)
and executes against real PostgreSQL 16 (FOR UPDATE SKIP LOCKED + JSONB/GIN)
and real Redis 7 Streams (Consumer Group + XACK).
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
import math
import os
from pathlib import Path
import statistics
import sys
import time
from typing import Any

# Hard guard: NEVER call real LLM APIs during load benchmarks.
os.environ["LLM_PROVIDER"] = "mock"

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import redis.asyncio as aioredis
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.base import init_db
from app.db.models import AgentTask, Incident, IncidentTrajectory
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    TRAJECTORY_CONSUMER_GROUP,
    RedisEventBus,
)
from app.skills.loader import SkillRegistry

FAULT_TYPES = [
    "db_connection_pool_exhausted",
    "slow_sql_lock_contention",
    "redis_cache_stampede",
]


@dataclass
class TierBenchmarkResult:
    concurrency: int
    worker_count: int
    archiver_count: int
    total_requests: int
    total_agent_tasks: int
    succeeded: int
    failed: int
    success_rate_pct: float
    wall_time_seconds: float
    throughput_req_per_sec: float
    task_throughput_per_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    max_latency_ms: float
    duplicate_task_executions: int
    archived_trajectories: int
    redis_stream_xpending: int


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    rank = max(1, math.ceil((pct / 100.0) * len(sorted_values)))
    return sorted_values[min(rank - 1, len(sorted_values) - 1)]


async def run_tier(
    *,
    concurrency: int,
    worker_count: int,
    archiver_count: int,
    database_url: str,
    redis_url: str,
) -> TierBenchmarkResult:
    get_settings.cache_clear()
    settings = get_settings()
    assert settings.llm_provider == "mock", "Benchmark must run with LLM_PROVIDER=mock"

    engine = create_async_engine(
        database_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=25,
        max_overflow=35,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    await init_db(engine)

    # Clean PG16 test database & Redis test DB before each concurrency tier.
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE incident_trajectories, trace_events, "
                "remediation_approvals, agent_tasks, incidents RESTART IDENTITY CASCADE"
            )
        )

    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    await redis_client.flushdb()
    redis_bus = RedisEventBus(redis_client)

    llm_provider = MockLLMProvider()
    assert isinstance(llm_provider, MockLLMProvider)

    skill_registry = SkillRegistry(settings.skills_dir)
    tool_registry = ToolRegistry()

    archivers = [
        TrajectoryStreamConsumer(
            session_factory=session_factory,
            redis_bus=redis_bus,
            consumer_name=f"bench-archiver-{i + 1}",
        )
        for i in range(archiver_count)
    ]
    await archivers[0].initialize()

    orchestrator = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=skill_registry,
        tool_registry=tool_registry,
        llm_provider=llm_provider,
        step_delay_ms=0,
        trajectory_consumer=archivers[0],
    )
    workers = [
        AgentWorker(orchestrator, worker_id=f"bench-worker-{i + 1}")
        for i in range(worker_count)
    ]

    for arch in archivers:
        arch.start()
    for w in workers:
        w.start()

    async def execute_single_incident(idx: int) -> tuple[bool, float]:
        fault_type = FAULT_TYPES[idx % len(FAULT_TYPES)]
        t_start = time.perf_counter()
        try:
            inc = await orchestrator.inject_incident(
                fault_type=fault_type,
                alert_payload={"bench_tier": concurrency, "bench_idx": idx},
            )
            # Wait until PENDING_APPROVAL
            deadline = time.perf_counter() + 45.0
            while time.perf_counter() < deadline:
                async with session_factory() as sess:
                    status_now = (
                        await sess.execute(select(Incident.status).where(Incident.id == inc.id))
                    ).scalar_one()
                if status_now == "PENDING_APPROVAL":
                    break
                if status_now in {"FAILED", "REJECTED"}:
                    return False, (time.perf_counter() - t_start) * 1000.0
                await asyncio.sleep(0.01)
            else:
                return False, (time.perf_counter() - t_start) * 1000.0

            await orchestrator.approve_remediation(
                inc.id,
                approved_by=f"bench-operator-{idx % 4}",
                comment=f"Auto-approved in concurrency={concurrency} benchmark",
            )

            # Wait until RESOLVED (archived by TrajectoryStreamConsumer via Redis Streams)
            while time.perf_counter() < deadline:
                async with session_factory() as sess:
                    status_now = (
                        await sess.execute(select(Incident.status).where(Incident.id == inc.id))
                    ).scalar_one()
                if status_now == "RESOLVED":
                    return True, (time.perf_counter() - t_start) * 1000.0
                if status_now in {"FAILED", "REJECTED"}:
                    return False, (time.perf_counter() - t_start) * 1000.0
                await asyncio.sleep(0.01)
            return False, (time.perf_counter() - t_start) * 1000.0
        except Exception:
            return False, (time.perf_counter() - t_start) * 1000.0

    try:
        wall_start = time.perf_counter()
        outcomes = await asyncio.gather(*(execute_single_incident(i) for i in range(concurrency)))
        wall_seconds = time.perf_counter() - wall_start

        # Allow trailing TRAJECTORY_ARCHIVED / INCIDENT_RESOLVED events to be XACKed.
        xpending = -1
        for _ in range(100):
            xpending = await redis_bus.pending_count(
                GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP
            )
            if xpending == 0:
                break
            await asyncio.sleep(0.02)

        async with session_factory() as sess:
            total_tasks = int(
                (await sess.execute(select(func.count(AgentTask.id)))).scalar_one()
            )
            extra_attempts = int(
                (
                    await sess.execute(
                        select(func.coalesce(func.sum(AgentTask.attempts - 1), 0)).where(
                            AgentTask.attempts > 1
                        )
                    )
                ).scalar_one()
            )
            dup_role_rows = (
                await sess.execute(
                    text(
                        """
                        SELECT count(*) FROM (
                            SELECT incident_id, agent_role
                            FROM agent_tasks
                            GROUP BY incident_id, agent_role
                            HAVING count(*) > 1
                        ) t
                        """
                    )
                )
            ).scalar_one()
            archived_count = int(
                (await sess.execute(select(func.count(IncidentTrajectory.id)))).scalar_one()
            )

        duplicate_task_executions = extra_attempts + int(dup_role_rows)
        latencies = sorted(lat for _, lat in outcomes)
        succeeded = sum(1 for ok, _ in outcomes if ok)
        failed = concurrency - succeeded

        result = TierBenchmarkResult(
            concurrency=concurrency,
            worker_count=worker_count,
            archiver_count=archiver_count,
            total_requests=concurrency,
            total_agent_tasks=total_tasks,
            succeeded=succeeded,
            failed=failed,
            success_rate_pct=round((succeeded / concurrency) * 100.0, 2),
            wall_time_seconds=round(wall_seconds, 3),
            throughput_req_per_sec=round(concurrency / wall_seconds, 2),
            task_throughput_per_sec=round(total_tasks / wall_seconds, 2),
            avg_latency_ms=round(statistics.mean(latencies), 2),
            p50_latency_ms=round(percentile(latencies, 50), 2),
            p95_latency_ms=round(percentile(latencies, 95), 2),
            p99_latency_ms=round(percentile(latencies, 99), 2),
            max_latency_ms=round(max(latencies), 2),
            duplicate_task_executions=duplicate_task_executions,
            archived_trajectories=archived_count,
            redis_stream_xpending=xpending,
        )

        # Hard reliability assertions
        assert result.failed == 0, f"Expected 0 failed incidents, got {result.failed}"
        assert (
            result.duplicate_task_executions == 0
        ), f"Expected 0 duplicate task executions, got {result.duplicate_task_executions}"
        assert (
            result.archived_trajectories == concurrency
        ), f"Expected {concurrency} trajectories, got {result.archived_trajectories}"
        assert (
            result.redis_stream_xpending == 0
        ), f"Expected Redis Stream XPENDING == 0, got {result.redis_stream_xpending}"

        return result
    finally:
        for w in workers:
            await w.stop()
        for arch in archivers:
            await arch.stop()
        await redis_client.aclose()
        await engine.dispose()


def _print_table(results: list[TierBenchmarkResult]) -> None:
    header = (
        f"{'Concurrency':>11} | {'Total':>5} | {'Success':>8} | {'Avg(ms)':>8} | "
        f"{'P95(ms)':>8} | {'P99(ms)':>8} | {'Req/s':>7} | {'Task/s':>7} | {'DupTasks':>8} | {'XPENDING':>8}"
    )
    print("\n" + "=" * len(header))
    print("AgentMesh AIOps — PG16 SKIP LOCKED + Redis Streams Concurrency Benchmark (LLM_PROVIDER=mock)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.concurrency:>11} | {r.total_requests:>5} | {r.success_rate_pct:>7.2f}% | "
            f"{r.avg_latency_ms:>8.2f} | {r.p95_latency_ms:>8.2f} | {r.p99_latency_ms:>8.2f} | "
            f"{r.throughput_req_per_sec:>7.2f} | {r.task_throughput_per_sec:>7.2f} | "
            f"{r.duplicate_task_executions:>8} | {r.redis_stream_xpending:>8}"
        )
    print("=" * len(header) + "\n")


async def main_async(args: argparse.Namespace) -> list[TierBenchmarkResult]:
    settings = get_settings()
    database_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    results: list[TierBenchmarkResult] = []
    for c in args.tiers:
        print(
            f"[Benchmark] Running concurrency={c} with {args.workers} PG16 workers "
            f"+ {args.archivers} Redis Stream archivers (LLM_PROVIDER=mock)..."
        )
        res = await run_tier(
            concurrency=c,
            worker_count=args.workers,
            archiver_count=args.archivers,
            database_url=database_url,
            redis_url=redis_url,
        )
        results.append(res)

    _print_table(results)

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps([asdict(r) for r in results], indent=2),
            encoding="utf-8",
        )
        print(f"[Benchmark] Saved JSON report to {out_path}")

    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AgentMesh AIOps concurrency benchmark (mock LLM)")
    parser.add_argument(
        "--tiers",
        type=int,
        nargs="+",
        default=[10, 30, 50],
        help="Concurrency tiers to execute (default: 10 30 50)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent PG16 FOR UPDATE SKIP LOCKED AgentWorkers (default: 4)",
    )
    parser.add_argument(
        "--archivers",
        type=int,
        default=2,
        help="Number of concurrent Redis Stream TrajectoryStreamConsumers (default: 2)",
    )
    parser.add_argument(
        "--json-out",
        type=str,
        default="",
        help="Optional path to write raw JSON benchmark results",
    )
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main_async(parse_args()))
