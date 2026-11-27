"""Standalone AgentWorker + LeaseWatchdog process entrypoint (`python -m app.entrypoints.worker`)."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys

import redis.asyncio as aioredis
from sqlalchemy import text

from app.agents.llm import build_llm_provider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.reliability import LeaseWatchdog, OutboxRelay
from app.agents.tools import ToolRegistry
from app.config import Settings, get_settings
from app.db.base import create_engine_and_sessionmaker
from app.observability.logging import configure_json_logging
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry

configure_json_logging()
logger = logging.getLogger(__name__)


async def run_healthcheck(settings: Settings | None = None) -> int:
    cfg = settings or get_settings()
    engine, session_factory = create_engine_and_sessionmaker(cfg.database_url)
    redis_client = aioredis.from_url(cfg.redis_url, decode_responses=True)
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
        ok = await redis_client.ping()
        return 0 if ok else 1
    finally:
        await redis_client.aclose()
        await engine.dispose()


async def run_worker_service(
    *,
    settings: Settings | None = None,
    database_url: str | None = None,
    redis_url: str | None = None,
    worker_id: str | None = None,
    stop_event: asyncio.Event | None = None,
) -> dict[str, object]:
    cfg = settings or get_settings()
    resolved_db = database_url or cfg.database_url
    resolved_redis = redis_url or cfg.redis_url
    resolved_worker_id = worker_id or os.environ.get(
        "WORKER_ID", f"worker-{socket.gethostname()}-{os.getpid()}"
    )

    engine, session_factory = create_engine_and_sessionmaker(resolved_db)
    redis_client = aioredis.from_url(resolved_redis, decode_responses=True)
    redis_bus = RedisEventBus(redis_client)
    skill_registry = SkillRegistry(cfg.skills_dir)
    tool_registry = ToolRegistry(execution_mode=cfg.tool_execution_mode)
    llm_provider = build_llm_provider(cfg)

    # Non-running OutboxRelay handle: worker only commits outbox_events in PG16;
    # the dedicated relay container publishes them to Redis Streams.
    passive_relay = OutboxRelay(session_factory=session_factory, redis_bus=redis_bus)
    orchestrator = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=skill_registry,
        tool_registry=tool_registry,
        llm_provider=llm_provider,
        step_delay_ms=cfg.step_delay_ms,
        trajectory_consumer=None,
        outbox_relay=passive_relay,
        lease_seconds=cfg.task_lease_seconds,
    )
    worker = AgentWorker(orchestrator, worker_id=resolved_worker_id, trajectory_consumer=None)
    watchdog = LeaseWatchdog(orchestrator, interval_seconds=cfg.watchdog_interval_seconds)

    shutdown_trigger = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_trigger.set)
        except NotImplementedError:
            pass

    logger.info(
        "Starting standalone worker=%s tool_mode=%s llm_provider=%s",
        resolved_worker_id,
        cfg.tool_execution_mode,
        cfg.llm_provider,
    )
    watchdog.start()
    worker.start()

    try:
        await shutdown_trigger.wait()
    finally:
        logger.info("Worker %s received shutdown signal; draining in-flight tasks...", resolved_worker_id)
        drained = await worker.stop(timeout=cfg.shutdown_timeout_seconds)
        await watchdog.stop()
        await redis_client.aclose()
        await engine.dispose()
        logger.info("Worker %s stopped cleanly (drained=%s).", resolved_worker_id, drained)

    return {"worker_id": resolved_worker_id, "worker_drained": drained}


def main() -> None:
    if "--healthcheck" in sys.argv:
        sys.exit(asyncio.run(run_healthcheck()))
    asyncio.run(run_worker_service())


if __name__ == "__main__":
    main()
