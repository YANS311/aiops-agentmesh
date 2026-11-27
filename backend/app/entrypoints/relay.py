"""Standalone OutboxRelay + TrajectoryStreamConsumer process entrypoint (`python -m app.entrypoints.relay`)."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys

import redis.asyncio as aioredis
from sqlalchemy import text

from app.agents.reliability import OutboxRelay
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import Settings, get_settings
from app.db.base import create_engine_and_sessionmaker
from app.observability.logging import configure_json_logging
from app.redis_bus.events import RedisEventBus

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


async def run_relay_service(
    *,
    settings: Settings | None = None,
    database_url: str | None = None,
    redis_url: str | None = None,
    consumer_name: str | None = None,
    stop_event: asyncio.Event | None = None,
) -> dict[str, object]:
    cfg = settings or get_settings()
    resolved_db = database_url or cfg.database_url
    resolved_redis = redis_url or cfg.redis_url
    resolved_consumer = consumer_name or os.environ.get(
        "RELAY_ID", f"archiver-{socket.gethostname()}-{os.getpid()}"
    )

    engine, session_factory = create_engine_and_sessionmaker(resolved_db)
    redis_client = aioredis.from_url(resolved_redis, decode_responses=True)
    redis_bus = RedisEventBus(redis_client)

    outbox_relay = OutboxRelay(
        session_factory=session_factory,
        redis_bus=redis_bus,
        batch_size=cfg.outbox_batch_size,
        poll_interval_seconds=cfg.outbox_poll_interval_seconds,
    )
    trajectory_consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name=resolved_consumer,
        outbox_relay=outbox_relay,
    )
    await trajectory_consumer.initialize()

    shutdown_trigger = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_trigger.set)
        except NotImplementedError:
            pass

    logger.info("Starting standalone relay & trajectory consumer=%s", resolved_consumer)
    outbox_relay.start()
    trajectory_consumer.start()

    try:
        await shutdown_trigger.wait()
    finally:
        logger.info("Relay %s received shutdown signal; flushing outbox & stopping consumer...", resolved_consumer)
        timeout = cfg.shutdown_timeout_seconds
        await outbox_relay.flush(timeout=timeout)
        await trajectory_consumer.stop(timeout=timeout)
        remaining = await outbox_relay.flush(timeout=min(timeout, 2.0))
        await outbox_relay.stop()
        await redis_client.aclose()
        await engine.dispose()
        logger.info("Relay %s stopped cleanly (outbox_remaining=%d).", resolved_consumer, remaining)

    return {"consumer_name": resolved_consumer, "outbox_pending_remaining": remaining}


def main() -> None:
    if "--healthcheck" in sys.argv:
        sys.exit(asyncio.run(run_healthcheck()))
    asyncio.run(run_relay_service())


if __name__ == "__main__":
    main()
