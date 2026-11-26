import asyncio
import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import get_settings
from app.db.base import create_engine_and_sessionmaker, init_db
from app.redis_bus.events import RedisEventBus


@pytest.fixture(autouse=True)
def force_mock_llm_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure pytest always uses LLM_PROVIDER=mock (zero external API cost)."""
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    get_settings.cache_clear()


@pytest_asyncio.fixture
async def pg_engine_and_factory() -> AsyncIterator[
    tuple[AsyncEngine, async_sessionmaker[AsyncSession]]
]:
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    engine, session_factory = create_engine_and_sessionmaker(test_db_url)
    await init_db(engine)

    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE incident_trajectories, trace_events, "
                "remediation_approvals, agent_tasks, incidents RESTART IDENTITY CASCADE"
            )
        )

    try:
        yield engine, session_factory
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db_session(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> AsyncIterator[AsyncSession]:
    _, session_factory = pg_engine_and_factory
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[aioredis.Redis]:
    settings = get_settings()
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)
    client = aioredis.from_url(test_redis_url, decode_responses=True)
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.fixture
def redis_bus(redis_client: aioredis.Redis) -> RedisEventBus:
    return RedisEventBus(redis_client)
