from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base declarative class for all PG16 ORM models."""


# Idempotent in-place migrations for databases created by earlier phases
# (create_all never alters existing tables).
_IDEMPOTENT_MIGRATIONS: list[str] = [
    """
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'agent_tasks' AND column_name = 'attempts'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'agent_tasks' AND column_name = 'attempt_count'
        ) THEN
            ALTER TABLE agent_tasks RENAME COLUMN attempts TO attempt_count;
        END IF;
    END $$
    """,
    "ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE agent_tasks ALTER COLUMN attempt_count SET DEFAULT 0",
    "ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3",
    "ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(128)",
    "ALTER TABLE agent_tasks ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ",
    "DROP INDEX IF EXISTS ix_agent_tasks_status",
    (
        "CREATE INDEX IF NOT EXISTS ix_agent_tasks_queued_partial "
        "ON agent_tasks (step_order, created_at) WHERE status = 'QUEUED'"
    ),
    (
        "CREATE INDEX IF NOT EXISTS ix_agent_tasks_running_lease "
        "ON agent_tasks (lease_expires_at) WHERE status = 'RUNNING'"
    ),
    # Legacy RUNNING rows without a lease are zombies from pre-lease builds: make them reclaimable.
    (
        "UPDATE agent_tasks SET lease_expires_at = now() "
        "WHERE status = 'RUNNING' AND lease_expires_at IS NULL"
    ),
]


def create_engine_and_sessionmaker(
    database_url: str,
    *,
    echo: bool = False,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    if not database_url.startswith("postgresql+asyncpg://"):
        raise ValueError(
            f"Only PostgreSQL 16 (postgresql+asyncpg://) is permitted; got {database_url!r}"
        )
    engine = create_async_engine(
        database_url,
        echo=echo,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=10,
    )
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    return engine, session_factory


async def init_db(engine: AsyncEngine) -> None:
    """Create all tables/indexes if missing, then apply idempotent in-place migrations."""
    # Import models so metadata is populated before create_all.
    from app.db import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for statement in _IDEMPOTENT_MIGRATIONS:
            await conn.execute(text(statement))
