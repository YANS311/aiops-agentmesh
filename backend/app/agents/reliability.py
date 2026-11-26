"""Production reliability primitives: Transactional Outbox relay and worker lease watchdog."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import logging
import time
from typing import TYPE_CHECKING

from redis.exceptions import RedisError
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AgentTask, Incident, OutboxEvent
from app.db.repositories import append_trace_event
from app.redis_bus.events import RedisEventBus

if TYPE_CHECKING:
    from app.agents.orchestrator import AIOpsOrchestrator

logger = logging.getLogger(__name__)

# Single-active-relay guard: preserves per-incident publish order across replicas.
OUTBOX_RELAY_ADVISORY_LOCK_KEY = 0x41474D4F5554  # "AGMOUT"
TERMINAL_INCIDENT_STATUSES = {"RESOLVED", "REJECTED", "FAILED"}


class OutboxRelay:
    """Publishes PENDING outbox_events to Redis Streams (at-least-once, ordered, zero-loss)."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        redis_bus: RedisEventBus,
        batch_size: int = 100,
        poll_interval_seconds: float = 0.2,
        base_backoff_seconds: float = 0.2,
        max_backoff_seconds: float = 5.0,
    ) -> None:
        self.session_factory = session_factory
        self.redis_bus = redis_bus
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.published_total = 0
        self.failed_attempts_total = 0
        self._wakeup = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def notify(self) -> None:
        self._wakeup.set()

    def _backoff_seconds(self, retry_count: int) -> float:
        return min(
            self.max_backoff_seconds,
            self.base_backoff_seconds * (2 ** max(0, retry_count - 1)),
        )

    async def relay_once(self) -> int:
        """Publish one ordered batch. Stops at the first failure to preserve ordering."""
        published = 0
        async with self.session_factory() as session:
            async with session.begin():
                acquired = (
                    await session.execute(
                        text("SELECT pg_try_advisory_xact_lock(:key)"),
                        {"key": OUTBOX_RELAY_ADVISORY_LOCK_KEY},
                    )
                ).scalar_one()
                if not acquired:
                    return 0

                rows = (
                    await session.execute(
                        select(OutboxEvent)
                        .where(
                            OutboxEvent.status == "PENDING",
                            OutboxEvent.available_at <= func.clock_timestamp(),
                        )
                        .order_by(OutboxEvent.id.asc())
                        .limit(self.batch_size)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().all()

                for row in rows:
                    try:
                        await self.redis_bus.publish_trace_event(row.incident_id, row.payload)
                    except (RedisError, OSError) as exc:
                        row.retry_count = row.retry_count + 1
                        row.last_error = f"{type(exc).__name__}: {exc}"[:500]
                        row.available_at = func.clock_timestamp() + timedelta(
                            seconds=self._backoff_seconds(row.retry_count)
                        )
                        self.failed_attempts_total += 1
                        logger.warning(
                            "Outbox publish failed for event %s (retry=%s): %s",
                            row.id,
                            row.retry_count,
                            exc,
                        )
                        break
                    row.status = "PUBLISHED"
                    row.published_at = func.clock_timestamp()
                    published += 1

        self.published_total += published
        return published

    async def drain(self, max_batches: int = 50) -> int:
        total = 0
        for _ in range(max_batches):
            count = await self.relay_once()
            total += count
            if count < self.batch_size:
                break
        return total

    async def pending_count(self) -> int:
        async with self.session_factory() as session:
            return int(
                (
                    await session.execute(
                        select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "PENDING")
                    )
                ).scalar_one()
            )

    async def flush(self, timeout: float) -> int:
        """Drain until no PENDING events remain or the timeout elapses; returns remaining count."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                await self.drain()
            except Exception:
                logger.exception("Outbox flush drain failed")
            remaining = await self.pending_count()
            if remaining == 0 or time.monotonic() >= deadline:
                return remaining
            await asyncio.sleep(
                min(self.poll_interval_seconds, max(0.0, deadline - time.monotonic()))
            )

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            self._wakeup.clear()
            try:
                if await self.relay_once() >= self.batch_size:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unhandled error in OutboxRelay loop")
            if self._stop_event.is_set():
                break
            with suppress(TimeoutError):
                await asyncio.wait_for(self._wakeup.wait(), timeout=self.poll_interval_seconds)

    def start(self) -> None:
        if self._task is None:
            self._stop_event.clear()
            self._task = asyncio.create_task(self._loop(), name="outbox-relay")

    async def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self._wakeup.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            except TimeoutError:
                self._task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._task
            self._task = None


class LeaseWatchdog:
    """Reclaims RUNNING tasks whose lease expired (crashed/zombie workers) or dead-letters them."""

    def __init__(
        self,
        orchestrator: "AIOpsOrchestrator",
        *,
        interval_seconds: float = 5.0,
        batch_size: int = 100,
    ) -> None:
        self.orchestrator = orchestrator
        self.interval_seconds = interval_seconds
        self.batch_size = batch_size
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def reclaim_expired_leases(self, now: datetime | None = None) -> tuple[int, int]:
        """Return (requeued, dead_lettered)."""
        now = now or datetime.now(timezone.utc)
        requeued = 0
        dead_lettered = 0

        async with self.orchestrator.session_factory() as session:
            async with session.begin():
                expired_tasks = (
                    await session.execute(
                        select(AgentTask)
                        .where(
                            AgentTask.status == "RUNNING",
                            AgentTask.lease_expires_at.is_not(None),
                            AgentTask.lease_expires_at < now,
                        )
                        .order_by(AgentTask.lease_expires_at.asc())
                        .limit(self.batch_size)
                        # Rows locked by a live executing transaction are skipped: holding the
                        # row lock is itself proof the owner is alive.
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().all()

                for task in expired_tasks:
                    # SKIP LOCKED on the incident too: never wait while holding a task lock (deadlock-free).
                    incident = (
                        await session.execute(
                            select(Incident)
                            .where(Incident.id == task.incident_id)
                            .with_for_update(skip_locked=True)
                        )
                    ).scalar_one_or_none()
                    if incident is None:
                        continue

                    audit = {
                        "task_id": str(task.id),
                        "agent_role": task.agent_role,
                        "expired_lease_owner": task.lease_owner,
                        "expired_at": task.lease_expires_at.isoformat() if task.lease_expires_at else None,
                        "attempt_count": task.attempt_count,
                        "max_attempts": task.max_attempts,
                    }
                    expired_owner = task.lease_owner

                    if task.attempt_count >= task.max_attempts:
                        task.status = "FAILED"
                        task.error_message = (
                            f"Dead-lettered: lease of {expired_owner} expired; "
                            f"{task.attempt_count}/{task.max_attempts} attempts exhausted."
                        )
                        task.completed_at = now
                        task.lease_owner = None
                        task.lease_expires_at = None
                        await append_trace_event(
                            session,
                            incident_id=incident.id,
                            agent_role="WATCHDOG",
                            event_type="TASK_DEAD_LETTERED",
                            risk_tier="NONE",
                            summary=(
                                f"{task.agent_role} task dead-lettered after "
                                f"{task.attempt_count}/{task.max_attempts} attempts "
                                f"(last lease owner {expired_owner} stopped heartbeating)."
                            ),
                            payload=audit,
                        )
                        if incident.status not in TERMINAL_INCIDENT_STATUSES:
                            incident.status = "FAILED"
                            incident.version += 1
                            await append_trace_event(
                                session,
                                incident_id=incident.id,
                                agent_role="SYSTEM",
                                event_type="INCIDENT_FAILED",
                                risk_tier="NONE",
                                summary=(
                                    f"Incident terminated Fail-Closed: {task.agent_role} task "
                                    f"exhausted its retry budget."
                                ),
                                payload=audit,
                            )
                        dead_lettered += 1
                    else:
                        task.status = "QUEUED"
                        task.lease_owner = None
                        task.lease_expires_at = None
                        task.locked_at = None
                        await append_trace_event(
                            session,
                            incident_id=incident.id,
                            agent_role="WATCHDOG",
                            event_type="LEASE_EXPIRED_REQUEUED",
                            risk_tier="NONE",
                            summary=(
                                f"Lease of {expired_owner} on {task.agent_role} task expired; "
                                f"requeued for retry ({task.attempt_count}/{task.max_attempts})."
                            ),
                            payload=audit,
                        )
                        requeued += 1

        if requeued or dead_lettered:
            self.orchestrator.outbox_relay.notify()
        if requeued:
            self.orchestrator.notify_worker()
        return requeued, dead_lettered

    async def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                requeued, dead = await self.reclaim_expired_leases()
                if requeued or dead:
                    logger.warning(
                        "LeaseWatchdog reclaimed %s task(s), dead-lettered %s task(s)", requeued, dead
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unhandled error in LeaseWatchdog loop")
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)

    def start(self) -> None:
        if self._task is None:
            self._stop_event.clear()
            self._task = asyncio.create_task(self._loop(), name="lease-watchdog")

    async def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            except TimeoutError:
                self._task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._task
            self._task = None
