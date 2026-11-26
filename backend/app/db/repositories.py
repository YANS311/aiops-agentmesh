from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import (
    AgentTask,
    Incident,
    IncidentTrajectory,
    OutboxEvent,
    RemediationApproval,
    TraceEvent,
)

DEFAULT_LEASE_SECONDS = 30.0


def serialize_trace_event(ev: TraceEvent) -> dict[str, Any]:
    return {
        "id": ev.id,
        "incident_id": str(ev.incident_id),
        "agent_role": ev.agent_role,
        "event_type": ev.event_type,
        "risk_tier": ev.risk_tier,
        "summary": ev.summary,
        "payload": ev.payload,
        "created_at": ev.created_at.isoformat() if ev.created_at else None,
    }


async def claim_next_queued_task(
    session: AsyncSession,
    worker_id: str,
    *,
    lease_seconds: float = DEFAULT_LEASE_SECONDS,
) -> AgentTask | None:
    """Atomically claim one QUEUED task (FOR UPDATE SKIP LOCKED) and grant a time-bound lease."""
    stmt = (
        select(AgentTask)
        .where(AgentTask.status == "QUEUED")
        .order_by(AgentTask.step_order.asc(), AgentTask.created_at.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    task = result.scalar_one_or_none()
    if task is None:
        return None

    now = datetime.now(timezone.utc)
    task.status = "RUNNING"
    task.worker_id = worker_id
    task.lease_owner = worker_id
    task.locked_at = now
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    task.attempt_count = task.attempt_count + 1
    await session.flush()
    return task


async def renew_task_lease(
    session: AsyncSession,
    *,
    task_id: UUID,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    """Heartbeat: extend the lease only if this worker still owns a RUNNING task.

    Uses a SKIP LOCKED sub-select so the heartbeat never blocks behind the worker's own
    execution transaction (which holds the row lock and is itself proof of liveness).
    """
    owned_row = (
        select(AgentTask.id)
        .where(
            AgentTask.id == task_id,
            AgentTask.status == "RUNNING",
            AgentTask.lease_owner == worker_id,
        )
        .with_for_update(skip_locked=True)
        .scalar_subquery()
    )
    result = await session.execute(
        update(AgentTask)
        .where(AgentTask.id == owned_row)
        .values(lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


async def append_trace_event(
    session: AsyncSession,
    *,
    incident_id: UUID,
    agent_role: str,
    event_type: str,
    risk_tier: str,
    summary: str,
    payload: dict[str, Any] | None = None,
) -> TraceEvent:
    event = TraceEvent(
        incident_id=incident_id,
        agent_role=agent_role,
        event_type=event_type,
        risk_tier=risk_tier,
        summary=summary,
        payload=payload or {},
    )
    session.add(event)
    await session.flush()
    # Transactional Outbox: the stream message commits atomically with the trace row.
    session.add(
        OutboxEvent(
            incident_id=incident_id,
            trace_event_id=event.id,
            event_type=event_type,
            payload=serialize_trace_event(event),
            status="PENDING",
        )
    )
    await session.flush()
    return event


async def list_incidents(
    session: AsyncSession,
    *,
    fault_type: str | None = None,
    blackboard_contains: dict[str, Any] | None = None,
    limit: int = 50,
) -> list[Incident]:
    stmt = select(Incident).order_by(Incident.created_at.desc()).limit(limit)
    if fault_type:
        stmt = stmt.where(Incident.fault_type == fault_type)
    if blackboard_contains:
        stmt = stmt.where(Incident.blackboard.contains(blackboard_contains))
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_incident_full(session: AsyncSession, incident_id: UUID) -> Incident | None:
    stmt = (
        select(Incident)
        .where(Incident.id == incident_id)
        .options(
            selectinload(Incident.tasks),
            selectinload(Incident.approval),
            selectinload(Incident.trajectory),
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_trace_events(
    session: AsyncSession, incident_id: UUID, after_id: int = 0
) -> list[TraceEvent]:
    stmt = (
        select(TraceEvent)
        .where(TraceEvent.incident_id == incident_id, TraceEvent.id > after_id)
        .order_by(TraceEvent.id.asc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def list_trajectories(
    session: AsyncSession, limit: int = 50
) -> list[IncidentTrajectory]:
    stmt = (
        select(IncidentTrajectory)
        .order_by(IncidentTrajectory.created_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_trajectory_by_incident(
    session: AsyncSession, incident_id: UUID
) -> IncidentTrajectory | None:
    stmt = select(IncidentTrajectory).where(IncidentTrajectory.incident_id == incident_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_approval_for_update(
    session: AsyncSession, incident_id: UUID
) -> RemediationApproval | None:
    stmt = (
        select(RemediationApproval)
        .where(RemediationApproval.incident_id == incident_id)
        .with_for_update()
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()
