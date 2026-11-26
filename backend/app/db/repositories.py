from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import (
    AgentTask,
    Incident,
    IncidentTrajectory,
    RemediationApproval,
    TraceEvent,
)


async def claim_next_queued_task(session: AsyncSession, worker_id: str) -> AgentTask | None:
    """Atomically claim one QUEUED task using PostgreSQL FOR UPDATE SKIP LOCKED."""
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

    task.status = "RUNNING"
    task.worker_id = worker_id
    task.locked_at = datetime.now(timezone.utc)
    task.attempts = task.attempts + 1
    await session.flush()
    return task


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
