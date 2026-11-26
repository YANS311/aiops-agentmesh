import asyncio
from datetime import datetime, timezone
import logging
import time
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.attributes import flag_modified

from app.db.models import Incident, IncidentTrajectory, TraceEvent
from app.db.repositories import (
    append_trace_event,
    get_approval_for_update,
    get_trajectory_by_incident,
    list_trace_events,
)
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    TRAJECTORY_CONSUMER_GROUP,
    RedisEventBus,
)

logger = logging.getLogger(__name__)


def _serialize_trace(ev: TraceEvent) -> dict[str, Any]:
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


class TrajectoryStreamConsumer:
    """Consumes global Redis Stream events via Consumer Group + XACK to archive trajectories."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        redis_bus: RedisEventBus,
        consumer_name: str = "archiver-1",
        group_name: str = TRAJECTORY_CONSUMER_GROUP,
        stream_key: str = GLOBAL_EVENTS_STREAM,
    ) -> None:
        self.session_factory = session_factory
        self.redis_bus = redis_bus
        self.consumer_name = consumer_name
        self.group_name = group_name
        self.stream_key = stream_key
        self._initialized = False
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def initialize(self) -> None:
        if self._initialized:
            return
        await self.redis_bus.ensure_consumer_group(
            self.stream_key, self.group_name, start_id="0"
        )
        self._initialized = True

    async def _handle_entry(self, entry_id: str, event_payload: dict[str, Any]) -> None:
        event_type = event_payload.get("event_type")
        if event_type != "HIGH_RISK_EXECUTED":
            await self.redis_bus.ack(self.stream_key, self.group_name, entry_id)
            return

        incident_id = UUID(str(event_payload["incident_id"]))
        pending_events: list[TraceEvent] = []
        blackboard_copy: dict[str, Any] | None = None

        async with self.session_factory() as session:
            async with session.begin():
                incident = (
                    await session.execute(
                        select(Incident).where(Incident.id == incident_id).with_for_update()
                    )
                ).scalar_one_or_none()
                if incident is None:
                    await self.redis_bus.ack(self.stream_key, self.group_name, entry_id)
                    return

                approval = await get_approval_for_update(session, incident.id)
                existing = await get_trajectory_by_incident(session, incident.id)
                if existing is not None:
                    if incident.status != "RESOLVED":
                        incident.status = "RESOLVED"
                        incident.resolved_at = incident.resolved_at or datetime.now(timezone.utc)
                    # Commit DB state before acknowledging duplicate stream entry.
                else:
                    now = datetime.now(timezone.utc)
                    prior_traces = await list_trace_events(session, incident.id)
                    exec_result = (
                        approval.execution_result
                        if approval is not None and approval.execution_result is not None
                        else event_payload.get("payload", {})
                    )
                    skill_id = (
                        approval.skill_id
                        if approval is not None
                        else str((incident.blackboard or {}).get("matched_skill", {}).get("skill_id", "unknown"))
                    )
                    resolution_cmd = (
                        approval.proposed_command
                        if approval is not None
                        else str((incident.blackboard or {}).get("remediation_proposal", {}).get("proposed_command", ""))
                    )

                    trajectory_doc = {
                        "incident_id": str(incident.id),
                        "title": incident.title,
                        "severity": incident.severity,
                        "service_name": incident.service_name,
                        "fault_type": incident.fault_type,
                        "skill_id": skill_id,
                        "root_cause": incident.root_cause_summary or "",
                        "evidence": (incident.blackboard or {}).get("evidence", {}),
                        "approval_audit": {
                            "approval_id": str(approval.id) if approval else None,
                            "approved_by": approval.approved_by if approval else None,
                            "approval_comment": approval.approval_comment if approval else None,
                            "decided_at": (
                                approval.decided_at.isoformat()
                                if approval and approval.decided_at
                                else None
                            ),
                            "executed_at": (
                                approval.executed_at.isoformat()
                                if approval and approval.executed_at
                                else now.isoformat()
                            ),
                        },
                        "execution_result": exec_result,
                        "stream_consumption": {
                            "stream": self.stream_key,
                            "consumer_group": self.group_name,
                            "consumer_name": self.consumer_name,
                            "trigger_entry_id": entry_id,
                        },
                        "trace_steps": [
                            {
                                "id": ev.id,
                                "agent_role": ev.agent_role,
                                "event_type": ev.event_type,
                                "risk_tier": ev.risk_tier,
                                "summary": ev.summary,
                            }
                            for ev in prior_traces
                        ],
                        "archived_at": now.isoformat(),
                    }
                    trajectory = IncidentTrajectory(
                        incident_id=incident.id,
                        skill_id=skill_id,
                        fault_type=incident.fault_type,
                        root_cause=incident.root_cause_summary or "",
                        resolution_command=resolution_cmd,
                        trajectory_doc=trajectory_doc,
                    )
                    session.add(trajectory)
                    await session.flush()

                    bb = dict(incident.blackboard or {})
                    bb["stage"] = "RESOLVED"
                    bb["execution_result"] = exec_result
                    incident.blackboard = bb
                    flag_modified(incident, "blackboard")
                    incident.status = "RESOLVED"
                    incident.resolved_at = now
                    incident.version += 1

                    pending_events.append(
                        await append_trace_event(
                            session,
                            incident_id=incident.id,
                            agent_role="ARCHIVER",
                            event_type="TRAJECTORY_ARCHIVED",
                            risk_tier="NONE",
                            summary=(
                                f"Archived resolution trajectory ({len(prior_traces) + 1} steps) "
                                f"via Redis Stream `{self.group_name}`"
                            ),
                            payload={
                                "trajectory_id": str(trajectory.id),
                                "skill_id": skill_id,
                                "stream_entry_id": entry_id,
                            },
                        )
                    )
                    pending_events.append(
                        await append_trace_event(
                            session,
                            incident_id=incident.id,
                            agent_role="SYSTEM",
                            event_type="INCIDENT_RESOLVED",
                            risk_tier="NONE",
                            summary=f"Incident {incident.id} resolved and trajectory archived.",
                            payload={"status": "RESOLVED", "trajectory_id": str(trajectory.id)},
                        )
                    )
                    blackboard_copy = dict(incident.blackboard)

        if blackboard_copy is not None:
            await self.redis_bus.set_blackboard_cache(incident_id, blackboard_copy)
        for ev in pending_events:
            await self.redis_bus.publish_trace_event(incident_id, _serialize_trace(ev))

        # XACK only after PostgreSQL 16 commit and downstream trace publish succeed.
        await self.redis_bus.ack(self.stream_key, self.group_name, entry_id)

    async def _process_entries(self, entries: list[tuple[str, dict[str, Any]]]) -> int:
        if not entries:
            return 0
        noop_ids: list[str] = []
        for entry_id, payload in entries:
            if payload.get("event_type") != "HIGH_RISK_EXECUTED":
                noop_ids.append(entry_id)
                continue
            await self._handle_entry(entry_id, payload)
        if noop_ids:
            await self.redis_bus.ack(self.stream_key, self.group_name, *noop_ids)
        return len(entries)

    async def process_batch(self, *, block_ms: int | None = 250, count: int = 20) -> int:
        await self.initialize()
        entries = await self.redis_bus.read_group(
            self.stream_key,
            self.group_name,
            self.consumer_name,
            count=count,
            block_ms=block_ms,
        )
        return await self._process_entries(entries)

    async def reclaim_and_process_pending(
        self, *, min_idle_ms: int = 5000, count: int = 20
    ) -> int:
        await self.initialize()
        entries = await self.redis_bus.autoclaim_pending(
            self.stream_key,
            self.group_name,
            self.consumer_name,
            min_idle_ms=min_idle_ms,
            count=count,
        )
        return await self._process_entries(entries)

    async def drain_until_empty(self, max_batches: int = 20) -> int:
        total = 0
        for _ in range(max_batches):
            count = await self.process_batch(block_ms=None, count=50)
            total += count
            if count == 0:
                break
        return total

    async def _loop(self) -> None:
        await self.initialize()
        last_reclaim = time.monotonic()
        while not self._stop_event.is_set():
            try:
                await self.process_batch(block_ms=100, count=50)
                now = time.monotonic()
                if now - last_reclaim >= 5.0:
                    await self.reclaim_and_process_pending(min_idle_ms=5000, count=20)
                    last_reclaim = now
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unhandled error in TrajectoryStreamConsumer loop")
                await asyncio.sleep(0.25)

    def start(self) -> None:
        if self._task is None:
            self._stop_event.clear()
            self._task = asyncio.create_task(
                self._loop(), name=f"trajectory-stream-consumer-{self.consumer_name}"
            )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
