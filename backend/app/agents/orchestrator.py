import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from redis.exceptions import RedisError

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.attributes import flag_modified

from app.agents.llm import LLMProvider, MalformedLLMOutputError
from app.agents.tools import (
    ApprovalRequiredError,
    PolicyGuard,
    PolicyViolationError,
    ToolRegistry,
)
from app.db.models import (
    AgentTask,
    Incident,
    RemediationApproval,
    TraceEvent,
)
from app.agents.reliability import OutboxRelay
from app.db.repositories import (
    DEFAULT_LEASE_SECONDS,
    append_trace_event,
    claim_next_queued_task,
    get_approval_for_update,
    get_incident_full,
    renew_task_lease,
    serialize_trace_event,
)
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry

if TYPE_CHECKING:
    from app.agents.trajectory_consumer import TrajectoryStreamConsumer

logger = logging.getLogger(__name__)

TERMINAL_INCIDENT_STATUSES = {"RESOLVED", "REJECTED", "FAILED"}


class IncidentNotFoundError(LookupError):
    pass


class ConflictStateError(RuntimeError):
    pass


class FailClosedBlockedError(PermissionError):
    def __init__(self, message: str, trace_event: TraceEvent) -> None:
        super().__init__(message)
        self.trace_event = trace_event


__all__ = [
    "AIOpsOrchestrator",
    "AgentWorker",
    "ConflictStateError",
    "FailClosedBlockedError",
    "IncidentNotFoundError",
    "TERMINAL_INCIDENT_STATUSES",
    "serialize_trace_event",
]


class AIOpsOrchestrator:
    """Coordinates multi-agent troubleshooting, Fail-Closed approval gate, and Trajectory archival."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        redis_bus: RedisEventBus,
        skill_registry: SkillRegistry,
        tool_registry: ToolRegistry,
        llm_provider: LLMProvider,
        step_delay_ms: int = 0,
        trajectory_consumer: "TrajectoryStreamConsumer | None" = None,
        outbox_relay: OutboxRelay | None = None,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> None:
        self.session_factory = session_factory
        self.redis_bus = redis_bus
        self.skill_registry = skill_registry
        self.tool_registry = tool_registry
        self.policy_guard = PolicyGuard(tool_registry)
        self.llm = llm_provider
        self.step_delay_ms = step_delay_ms
        self.trajectory_consumer = trajectory_consumer
        self.outbox_relay = outbox_relay or OutboxRelay(
            session_factory=session_factory, redis_bus=redis_bus
        )
        self.lease_seconds = lease_seconds
        self._worker_wakeups: set[asyncio.Event] = set()

    def register_worker_wakeup(self, ev: asyncio.Event) -> None:
        self._worker_wakeups.add(ev)

    def unregister_worker_wakeup(self, ev: asyncio.Event) -> None:
        self._worker_wakeups.discard(ev)

    def notify_worker(self) -> None:
        for ev in tuple(self._worker_wakeups):
            ev.set()

    async def _publish_pending(
        self, incident_id: UUID, events: list[TraceEvent], blackboard: dict[str, Any] | None = None
    ) -> None:
        """Trace events were already written to outbox_events inside the business transaction.

        No bare XADD here: we only refresh the (non-authoritative) blackboard cache and wake
        the OutboxRelay, so a Redis outage can never lose an event or break the business path.
        """
        if blackboard is not None:
            try:
                await self.redis_bus.set_blackboard_cache(incident_id, blackboard)
            except RedisError:
                logger.warning("Blackboard cache refresh skipped (Redis unavailable)", exc_info=True)
        if events:
            self.outbox_relay.notify()

    async def inject_incident(
        self,
        *,
        fault_type: str,
        title: str | None = None,
        severity: str | None = None,
        service_name: str | None = None,
        alert_payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> Incident:
        skill = self.skill_registry.match_for_fault(fault_type)
        resolved_severity = severity or (skill.default_severity if skill else "P1")
        resolved_service = service_name or (skill.default_service if skill else "unknown-service")
        resolved_title = title or (
            f"[{resolved_severity}] {skill.title} on {resolved_service}"
            if skill
            else f"[{resolved_severity}] {fault_type} on {resolved_service}"
        )
        payload = dict(alert_payload or {})
        payload.setdefault("fault_type", fault_type)
        payload.setdefault("service_name", resolved_service)

        pending_events: list[TraceEvent] = []
        async with self.session_factory() as session:
            async with session.begin():
                if idempotency_key:
                    existing = (
                        await session.execute(
                            select(Incident).where(Incident.idempotency_key == idempotency_key)
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        return existing

                incident = Incident(
                    idempotency_key=idempotency_key,
                    title=resolved_title,
                    severity=resolved_severity,
                    service_name=resolved_service,
                    fault_type=fault_type,
                    status="OPEN",
                    alert_payload=payload,
                    blackboard={"stage": "OPEN", "evidence": {}},
                )
                session.add(incident)
                await session.flush()

                pending_events.append(
                    await append_trace_event(
                        session,
                        incident_id=incident.id,
                        agent_role="SYSTEM",
                        event_type="ALERT_INJECTED",
                        risk_tier="NONE",
                        summary=f"Fault alert injected: {resolved_title}",
                        payload={"fault_type": fault_type, "service_name": resolved_service},
                    )
                )

                coord_task = AgentTask(
                    incident_id=incident.id,
                    agent_role="COORDINATOR",
                    step_order=10,
                    action_type="ORCHESTRATE",
                    status="QUEUED",
                    input_payload={"fault_type": fault_type},
                )
                session.add(coord_task)
                await session.flush()
                incident_id = incident.id
                blackboard_copy = dict(incident.blackboard)

        await self._publish_pending(incident_id, pending_events, blackboard_copy)
        self.notify_worker()
        return incident

    async def process_one_task(self, worker_id: str = "worker-1") -> bool:
        """Claim one QUEUED task (FOR UPDATE SKIP LOCKED + lease) and execute it."""
        async with self.session_factory() as claim_session:
            async with claim_session.begin():
                task = await claim_next_queued_task(
                    claim_session, worker_id, lease_seconds=self.lease_seconds
                )
                if task is None:
                    return False
                task_id = task.id

        await self.execute_claimed_task(task_id, worker_id)
        return True

    async def _lease_heartbeat(self, task_id: UUID, worker_id: str) -> None:
        interval = max(0.05, self.lease_seconds / 3.0)
        while True:
            await asyncio.sleep(interval)
            try:
                async with self.session_factory() as hb_session:
                    async with hb_session.begin():
                        await renew_task_lease(
                            hb_session,
                            task_id=task_id,
                            worker_id=worker_id,
                            lease_seconds=self.lease_seconds,
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Lease heartbeat failed for task %s (worker=%s)", task_id, worker_id, exc_info=True
                )

    async def execute_claimed_task(self, task_id: UUID, worker_id: str) -> bool:
        """Execute a claimed task while renewing its lease.

        Returns False when the lease was lost (fencing): a zombie worker whose task was
        reclaimed by the watchdog can never commit duplicate side effects.
        """
        heartbeat = asyncio.create_task(
            self._lease_heartbeat(task_id, worker_id), name=f"lease-heartbeat-{task_id}"
        )
        try:
            return await self._execute_claimed_task(task_id, worker_id)
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def _execute_claimed_task(self, task_id: UUID, worker_id: str) -> bool:
        if self.step_delay_ms > 0:
            await asyncio.sleep(self.step_delay_ms / 1000.0)

        pending_events: list[TraceEvent] = []
        blackboard_snapshot: dict[str, Any] | None = None
        enqueued_more = False

        async with self.session_factory() as session:
            async with session.begin():
                task = (
                    await session.execute(
                        select(AgentTask).where(AgentTask.id == task_id).with_for_update()
                    )
                ).scalar_one_or_none()
                # Fencing token check: only the current lease owner may commit side effects.
                if task is None or task.status != "RUNNING" or task.lease_owner != worker_id:
                    logger.warning(
                        "Fencing: worker %s no longer owns task %s (status=%s, owner=%s); aborting.",
                        worker_id,
                        task_id,
                        task.status if task is not None else None,
                        task.lease_owner if task is not None else None,
                    )
                    return False
                incident_id = task.incident_id
                agent_role = task.agent_role
                incident = (
                    await session.execute(
                        select(Incident).where(Incident.id == incident_id).with_for_update()
                    )
                ).scalar_one()

                try:
                    if agent_role == "COORDINATOR":
                        enqueued_more = await self._handle_coordinator(
                            session, incident, task, pending_events
                        )
                    elif agent_role in {"LOG_DIAGNOSTICS", "DB_DIAGNOSTICS"}:
                        enqueued_more = await self._handle_diagnostic_agent(
                            session, incident, task, pending_events
                        )
                    elif agent_role == "REMEDIATION":
                        await self._handle_remediation_agent(
                            session, incident, task, pending_events
                        )
                    elif agent_role == "EXECUTOR":
                        approval = await get_approval_for_update(session, incident.id)
                        await self._execute_approved_remediation(
                            session, incident, task, approval, pending_events
                        )
                    else:
                        raise RuntimeError(f"Unsupported agent_role: {agent_role}")

                    blackboard_snapshot = dict(incident.blackboard or {})
                except (PolicyViolationError, ApprovalRequiredError, MalformedLLMOutputError) as exc:
                    now = datetime.now(timezone.utc)
                    task.status = "FAILED"
                    task.error_message = str(exc)
                    task.completed_at = now
                    incident.status = "FAILED"
                    incident.version += 1

                    ev_type = (
                        "POLICY_VIOLATION_BLOCKED"
                        if isinstance(exc, (PolicyViolationError, ApprovalRequiredError))
                        else "INCIDENT_FAILED"
                    )
                    risk_tier = (
                        "HIGH_RISK_WRITE"
                        if isinstance(exc, (PolicyViolationError, ApprovalRequiredError))
                        else "NONE"
                    )
                    pending_events.append(
                        await append_trace_event(
                            session,
                            incident_id=incident.id,
                            agent_role=agent_role,
                            event_type=ev_type,
                            risk_tier=risk_tier,
                            summary=str(exc),
                            payload={"task_id": str(task.id), "error": str(exc)},
                        )
                    )
                    if ev_type != "INCIDENT_FAILED":
                        pending_events.append(
                            await append_trace_event(
                                session,
                                incident_id=incident.id,
                                agent_role="SYSTEM",
                                event_type="INCIDENT_FAILED",
                                risk_tier=risk_tier,
                                summary=f"Incident terminated Fail-Closed due to {ev_type}",
                                payload={"reason": str(exc)},
                            )
                        )
                    blackboard_snapshot = dict(incident.blackboard or {})

                if task.status != "RUNNING":
                    task.lease_expires_at = None

        await self._publish_pending(incident_id, pending_events, blackboard_snapshot)
        if enqueued_more:
            self.notify_worker()
        return True

    async def _handle_coordinator(
        self,
        session: AsyncSession,
        incident: Incident,
        task: AgentTask,
        pending_events: list[TraceEvent],
    ) -> bool:
        incident.status = "TRIAGING"
        skill = self.skill_registry.match_for_fault(incident.fault_type)
        if skill is None:
            raise MalformedLLMOutputError(
                f"No declarative Runbook Skill matched fault_type={incident.fault_type!r}"
            )

        plan = await self.llm.plan_triage(
            fault_type=incident.fault_type,
            service_name=incident.service_name,
            alert_payload=incident.alert_payload or {},
            skill=skill,
        )

        bb = dict(incident.blackboard or {})
        bb["stage"] = "DIAGNOSING"
        bb["matched_skill"] = {
            "skill_id": skill.skill_id,
            "title": skill.title,
            "risk_level": skill.remediation.risk_level,
        }
        bb.setdefault("evidence", {})
        incident.blackboard = bb
        flag_modified(incident, "blackboard")
        incident.status = "DIAGNOSING"
        incident.version += 1

        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role="COORDINATOR",
                event_type="SKILL_MATCHED",
                risk_tier="NONE",
                summary=f"Matched declarative Runbook Skill {skill.skill_id}: {skill.title}",
                payload={"skill_id": skill.skill_id, "fault_type": incident.fault_type},
            )
        )
        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role="COORDINATOR",
                event_type="PLAN_CREATED",
                risk_tier="NONE",
                summary=plan.summary,
                payload={"fanout": [plan.log_tool, plan.db_tool]},
            )
        )

        session.add_all(
            [
                AgentTask(
                    incident_id=incident.id,
                    agent_role="LOG_DIAGNOSTICS",
                    step_order=20,
                    action_type="READ_ONLY_DIAGNOSE",
                    status="QUEUED",
                    input_payload={"skill_id": skill.skill_id, "tool_hint": plan.log_tool},
                ),
                AgentTask(
                    incident_id=incident.id,
                    agent_role="DB_DIAGNOSTICS",
                    step_order=20,
                    action_type="READ_ONLY_DIAGNOSE",
                    status="QUEUED",
                    input_payload={"skill_id": skill.skill_id, "tool_hint": plan.db_tool},
                ),
            ]
        )

        task.status = "COMPLETED"
        task.output_payload = {"matched_skill": skill.skill_id, "summary": plan.summary}
        task.completed_at = datetime.now(timezone.utc)
        await session.flush()
        return True

    async def _handle_diagnostic_agent(
        self,
        session: AsyncSession,
        incident: Incident,
        task: AgentTask,
        pending_events: list[TraceEvent],
    ) -> bool:
        skill = self.skill_registry.match_for_fault(incident.fault_type)
        if skill is None:
            raise MalformedLLMOutputError(f"Missing skill for {incident.fault_type}")

        plan = await self.llm.plan_diagnostic_step(
            agent_role=task.agent_role,
            alert_payload=incident.alert_payload or {},
            skill=skill,
        )

        spec, tool_output = self.policy_guard.execute_authorized(
            agent_role=task.agent_role,
            tool_name=plan.tool_name,
            incident=incident,
            context={"service_name": incident.service_name, "alert_payload": incident.alert_payload},
            approval=None,
        )

        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role=task.agent_role,
                event_type="TOOL_CALL_AUTO_ALLOWED",
                risk_tier=spec.permission.value,
                summary=f"Auto-allowed READ_ONLY tool `{spec.name}` for {task.agent_role}",
                payload={"tool_name": spec.name, "permission": spec.permission.value},
            )
        )

        bb = dict(incident.blackboard or {})
        evidence = dict(bb.get("evidence") or {})
        evidence_key = "log_diagnostics" if task.agent_role == "LOG_DIAGNOSTICS" else "db_diagnostics"
        evidence[evidence_key] = tool_output
        bb["evidence"] = evidence
        incident.blackboard = bb
        flag_modified(incident, "blackboard")
        incident.version += 1

        task.status = "COMPLETED"
        task.output_payload = {"tool_name": spec.name, "result": tool_output}
        task.completed_at = datetime.now(timezone.utc)
        await session.flush()

        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role=task.agent_role,
                event_type="TOOL_RESULT",
                risk_tier=spec.permission.value,
                summary=f"{task.agent_role} collected telemetry from {tool_output.get('source', spec.name)}",
                payload=tool_output,
            )
        )

        diag_rows = (
            await session.execute(
                select(AgentTask).where(
                    AgentTask.incident_id == incident.id,
                    AgentTask.agent_role.in_(["LOG_DIAGNOSTICS", "DB_DIAGNOSTICS"]),
                )
            )
        ).scalars().all()
        all_diag_done = len(diag_rows) == 2 and all(r.status == "COMPLETED" for r in diag_rows)
        if not all_diag_done:
            return False

        existing_rem = (
            await session.execute(
                select(AgentTask.id).where(
                    AgentTask.incident_id == incident.id,
                    AgentTask.agent_role == "REMEDIATION",
                )
            )
        ).scalar_one_or_none()
        if existing_rem is not None:
            return False

        session.add(
            AgentTask(
                incident_id=incident.id,
                agent_role="REMEDIATION",
                step_order=30,
                action_type="PROPOSE_REMEDIATION",
                status="QUEUED",
                input_payload={"skill_id": skill.skill_id},
            )
        )
        await session.flush()
        return True

    async def _handle_remediation_agent(
        self,
        session: AsyncSession,
        incident: Incident,
        task: AgentTask,
        pending_events: list[TraceEvent],
    ) -> None:
        skill = self.skill_registry.match_for_fault(incident.fault_type)
        if skill is None:
            raise MalformedLLMOutputError(f"Missing skill for {incident.fault_type}")

        evidence = dict((incident.blackboard or {}).get("evidence") or {})
        proposal = await self.llm.propose_remediation(
            fault_type=incident.fault_type,
            service_name=incident.service_name,
            blackboard=evidence,
            alert_payload=incident.alert_payload or {},
            skill=skill,
        )

        incident.root_cause_summary = proposal.root_cause_summary
        bb = dict(incident.blackboard or {})
        bb["stage"] = "PENDING_APPROVAL"
        bb["remediation_proposal"] = {
            "tool_name": proposal.tool_name,
            "proposed_command": proposal.proposed_command,
            "blast_radius": proposal.blast_radius,
            "rollback_command": proposal.rollback_command,
            "rationale": proposal.rationale,
        }
        incident.blackboard = bb
        flag_modified(incident, "blackboard")
        incident.status = "PENDING_APPROVAL"
        incident.version += 1

        now = datetime.now(timezone.utc)
        task.status = "COMPLETED"
        task.output_payload = bb["remediation_proposal"]
        task.completed_at = now

        executor_task = AgentTask(
            incident_id=incident.id,
            agent_role="EXECUTOR",
            step_order=40,
            action_type="HIGH_RISK_WRITE",
            status="BLOCKED_APPROVAL",
            input_payload={"tool_name": proposal.tool_name, "skill_id": skill.skill_id},
        )
        session.add(executor_task)
        await session.flush()

        approval = RemediationApproval(
            incident_id=incident.id,
            task_id=executor_task.id,
            skill_id=skill.skill_id,
            risk_level=skill.remediation.risk_level,
            proposed_command=proposal.proposed_command,
            action_plan={
                "tool_name": proposal.tool_name,
                "target_resource": incident.service_name,
                "blast_radius": proposal.blast_radius,
                "rollback_command": proposal.rollback_command,
                "rationale": proposal.rationale,
            },
            status="PENDING",
        )
        session.add(approval)
        await session.flush()

        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role="REMEDIATION",
                event_type="REMEDIATION_PROPOSED",
                risk_tier="HIGH_RISK_WRITE",
                summary=f"Proposed high-risk write action `{proposal.tool_name}`",
                payload=bb["remediation_proposal"],
            )
        )
        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role="POLICY_GATE",
                event_type="APPROVAL_REQUIRED",
                risk_tier="HIGH_RISK_WRITE",
                summary=(
                    f"Execution suspended in PENDING_APPROVAL: human sign-off required "
                    f"for `{proposal.tool_name}` (risk={skill.remediation.risk_level})."
                ),
                payload={"approval_id": str(approval.id), "risk_level": skill.remediation.risk_level},
            )
        )

    async def _execute_approved_remediation(
        self,
        session: AsyncSession,
        incident: Incident,
        task: AgentTask,
        approval: RemediationApproval | None,
        pending_events: list[TraceEvent],
    ) -> None:
        tool_name = (task.input_payload or {}).get("tool_name") or (
            (approval.action_plan or {}).get("tool_name") if approval else ""
        )
        spec, exec_result = self.policy_guard.execute_authorized(
            agent_role="EXECUTOR",
            tool_name=tool_name,
            incident=incident,
            context={"service_name": incident.service_name, "alert_payload": incident.alert_payload},
            approval=approval,
        )

        now = datetime.now(timezone.utc)
        assert approval is not None
        approval.status = "EXECUTED"
        approval.executed_at = now
        approval.execution_result = exec_result

        task.status = "COMPLETED"
        task.output_payload = exec_result
        task.completed_at = now

        pending_events.append(
            await append_trace_event(
                session,
                incident_id=incident.id,
                agent_role="EXECUTOR",
                event_type="HIGH_RISK_EXECUTED",
                risk_tier=spec.permission.value,
                summary=f"Executed approved high-risk remediation `{spec.name}`",
                payload=exec_result,
            )
        )

        # Trajectory archival & RESOLVED transition are consumed downstream via Redis Streams (Consumer Group + XACK).
        bb = dict(incident.blackboard or {})
        bb["stage"] = "AWAITING_TRAJECTORY_ARCHIVE"
        bb["execution_result"] = exec_result
        incident.blackboard = bb
        flag_modified(incident, "blackboard")
        incident.version += 1

    async def approve_remediation(
        self,
        incident_id: UUID,
        *,
        approved_by: str,
        comment: str | None = None,
    ) -> Incident:
        pending_events: list[TraceEvent] = []
        async with self.session_factory() as session:
            async with session.begin():
                incident = (
                    await session.execute(
                        select(Incident).where(Incident.id == incident_id).with_for_update()
                    )
                ).scalar_one_or_none()
                if incident is None:
                    raise IncidentNotFoundError(f"Incident {incident_id} not found")

                approval = await get_approval_for_update(session, incident_id)
                if (
                    incident.status != "PENDING_APPROVAL"
                    or approval is None
                    or approval.status != "PENDING"
                ):
                    raise ConflictStateError(
                        f"Cannot approve incident in status={incident.status}, "
                        f"approval_status={approval.status if approval else 'NONE'}"
                    )

                now = datetime.now(timezone.utc)
                approval.status = "APPROVED"
                approval.approved_by = approved_by
                approval.approval_comment = comment
                approval.decided_at = now

                incident.status = "EXECUTING_REMEDIATION"
                bb = dict(incident.blackboard or {})
                bb["stage"] = "EXECUTING_REMEDIATION"
                incident.blackboard = bb
                flag_modified(incident, "blackboard")
                incident.version += 1

                exec_task = (
                    await session.execute(
                        select(AgentTask)
                        .where(AgentTask.id == approval.task_id)
                        .with_for_update()
                    )
                ).scalar_one()
                exec_task.status = "QUEUED"

                pending_events.append(
                    await append_trace_event(
                        session,
                        incident_id=incident.id,
                        agent_role="HUMAN_OPERATOR",
                        event_type="APPROVAL_GRANTED",
                        risk_tier="HIGH_RISK_WRITE",
                        summary=f"Operator {approved_by} approved high-risk remediation execution.",
                        payload={
                            "approved_by": approved_by,
                            "comment": comment or "",
                            "approval_id": str(approval.id),
                        },
                    )
                )
                blackboard_copy = dict(incident.blackboard)

        await self._publish_pending(incident_id, pending_events, blackboard_copy)
        self.notify_worker()
        async with self.session_factory() as read_sess:
            refreshed = await get_incident_full(read_sess, incident_id)
            assert refreshed is not None
            return refreshed

    async def reject_remediation(
        self,
        incident_id: UUID,
        *,
        rejected_by: str,
        comment: str | None = None,
    ) -> Incident:
        pending_events: list[TraceEvent] = []
        async with self.session_factory() as session:
            async with session.begin():
                incident = (
                    await session.execute(
                        select(Incident).where(Incident.id == incident_id).with_for_update()
                    )
                ).scalar_one_or_none()
                if incident is None:
                    raise IncidentNotFoundError(f"Incident {incident_id} not found")

                approval = await get_approval_for_update(session, incident_id)
                if (
                    incident.status != "PENDING_APPROVAL"
                    or approval is None
                    or approval.status != "PENDING"
                ):
                    raise ConflictStateError(
                        f"Cannot reject incident in status={incident.status}, "
                        f"approval_status={approval.status if approval else 'NONE'}"
                    )

                now = datetime.now(timezone.utc)
                approval.status = "REJECTED"
                approval.approved_by = rejected_by
                approval.approval_comment = comment
                approval.decided_at = now

                incident.status = "REJECTED"
                bb = dict(incident.blackboard or {})
                bb["stage"] = "REJECTED"
                incident.blackboard = bb
                flag_modified(incident, "blackboard")
                incident.version += 1

                exec_task = (
                    await session.execute(
                        select(AgentTask)
                        .where(AgentTask.id == approval.task_id)
                        .with_for_update()
                    )
                ).scalar_one()
                exec_task.status = "FAILED"
                exec_task.error_message = f"Rejected by {rejected_by}"
                exec_task.completed_at = now

                pending_events.append(
                    await append_trace_event(
                        session,
                        incident_id=incident.id,
                        agent_role="HUMAN_OPERATOR",
                        event_type="APPROVAL_REJECTED",
                        risk_tier="HIGH_RISK_WRITE",
                        summary=f"Operator {rejected_by} rejected remediation; high-risk write aborted.",
                        payload={"rejected_by": rejected_by, "comment": comment or ""},
                    )
                )
                blackboard_copy = dict(incident.blackboard)

        await self._publish_pending(incident_id, pending_events, blackboard_copy)
        async with self.session_factory() as read_sess:
            refreshed = await get_incident_full(read_sess, incident_id)
            assert refreshed is not None
            return refreshed

    async def execute_remediation_direct(self, incident_id: UUID) -> Incident:
        """Explicit execution entrypoint enforcing Fail-Closed human approval gate."""
        pending_events: list[TraceEvent] = []
        blocked_exc: ApprovalRequiredError | PolicyViolationError | None = None
        blocked_trace: TraceEvent | None = None
        blackboard_copy: dict[str, Any] | None = None

        async with self.session_factory() as session:
            async with session.begin():
                incident = (
                    await session.execute(
                        select(Incident).where(Incident.id == incident_id).with_for_update()
                    )
                ).scalar_one_or_none()
                if incident is None:
                    raise IncidentNotFoundError(f"Incident {incident_id} not found")

                approval = await get_approval_for_update(session, incident_id)
                tool_name = (
                    (approval.action_plan or {}).get("tool_name")
                    if approval is not None
                    else ""
                )

                try:
                    self.policy_guard.authorize_tool_call(
                        agent_role="EXECUTOR",
                        tool_name=tool_name,
                        incident=incident,
                        approval=approval,
                    )
                except (ApprovalRequiredError, PolicyViolationError) as exc:
                    blocked_exc = exc
                    blocked_trace = await append_trace_event(
                        session,
                        incident_id=incident.id,
                        agent_role="POLICY_GATE",
                        event_type="EXECUTION_BLOCKED_FAIL_CLOSED",
                        risk_tier="HIGH_RISK_WRITE",
                        summary=str(exc),
                        payload={
                            "incident_status": incident.status,
                            "approval_status": approval.status if approval else "NONE",
                            "code": "APPROVAL_REQUIRED_FAIL_CLOSED",
                        },
                    )
                    pending_events.append(blocked_trace)
                    blackboard_copy = dict(incident.blackboard or {})
                else:
                    assert approval is not None
                    exec_task = (
                        await session.execute(
                            select(AgentTask)
                            .where(AgentTask.id == approval.task_id)
                            .with_for_update()
                        )
                    ).scalar_one()
                    if exec_task.status != "COMPLETED":
                        await self._execute_approved_remediation(
                            session, incident, exec_task, approval, pending_events
                        )
                    blackboard_copy = dict(incident.blackboard or {})

        await self._publish_pending(incident_id, pending_events, blackboard_copy)
        if blocked_exc is not None:
            assert blocked_trace is not None
            raise FailClosedBlockedError(str(blocked_exc), blocked_trace)

        await self.outbox_relay.drain()
        if self.trajectory_consumer is not None:
            await self.trajectory_consumer.drain_until_empty()
            await self.outbox_relay.drain()

        async with self.session_factory() as read_sess:
            refreshed = await get_incident_full(read_sess, incident_id)
            assert refreshed is not None
            return refreshed


class AgentWorker:
    """Async background task runner consuming PG16 FOR UPDATE SKIP LOCKED queue."""

    def __init__(
        self,
        orchestrator: AIOpsOrchestrator,
        worker_id: str = "worker-main",
        trajectory_consumer: "TrajectoryStreamConsumer | None" = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.worker_id = worker_id
        self.trajectory_consumer = trajectory_consumer
        self._stop_event = asyncio.Event()
        self._wakeup_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def run_until_quiescent(self, max_steps: int = 25) -> int:
        """Deterministically drive tasks -> outbox relay -> stream consumer until idle.

        Returns the number of PG16 agent tasks executed.
        """
        processed = 0
        consumer_rounds = 0
        relay = self.orchestrator.outbox_relay
        while processed < max_steps and consumer_rounds < max_steps:
            if await self.orchestrator.process_one_task(self.worker_id):
                processed += 1
                continue
            await relay.drain()
            if self.trajectory_consumer is not None:
                drained = await self.trajectory_consumer.drain_until_empty()
                if drained > 0:
                    consumer_rounds += 1
                    continue
            break
        await relay.drain()
        return processed

    async def _loop(self) -> None:
        self.orchestrator.register_worker_wakeup(self._wakeup_event)
        try:
            while not self._stop_event.is_set():
                self._wakeup_event.clear()
                try:
                    did_work = await self.orchestrator.process_one_task(self.worker_id)
                    if did_work:
                        continue
                except Exception:
                    logger.exception("Unhandled error in AgentWorker loop")

                if self._stop_event.is_set():
                    break
                try:
                    await asyncio.wait_for(self._wakeup_event.wait(), timeout=0.2)
                except TimeoutError:
                    pass
        finally:
            self.orchestrator.unregister_worker_wakeup(self._wakeup_event)

    def start(self) -> None:
        if self._task is None:
            self._stop_event.clear()
            self._task = asyncio.create_task(self._loop(), name=f"agent-worker-{self.worker_id}")

    async def stop(self, timeout: float = 10.0) -> bool:
        """Graceful drain: stop claiming new tasks and wait (bounded) for the in-flight task.

        Returns True if the in-flight work finished within the timeout. On timeout the task is
        cancelled; its lease then expires and the LeaseWatchdog reclaims it (no work is lost).
        """
        self._stop_event.set()
        self._wakeup_event.set()
        self.orchestrator.notify_worker()
        drained = True
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=timeout)
            except TimeoutError:
                drained = False
                self._task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._task
            self._task = None
        return drained
