import hashlib
import json
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import select, text

from app.agents.orchestrator import (
    TERMINAL_INCIDENT_STATUSES,
    AIOpsOrchestrator,
    ConflictStateError,
    FailClosedBlockedError,
    IncidentNotFoundError,
    serialize_trace_event,
)
from app.api.schemas import (
    ActorOut,
    AlertmanagerItemResult,
    AlertmanagerWebhookPayload,
    AlertmanagerWebhookResponse,
    ApprovalDecisionRequest,
    HealthResponse,
    IncidentDetailOut,
    IncidentSummaryOut,
    InjectIncidentRequest,
    SkillOut,
    TraceEventOut,
    TrajectoryOut,
)
from app.auth.rbac import Actor, require_authenticated, require_sre_approver
from app.db.models import Incident
from app.db.repositories import (
    get_incident_full,
    get_trajectory_by_incident,
    list_incidents,
    list_trace_events,
    list_trajectories,
)
from app.redis_bus.events import incident_events_stream

router = APIRouter(prefix="/api/v1", tags=["aiops"])


def _get_orchestrator(request: Request) -> AIOpsOrchestrator:
    return request.app.state.orchestrator


@router.get("/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    session_factory = request.app.state.session_factory
    redis_bus = request.app.state.redis_bus
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))
    redis_ok = await redis_bus.ping()
    if not redis_ok:
        raise HTTPException(status_code=503, detail="Redis ping failed")
    return HealthResponse(status="ok", postgres="ok", redis="ok")


@router.get("/auth/me", response_model=ActorOut)
async def get_current_actor(actor: Actor = Depends(require_authenticated)) -> ActorOut:
    return ActorOut(actor_id=actor.actor_id, role=actor.role.value)


@router.get("/skills", response_model=list[SkillOut])
async def get_skills(request: Request) -> list[SkillOut]:
    skills = request.app.state.skill_registry.list_skills()
    return [
        SkillOut(
            skill_id=s.skill_id,
            fault_type=s.fault_type,
            title=s.title,
            description=s.description.strip(),
            default_severity=s.default_severity,
            default_service=s.default_service,
            diagnostic_tools={
                "log_agent": s.diagnostic_tools.log_agent,
                "db_agent": s.diagnostic_tools.db_agent,
            },
            remediation_tool=s.remediation.tool_name,
            risk_level=s.remediation.risk_level,
        )
        for s in skills
    ]


@router.post("/incidents/inject", response_model=IncidentSummaryOut, status_code=status.HTTP_201_CREATED)
async def inject_incident(body: InjectIncidentRequest, request: Request) -> IncidentSummaryOut:
    orch = _get_orchestrator(request)
    incident = await orch.inject_incident(
        fault_type=body.fault_type,
        title=body.title,
        severity=body.severity,
        service_name=body.service_name,
        alert_payload=body.alert_payload,
        idempotency_key=body.idempotency_key,
    )
    return IncidentSummaryOut.model_validate(incident)


def _infer_fault_type(labels: dict[str, str]) -> str:
    explicit = labels.get("fault_type", "").strip()
    if explicit:
        return explicit
    alertname = labels.get("alertname", "").lower()
    if "redis" in alertname or "cache" in alertname or "stampede" in alertname:
        return "redis_cache_stampede"
    if "lock" in alertname or "slow" in alertname:
        return "slow_sql_lock_contention"
    return "db_connection_pool_exhausted"


def _normalize_severity(raw: str | None) -> str | None:
    if not raw:
        return None
    upper = raw.strip().upper()
    if upper in {"P0", "P1", "P2", "P3"}:
        return upper
    mapping = {"CRITICAL": "P0", "HIGH": "P0", "WARNING": "P1", "MEDIUM": "P2", "INFO": "P3"}
    return mapping.get(upper, "P1")


@router.post("/webhooks/alertmanager", response_model=AlertmanagerWebhookResponse)
async def alertmanager_webhook(
    body: AlertmanagerWebhookPayload, request: Request
) -> AlertmanagerWebhookResponse:
    orch = _get_orchestrator(request)
    results: list[AlertmanagerItemResult] = []
    for alert in body.alerts:
        merged_labels = {**body.commonLabels, **alert.labels}
        merged_annotations = {**body.commonAnnotations, **alert.annotations}
        fingerprint = (alert.fingerprint or "").strip()
        if not fingerprint:
            canonical = json.dumps(merged_labels, sort_keys=True, ensure_ascii=False)
            fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]

        if alert.status.strip().lower() == "resolved":
            results.append(
                AlertmanagerItemResult(
                    fingerprint=fingerprint,
                    incident_id=None,
                    action="ignored_resolved",
                    dedup_count=0,
                    incident_status=None,
                )
            )
            continue

        fault_type = _infer_fault_type(merged_labels)
        severity = _normalize_severity(merged_labels.get("severity"))
        service_name = merged_labels.get("service") or merged_labels.get("instance")
        title = merged_annotations.get("summary") or merged_labels.get("alertname")

        incident, action = await orch.ingest_alertmanager_alert(
            fingerprint=fingerprint,
            fault_type=fault_type,
            title=title,
            severity=severity,
            service_name=service_name,
            alert_payload={
                "source": "alertmanager",
                "receiver": body.receiver,
                "labels": merged_labels,
                "annotations": merged_annotations,
                "startsAt": alert.startsAt,
                "generatorURL": alert.generatorURL,
            },
        )
        results.append(
            AlertmanagerItemResult(
                fingerprint=fingerprint,
                incident_id=incident.id,
                action=action,
                dedup_count=incident.dedup_count,
                incident_status=incident.status,
            )
        )

    return AlertmanagerWebhookResponse(status="ok", processed=len(results), results=results)


@router.get("/incidents", response_model=list[IncidentSummaryOut])
async def get_incidents(
    request: Request,
    fault_type: str | None = Query(default=None),
    blackboard_contains: str | None = Query(default=None),
) -> list[IncidentSummaryOut]:
    parsed_bb: dict[str, Any] | None = None
    if blackboard_contains:
        try:
            parsed_bb = json.loads(blackboard_contains)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid blackboard_contains JSON: {exc}") from exc

    async with request.app.state.session_factory() as session:
        rows = await list_incidents(
            session, fault_type=fault_type, blackboard_contains=parsed_bb
        )
    return [IncidentSummaryOut.model_validate(r) for r in rows]


@router.get("/incidents/{incident_id}", response_model=IncidentDetailOut)
async def get_incident_detail(incident_id: UUID, request: Request) -> IncidentDetailOut:
    async with request.app.state.session_factory() as session:
        incident = await get_incident_full(session, incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="Incident not found")
    return IncidentDetailOut.model_validate(incident)


@router.get("/incidents/{incident_id}/traces", response_model=list[TraceEventOut])
async def get_incident_traces(
    incident_id: UUID,
    request: Request,
    after_id: int = Query(default=0, ge=0),
) -> list[TraceEventOut]:
    async with request.app.state.session_factory() as session:
        exists = (
            await session.execute(select(Incident.id).where(Incident.id == incident_id))
        ).scalar_one_or_none()
        if exists is None:
            raise HTTPException(status_code=404, detail="Incident not found")
        traces = await list_trace_events(session, incident_id, after_id=after_id)
    return [TraceEventOut.model_validate(t) for t in traces]


@router.get("/incidents/{incident_id}/events")
async def stream_incident_events(
    incident_id: UUID,
    request: Request,
    after_id: int = Query(default=0, ge=0),
) -> StreamingResponse:
    session_factory = request.app.state.session_factory
    redis_bus = request.app.state.redis_bus

    async with session_factory() as session:
        exists = (
            await session.execute(select(Incident.id).where(Incident.id == incident_id))
        ).scalar_one_or_none()
        if exists is None:
            raise HTTPException(status_code=404, detail="Incident not found")

    header_last_id = request.headers.get("last-event-id")
    parsed_header_id = int(header_last_id) if header_last_id and header_last_id.isdigit() else 0
    resume_after_id = max(after_id, parsed_header_id)

    async def event_generator():
        stream_key = incident_events_stream(incident_id)
        group_name = f"cg:sse:{uuid4().hex}"
        consumer_name = "sse-1"
        # Create Consumer Group BEFORE reading PG16 history so zero stream entries are missed.
        await redis_bus.ensure_consumer_group(stream_key, group_name, start_id="0")
        last_sent_id = resume_after_id
        try:
            async with session_factory() as session:
                history = await list_trace_events(session, incident_id, after_id=resume_after_id)
                current_status = (
                    await session.execute(
                        select(Incident.status).where(Incident.id == incident_id)
                    )
                ).scalar_one()

            for ev in history:
                payload = serialize_trace_event(ev)
                last_sent_id = max(last_sent_id, ev.id)
                yield f"id: {ev.id}\nevent: trace\ndata: {json.dumps(payload)}\n\n"

            if current_status in TERMINAL_INCIDENT_STATUSES:
                # Drain any backlog in this ephemeral group cleanly before finishing.
                backlog = await redis_bus.read_group(
                    stream_key, group_name, consumer_name, count=100, block_ms=None
                )
                if backlog:
                    await redis_bus.ack(stream_key, group_name, *(eid for eid, _ in backlog))
                yield f"event: complete\ndata: {json.dumps({'status': current_status})}\n\n"
                return

            while True:
                if await request.is_disconnected():
                    return

                entries = await redis_bus.read_group(
                    stream_key, group_name, consumer_name, count=50, block_ms=1000
                )
                if not entries:
                    async with session_factory() as check_sess:
                        status_now = (
                            await check_sess.execute(
                                select(Incident.status).where(Incident.id == incident_id)
                            )
                        ).scalar_one()
                    if status_now in TERMINAL_INCIDENT_STATUSES:
                        flush_entries = await redis_bus.read_group(
                            stream_key, group_name, consumer_name, count=100, block_ms=None
                        )
                        for entry_id, msg in flush_entries:
                            ev_id = int(msg["id"])
                            if ev_id > last_sent_id:
                                last_sent_id = ev_id
                                yield f"id: {ev_id}\nevent: trace\ndata: {json.dumps(msg)}\n\n"
                            await redis_bus.ack(stream_key, group_name, entry_id)
                        yield f"event: complete\ndata: {json.dumps({'status': status_now})}\n\n"
                        return
                    yield ": keepalive\n\n"
                    continue

                for entry_id, msg in entries:
                    ev_id = int(msg["id"])
                    if ev_id > last_sent_id:
                        last_sent_id = ev_id
                        yield f"id: {ev_id}\nevent: trace\ndata: {json.dumps(msg)}\n\n"
                    await redis_bus.ack(stream_key, group_name, entry_id)

                    ev_type = msg.get("event_type")
                    if ev_type in {"INCIDENT_RESOLVED", "APPROVAL_REJECTED", "INCIDENT_FAILED"}:
                        terminal_map = {
                            "INCIDENT_RESOLVED": "RESOLVED",
                            "APPROVAL_REJECTED": "REJECTED",
                            "INCIDENT_FAILED": "FAILED",
                        }
                        yield (
                            f"event: complete\n"
                            f"data: {json.dumps({'status': terminal_map[ev_type]})}\n\n"
                        )
                        return
        finally:
            await redis_bus.destroy_consumer_group(stream_key, group_name)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/incidents/{incident_id}/approve", response_model=IncidentDetailOut)
async def approve_incident(
    incident_id: UUID,
    body: ApprovalDecisionRequest,
    request: Request,
    actor: Actor = Depends(require_sre_approver),
) -> IncidentDetailOut:
    orch = _get_orchestrator(request)
    try:
        incident = await orch.approve_remediation(
            incident_id,
            approved_by=actor.actor_id,
            approved_by_role=actor.role.value,
            comment=body.comment,
        )
    except IncidentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return IncidentDetailOut.model_validate(incident)


@router.post("/incidents/{incident_id}/reject", response_model=IncidentDetailOut)
async def reject_incident(
    incident_id: UUID,
    body: ApprovalDecisionRequest,
    request: Request,
    actor: Actor = Depends(require_sre_approver),
) -> IncidentDetailOut:
    orch = _get_orchestrator(request)
    try:
        incident = await orch.reject_remediation(
            incident_id,
            rejected_by=actor.actor_id,
            rejected_by_role=actor.role.value,
            comment=body.comment,
        )
    except IncidentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return IncidentDetailOut.model_validate(incident)


@router.post("/incidents/{incident_id}/execute", response_model=IncidentDetailOut)
async def execute_incident_remediation(
    incident_id: UUID,
    request: Request,
    actor: Actor = Depends(require_sre_approver),
) -> IncidentDetailOut:
    orch = _get_orchestrator(request)
    try:
        incident = await orch.execute_remediation_direct(
            incident_id,
            actor_id=actor.actor_id,
            actor_role=actor.role.value,
        )
    except IncidentNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FailClosedBlockedError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "APPROVAL_REQUIRED_FAIL_CLOSED",
                "message": str(exc),
                "trace_event_id": exc.trace_event.id,
            },
        ) from exc
    return IncidentDetailOut.model_validate(incident)


@router.get("/trajectories", response_model=list[TrajectoryOut])
async def get_trajectories(request: Request) -> list[TrajectoryOut]:
    async with request.app.state.session_factory() as session:
        rows = await list_trajectories(session)
    return [TrajectoryOut.model_validate(r) for r in rows]


@router.get("/trajectories/{incident_id}", response_model=TrajectoryOut)
async def get_trajectory(incident_id: UUID, request: Request) -> TrajectoryOut:
    async with request.app.state.session_factory() as session:
        row = await get_trajectory_by_incident(session, incident_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Trajectory not found")
    return TrajectoryOut.model_validate(row)
