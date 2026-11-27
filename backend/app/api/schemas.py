from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class HealthResponse(BaseModel):
    status: str
    postgres: str
    redis: str


class ActorOut(BaseModel):
    actor_id: str
    role: str


class SkillOut(BaseModel):
    skill_id: str
    fault_type: str
    title: str
    description: str
    default_severity: str
    default_service: str
    diagnostic_tools: dict[str, str]
    remediation_tool: str
    risk_level: str


class InjectIncidentRequest(BaseModel):
    fault_type: str = Field(default="db_connection_pool_exhausted", min_length=1, max_length=64)
    title: str | None = Field(default=None, max_length=255)
    severity: str | None = Field(default=None, pattern="^(P0|P1|P2|P3)$")
    service_name: str | None = Field(default=None, max_length=128)
    alert_payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=128)


class ApprovalDecisionRequest(BaseModel):
    operator: str = Field(default="sre-oncall", min_length=1, max_length=128)
    comment: str | None = Field(default=None, max_length=500)


class AgentTaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    agent_role: str
    step_order: int
    action_type: str
    status: str
    input_payload: dict[str, Any]
    output_payload: dict[str, Any]
    worker_id: str | None
    lease_owner: str | None
    lease_expires_at: datetime | None
    attempt_count: int
    max_attempts: int
    error_message: str | None
    created_at: datetime
    completed_at: datetime | None


class ApprovalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    task_id: UUID
    skill_id: str
    risk_level: str
    proposed_command: str
    action_plan: dict[str, Any]
    status: str
    approved_by: str | None
    approved_by_role: str | None
    approval_comment: str | None
    decided_at: datetime | None
    executed_at: datetime | None
    execution_result: dict[str, Any] | None
    created_at: datetime


class TraceEventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    incident_id: UUID
    agent_role: str
    event_type: str
    risk_tier: str
    summary: str
    payload: dict[str, Any]
    created_at: datetime


class TrajectoryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    skill_id: str
    fault_type: str
    root_cause: str
    resolution_command: str
    trajectory_doc: dict[str, Any]
    created_at: datetime


class IncidentSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    idempotency_key: str | None
    alert_fingerprint: str | None = None
    dedup_count: int = 1
    last_triggered_at: datetime | None = None
    title: str
    severity: str
    service_name: str
    fault_type: str
    status: str
    alert_payload: dict[str, Any]
    blackboard: dict[str, Any]
    root_cause_summary: str | None
    version: int
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None


class AlertmanagerAlertIn(BaseModel):
    status: str = "firing"
    fingerprint: str | None = Field(default=None, max_length=128)
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    startsAt: str | None = None
    endsAt: str | None = None
    generatorURL: str | None = None


class AlertmanagerWebhookPayload(BaseModel):
    version: str = "4"
    status: str = "firing"
    receiver: str | None = None
    groupLabels: dict[str, str] = Field(default_factory=dict)
    commonLabels: dict[str, str] = Field(default_factory=dict)
    commonAnnotations: dict[str, str] = Field(default_factory=dict)
    alerts: list[AlertmanagerAlertIn] = Field(default_factory=list)


class AlertmanagerItemResult(BaseModel):
    fingerprint: str
    incident_id: UUID | None = None
    action: str
    dedup_count: int = 0
    incident_status: str | None = None


class AlertmanagerWebhookResponse(BaseModel):
    status: str = "ok"
    processed: int
    results: list[AlertmanagerItemResult]


class A2APeerDiscoverRequest(BaseModel):
    base_url: str = Field(min_length=8, max_length=512)


class A2APeerDiscoverResponse(BaseModel):
    base_url: str
    card: dict[str, Any]


class A2APeerRegisterRequest(BaseModel):
    base_url: str = Field(min_length=8, max_length=512)
    enabled: bool = True


class A2APeerToggleRequest(BaseModel):
    enabled: bool


class A2APeerOut(BaseModel):
    id: UUID | None = None
    base_url: str
    status: str
    enabled: bool = True
    source: str = "db"
    card: dict[str, Any] | None = None
    error: str | None = None
    created_by: str | None = None
    created_at: datetime | None = None


class NLParseRequest(BaseModel):
    query: str = Field(min_length=2, max_length=2000)


class NLParseResponse(BaseModel):
    query: str
    fault_type: str
    service_name: str
    matched_runbook: str
    target_a2a_peer: str | None = None
    suggested_skill: str | None = None
    severity: str = Field(default="P1", pattern="^(P0|P1|P2|P3)$")
    parsed_summary: str


class NLConfirmRequest(BaseModel):
    query: str = Field(min_length=2, max_length=2000)
    fault_type: str = Field(min_length=1, max_length=64)
    service_name: str = Field(min_length=1, max_length=128)
    matched_runbook: str = Field(min_length=1, max_length=128)
    target_a2a_peer: str | None = Field(default=None, max_length=512)
    suggested_skill: str | None = Field(default=None, max_length=128)
    severity: str = Field(default="P1", pattern="^(P0|P1|P2|P3)$")
    parsed_summary: str = Field(min_length=1, max_length=1000)


class IncidentDetailOut(IncidentSummaryOut):
    tasks: list[AgentTaskOut]
    approval: ApprovalOut | None
    trajectory: TrajectoryOut | None
