from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Incident(Base):
    __tablename__ = "incidents"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), unique=True, nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), nullable=False)
    service_name: Mapped[str] = mapped_column(String(128), nullable=False)
    fault_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True, default="OPEN")
    alert_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    blackboard: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    root_cause_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    tasks: Mapped[list["AgentTask"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan", order_by="AgentTask.step_order"
    )
    approval: Mapped["RemediationApproval | None"] = relationship(
        back_populates="incident", cascade="all, delete-orphan", uselist=False
    )
    traces: Mapped[list["TraceEvent"]] = relationship(
        back_populates="incident", cascade="all, delete-orphan", order_by="TraceEvent.id"
    )
    trajectory: Mapped["IncidentTrajectory | None"] = relationship(
        back_populates="incident", cascade="all, delete-orphan", uselist=False
    )

    __table_args__ = (
        Index("ix_incidents_alert_payload_gin", "alert_payload", postgresql_using="gin"),
        Index("ix_incidents_blackboard_gin", "blackboard", postgresql_using="gin"),
    )


class AgentTask(Base):
    __tablename__ = "agent_tasks"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_role: Mapped[str] = mapped_column(String(64), nullable=False)
    step_order: Mapped[int] = mapped_column(Integer, nullable=False)
    action_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="QUEUED")
    input_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    output_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    incident: Mapped[Incident] = relationship(back_populates="tasks")

    __table_args__ = (
        Index("ix_agent_tasks_queue_order", "status", "step_order", "created_at"),
        Index(
            "ix_agent_tasks_queued_partial",
            "step_order",
            "created_at",
            postgresql_where=text("status = 'QUEUED'"),
        ),
        Index(
            "ix_agent_tasks_running_lease",
            "lease_expires_at",
            postgresql_where=text("status = 'RUNNING'"),
        ),
    )


class OutboxEvent(Base):
    """Transactional Outbox: written in the same PG16 transaction as its TraceEvent."""

    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    trace_event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("trace_events.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="PENDING")
    retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index(
            "ix_outbox_events_pending",
            "id",
            postgresql_where=text("status = 'PENDING'"),
        ),
    )


class RemediationApproval(Base):
    __tablename__ = "remediation_approvals"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    task_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("agent_tasks.id", ondelete="CASCADE"), nullable=False
    )
    skill_id: Mapped[str] = mapped_column(String(128), nullable=False)
    risk_level: Mapped[str] = mapped_column(String(32), nullable=False)
    proposed_command: Mapped[str] = mapped_column(Text, nullable=False)
    action_plan: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True, default="PENDING")
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    approved_by_role: Mapped[str | None] = mapped_column(String(32), nullable=True)
    approval_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    execution_result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="approval")


class TraceEvent(Base):
    __tablename__ = "trace_events"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    incident_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_role: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    risk_tier: Mapped[str] = mapped_column(String(32), nullable=False, default="NONE")
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="traces")

    __table_args__ = (
        Index("ix_trace_events_payload_gin", "payload", postgresql_using="gin"),
    )


class IncidentTrajectory(Base):
    __tablename__ = "incident_trajectories"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    incident_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("incidents.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    skill_id: Mapped[str] = mapped_column(String(128), nullable=False)
    fault_type: Mapped[str] = mapped_column(String(64), nullable=False)
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    resolution_command: Mapped[str] = mapped_column(Text, nullable=False)
    trajectory_doc: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    incident: Mapped[Incident] = relationship(back_populates="trajectory")

    __table_args__ = (
        Index("ix_incident_trajectories_doc_gin", "trajectory_doc", postgresql_using="gin"),
    )
