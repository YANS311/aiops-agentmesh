"""Initial PostgreSQL 16 schema (6 tables, 4 JSONB GIN indexes, 3 partial B-tree indexes).

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-11-26 20:38:00
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0001_initial_schema"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. incidents
    op.create_table(
        "incidents",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=True, unique=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("service_name", sa.String(length=128), nullable=False),
        sa.Column("fault_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'OPEN'")),
        sa.Column("alert_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("blackboard", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("root_cause_summary", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_incidents_fault_type", "incidents", ["fault_type"])
    op.create_index("ix_incidents_status", "incidents", ["status"])
    op.create_index(
        "ix_incidents_alert_payload_gin",
        "incidents",
        ["alert_payload"],
        postgresql_using="gin",
    )
    op.create_index(
        "ix_incidents_blackboard_gin",
        "incidents",
        ["blackboard"],
        postgresql_using="gin",
    )

    # 2. agent_tasks
    op.create_table(
        "agent_tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("agent_role", sa.String(length=64), nullable=False),
        sa.Column("step_order", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'QUEUED'")),
        sa.Column("input_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("3")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_agent_tasks_incident_id", "agent_tasks", ["incident_id"])
    op.create_index(
        "ix_agent_tasks_queue_order",
        "agent_tasks",
        ["status", "step_order", "created_at"],
    )
    op.create_index(
        "ix_agent_tasks_queued_partial",
        "agent_tasks",
        ["step_order", "created_at"],
        postgresql_where=sa.text("status = 'QUEUED'"),
    )
    op.create_index(
        "ix_agent_tasks_running_lease",
        "agent_tasks",
        ["lease_expires_at"],
        postgresql_where=sa.text("status = 'RUNNING'"),
    )

    # 3. remediation_approvals
    op.create_table(
        "remediation_approvals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "task_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("agent_tasks.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("skill_id", sa.String(length=128), nullable=False),
        sa.Column("risk_level", sa.String(length=32), nullable=False),
        sa.Column("proposed_command", sa.Text(), nullable=False),
        sa.Column("action_plan", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'PENDING'")),
        sa.Column("approved_by", sa.String(length=128), nullable=True),
        sa.Column("approved_by_role", sa.String(length=32), nullable=True),
        sa.Column("approval_comment", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_result", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_remediation_approvals_status", "remediation_approvals", ["status"])

    # 4. trace_events
    op.create_table(
        "trace_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), primary_key=True, nullable=False),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("agent_role", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("risk_tier", sa.String(length=32), nullable=False, server_default=sa.text("'NONE'")),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_trace_events_incident_id", "trace_events", ["incident_id"])
    op.create_index(
        "ix_trace_events_payload_gin",
        "trace_events",
        ["payload"],
        postgresql_using="gin",
    )

    # 5. incident_trajectories
    op.create_table(
        "incident_trajectories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("skill_id", sa.String(length=128), nullable=False),
        sa.Column("fault_type", sa.String(length=64), nullable=False),
        sa.Column("root_cause", sa.Text(), nullable=False),
        sa.Column("resolution_command", sa.Text(), nullable=False),
        sa.Column("trajectory_doc", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_incident_trajectories_doc_gin",
        "incident_trajectories",
        ["trajectory_doc"],
        postgresql_using="gin",
    )

    # 6. outbox_events
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), primary_key=True, nullable=False),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "trace_event_id",
            sa.BigInteger(),
            sa.ForeignKey("trace_events.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'PENDING'")),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_outbox_events_incident_id", "outbox_events", ["incident_id"])
    op.create_index(
        "ix_outbox_events_pending",
        "outbox_events",
        ["id"],
        postgresql_where=sa.text("status = 'PENDING'"),
    )


def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("incident_trajectories")
    op.drop_table("trace_events")
    op.drop_table("remediation_approvals")
    op.drop_table("agent_tasks")
    op.drop_table("incidents")
