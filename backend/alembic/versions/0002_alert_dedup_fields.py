"""Add alert_fingerprint, dedup_count, last_triggered_at and active partial unique index to incidents.

Revision ID: 0002_alert_dedup_fields
Revises: 0001_initial_schema
Create Date: 2026-11-27 20:45:00+08:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_alert_dedup_fields"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "incidents",
        sa.Column("alert_fingerprint", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column("dedup_count", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.add_column(
        "incidents",
        sa.Column(
            "last_triggered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "uq_incidents_active_fingerprint",
        "incidents",
        ["alert_fingerprint"],
        unique=True,
        postgresql_where=sa.text(
            "alert_fingerprint IS NOT NULL AND status NOT IN ('RESOLVED', 'FAILED', 'REJECTED')"
        ),
    )


def downgrade() -> None:
    op.drop_index("uq_incidents_active_fingerprint", table_name="incidents")
    op.drop_column("incidents", "last_triggered_at")
    op.drop_column("incidents", "dedup_count")
    op.drop_column("incidents", "alert_fingerprint")
