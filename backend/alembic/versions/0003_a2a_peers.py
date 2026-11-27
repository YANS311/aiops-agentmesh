"""Add a2a_peers table for dynamically registered external A2A agents.

Revision ID: 0003_a2a_peers
Revises: 0002_alert_dedup_fields
Create Date: 2026-11-27 21:20:00+08:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003_a2a_peers"
down_revision = "0002_alert_dedup_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "a2a_peers",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("base_url", sa.String(length=512), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=sa.text("''")),
        sa.Column("version", sa.String(length=32), nullable=False, server_default=sa.text("'1.0.0'")),
        sa.Column(
            "agent_card",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_by", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("base_url"),
    )


def downgrade() -> None:
    op.drop_table("a2a_peers")
