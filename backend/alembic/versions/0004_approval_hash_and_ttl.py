"""Add execution_plan_hash and expires_at to remediation_approvals.

Revision ID: 0004_approval_hash_and_ttl
Revises: 0003_a2a_peers
Create Date: 2026-11-27 21:35:00+08:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004_approval_hash_and_ttl"
down_revision = "0003_a2a_peers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "remediation_approvals",
        sa.Column("execution_plan_hash", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "remediation_approvals",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("remediation_approvals", "expires_at")
    op.drop_column("remediation_approvals", "execution_plan_hash")
