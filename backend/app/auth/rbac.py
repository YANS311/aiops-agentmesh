from dataclasses import dataclass
from enum import StrEnum
import logging

from fastapi import Depends, HTTPException, Request, status

from app.observability.logging import bind_log_context
from app.observability.metrics import FAIL_CLOSED_BLOCKS_TOTAL

logger = logging.getLogger(__name__)


class Role(StrEnum):
    VIEWER = "VIEWER"
    SRE_APPROVER = "SRE_APPROVER"


@dataclass(frozen=True)
class Actor:
    actor_id: str
    role: Role


def resolve_actor_from_token(request: Request) -> Actor | None:
    """Resolve authenticated Actor from Authorization: Bearer <token> header."""
    auth_header = request.headers.get("authorization", "").strip()
    if not auth_header or not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:].strip()
    if not token:
        return None

    settings = request.app.state.settings
    if token == settings.rbac_approver_token:
        return Actor(actor_id="sre-lead-01", role=Role.SRE_APPROVER)
    if token == settings.rbac_viewer_token:
        return Actor(actor_id="viewer-demo-01", role=Role.VIEWER)
    return None


def require_authenticated(request: Request) -> Actor:
    actor = resolve_actor_from_token(request)
    if actor is None:
        FAIL_CLOSED_BLOCKS_TOTAL.labels(reason="AUTH_REQUIRED").inc()
        logger.warning("Unauthenticated request blocked on %s", request.url.path)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "AUTH_REQUIRED",
                "message": "Valid Bearer token is required for this endpoint.",
            },
            headers={"WWW-Authenticate": "Bearer"},
        )
    return actor


def require_sre_approver(
    request: Request,
    actor: Actor = Depends(require_authenticated),
) -> Actor:
    if actor.role != Role.SRE_APPROVER:
        FAIL_CLOSED_BLOCKS_TOTAL.labels(reason="RBAC_FORBIDDEN").inc()
        with bind_log_context(actor_id=actor.actor_id, actor_role=actor.role.value):
            logger.warning(
                "RBAC Fail-Closed block: role=%s denied on %s",
                actor.role.value,
                request.url.path,
            )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "code": "RBAC_FORBIDDEN",
                "message": (
                    f"Role {actor.role.value} is not permitted to perform high-risk approval/execution; "
                    "requires SRE_APPROVER."
                ),
                "actor_id": actor.actor_id,
                "actor_role": actor.role.value,
            },
        )
    return actor
