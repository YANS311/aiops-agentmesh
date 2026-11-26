from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import logging
from typing import Any

_incident_id_ctx: ContextVar[str | None] = ContextVar("incident_id", default=None)
_task_id_ctx: ContextVar[str | None] = ContextVar("task_id", default=None)
_actor_id_ctx: ContextVar[str | None] = ContextVar("actor_id", default=None)
_actor_role_ctx: ContextVar[str | None] = ContextVar("actor_role", default=None)


@contextmanager
def bind_log_context(
    *,
    incident_id: str | None = None,
    task_id: str | None = None,
    actor_id: str | None = None,
    actor_role: str | None = None,
) -> Iterator[None]:
    tokens = []
    if incident_id is not None:
        tokens.append((_incident_id_ctx, _incident_id_ctx.set(str(incident_id))))
    if task_id is not None:
        tokens.append((_task_id_ctx, _task_id_ctx.set(str(task_id))))
    if actor_id is not None:
        tokens.append((_actor_id_ctx, _actor_id_ctx.set(str(actor_id))))
    if actor_role is not None:
        tokens.append((_actor_role_ctx, _actor_role_ctx.set(str(actor_role))))
    try:
        yield
    finally:
        for var, tok in reversed(tokens):
            var.reset(tok)


class JsonLogFormatter(logging.Formatter):
    """Single-line JSON structured formatter enriched with incident/task/actor context."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        inc_id = _incident_id_ctx.get()
        task_id = _task_id_ctx.get()
        actor_id = _actor_id_ctx.get()
        actor_role = _actor_role_ctx.get()
        if inc_id is not None:
            payload["incident_id"] = inc_id
        if task_id is not None:
            payload["task_id"] = task_id
        if actor_id is not None:
            payload["actor_id"] = actor_id
        if actor_role is not None:
            payload["actor_role"] = actor_role
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_json_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        root.addHandler(handler)
    else:
        for h in root.handlers:
            h.setFormatter(JsonLogFormatter())
