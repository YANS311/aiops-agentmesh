from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable

from app.db.models import Incident, RemediationApproval


class ToolPermission(StrEnum):
    READ_ONLY = "READ_ONLY"
    HIGH_RISK_WRITE = "HIGH_RISK_WRITE"


class PolicyViolationError(PermissionError):
    """Raised when an agent attempts a tool call outside its authorized role/policy."""


class ApprovalRequiredError(PermissionError):
    """Raised Fail-Closed when a high-risk write tool is invoked without APPROVED record."""


@dataclass(frozen=True)
class ToolSpec:
    name: str
    permission: ToolPermission
    description: str
    handler: Callable[[dict[str, Any]], dict[str, Any]]


def _query_service_error_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    service = ctx.get("service_name", "payment-core")
    return {
        "source": "loki-prod",
        "service": service,
        "error_count_5m": 418,
        "top_signature": "asyncpg.exceptions.TooManyConnectionsError / QueuePool limit of size 20 overflow 10 reached",
        "sample_trace_ids": ["tr-98f1a2", "tr-98f1b7"],
    }


def _inspect_pg_stat_activity(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "pg16-primary",
        "total_connections": 200,
        "max_connections": 200,
        "idle_in_transaction_count": 14,
        "oldest_idle_tx_seconds": 384,
        "suspect_pids": [31042, 31088, 31109],
    }


def _query_slow_sql_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "pg-stat-statements",
        "service": ctx.get("service_name", "order-ledger"),
        "blocked_queries": 63,
        "p99_latency_ms": 14200,
        "top_signature": "UPDATE orders_ledger SET settled = true WHERE batch_date < now() - interval '1 day'",
    }


def _inspect_pg_locks(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "pg_locks",
        "blocking_pids": [41892, 41907],
        "wait_event_type": "Lock",
        "lock_mode": "ExclusiveLock",
        "blocked_relation": "orders_ledger",
    }


def _query_gateway_latency_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "envoy-access-logs",
        "service": ctx.get("service_name", "catalog-gateway"),
        "http_504_count_1m": 219,
        "upstream_timeout_rate": 0.34,
        "hot_endpoint": "/api/catalog/flash-sale/items",
    }


def _inspect_redis_hotkeys(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": "redis7-monitor",
        "expired_hotkey": "catalog:flash_sale:top100",
        "miss_rate": 0.91,
        "db_qps_amplification": "18.4x",
    }


def _terminate_idle_in_tx_and_recycle_pool(ctx: dict[str, Any]) -> dict[str, Any]:
    service = ctx.get("service_name", "payment-core")
    return {
        "executed_tool": "terminate_idle_in_tx_and_recycle_pool",
        "terminated_pids": [31042, 31088, 31109],
        "terminated_total": 14,
        "pool_recycled_service": service,
        "post_check_active_connections": 27,
        "status": "SUCCESS",
    }


def _cancel_blocking_backend_pid(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "executed_tool": "cancel_blocking_backend_pid",
        "cancelled_pids": [41892, 41907],
        "lock_queue_depth_after": 0,
        "status": "SUCCESS",
    }


def _enable_singleflight_and_prewarm_hotkey(ctx: dict[str, Any]) -> dict[str, Any]:
    return {
        "executed_tool": "enable_singleflight_and_prewarm_hotkey",
        "singleflight_enabled": True,
        "prewarmed_keys": 100,
        "cache_hit_ratio_after": 0.985,
        "status": "SUCCESS",
    }


class ToolRegistry:
    """Registry of read-only diagnostic and high-risk write remediation tools."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}
        for spec in [
            ToolSpec(
                name="query_service_error_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query application error logs for pool exhaustion or timeouts.",
                handler=_query_service_error_logs,
            ),
            ToolSpec(
                name="inspect_pg_stat_activity",
                permission=ToolPermission.READ_ONLY,
                description="Inspect PostgreSQL 16 pg_stat_activity connection states.",
                handler=_inspect_pg_stat_activity,
            ),
            ToolSpec(
                name="query_slow_sql_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query slow SQL digests and blocked query counts.",
                handler=_query_slow_sql_logs,
            ),
            ToolSpec(
                name="inspect_pg_locks",
                permission=ToolPermission.READ_ONLY,
                description="Inspect PostgreSQL 16 lock tree and blocking PIDs.",
                handler=_inspect_pg_locks,
            ),
            ToolSpec(
                name="query_gateway_latency_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query API gateway latency spikes and upstream 504s.",
                handler=_query_gateway_latency_logs,
            ),
            ToolSpec(
                name="inspect_redis_hotkeys",
                permission=ToolPermission.READ_ONLY,
                description="Inspect Redis 7 hot-key miss ratio and QPS amplification.",
                handler=_inspect_redis_hotkeys,
            ),
            ToolSpec(
                name="terminate_idle_in_tx_and_recycle_pool",
                permission=ToolPermission.HIGH_RISK_WRITE,
                description="Terminate idle-in-transaction PG16 backends and recycle pool.",
                handler=_terminate_idle_in_tx_and_recycle_pool,
            ),
            ToolSpec(
                name="cancel_blocking_backend_pid",
                permission=ToolPermission.HIGH_RISK_WRITE,
                description="Cancel blocking PostgreSQL backend queries via pg_cancel_backend.",
                handler=_cancel_blocking_backend_pid,
            ),
            ToolSpec(
                name="enable_singleflight_and_prewarm_hotkey",
                permission=ToolPermission.HIGH_RISK_WRITE,
                description="Enable singleflight coalescing and prewarm Redis hot keys.",
                handler=_enable_singleflight_and_prewarm_hotkey,
            ),
        ]:
            self._tools[spec.name] = spec

    def get(self, name: str) -> ToolSpec | None:
        return self._tools.get(name)


class PolicyGuard:
    """Fail-Closed policy enforcement for all agent tool invocations."""

    READ_ONLY_ROLES = {"LOG_DIAGNOSTICS", "DB_DIAGNOSTICS"}

    def __init__(self, registry: ToolRegistry) -> None:
        self.registry = registry

    def authorize_tool_call(
        self,
        *,
        agent_role: str,
        tool_name: str,
        incident: Incident,
        approval: RemediationApproval | None = None,
    ) -> ToolSpec:
        spec = self.registry.get(tool_name)
        if spec is None:
            raise PolicyViolationError(f"Unknown tool {tool_name!r} rejected by Fail-Closed policy.")

        if spec.permission == ToolPermission.READ_ONLY:
            if agent_role not in self.READ_ONLY_ROLES:
                raise PolicyViolationError(
                    f"Role {agent_role!r} is not permitted to invoke diagnostic tool {tool_name!r}."
                )
            return spec

        # HIGH_RISK_WRITE: strictly Fail-Closed.
        if agent_role != "EXECUTOR":
            raise PolicyViolationError(
                f"Fail-Closed block: role {agent_role!r} attempted HIGH_RISK_WRITE tool {tool_name!r}."
            )
        if approval is None or approval.status != "APPROVED":
            current_approval = approval.status if approval is not None else "NONE"
            raise ApprovalRequiredError(
                f"Fail-Closed block: tool {tool_name!r} requires APPROVED human gate "
                f"(current approval={current_approval}, incident_status={incident.status})."
            )
        if incident.status != "EXECUTING_REMEDIATION":
            raise ApprovalRequiredError(
                f"Fail-Closed block: incident status must be EXECUTING_REMEDIATION "
                f"(got {incident.status})."
            )
        return spec

    def execute_authorized(
        self,
        *,
        agent_role: str,
        tool_name: str,
        incident: Incident,
        context: dict[str, Any],
        approval: RemediationApproval | None = None,
    ) -> tuple[ToolSpec, dict[str, Any]]:
        spec = self.authorize_tool_call(
            agent_role=agent_role,
            tool_name=tool_name,
            incident=incident,
            approval=approval,
        )
        return spec, spec.handler(context)
