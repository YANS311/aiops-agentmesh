from collections.abc import Awaitable
from dataclasses import dataclass
from enum import StrEnum
import inspect
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import text

from app.a2a.client import A2AClient, A2AClientError
from app.a2a.models import A2ATaskRequest
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
    live_handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]] | None = None


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


# ---------------------------------------------------------------------------
# Live Read-Only Probes (TOOL_EXECUTION_MODE=live): real PG16 + Redis 7 stats
# ---------------------------------------------------------------------------


async def _live_inspect_pg_stat_activity(ctx: dict[str, Any]) -> dict[str, Any]:
    session = ctx.get("db_session")
    if session is None:
        return {**_inspect_pg_stat_activity(ctx), "mode": "mock_fallback"}

    max_conn_raw = (await session.execute(text("SHOW max_connections"))).scalar_one()
    row = (
        await session.execute(
            text(
                """
                SELECT
                    count(*)::int AS total_connections,
                    count(*) FILTER (WHERE state = 'active')::int AS active_connections,
                    count(*) FILTER (WHERE state = 'idle in transaction')::int AS idle_in_tx,
                    COALESCE(
                        EXTRACT(
                            EPOCH FROM max(now() - state_change)
                            FILTER (WHERE state = 'idle in transaction')
                        ),
                        0
                    )::float AS oldest_idle_tx_seconds
                FROM pg_stat_activity
                """
            )
        )
    ).one()
    pids = (
        await session.execute(
            text(
                """
                SELECT pid
                FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                ORDER BY state_change ASC
                LIMIT 5
                """
            )
        )
    ).scalars().all()
    return {
        "mode": "live",
        "source": "pg16-live:pg_stat_activity",
        "total_connections": int(row.total_connections),
        "active_connections": int(row.active_connections),
        "max_connections": int(str(max_conn_raw)),
        "idle_in_transaction_count": int(row.idle_in_tx),
        "oldest_idle_tx_seconds": round(float(row.oldest_idle_tx_seconds), 2),
        "suspect_pids": [int(p) for p in pids],
    }


async def _live_inspect_pg_locks(ctx: dict[str, Any]) -> dict[str, Any]:
    session = ctx.get("db_session")
    if session is None:
        return {**_inspect_pg_locks(ctx), "mode": "mock_fallback"}

    row = (
        await session.execute(
            text(
                """
                SELECT
                    count(*)::int AS total_locks,
                    count(*) FILTER (WHERE NOT granted)::int AS waiting_locks
                FROM pg_locks
                """
            )
        )
    ).one()
    blocking_rows = (
        await session.execute(
            text(
                """
                SELECT DISTINCT unnest(pg_blocking_pids(pid)) AS blocker_pid
                FROM pg_stat_activity
                WHERE wait_event_type = 'Lock'
                LIMIT 10
                """
            )
        )
    ).scalars().all()
    modes = (
        await session.execute(
            text(
                """
                SELECT mode, count(*)::int AS cnt
                FROM pg_locks
                GROUP BY mode
                ORDER BY cnt DESC
                LIMIT 5
                """
            )
        )
    ).all()
    return {
        "mode": "live",
        "source": "pg16-live:pg_locks",
        "total_locks": int(row.total_locks),
        "waiting_locks": int(row.waiting_locks),
        "blocking_pids": [int(p) for p in blocking_rows],
        "wait_event_type": "Lock" if int(row.waiting_locks) > 0 else "None",
        "lock_mode": modes[0][0] if modes else "AccessShareLock",
        "lock_modes": {str(m): int(c) for m, c in modes},
        "blocked_relation": "pg_locks",
    }


async def _live_inspect_redis_hotkeys(ctx: dict[str, Any]) -> dict[str, Any]:
    redis_client = ctx.get("redis_client")
    if redis_client is None:
        return {**_inspect_redis_hotkeys(ctx), "mode": "mock_fallback"}

    mem_info = await redis_client.info("memory")
    stats_info = await redis_client.info("stats")
    slowlog = await redis_client.slowlog_get(5)

    hits = int(stats_info.get("keyspace_hits", 0))
    misses = int(stats_info.get("keyspace_misses", 0))
    total_ops = hits + misses
    miss_rate = round(misses / total_ops, 4) if total_ops > 0 else 0.0

    return {
        "mode": "live",
        "source": "redis7-live:info_and_slowlog",
        "used_memory_bytes": int(mem_info.get("used_memory", 0)),
        "used_memory_human": str(mem_info.get("used_memory_human", "0B")),
        "keyspace_hits": hits,
        "keyspace_misses": misses,
        "miss_rate": miss_rate,
        "instantaneous_ops_per_sec": int(stats_info.get("instantaneous_ops_per_sec", 0)),
        "evicted_keys": int(stats_info.get("evicted_keys", 0)),
        "slowlog_count": len(slowlog),
        "slowlog_sample": [
            {
                "id": int(item.get("id", 0)),
                "duration_us": int(item.get("duration", 0)),
                "command": str(item.get("command", ""))[:120],
            }
            for item in slowlog
        ],
    }


async def _live_query_service_error_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    base = _query_service_error_logs(ctx)
    pg_live = await _live_inspect_pg_stat_activity(ctx)
    return {
        **base,
        "mode": "live",
        "source": "pg16-live:error_correlation",
        "live_pg_connections": pg_live.get("total_connections"),
        "live_pg_max_connections": pg_live.get("max_connections"),
    }


async def _live_query_slow_sql_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    base = _query_slow_sql_logs(ctx)
    locks_live = await _live_inspect_pg_locks(ctx)
    return {
        **base,
        "mode": "live",
        "source": "pg16-live:slow_sql_correlation",
        "live_waiting_locks": locks_live.get("waiting_locks"),
        "live_total_locks": locks_live.get("total_locks"),
    }


async def _live_query_gateway_latency_logs(ctx: dict[str, Any]) -> dict[str, Any]:
    base = _query_gateway_latency_logs(ctx)
    redis_live = await _live_inspect_redis_hotkeys(ctx)
    return {
        **base,
        "mode": "live",
        "source": "redis7-live:gateway_correlation",
        "live_redis_ops_per_sec": redis_live.get("instantaneous_ops_per_sec"),
        "live_redis_used_memory": redis_live.get("used_memory_human"),
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


def _create_neo4j_index_and_throttle_cypher(ctx: dict[str, Any]) -> dict[str, Any]:
    service = ctx.get("service_name", "guidance-knowledge-graph")
    return {
        "executed_tool": "create_neo4j_index_and_throttle_cypher",
        "service": service,
        "created_indexes": ["idx_concept_course_id"],
        "cypher_max_concurrent": 16,
        "post_check_p99_cypher_ms": 185,
        "status": "SUCCESS",
    }


def _tune_rag_retrieval_and_enable_degrade(ctx: dict[str, Any]) -> dict[str, Any]:
    service = ctx.get("service_name", "guidance-rag-service")
    return {
        "executed_tool": "tune_rag_retrieval_and_enable_degrade",
        "service": service,
        "efSearch": 128,
        "top_k": 8,
        "bm25_fallback": True,
        "post_check_rag_p99_ms": 410,
        "status": "SUCCESS",
    }


def _delegate_a2a_diagnosis_sync(ctx: dict[str, Any]) -> dict[str, Any]:
    fault_type = str(ctx.get("fault_type", "guidance_neo4j_slow_cypher"))
    return {
        "a2a_status": "DEGRADED",
        "source": "a2a-sync-fallback",
        "fault_type": fault_type,
        "error": "Synchronous fallback invoked without async A2AClient.",
    }


async def _delegate_a2a_diagnosis_async(ctx: dict[str, Any]) -> dict[str, Any]:
    """Discover peer Agent Card via A2AClient and delegate read-only diagnostic task."""
    a2a_client: A2AClient | None = ctx.get("a2a_client")
    peer_urls: list[str] = list(ctx.get("a2a_peer_urls") or [])
    fault_type = str(ctx.get("fault_type", ""))
    service_name = str(ctx.get("service_name", "guidance-service"))
    incident_id = str(ctx.get("incident_id", ""))

    target_skill = (
        "diagnose_rag_pipeline"
        if "rag" in fault_type.lower()
        else "diagnose_neo4j_cypher"
    )
    if a2a_client is None or not peer_urls:
        return {
            "a2a_status": "DEGRADED",
            "target_skill": target_skill,
            "source": "a2a-unconfigured",
            "error": "No A2A peer URLs configured; safely degraded without blocking worker.",
            "artifacts": {},
        }

    try:
        found = await a2a_client.find_peer_for_skill(peer_urls, target_skill)
    except Exception as exc:
        return {
            "a2a_status": "FAILED",
            "target_skill": target_skill,
            "source": "a2a-discovery-error",
            "error": str(exc),
            "artifacts": {},
        }

    if found is None:
        return {
            "a2a_status": "UNREACHABLE",
            "target_skill": target_skill,
            "source": "a2a-discovery",
            "error": f"No reachable A2A peer advertises skill `{target_skill}`.",
            "artifacts": {},
        }

    peer_url, peer_card = found
    req = A2ATaskRequest(
        task_id=f"a2a-{uuid4().hex[:12]}",
        skill_id=target_skill,
        input={"service_name": service_name, "fault_type": fault_type},
        metadata={"incident_id": incident_id, "fault_type": fault_type},
    )
    try:
        resp = await a2a_client.send_task(peer_url, req)
    except A2AClientError as exc:
        return {
            "a2a_status": "FAILED",
            "peer_name": peer_card.name,
            "peer_url": peer_url,
            "target_skill": target_skill,
            "task_id": req.task_id,
            "source": f"a2a:{peer_card.name}",
            "error": f"{exc.code}: {exc.message}",
            "artifacts": {},
        }

    if resp.state != "COMPLETED":
        return {
            "a2a_status": resp.state,
            "peer_name": peer_card.name,
            "peer_url": peer_url,
            "target_skill": target_skill,
            "task_id": resp.task_id,
            "source": f"a2a:{peer_card.name}",
            "error": resp.error or f"Peer returned state={resp.state}",
            "artifacts": resp.artifacts,
        }

    return {
        "a2a_status": "COMPLETED",
        "peer_name": peer_card.name,
        "peer_url": peer_url,
        "target_skill": target_skill,
        "task_id": resp.task_id,
        "source": f"a2a:{peer_card.name}",
        "artifacts": resp.artifacts,
        **resp.artifacts,
    }


class ToolRegistry:
    """Registry of read-only diagnostic and high-risk write remediation tools."""

    def __init__(self, execution_mode: str = "mock") -> None:
        mode = execution_mode.strip().lower()
        if mode not in {"mock", "live"}:
            raise ValueError(f"Unsupported TOOL_EXECUTION_MODE={execution_mode!r}; expected 'mock' or 'live'.")
        self.execution_mode = mode
        self._tools: dict[str, ToolSpec] = {}
        for spec in [
            ToolSpec(
                name="query_service_error_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query application error logs for pool exhaustion or timeouts.",
                handler=_query_service_error_logs,
                live_handler=_live_query_service_error_logs,
            ),
            ToolSpec(
                name="inspect_pg_stat_activity",
                permission=ToolPermission.READ_ONLY,
                description="Inspect PostgreSQL 16 pg_stat_activity connection states.",
                handler=_inspect_pg_stat_activity,
                live_handler=_live_inspect_pg_stat_activity,
            ),
            ToolSpec(
                name="query_slow_sql_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query slow SQL digests and blocked query counts.",
                handler=_query_slow_sql_logs,
                live_handler=_live_query_slow_sql_logs,
            ),
            ToolSpec(
                name="inspect_pg_locks",
                permission=ToolPermission.READ_ONLY,
                description="Inspect PostgreSQL 16 lock tree and blocking PIDs.",
                handler=_inspect_pg_locks,
                live_handler=_live_inspect_pg_locks,
            ),
            ToolSpec(
                name="query_gateway_latency_logs",
                permission=ToolPermission.READ_ONLY,
                description="Query API gateway latency spikes and upstream 504s.",
                handler=_query_gateway_latency_logs,
                live_handler=_live_query_gateway_latency_logs,
            ),
            ToolSpec(
                name="inspect_redis_hotkeys",
                permission=ToolPermission.READ_ONLY,
                description="Inspect Redis 7 hot-key miss ratio and QPS amplification.",
                handler=_inspect_redis_hotkeys,
                live_handler=_live_inspect_redis_hotkeys,
            ),
            ToolSpec(
                name="delegate_a2a_diagnosis",
                permission=ToolPermission.READ_ONLY,
                description="Discover external peer via A2A /.well-known/agent.json and delegate read-only diagnosis.",
                handler=_delegate_a2a_diagnosis_sync,
                live_handler=_delegate_a2a_diagnosis_async,
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
            ToolSpec(
                name="create_neo4j_index_and_throttle_cypher",
                permission=ToolPermission.HIGH_RISK_WRITE,
                description="Create missing Neo4j Concept index and throttle concurrent Cypher queries.",
                handler=_create_neo4j_index_and_throttle_cypher,
            ),
            ToolSpec(
                name="tune_rag_retrieval_and_enable_degrade",
                permission=ToolPermission.HIGH_RISK_WRITE,
                description="Tune RAG HNSW efSearch and enable BM25 hybrid fallback.",
                handler=_tune_rag_retrieval_and_enable_degrade,
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

    async def execute_authorized_async(
        self,
        *,
        agent_role: str,
        tool_name: str,
        incident: Incident,
        context: dict[str, Any],
        approval: RemediationApproval | None = None,
    ) -> tuple[ToolSpec, dict[str, Any]]:
        """Authorize via Fail-Closed policy, then invoke live async probe (if live mode) or mock handler."""
        spec = self.authorize_tool_call(
            agent_role=agent_role,
            tool_name=tool_name,
            incident=incident,
            approval=approval,
        )
        if (
            (self.registry.execution_mode == "live" or spec.name == "delegate_a2a_diagnosis")
            and spec.permission == ToolPermission.READ_ONLY
            and spec.live_handler is not None
        ):
            res = spec.live_handler(context)
            if inspect.isawaitable(res):
                return spec, await res
            return spec, res
        return spec, spec.handler(context)
