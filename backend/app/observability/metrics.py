from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models import AgentTask, Incident, OutboxEvent

KNOWN_INCIDENT_STATUSES = (
    "OPEN",
    "TRIAGING",
    "DIAGNOSING",
    "NEEDS_HUMAN_REVIEW",
    "PENDING_APPROVAL",
    "EXECUTING_REMEDIATION",
    "RESOLVED",
    "REJECTED",
    "FAILED",
)

INCIDENTS_BY_STATUS = Gauge(
    "aiops_incidents_total",
    "Current number of AIOps incidents grouped by lifecycle status",
    ["status"],
)

TASK_DURATION_SECONDS = Histogram(
    "aiops_task_duration_seconds",
    "Duration of individual agent task executions in seconds",
    ["agent_role", "status"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

FAIL_CLOSED_BLOCKS_TOTAL = Counter(
    "aiops_fail_closed_blocks_total",
    "Total number of Fail-Closed security/policy/RBAC interceptions",
    ["reason"],
)

OUTBOX_PENDING_GAUGE = Gauge(
    "aiops_outbox_pending_events",
    "Current backlog of PENDING transactional outbox events awaiting Redis Streams relay",
)

DEAD_LETTER_TASKS_GAUGE = Gauge(
    "aiops_dead_letter_tasks_total",
    "Current number of dead-lettered agent tasks that exhausted max_attempts",
)


async def refresh_db_gauges(session_factory: async_sessionmaker[AsyncSession]) -> None:
    """Refresh live PG16 gauges before rendering Prometheus /metrics."""
    async with session_factory() as session:
        status_rows = (
            await session.execute(
                select(Incident.status, func.count(Incident.id)).group_by(Incident.status)
            )
        ).all()
        counts = {row[0]: int(row[1]) for row in status_rows}
        for st in KNOWN_INCIDENT_STATUSES:
            INCIDENTS_BY_STATUS.labels(status=st).set(counts.get(st, 0))

        outbox_pending = int(
            (
                await session.execute(
                    select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "PENDING")
                )
            ).scalar_one()
        )
        OUTBOX_PENDING_GAUGE.set(outbox_pending)

        dead_letters = int(
            (
                await session.execute(
                    select(func.count(AgentTask.id)).where(
                        AgentTask.status == "FAILED",
                        AgentTask.attempt_count >= AgentTask.max_attempts,
                    )
                )
            ).scalar_one()
        )
        DEAD_LETTER_TASKS_GAUGE.set(dead_letters)


def render_prometheus_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
