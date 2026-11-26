from dataclasses import dataclass
from typing import Any, Protocol

from app.skills.loader import RunbookSkill


class MalformedLLMOutputError(RuntimeError):
    """Raised when an LLM returns unparseable or invalid structured output."""


@dataclass(frozen=True)
class CoordinatorPlan:
    summary: str
    log_tool: str
    db_tool: str


@dataclass(frozen=True)
class DiagnosticPlan:
    tool_name: str
    reasoning: str


@dataclass(frozen=True)
class RemediationProposal:
    root_cause_summary: str
    tool_name: str
    proposed_command: str
    blast_radius: str
    rollback_command: str
    rationale: str


class LLMProvider(Protocol):
    async def plan_triage(
        self, *, fault_type: str, service_name: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> CoordinatorPlan: ...

    async def plan_diagnostic_step(
        self, *, agent_role: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> DiagnosticPlan: ...

    async def propose_remediation(
        self,
        *,
        fault_type: str,
        service_name: str,
        blackboard: dict[str, Any],
        alert_payload: dict[str, Any],
        skill: RunbookSkill,
    ) -> RemediationProposal: ...


class MockLLMProvider:
    """Deterministic, fault-injectable LLM provider for Phase 1 verification."""

    async def plan_triage(
        self, *, fault_type: str, service_name: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> CoordinatorPlan:
        fault_mode = alert_payload.get("_mock_fault_mode", "normal")
        if fault_mode == "llm_malformed_output":
            raise MalformedLLMOutputError(
                "MockLLM fault injection: malformed JSON returned during Coordinator triage."
            )

        return CoordinatorPlan(
            summary=(
                f"Matched skill {skill.skill_id} for {fault_type} on {service_name}; "
                f"fanning out read-only log and DB diagnostics."
            ),
            log_tool=skill.diagnostic_tools.log_agent,
            db_tool=skill.diagnostic_tools.db_agent,
        )

    async def plan_diagnostic_step(
        self, *, agent_role: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> DiagnosticPlan:
        fault_mode = alert_payload.get("_mock_fault_mode", "normal")
        if fault_mode == "unauthorized_diag_write" and agent_role == "DB_DIAGNOSTICS":
            return DiagnosticPlan(
                tool_name=skill.remediation.tool_name,
                reasoning="Injected fault: rogue diagnostic agent attempts direct high-risk write.",
            )

        tool_name = (
            skill.diagnostic_tools.log_agent
            if agent_role == "LOG_DIAGNOSTICS"
            else skill.diagnostic_tools.db_agent
        )
        return DiagnosticPlan(
            tool_name=tool_name,
            reasoning=f"Execute read-only diagnostic tool {tool_name} per {skill.skill_id}.",
        )

    async def propose_remediation(
        self,
        *,
        fault_type: str,
        service_name: str,
        blackboard: dict[str, Any],
        alert_payload: dict[str, Any],
        skill: RunbookSkill,
    ) -> RemediationProposal:
        log_ev = blackboard.get("log_diagnostics", {})
        db_ev = blackboard.get("db_diagnostics", {})
        command = skill.remediation.command_template.format(service_name=service_name).strip()
        rollback = skill.remediation.rollback_command.format(service_name=service_name).strip()

        if fault_type == "db_connection_pool_exhausted":
            root_cause = (
                f"{service_name} saturated all {db_ev.get('max_connections', 200)} PostgreSQL 16 "
                f"connections ({db_ev.get('idle_in_transaction_count', 14)} idle-in-transaction "
                f"sessions up to {db_ev.get('oldest_idle_tx_seconds', 384)}s), causing "
                f"{log_ev.get('error_count_5m', 418)} TooManyConnections errors."
            )
        elif fault_type == "slow_sql_lock_contention":
            root_cause = (
                f"Bulk batch UPDATE PIDs {db_ev.get('blocking_pids', [41892, 41907])} hold "
                f"{db_ev.get('lock_mode', 'ExclusiveLock')} on {db_ev.get('blocked_relation', 'orders_ledger')}, "
                f"blocking {log_ev.get('blocked_queries', 63)} queries (p99={log_ev.get('p99_latency_ms', 14200)}ms)."
            )
        else:
            root_cause = (
                f"Hot key {db_ev.get('expired_hotkey', 'catalog:flash_sale:top100')} expired "
                f"simultaneously (miss_rate={db_ev.get('miss_rate', 0.91)}), amplifying DB QPS by "
                f"{db_ev.get('db_qps_amplification', '18.4x')} and triggering {log_ev.get('http_504_count_1m', 219)} 504s."
            )

        return RemediationProposal(
            root_cause_summary=root_cause,
            tool_name=skill.remediation.tool_name,
            proposed_command=command,
            blast_radius=skill.remediation.blast_radius.strip(),
            rollback_command=rollback,
            rationale=(
                f"Correlated {skill.diagnostic_tools.log_agent} + {skill.diagnostic_tools.db_agent} "
                f"evidence under {skill.skill_id}. High-risk write action requires human approval."
            ),
        )
