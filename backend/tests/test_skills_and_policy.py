import pytest

from app.agents.tools import (
    ApprovalRequiredError,
    PolicyGuard,
    PolicyViolationError,
    ToolPermission,
    ToolRegistry,
)
from app.config import get_settings
from app.db.models import Incident, RemediationApproval
from app.skills.loader import SkillRegistry


def test_declarative_runbook_skills_load() -> None:
    settings = get_settings()
    registry = SkillRegistry(settings.skills_dir)
    skills = registry.list_skills()
    assert len(skills) == 5

    pool_skill = registry.match_for_fault("db_connection_pool_exhausted")
    assert pool_skill is not None
    assert pool_skill.skill_id == "skill.pg16.pool_exhaustion"
    assert pool_skill.diagnostic_tools.log_agent == "query_service_error_logs"
    assert pool_skill.diagnostic_tools.db_agent == "inspect_pg_stat_activity"
    assert pool_skill.remediation.requires_human_approval is True
    assert pool_skill.remediation.risk_level == "HIGH"


def test_policy_guard_read_only_auto_allowed_and_write_fail_closed() -> None:
    tools = ToolRegistry()
    guard = PolicyGuard(tools)

    incident = Incident(
        title="Pool exhaustion",
        severity="P0",
        service_name="payment-core",
        fault_type="db_connection_pool_exhausted",
        status="DIAGNOSING",
        alert_payload={},
        blackboard={},
    )

    spec_log = guard.authorize_tool_call(
        agent_role="LOG_DIAGNOSTICS",
        tool_name="query_service_error_logs",
        incident=incident,
    )
    assert spec_log.permission == ToolPermission.READ_ONLY

    spec_db = guard.authorize_tool_call(
        agent_role="DB_DIAGNOSTICS",
        tool_name="inspect_pg_stat_activity",
        incident=incident,
    )
    assert spec_db.permission == ToolPermission.READ_ONLY

    # Diagnostic role attempting HIGH_RISK_WRITE must Fail-Closed
    with pytest.raises(PolicyViolationError):
        guard.authorize_tool_call(
            agent_role="DB_DIAGNOSTICS",
            tool_name="terminate_idle_in_tx_and_recycle_pool",
            incident=incident,
        )

    # EXECUTOR without approval must Fail-Closed
    incident.status = "PENDING_APPROVAL"
    with pytest.raises(ApprovalRequiredError):
        guard.authorize_tool_call(
            agent_role="EXECUTOR",
            tool_name="terminate_idle_in_tx_and_recycle_pool",
            incident=incident,
            approval=None,
        )

    # EXECUTOR with PENDING approval must Fail-Closed
    pending = RemediationApproval(
        incident_id=incident.id,
        task_id=incident.id,
        skill_id="skill.pg16.pool_exhaustion",
        risk_level="HIGH",
        proposed_command="SELECT 1;",
        action_plan={},
        status="PENDING",
    )
    with pytest.raises(ApprovalRequiredError):
        guard.authorize_tool_call(
            agent_role="EXECUTOR",
            tool_name="terminate_idle_in_tx_and_recycle_pool",
            incident=incident,
            approval=pending,
        )

    # Unknown tool must Fail-Closed
    with pytest.raises(PolicyViolationError):
        guard.authorize_tool_call(
            agent_role="EXECUTOR",
            tool_name="drop_database_unknown",
            incident=incident,
            approval=pending,
        )
