from dataclasses import dataclass
import json
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.config import Settings
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


class _CoordinatorPlanSchema(BaseModel):
    summary: str = Field(min_length=1)
    log_tool: str = Field(min_length=1)
    db_tool: str = Field(min_length=1)


class _DiagnosticPlanSchema(BaseModel):
    tool_name: str = Field(min_length=1)
    reasoning: str = Field(min_length=1)


class _RemediationProposalSchema(BaseModel):
    root_cause_summary: str = Field(min_length=1)
    tool_name: str = Field(min_length=1)
    proposed_command: str = Field(min_length=1)
    blast_radius: str = Field(min_length=1)
    rollback_command: str = Field(min_length=1)
    rationale: str = Field(min_length=1)


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


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MalformedLLMOutputError(f"LLM returned invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise MalformedLLMOutputError("LLM JSON output must be a JSON object.")
    return parsed


class OpenAICompatibleLLMProvider:
    """OpenAI-compatible async LLM client (supports DeepSeek & OpenAI) with strict Pydantic validation."""

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout_seconds: float = 45.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("LLM_API_KEY must be non-empty for OpenAICompatibleLLMProvider.")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds
        self.transport = transport

    async def _complete_json(
        self,
        *,
        system_prompt: str,
        user_payload: dict[str, Any],
        schema: type[BaseModel],
    ) -> BaseModel:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        body = {
            "model": self.model,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
            ],
        }
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
            ) as client:
                resp = await client.post(url, headers=headers, json=body)
        except httpx.HTTPError as exc:
            raise MalformedLLMOutputError(f"LLM HTTP request failed: {exc}") from exc

        if resp.status_code != 200:
            raise MalformedLLMOutputError(
                f"LLM API returned HTTP {resp.status_code}: {resp.text[:300]}"
            )

        try:
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise MalformedLLMOutputError(f"Malformed LLM response envelope: {exc}") from exc

        if not isinstance(content, str) or not content.strip():
            raise MalformedLLMOutputError("LLM returned empty message content.")

        parsed_obj = _extract_json_object(content)
        try:
            return schema.model_validate(parsed_obj)
        except ValidationError as exc:
            raise MalformedLLMOutputError(
                f"LLM JSON failed Pydantic validation: {exc}"
            ) from exc

    async def plan_triage(
        self, *, fault_type: str, service_name: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> CoordinatorPlan:
        system_prompt = (
            "You are the AIOps Coordinator Agent. Analyze the incident and matched Runbook Skill, "
            "and output ONLY a JSON object with keys: "
            '"summary" (concise triage plan), "log_tool" (exact read-only log tool name), '
            '"db_tool" (exact read-only DB/cache tool name).'
        )
        user_payload = {
            "fault_type": fault_type,
            "service_name": service_name,
            "alert_payload": alert_payload,
            "matched_skill": {
                "skill_id": skill.skill_id,
                "title": skill.title,
                "description": skill.description.strip(),
                "allowed_diagnostic_tools": {
                    "log_agent": skill.diagnostic_tools.log_agent,
                    "db_agent": skill.diagnostic_tools.db_agent,
                },
            },
        }
        validated = await self._complete_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            schema=_CoordinatorPlanSchema,
        )
        assert isinstance(validated, _CoordinatorPlanSchema)
        return CoordinatorPlan(
            summary=validated.summary,
            log_tool=validated.log_tool,
            db_tool=validated.db_tool,
        )

    async def plan_diagnostic_step(
        self, *, agent_role: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> DiagnosticPlan:
        expected_tool = (
            skill.diagnostic_tools.log_agent
            if agent_role == "LOG_DIAGNOSTICS"
            else skill.diagnostic_tools.db_agent
        )
        system_prompt = (
            f"You are the {agent_role} AIOps Agent. Select ONLY a READ_ONLY diagnostic tool from "
            "the matched Runbook Skill. Output ONLY a JSON object with keys: "
            '"tool_name" (exact tool name) and "reasoning" (brief justification).'
        )
        user_payload = {
            "agent_role": agent_role,
            "alert_payload": alert_payload,
            "skill_id": skill.skill_id,
            "recommended_read_only_tool": expected_tool,
        }
        validated = await self._complete_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            schema=_DiagnosticPlanSchema,
        )
        assert isinstance(validated, _DiagnosticPlanSchema)
        return DiagnosticPlan(
            tool_name=validated.tool_name,
            reasoning=validated.reasoning,
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
        system_prompt = (
            "You are the AIOps Remediation Agent. Correlate the shared blackboard diagnostic evidence "
            "and Runbook Skill remediation template. Output ONLY a JSON object with keys: "
            '"root_cause_summary", "tool_name", "proposed_command", "blast_radius", '
            '"rollback_command", "rationale".'
        )
        user_payload = {
            "fault_type": fault_type,
            "service_name": service_name,
            "blackboard_evidence": blackboard,
            "alert_payload": alert_payload,
            "skill_remediation_spec": {
                "skill_id": skill.skill_id,
                "tool_name": skill.remediation.tool_name,
                "risk_level": skill.remediation.risk_level,
                "command_template": skill.remediation.command_template.format(
                    service_name=service_name
                ).strip(),
                "blast_radius": skill.remediation.blast_radius.strip(),
                "rollback_command": skill.remediation.rollback_command.format(
                    service_name=service_name
                ).strip(),
            },
        }
        validated = await self._complete_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            schema=_RemediationProposalSchema,
        )
        assert isinstance(validated, _RemediationProposalSchema)
        return RemediationProposal(
            root_cause_summary=validated.root_cause_summary,
            tool_name=validated.tool_name,
            proposed_command=validated.proposed_command,
            blast_radius=validated.blast_radius,
            rollback_command=validated.rollback_command,
            rationale=validated.rationale,
        )


class MockLLMProvider:
    """Deterministic, fault-injectable LLM provider for zero-cost automated verification."""

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


def build_llm_provider(settings: Settings) -> LLMProvider:
    provider = settings.llm_provider.strip().lower()
    if provider == "mock":
        return MockLLMProvider()
    if provider in {"deepseek", "openai", "openai_compatible"}:
        if not settings.llm_api_key.strip():
            raise ValueError(
                f"LLM_API_KEY must be configured when LLM_PROVIDER={settings.llm_provider!r}."
            )
        return OpenAICompatibleLLMProvider(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            model=settings.llm_model,
            timeout_seconds=settings.llm_timeout_seconds,
        )
    raise ValueError(f"Unsupported LLM_PROVIDER={settings.llm_provider!r}")
