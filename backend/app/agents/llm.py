from dataclasses import dataclass
import json
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field, ValidationError

from app.agents.shapley import DiagnosticHypothesis
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


class _SpecialistHypothesisSchema(BaseModel):
    root_cause_label: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_summary: str = Field(min_length=1)


class LLMProvider(Protocol):
    async def plan_triage(
        self, *, fault_type: str, service_name: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> CoordinatorPlan: ...

    async def plan_diagnostic_step(
        self, *, agent_role: str, alert_payload: dict[str, Any], skill: RunbookSkill
    ) -> DiagnosticPlan: ...

    async def diagnose_specialist_hypothesis(
        self,
        *,
        agent_id: str,
        fault_type: str,
        service_name: str,
        evidence: dict[str, Any],
        alert_payload: dict[str, Any],
        skill: RunbookSkill,
    ) -> DiagnosticHypothesis: ...

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
            "你是 AIOps 协调规划智能体（Coordinator Agent）。请结合告警信息和匹配的 Runbook Skill 制定排查计划，"
            "并仅输出一个 JSON 对象，包含以下键："
            '"summary"（使用简体中文撰写简明分诊与排障计划）、'
            '"log_tool"（必须原样保留匹配的只读日志工具名）、'
            '"db_tool"（必须原样保留匹配的只读数据库/缓存工具名）。'
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
            f"你是 {agent_role} AIOps 只读诊断智能体。请从匹配的 Runbook Skill 中选择唯一对应的 READ_ONLY 只读诊断工具。"
            "仅输出一个 JSON 对象，包含以下键："
            '"tool_name"（原样保留精确工具名）和 "reasoning"（使用简体中文简述选择依据）。'
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

    async def diagnose_specialist_hypothesis(
        self,
        *,
        agent_id: str,
        fault_type: str,
        service_name: str,
        evidence: dict[str, Any],
        alert_payload: dict[str, Any],
        skill: RunbookSkill,
    ) -> DiagnosticHypothesis:
        system_prompt = (
            f"你是 {agent_id} 专业诊断智能体。请基于当前故障上下文、只读遥测证据与匹配的 Runbook Skill 独立给出根因假设。"
            "仅输出一个 JSON 对象，包含以下键："
            '"root_cause_label"（标准英文故障标签，如 db_connection_pool_exhausted）、'
            '"confidence"（0.0 到 1.0 之间的浮点数置信度）、'
            '"evidence_summary"（使用简体中文简述支持该假设的关键证据）。'
        )
        user_payload = {
            "agent_id": agent_id,
            "fault_type": fault_type,
            "service_name": service_name,
            "evidence": evidence,
            "alert_payload": alert_payload,
            "skill_id": skill.skill_id,
        }
        validated = await self._complete_json(
            system_prompt=system_prompt,
            user_payload=user_payload,
            schema=_SpecialistHypothesisSchema,
        )
        assert isinstance(validated, _SpecialistHypothesisSchema)
        return DiagnosticHypothesis(
            agent_id=agent_id,
            root_cause_label=validated.root_cause_label,
            confidence=validated.confidence,
            weight=1.0,
            evidence_summary=validated.evidence_summary,
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
            "你是 AIOps 修复方案生成智能体（Remediation Agent）。请综合共享黑板中的只读诊断证据与 Runbook Skill 模板，"
            "仅输出一个 JSON 对象，包含以下键："
            '"root_cause_summary"（使用简体中文精准总结根因与关键指标）、'
            '"tool_name"（原样保留精确高危修复工具名）、'
            '"proposed_command"（原样保留可执行修复命令）、'
            '"blast_radius"（使用简体中文说明影响面）、'
            '"rollback_command"（原样保留回滚命令）、'
            '"rationale"（使用简体中文说明方案依据与风险提示）。'
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

    async def diagnose_specialist_hypothesis(
        self,
        *,
        agent_id: str,
        fault_type: str,
        service_name: str,
        evidence: dict[str, Any],
        alert_payload: dict[str, Any],
        skill: RunbookSkill,
    ) -> DiagnosticHypothesis:
        fault_mode = alert_payload.get("_mock_fault_mode", "normal")
        if fault_mode == "consensus_conflict":
            conflict_map = {
                "DB_AGENT": (
                    "db_connection_pool_exhausted",
                    0.90,
                    "DB_AGENT 检测到 PG16 活跃连接打满，判定为数据库连接池耗尽。",
                ),
                "CACHE_AGENT": (
                    "redis_cache_stampede",
                    0.88,
                    "CACHE_AGENT 怀疑 Redis 热点 Key 失效引发缓存击穿，与数据库诊断存在分歧。",
                ),
                "LOG_AGENT": (
                    "upstream_network_partition",
                    0.85,
                    "LOG_AGENT 捕获到网关批量超时日志，怀疑上游网络分区导致级联异常。",
                ),
            }
            label, conf, summary = conflict_map.get(
                agent_id, ("unknown_conflict", 0.50, f"{agent_id} 存在未决诊断分歧。")
            )
            return DiagnosticHypothesis(
                agent_id=agent_id,
                root_cause_label=label,
                confidence=conf,
                weight=1.0,
                evidence_summary=summary,
            )

        if fault_mode == "consensus_low_confidence":
            low_conf = {"DB_AGENT": 0.35, "CACHE_AGENT": 0.28, "LOG_AGENT": 0.22}.get(agent_id, 0.25)
            return DiagnosticHypothesis(
                agent_id=agent_id,
                root_cause_label=fault_type,
                confidence=low_conf,
                weight=1.0,
                evidence_summary=f"{agent_id} 仅观测到弱相关遥测信号，对 `{fault_type}` 的置信度不足。",
            )

        log_ev = evidence.get("log_diagnostics", {})
        db_ev = evidence.get("db_diagnostics", {})

        if fault_type == "db_connection_pool_exhausted":
            mapping = {
                "DB_AGENT": (
                    0.94,
                    f"PG16 连接数打满 ({db_ev.get('total_connections', 200)}/{db_ev.get('max_connections', 200)})，"
                    f"存在 {db_ev.get('idle_in_transaction_count', 14)} 个长时 idle-in-transaction 会话（最长 {db_ev.get('oldest_idle_tx_seconds', 384)}s）。",
                ),
                "LOG_AGENT": (
                    0.86,
                    f"应用日志 5 分钟内出现 {log_ev.get('error_count_5m', 418)} 次 TooManyConnectionsError，"
                    f"与 {service_name} 连接池耗尽特征高度吻合。",
                ),
                "CACHE_AGENT": (
                    0.35,
                    "Redis 7 命中率与延迟处于正常区间，排除缓存雪崩主因，间接佐证瓶颈位于下游数据库连接池。",
                ),
            }
        elif fault_type == "slow_sql_lock_contention":
            mapping = {
                "DB_AGENT": (
                    0.93,
                    f"pg_locks 检测到 PID {db_ev.get('blocking_pids', [41892, 41907])} 持有 "
                    f"{db_ev.get('blocked_relation', 'orders_ledger')} 表 {db_ev.get('lock_mode', 'ExclusiveLock')} 排他锁。",
                ),
                "LOG_AGENT": (
                    0.84,
                    f"慢查询日志显示 {log_ev.get('blocked_queries', 63)} 条请求阻塞，P99 延迟飙升至 {log_ev.get('p99_latency_ms', 14200)}ms。",
                ),
                "CACHE_AGENT": (
                    0.28,
                    "缓存层未出现异常穿透或淘汰风暴，排除 Redis 热点问题，佐证为数据库行/表锁争用。",
                ),
            }
        elif fault_type == "guidance_neo4j_slow_cypher":
            a2a_ev = evidence.get("a2a_diagnosis", {})
            mapping = {
                "DB_AGENT": (
                    0.92,
                    f"通过 A2A 导学适配器采集到 Neo4j 5分钟慢 Cypher 达 {a2a_ev.get('slow_cypher_count_5m', 27)} 次，"
                    f"连接池使用率 {a2a_ev.get('connection_pool_utilization', 0.94)}，存在缺失索引告警。",
                ),
                "LOG_AGENT": (
                    0.85,
                    f"导学服务错误日志显示 {log_ev.get('error_count_5m', 418)} 次图谱查询超时异常。",
                ),
                "CACHE_AGENT": (
                    0.30,
                    "Redis 缓存状态平稳，排除热点缓存击穿，佐证瓶颈位于 Neo4j 图谱慢查询。",
                ),
            }
        elif fault_type == "guidance_rag_retrieval_degradation":
            a2a_ev = evidence.get("a2a_diagnosis", {})
            mapping = {
                "CACHE_AGENT": (
                    0.91,
                    f"通过 A2A 导学适配器观测到 RAG 检索 P99 延迟 {a2a_ev.get('rag_p99_ms', 4910)}ms，"
                    f"空召回率升至 {a2a_ev.get('empty_recall_rate', 0.31)}。",
                ),
                "LOG_AGENT": (
                    0.84,
                    f"网关捕获 {log_ev.get('http_504_count_1m', 219)} 次 504 超时，集中于导学 RAG 检索链路。",
                ),
                "DB_AGENT": (
                    0.35,
                    "关系库与图库连接正常，符合向量检索参数过高引发的检索退化特征。",
                ),
            }
        else:
            mapping = {
                "CACHE_AGENT": (
                    0.95,
                    f"Redis 7 热点 Key `{db_ev.get('expired_hotkey', 'catalog:flash_sale:top100')}` 集中失效，"
                    f"miss_rate 达 {db_ev.get('miss_rate', 0.91)}，回源 QPS 放大 {db_ev.get('db_qps_amplification', '18.4x')}。",
                ),
                "LOG_AGENT": (
                    0.82,
                    f"网关 1 分钟内捕获 {log_ev.get('http_504_count_1m', 219)} 次 HTTP 504，集中于热点接口 {log_ev.get('hot_endpoint', '/api/catalog/flash-sale/items')}。",
                ),
                "DB_AGENT": (
                    0.42,
                    "数据库只读 QPS 短时飙升但无排他锁或连接泄漏，符合上游缓存击穿引发的被动回源特征。",
                ),
            }

        conf, summary = mapping.get(
            agent_id, (0.75, f"{agent_id} 基于 Runbook {skill.skill_id} 佐证根因为 {fault_type}。")
        )
        return DiagnosticHypothesis(
            agent_id=agent_id,
            root_cause_label=fault_type,
            confidence=conf,
            weight=1.0,
            evidence_summary=summary,
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
        elif fault_type == "guidance_neo4j_slow_cypher":
            a2a_ev = blackboard.get("a2a_diagnosis", {})
            root_cause = (
                f"{service_name} Neo4j 知识图谱出现 {a2a_ev.get('slow_cypher_count_5m', 27)} 次慢 Cypher 查询"
                f"（连接池使用率 {a2a_ev.get('connection_pool_utilization', 0.94)}），需补建 Concept 索引并限流。"
            )
        elif fault_type == "guidance_rag_retrieval_degradation":
            a2a_ev = blackboard.get("a2a_diagnosis", {})
            root_cause = (
                f"{service_name} RAG 检索链路 P99 延迟达 {a2a_ev.get('rag_p99_ms', 4910)}ms，"
                f"空召回率 {a2a_ev.get('empty_recall_rate', 0.31)}，需下调 efSearch 并开启 BM25 降级。"
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
