"""Multi-agent parallel diagnosis & weighted Consensus Arbiter (Fail-Closed on conflict)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel

from app.agents.llm import LLMProvider
from app.agents.shapley import (
    DiagnosticHypothesis,
    ShapleyAttributionReport,
    compute_exact_shapley,
)
from app.skills.loader import RunbookSkill

SPECIALIST_AGENT_ROLES: tuple[str, ...] = ("DB_AGENT", "CACHE_AGENT", "LOG_AGENT")

DEFAULT_CONFIDENCE_THRESHOLD = 0.60
DEFAULT_MAX_DISAGREEMENT = 0.40


class ConsensusReport(BaseModel):
    """Structured output produced by the Consensus Arbiter."""

    status: Literal["CONSENSUS_REACHED", "CONFLICT_FAIL_CLOSED"]
    agreed_root_cause: str | None
    top_candidate_root_cause: str
    consensus_confidence: float
    agreement_ratio: float
    disagreement_score: float
    confidence_threshold: float
    max_disagreement: float
    reason: str
    label_scores: dict[str, float]
    hypotheses: list[DiagnosticHypothesis]

    def to_json_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ConsensusArbiter:
    """Weighted consensus aggregator with Fail-Closed conflict detection."""

    def __init__(
        self,
        *,
        confidence_threshold: float = DEFAULT_CONFIDENCE_THRESHOLD,
        max_disagreement: float = DEFAULT_MAX_DISAGREEMENT,
    ) -> None:
        self.confidence_threshold = confidence_threshold
        self.max_disagreement = max_disagreement

    def arbitrate(self, hypotheses: Sequence[DiagnosticHypothesis]) -> ConsensusReport:
        items = list(hypotheses)
        if not items:
            return ConsensusReport(
                status="CONFLICT_FAIL_CLOSED",
                agreed_root_cause=None,
                top_candidate_root_cause="unknown",
                consensus_confidence=0.0,
                agreement_ratio=0.0,
                disagreement_score=1.0,
                confidence_threshold=self.confidence_threshold,
                max_disagreement=self.max_disagreement,
                reason="未收到任何诊断 Agent 的有效假设，触发 Fail-Closed 挂起等待人工研判。",
                label_scores={},
                hypotheses=[],
            )

        label_scores: dict[str, float] = {}
        total_weight = 0.0
        for hyp in items:
            weighted_conf = float(hyp.weight) * float(hyp.confidence)
            label_scores[hyp.root_cause_label] = (
                label_scores.get(hyp.root_cause_label, 0.0) + weighted_conf
            )
            total_weight += float(hyp.weight)

        total_mass = sum(label_scores.values())
        top_label = max(label_scores.items(), key=lambda kv: (kv[1], kv[0]))[0]
        top_score = label_scores[top_label]

        agreement_ratio = (top_score / total_mass) if total_mass > 0.0 else 0.0
        disagreement_score = max(0.0, 1.0 - agreement_ratio)
        consensus_confidence = (top_score / total_weight) if total_weight > 0.0 else 0.0

        rounded_scores = {k: round(v, 4) for k, v in label_scores.items()}

        has_conflict = disagreement_score > self.max_disagreement
        below_threshold = consensus_confidence < self.confidence_threshold

        if has_conflict or below_threshold:
            reasons: list[str] = []
            if has_conflict:
                reasons.append(
                    f"多智能体诊断意见存在严重分歧（分歧度 {disagreement_score:.2f} > 阈值 {self.max_disagreement:.2f}）"
                )
            if below_threshold:
                reasons.append(
                    f"最高候选根因 `{top_label}` 加权共识置信度不足（{consensus_confidence:.2f} < 阈值 {self.confidence_threshold:.2f}）"
                )
            return ConsensusReport(
                status="CONFLICT_FAIL_CLOSED",
                agreed_root_cause=None,
                top_candidate_root_cause=top_label,
                consensus_confidence=round(consensus_confidence, 4),
                agreement_ratio=round(agreement_ratio, 4),
                disagreement_score=round(disagreement_score, 4),
                confidence_threshold=self.confidence_threshold,
                max_disagreement=self.max_disagreement,
                reason="；".join(reasons) + "。已触发 Fail-Closed 共识冲突告警，强制挂起等待人工研判，严禁盲目生成高危修复。",
                label_scores=rounded_scores,
                hypotheses=items,
            )

        return ConsensusReport(
            status="CONSENSUS_REACHED",
            agreed_root_cause=top_label,
            top_candidate_root_cause=top_label,
            consensus_confidence=round(consensus_confidence, 4),
            agreement_ratio=round(agreement_ratio, 4),
            disagreement_score=round(disagreement_score, 4),
            confidence_threshold=self.confidence_threshold,
            max_disagreement=self.max_disagreement,
            reason=(
                f"多智能体达成共识：确认根因为 `{top_label}`"
                f"（加权共识置信度 {consensus_confidence:.2f}，一致率 {agreement_ratio * 100:.1f}%）。"
            ),
            label_scores=rounded_scores,
            hypotheses=items,
        )


async def run_parallel_diagnosis_and_attribution(
    *,
    llm_provider: LLMProvider,
    fault_type: str,
    service_name: str,
    evidence: dict[str, Any],
    alert_payload: dict[str, Any],
    skill: RunbookSkill,
    arbiter: ConsensusArbiter | None = None,
) -> tuple[ConsensusReport, ShapleyAttributionReport]:
    """Dispatch DB_AGENT, CACHE_AGENT, LOG_AGENT in parallel, arbitrate consensus, and compute exact Shapley."""
    hypotheses = list(
        await asyncio.gather(
            *(
                llm_provider.diagnose_specialist_hypothesis(
                    agent_id=agent_id,
                    fault_type=fault_type,
                    service_name=service_name,
                    evidence=evidence,
                    alert_payload=alert_payload,
                    skill=skill,
                )
                for agent_id in SPECIALIST_AGENT_ROLES
            )
        )
    )
    active_arbiter = arbiter or ConsensusArbiter()
    consensus = active_arbiter.arbitrate(hypotheses)
    target_label = consensus.agreed_root_cause or consensus.top_candidate_root_cause
    shapley = compute_exact_shapley(hypotheses, target_root_cause=target_label)
    return consensus, shapley
