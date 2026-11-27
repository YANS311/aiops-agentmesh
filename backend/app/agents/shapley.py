"""Exact Shapley value attribution over all 2^N coalitions for multi-agent diagnosis."""

from __future__ import annotations

from collections.abc import Sequence
from math import factorial
from typing import Any

from pydantic import BaseModel, Field


class DiagnosticHypothesis(BaseModel):
    """Structured hypothesis emitted by an individual specialist diagnostic agent."""

    agent_id: str = Field(min_length=1)
    root_cause_label: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    weight: float = Field(default=1.0, gt=0.0)
    evidence_summary: str = Field(min_length=1)


class AgentShapleyAttribution(BaseModel):
    """Exact marginal contribution and normalized share for one diagnostic agent."""

    agent_id: str
    root_cause_label: str
    confidence: float
    supports_target: bool
    shapley_value: float
    normalized_share: float


class ShapleyAttributionReport(BaseModel):
    """Full exact Shapley attribution report across all 2^N coalitions."""

    target_root_cause: str
    coalition_count: int
    empty_coalition_value: float
    grand_coalition_value: float
    efficiency_sum: float
    efficiency_delta: float
    coalition_values: dict[str, float]
    attributions: list[AgentShapleyAttribution]

    def to_json_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def coalition_confidence_gain(
    subset: Sequence[DiagnosticHypothesis], target_root_cause: str
) -> float:
    """Characteristic function v(S) measuring coalition confidence gain on target_root_cause.

    Uses noisy-OR independent evidence aggregation:
        v(S) = 1 - prod_{i in S} (1 - p_i)
    where p_i = confidence_i if agent i supports target_root_cause else 0.0.

    Mathematical properties guaranteed:
      - v(empty) = 0.0
      - Monotonicity: S <= T => v(S) <= v(T)
      - Symmetry: identical (root_cause_label, confidence) => identical Shapley values
      - Null player: non-supporting or 0-confidence agent => Shapley value == 0.0
      - Efficiency: sum_i phi_i == v(N) - v(empty)
    """
    if not subset:
        return 0.0
    complement = 1.0
    for hyp in subset:
        if hyp.root_cause_label == target_root_cause:
            p = min(1.0, max(0.0, float(hyp.confidence)))
            complement *= 1.0 - p
    return 1.0 - complement


def compute_exact_shapley(
    hypotheses: Sequence[DiagnosticHypothesis],
    target_root_cause: str,
) -> ShapleyAttributionReport:
    """Compute exact Shapley values by enumerating all 2^N coalitions."""
    agents = list(hypotheses)
    n = len(agents)
    if n == 0:
        return ShapleyAttributionReport(
            target_root_cause=target_root_cause,
            coalition_count=1,
            empty_coalition_value=0.0,
            grand_coalition_value=0.0,
            efficiency_sum=0.0,
            efficiency_delta=0.0,
            coalition_values={"∅": 0.0},
            attributions=[],
        )

    n_fact = factorial(n)
    mask_values: dict[int, float] = {}
    coalition_values: dict[str, float] = {}

    for mask in range(1 << n):
        subset = [agents[i] for i in range(n) if (mask & (1 << i))]
        val = coalition_confidence_gain(subset, target_root_cause)
        mask_values[mask] = val
        key = "+".join(h.agent_id for h in subset) if subset else "∅"
        coalition_values[key] = round(val, 6)

    raw_phi: list[float] = []
    for i in range(n):
        bit_i = 1 << i
        phi_i = 0.0
        for mask in range(1 << n):
            if mask & bit_i:
                continue
            s_size = mask.bit_count()
            weight = (factorial(s_size) * factorial(n - s_size - 1)) / n_fact
            marginal = mask_values[mask | bit_i] - mask_values[mask]
            phi_i += weight * marginal
        raw_phi.append(phi_i)

    empty_val = mask_values[0]
    grand_val = mask_values[(1 << n) - 1]
    efficiency_sum = sum(raw_phi)
    efficiency_delta = abs(efficiency_sum - (grand_val - empty_val))

    total_positive = sum(max(0.0, v) for v in raw_phi)
    attributions: list[AgentShapleyAttribution] = []
    for hyp, phi_i in zip(agents, raw_phi, strict=True):
        share = (max(0.0, phi_i) / total_positive) if total_positive > 0.0 else 0.0
        attributions.append(
            AgentShapleyAttribution(
                agent_id=hyp.agent_id,
                root_cause_label=hyp.root_cause_label,
                confidence=round(float(hyp.confidence), 4),
                supports_target=hyp.root_cause_label == target_root_cause,
                shapley_value=round(phi_i, 6),
                normalized_share=round(share, 6),
            )
        )

    return ShapleyAttributionReport(
        target_root_cause=target_root_cause,
        coalition_count=1 << n,
        empty_coalition_value=round(empty_val, 6),
        grand_coalition_value=round(grand_val, 6),
        efficiency_sum=round(efficiency_sum, 6),
        efficiency_delta=round(efficiency_delta, 10),
        coalition_values=coalition_values,
        attributions=attributions,
    )
