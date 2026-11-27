"""Phase 4 tests: multi-agent parallel diagnosis, Consensus Arbiter Fail-Closed, and exact Shapley axioms."""

from __future__ import annotations

import asyncio
import math
import os

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.consensus import ConsensusArbiter, SPECIALIST_AGENT_ROLES
from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.shapley import DiagnosticHypothesis, compute_exact_shapley
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.config import get_settings
from app.db.repositories import (
    get_incident_full,
    get_trajectory_by_incident,
    list_trace_events,
)
from app.main import create_app
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry


def _build_orchestrator(
    session_factory: async_sessionmaker[AsyncSession],
    redis_bus: RedisEventBus,
) -> tuple[AIOpsOrchestrator, AgentWorker]:
    settings = get_settings()
    consumer = TrajectoryStreamConsumer(
        session_factory=session_factory,
        redis_bus=redis_bus,
        consumer_name="phase4-archiver",
    )
    orch = AIOpsOrchestrator(
        session_factory=session_factory,
        redis_bus=redis_bus,
        skill_registry=SkillRegistry(settings.skills_dir),
        tool_registry=ToolRegistry(),
        llm_provider=MockLLMProvider(),
        step_delay_ms=0,
        trajectory_consumer=consumer,
    )
    return orch, AgentWorker(orch, worker_id="phase4-worker", trajectory_consumer=consumer)


@pytest.mark.asyncio
async def test_parallel_diagnosis_consensus_reached_and_archived_in_trajectory(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """① 3 specialist agents run in parallel, reach consensus, and archive consensus + Shapley in Trajectory."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
    )
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}

    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            inj = await client.post(
                "/api/v1/incidents/inject",
                json={"fault_type": "redis_cache_stampede"},
            )
            assert inj.status_code == 201
            incident_id = inj.json()["id"]

            detail = None
            for _ in range(60):
                detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)

            assert detail is not None
            assert detail["status"] == "PENDING_APPROVAL"

            consensus = detail["blackboard"]["consensus_report"]
            shapley = detail["blackboard"]["shapley_attribution"]

            assert consensus["status"] == "CONSENSUS_REACHED"
            assert consensus["agreed_root_cause"] == "redis_cache_stampede"
            assert consensus["consensus_confidence"] >= consensus["confidence_threshold"]
            assert [h["agent_id"] for h in consensus["hypotheses"]] == list(SPECIALIST_AGENT_ROLES)

            assert shapley["coalition_count"] == 8
            assert math.isclose(
                shapley["efficiency_sum"],
                shapley["grand_coalition_value"] - shapley["empty_coalition_value"],
                abs_tol=1e-5,
            )
            by_agent = {a["agent_id"]: a for a in shapley["attributions"]}
            # For redis_cache_stampede, CACHE_AGENT has highest confidence -> highest Shapley contribution.
            assert by_agent["CACHE_AGENT"]["shapley_value"] > by_agent["LOG_AGENT"]["shapley_value"]
            assert by_agent["LOG_AGENT"]["shapley_value"] > by_agent["DB_AGENT"]["shapley_value"]

            # Approve and wait for Trajectory archival
            app_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/approve",
                headers=approver_headers,
                json={"comment": "Phase 4 consensus verified"},
            )
            assert app_resp.status_code == 200

            for _ in range(60):
                detail = (await client.get(f"/api/v1/incidents/{incident_id}")).json()
                if detail["status"] == "RESOLVED":
                    break
                await asyncio.sleep(0.05)

            assert detail["status"] == "RESOLVED"
            traj_doc = detail["trajectory"]["trajectory_doc"]
            assert traj_doc["consensus_report"]["status"] == "CONSENSUS_REACHED"
            assert traj_doc["consensus_report"]["agreed_root_cause"] == "redis_cache_stampede"
            assert traj_doc["shapley_attribution"]["coalition_count"] == 8
            assert len(traj_doc["shapley_attribution"]["attributions"]) == 3


@pytest.mark.asyncio
async def test_consensus_conflict_and_low_confidence_fail_closed_blocks_remediation(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """② Fault-injected severe agent disagreement or low confidence MUST Fail-Closed without creating remediation."""
    _, session_factory = pg_engine_and_factory
    orch, worker = _build_orchestrator(session_factory, redis_bus)

    for fault_mode in ("consensus_conflict", "consensus_low_confidence"):
        incident = await orch.inject_incident(
            fault_type="db_connection_pool_exhausted",
            alert_payload={"_mock_fault_mode": fault_mode},
        )
        processed = await worker.run_until_quiescent()
        assert processed == 4  # COORDINATOR, LOG_DIAGNOSTICS, DB_DIAGNOSTICS, REMEDIATION (blocked)

        async with session_factory() as sess:
            final_inc = await get_incident_full(sess, incident.id)
            traces = await list_trace_events(sess, incident.id)
            traj = await get_trajectory_by_incident(sess, incident.id)

        assert final_inc is not None
        assert final_inc.status == "NEEDS_HUMAN_REVIEW"
        assert final_inc.approval is None  # Strictly NO high-risk remediation generated
        assert not any(t.agent_role == "EXECUTOR" for t in final_inc.tasks)
        assert traj is None

        consensus = final_inc.blackboard["consensus_report"]
        assert consensus["status"] == "CONFLICT_FAIL_CLOSED"
        assert consensus["agreed_root_cause"] is None
        assert len(consensus["hypotheses"]) == 3

        event_types = [t.event_type for t in traces]
        assert "CONSENSUS_CONFLICT_ALERT" in event_types
        assert "REMEDIATION_PROPOSED" not in event_types
        assert "HIGH_RISK_EXECUTED" not in event_types


def test_shapley_exact_axioms_efficiency_symmetry_and_null_player() -> None:
    """③ Verify exact 2^3 Shapley mathematical properties: efficiency, symmetry, and zero-contribution null player."""
    # Case A: Symmetry (DB_AGENT & LOG_AGENT identical) + Null Player (CACHE_AGENT dissents)
    hypotheses_sym = [
        DiagnosticHypothesis(
            agent_id="DB_AGENT",
            root_cause_label="db_connection_pool_exhausted",
            confidence=0.80,
            evidence_summary="DB saturation",
        ),
        DiagnosticHypothesis(
            agent_id="LOG_AGENT",
            root_cause_label="db_connection_pool_exhausted",
            confidence=0.80,
            evidence_summary="Log TooManyConnections",
        ),
        DiagnosticHypothesis(
            agent_id="CACHE_AGENT",
            root_cause_label="redis_cache_stampede",  # Dissents -> zero contribution to target
            confidence=0.90,
            evidence_summary="Dissenting cache hypothesis",
        ),
    ]
    report_sym = compute_exact_shapley(hypotheses_sym, target_root_cause="db_connection_pool_exhausted")

    assert report_sym.coalition_count == 8  # Exact 2^3 subsets
    assert report_sym.empty_coalition_value == 0.0
    # Efficiency axiom: sum(phi_i) == v(N) - v(empty)
    expected_v_n = 1.0 - (1.0 - 0.80) * (1.0 - 0.80)  # 0.96
    assert math.isclose(report_sym.grand_coalition_value, expected_v_n, abs_tol=1e-6)
    assert math.isclose(
        report_sym.efficiency_sum,
        report_sym.grand_coalition_value - report_sym.empty_coalition_value,
        abs_tol=1e-6,
    )
    assert report_sym.efficiency_delta < 1e-9

    by_id = {a.agent_id: a for a in report_sym.attributions}
    # Symmetry axiom: identical agents receive identical Shapley values
    assert math.isclose(by_id["DB_AGENT"].shapley_value, by_id["LOG_AGENT"].shapley_value, abs_tol=1e-9)
    assert math.isclose(by_id["DB_AGENT"].normalized_share, 0.5, abs_tol=1e-6)
    assert math.isclose(by_id["LOG_AGENT"].normalized_share, 0.5, abs_tol=1e-6)
    # Null player axiom: dissenting agent receives strictly 0
    assert by_id["CACHE_AGENT"].shapley_value == 0.0
    assert by_id["CACHE_AGENT"].normalized_share == 0.0
    assert by_id["CACHE_AGENT"].supports_target is False

    # Case B: Zero-confidence supporting agent also gets strictly 0; shares sum to 1.0
    hypotheses_zero = [
        DiagnosticHypothesis(
            agent_id="DB_AGENT",
            root_cause_label="slow_sql_lock_contention",
            confidence=0.90,
            evidence_summary="Strong lock evidence",
        ),
        DiagnosticHypothesis(
            agent_id="LOG_AGENT",
            root_cause_label="slow_sql_lock_contention",
            confidence=0.50,
            evidence_summary="Moderate slow-query evidence",
        ),
        DiagnosticHypothesis(
            agent_id="CACHE_AGENT",
            root_cause_label="slow_sql_lock_contention",
            confidence=0.0,
            evidence_summary="No telemetry available",
        ),
    ]
    report_zero = compute_exact_shapley(hypotheses_zero, target_root_cause="slow_sql_lock_contention")
    by_id_z = {a.agent_id: a for a in report_zero.attributions}
    assert by_id_z["CACHE_AGENT"].shapley_value == 0.0
    assert by_id_z["CACHE_AGENT"].normalized_share == 0.0
    assert by_id_z["DB_AGENT"].shapley_value > by_id_z["LOG_AGENT"].shapley_value > 0.0
    assert math.isclose(
        sum(a.normalized_share for a in report_zero.attributions), 1.0, abs_tol=1e-5
    )

    # Arbiter sanity check: unanimous strong consensus passes
    arbiter = ConsensusArbiter()
    consensus = arbiter.arbitrate(hypotheses_zero[:2])
    assert consensus.status == "CONSENSUS_REACHED"
    assert consensus.agreed_root_cause == "slow_sql_lock_contention"
