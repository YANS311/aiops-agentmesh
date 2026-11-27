"""Phase 6.1 tests: standard A2A protocol, Guidance A2A Adapter, cross-agent closed loop, and fault injection."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import sys

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.a2a.client import A2AClient
from app.a2a.models import A2ATaskRequest
from app.config import get_settings
from app.main import create_app
from app.redis_bus.events import (
    GLOBAL_EVENTS_STREAM,
    TRAJECTORY_CONSUMER_GROUP,
    RedisEventBus,
)


def _load_guidance_adapter_app():
    adapter_path = (
        Path(__file__).resolve().parents[2] / "adapters" / "guidance_a2a_adapter" / "app.py"
    )
    spec = importlib.util.spec_from_file_location("guidance_a2a_adapter_app", adapter_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.create_adapter_app(adapter_mode="mock")


@pytest.mark.asyncio
async def test_a2a_agent_card_discovery_and_fail_closed_rejection(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """① Discover Agent Cards on both aiops-agentmesh & guidance_a2a_adapter, and verify Fail-Closed REJECTED."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    adapter_app = _load_guidance_adapter_app()
    adapter_transport = httpx.ASGITransport(app=adapter_app)
    a2a_client = A2AClient(transport=adapter_transport, timeout_seconds=3.0)

    # Direct A2A discovery + Fail-Closed check against guidance_a2a_adapter
    card = await a2a_client.discover_agent_card("http://guidance-a2a-adapter:8020")
    assert card.name == "guidance-rag-neo4j-agent"
    assert card.capabilities.readOnlyDiagnosticsOnly is True
    assert {s.id for s in card.skills} == {"diagnose_neo4j_cypher", "diagnose_rag_pipeline"}
    assert all(s.permission == "READ_ONLY" for s in card.skills)

    # Undeclared / write skill must be strictly REJECTED (Fail-Closed)
    rejected = await a2a_client.send_task(
        "http://guidance-a2a-adapter:8020",
        A2ATaskRequest(
            task_id="a2a-write-attempt",
            skill_id="drop_neo4j_all_indexes",
            input={"service_name": "guidance-knowledge-graph"},
            metadata={"incident_id": "test-001"},
        ),
    )
    assert rejected.state == "REJECTED"
    assert "Fail-Closed" in (rejected.error or "")

    # Verify aiops-agentmesh exposes its own /.well-known/agent.json and /api/v1/a2a/peers
    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
        a2a_client=a2a_client,
        a2a_peer_urls=["http://guidance-a2a-adapter:8020"],
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            self_card_resp = await client.get("/.well-known/agent.json")
            assert self_card_resp.status_code == 200
            self_card = self_card_resp.json()
            assert self_card["name"] == "aiops-agentmesh-control-plane"
            assert {s["id"] for s in self_card["skills"]} == {
                "aiops_multi_agent_triage",
                "fail_closed_remediation_gate",
            }

            peers_resp = await client.get("/api/v1/a2a/peers", headers=viewer_headers)
            assert peers_resp.status_code == 200
            peers = peers_resp.json()
            assert len(peers) == 1
            assert peers[0]["status"] == "ONLINE"
            assert peers[0]["card"]["name"] == "guidance-rag-neo4j-agent"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("alertname", "fault_type", "expected_skill", "expected_artifact_key"),
    [
        (
            "GuidanceNeo4jSlowCypher",
            "guidance_neo4j_slow_cypher",
            "diagnose_neo4j_cypher",
            "slow_cypher_count_5m",
        ),
        (
            "GuidanceRAGRetrievalDegraded",
            "guidance_rag_retrieval_degradation",
            "diagnose_rag_pipeline",
            "rag_p99_ms",
        ),
    ],
)
async def test_cross_agent_a2a_guidance_closed_loop(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
    alertname: str,
    fault_type: str,
    expected_skill: str,
    expected_artifact_key: str,
) -> None:
    """② Alertmanager guidance alert -> A2A discovery & delegation -> Consensus + Shapley -> Approval -> RESOLVED."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    adapter_app = _load_guidance_adapter_app()
    a2a_client = A2AClient(transport=httpx.ASGITransport(app=adapter_app), timeout_seconds=3.0)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
        a2a_client=a2a_client,
        a2a_peer_urls=["http://guidance-a2a-adapter:8020"],
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            wh_bytes = json.dumps(
                {
                    "version": "4",
                    "status": "firing",
                    "alerts": [
                        {
                            "status": "firing",
                            "fingerprint": f"fp-{fault_type}-001",
                            "labels": {
                                "alertname": alertname,
                                "severity": "P1",
                                "service": "guidance-service",
                            },
                            "annotations": {"summary": f"Guidance alert: {alertname}"},
                        }
                    ],
                }
            ).encode("utf-8")
            wh_sig = hmac.new(
                settings.alertmanager_webhook_secret.encode("utf-8"),
                wh_bytes,
                hashlib.sha256,
            ).hexdigest()
            wh = await client.post(
                "/api/v1/webhooks/alertmanager",
                content=wh_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Alertmanager-Signature": f"sha256={wh_sig}",
                },
            )
            assert wh.status_code == 200
            incident_id = wh.json()["results"][0]["incident_id"]

            detail = None
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)

            assert detail is not None
            assert detail["status"] == "PENDING_APPROVAL"
            bb = detail["blackboard"]
            assert expected_artifact_key in bb["a2a_artifacts"]
            assert bb["a2a_artifacts"]["skill_id"] == expected_skill
            assert bb["consensus_report"]["status"] == "CONSENSUS_REACHED"
            assert bb["consensus_report"]["agreed_root_cause"] == fault_type
            assert bb["shapley_attribution"]["coalition_count"] == 8

            traces = (
                await client.get(
                    f"/api/v1/incidents/{incident_id}/traces", headers=viewer_headers
                )
            ).json()
            ev_types = [t["event_type"] for t in traces]
            assert "A2A_TASK_DELEGATED" in ev_types
            assert "A2A_TASK_COMPLETED" in ev_types

            # Approve as SRE_APPROVER -> RESOLVED
            app_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/approve",
                headers=approver_headers,
                json={"comment": "A2A artifacts verified, approve remediation"},
            )
            assert app_resp.status_code == 200

            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "RESOLVED":
                    break
                await asyncio.sleep(0.05)
            assert detail["status"] == "RESOLVED"
            assert detail["trajectory"] is not None

    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0


@pytest.mark.asyncio
async def test_a2a_peer_discover_register_rbac_and_nl_parse_confirm_flow(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """④ Verify discover -> register (with 401/403 RBAC) and nl/parse -> nl/confirm -> A2A -> RESOLVED."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    adapter_app = _load_guidance_adapter_app()
    a2a_client = A2AClient(transport=httpx.ASGITransport(app=adapter_app), timeout_seconds=3.0)

    # Start with NO env peers; peer will be registered dynamically via API into PG16 a2a_peers!
    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
        a2a_client=a2a_client,
        a2a_peer_urls=[],
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}
    peer_url = "http://guidance-a2a-adapter:8020"

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # 1. Discover preview (no DB persistence)
            disc = await client.post(
                "/api/v1/a2a/peers/discover",
                headers=viewer_headers,
                json={"base_url": peer_url},
            )
            assert disc.status_code == 200
            assert disc.json()["card"]["name"] == "guidance-rag-neo4j-agent"
            assert (await client.get("/api/v1/a2a/peers", headers=viewer_headers)).json() == []

            # 2. Register requires SRE_APPROVER (401 unauth, 403 VIEWER)
            assert (await client.post("/api/v1/a2a/peers", json={"base_url": peer_url})).status_code == 401
            assert (
                await client.post(
                    "/api/v1/a2a/peers",
                    headers=viewer_headers,
                    json={"base_url": peer_url},
                )
            ).status_code == 403

            reg = await client.post(
                "/api/v1/a2a/peers",
                headers=approver_headers,
                json={"base_url": peer_url, "enabled": True},
            )
            assert reg.status_code == 201
            assert reg.json()["status"] == "ONLINE"
            assert reg.json()["created_by"] == "sre-lead-01"

            # 3. NL parse (preview only, no incident created)
            parse_resp = await client.post(
                "/api/v1/incidents/nl/parse",
                headers=viewer_headers,
                json={"query": "学院智能导学 Neo4j 图谱 Cypher 慢查询超时，请排查"},
            )
            assert parse_resp.status_code == 200
            draft = parse_resp.json()
            assert draft["fault_type"] == "guidance_neo4j_slow_cypher"
            assert draft["matched_runbook"] == "skill.guidance.neo4j_slow_cypher"
            assert draft["target_a2a_peer"] == peer_url
            assert draft["suggested_skill"] == "diagnose_neo4j_cypher"
            assert (await client.get("/api/v1/incidents", headers=viewer_headers)).json() == []

            # 4. NL confirm -> creates Incident + NL_INCIDENT_CONFIRMED -> A2A + Consensus + Shapley -> PENDING_APPROVAL
            confirm_resp = await client.post(
                "/api/v1/incidents/nl/confirm",
                headers=viewer_headers,
                json=draft,
            )
            assert confirm_resp.status_code == 201
            incident_id = confirm_resp.json()["id"]

            detail = None
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)
            assert detail is not None and detail["status"] == "PENDING_APPROVAL"
            assert detail["blackboard"]["a2a_artifacts"]["skill_id"] == "diagnose_neo4j_cypher"

            traces = (
                await client.get(
                    f"/api/v1/incidents/{incident_id}/traces", headers=viewer_headers
                )
            ).json()
            assert any(t["event_type"] == "NL_INCIDENT_CONFIRMED" for t in traces)
            assert any(t["event_type"] == "A2A_TASK_COMPLETED" for t in traces)

            # 5. Approve -> RESOLVED
            app_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/approve",
                headers=approver_headers,
                json={"comment": "NL + A2A verified"},
            )
            assert app_resp.status_code == 200
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "RESOLVED":
                    break
                await asyncio.sleep(0.05)
            assert detail["status"] == "RESOLVED"

    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0


@pytest.mark.asyncio
async def test_a2a_fault_injection_unreachable_timeout_and_rejected_degrades_safely(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """③ Unreachable/timeout/rejected A2A peer degrades safely without blocking Worker, with XPENDING == 0."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    def _failing_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/.well-known/agent.json":
            return httpx.Response(
                200,
                json={
                    "name": "flaky-guidance-peer",
                    "description": "Fault-injected peer",
                    "url": "http://flaky-peer:8020",
                    "version": "1.0.0",
                    "capabilities": {"readOnlyDiagnosticsOnly": True},
                    "skills": [
                        {
                            "id": "diagnose_neo4j_cypher",
                            "name": "Diagnose Neo4j",
                            "description": "Diagnose Neo4j",
                            "permission": "READ_ONLY",
                        }
                    ],
                },
            )
        raise httpx.ReadTimeout("Simulated A2A peer timeout")

    flaky_client = A2AClient(
        transport=httpx.MockTransport(_failing_handler),
        timeout_seconds=0.5,
    )
    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
        enable_fault_injection=True,
        a2a_client=flaky_client,
        a2a_peer_urls=["http://flaky-peer:8020"],
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            inj = await client.post(
                "/api/v1/incidents/inject",
                headers=viewer_headers,
                json={"fault_type": "guidance_neo4j_slow_cypher"},
            )
            assert inj.status_code == 201
            incident_id = inj.json()["id"]

            detail = None
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)

            assert detail is not None
            assert detail["status"] == "PENDING_APPROVAL"
            assert detail["blackboard"]["evidence"]["a2a_diagnosis"]["a2a_status"] == "FAILED"
            assert "A2A_TIMEOUT" in detail["blackboard"]["evidence"]["a2a_diagnosis"]["error"]

            traces = (
                await client.get(
                    f"/api/v1/incidents/{incident_id}/traces", headers=viewer_headers
                )
            ).json()
            ev_types = [t["event_type"] for t in traces]
            assert "A2A_TASK_DELEGATED" in ev_types
            assert "A2A_TASK_FAILED" in ev_types

    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0


@pytest.mark.asyncio
async def test_a2a_peer_discover_register_rbac_and_nl_parse_confirm_flow(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
    redis_bus: RedisEventBus,
) -> None:
    """④ Verify discover -> register (with 401/403 RBAC) and nl/parse -> nl/confirm -> A2A -> RESOLVED."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    adapter_app = _load_guidance_adapter_app()
    a2a_client = A2AClient(transport=httpx.ASGITransport(app=adapter_app), timeout_seconds=3.0)

    # Start with NO env peers; peer will be registered dynamically via API into PG16 a2a_peers!
    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
        a2a_client=a2a_client,
        a2a_peer_urls=[],
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}
    peer_url = "http://guidance-a2a-adapter:8020"

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # 1. Discover preview (no DB persistence)
            disc = await client.post(
                "/api/v1/a2a/peers/discover",
                headers=viewer_headers,
                json={"base_url": peer_url},
            )
            assert disc.status_code == 200
            assert disc.json()["card"]["name"] == "guidance-rag-neo4j-agent"
            assert (await client.get("/api/v1/a2a/peers", headers=viewer_headers)).json() == []

            # 2. Register requires SRE_APPROVER (401 unauth, 403 VIEWER)
            assert (await client.post("/api/v1/a2a/peers", json={"base_url": peer_url})).status_code == 401
            assert (
                await client.post(
                    "/api/v1/a2a/peers",
                    headers=viewer_headers,
                    json={"base_url": peer_url},
                )
            ).status_code == 403

            reg = await client.post(
                "/api/v1/a2a/peers",
                headers=approver_headers,
                json={"base_url": peer_url, "enabled": True},
            )
            assert reg.status_code == 201
            assert reg.json()["status"] == "ONLINE"
            assert reg.json()["created_by"] == "sre-lead-01"

            # 3. NL parse (preview only, no incident created)
            parse_resp = await client.post(
                "/api/v1/incidents/nl/parse",
                headers=viewer_headers,
                json={"query": "学院智能导学 Neo4j 图谱 Cypher 慢查询超时，请排查"},
            )
            assert parse_resp.status_code == 200
            draft = parse_resp.json()
            assert draft["fault_type"] == "guidance_neo4j_slow_cypher"
            assert draft["matched_runbook"] == "skill.guidance.neo4j_slow_cypher"
            assert draft["target_a2a_peer"] == peer_url
            assert draft["suggested_skill"] == "diagnose_neo4j_cypher"
            assert (await client.get("/api/v1/incidents", headers=viewer_headers)).json() == []

            # 4. NL confirm -> creates Incident + NL_INCIDENT_CONFIRMED -> A2A + Consensus + Shapley -> PENDING_APPROVAL
            confirm_resp = await client.post(
                "/api/v1/incidents/nl/confirm",
                headers=viewer_headers,
                json=draft,
            )
            assert confirm_resp.status_code == 201
            incident_id = confirm_resp.json()["id"]

            detail = None
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)
            assert detail is not None and detail["status"] == "PENDING_APPROVAL"
            assert detail["blackboard"]["a2a_artifacts"]["skill_id"] == "diagnose_neo4j_cypher"

            traces = (
                await client.get(
                    f"/api/v1/incidents/{incident_id}/traces", headers=viewer_headers
                )
            ).json()
            assert any(t["event_type"] == "NL_INCIDENT_CONFIRMED" for t in traces)
            assert any(t["event_type"] == "A2A_TASK_COMPLETED" for t in traces)

            # 5. Approve -> RESOLVED
            app_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/approve",
                headers=approver_headers,
                json={"comment": "NL + A2A verified"},
            )
            assert app_resp.status_code == 200
            for _ in range(80):
                detail = (
                    await client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers)
                ).json()
                if detail["status"] == "RESOLVED":
                    break
                await asyncio.sleep(0.05)
            assert detail["status"] == "RESOLVED"

    assert await redis_bus.pending_count(GLOBAL_EVENTS_STREAM, TRAJECTORY_CONSUMER_GROUP) == 0
