"""Phase 6 Part 2 tests: all-route RBAC, Alertmanager HMAC, fault-injection gate, approval TTL/hash, production Fail-Fast."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from uuid import uuid4

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.agents.orchestrator import AgentWorker
from app.config import Settings, get_settings
from app.db.models import RemediationApproval
from app.db.repositories import list_trace_events
from app.main import create_app


@pytest.mark.asyncio
async def test_all_business_routes_require_auth_and_whitelist_stays_public(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """① Anonymous access to ALL business GET/POST routes returns 401; only /health & /.well-known/agent.json are public."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
        enable_fault_injection=False,
    )
    fake_id = uuid4()

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # Public whitelist ONLY
            assert (await client.get("/api/v1/health")).status_code == 200
            assert (await client.get("/.well-known/agent.json")).status_code == 200

            # Every business route without Bearer token MUST return 401 AUTH_REQUIRED
            unauth_calls = [
                ("GET", "/api/v1/auth/me", None),
                ("GET", "/api/v1/skills", None),
                ("GET", "/api/v1/incidents", None),
                ("GET", f"/api/v1/incidents/{fake_id}", None),
                ("GET", f"/api/v1/incidents/{fake_id}/traces", None),
                ("GET", f"/api/v1/incidents/{fake_id}/events", None),
                ("GET", "/api/v1/trajectories", None),
                ("GET", f"/api/v1/trajectories/{fake_id}", None),
                ("GET", "/api/v1/a2a/peers", None),
                ("POST", "/api/v1/a2a/peers/discover", {"base_url": "http://127.0.0.1:8020"}),
                ("POST", "/api/v1/a2a/peers", {"base_url": "http://127.0.0.1:8020"}),
                ("POST", "/api/v1/incidents/inject", {"fault_type": "db_connection_pool_exhausted"}),
                ("POST", "/api/v1/incidents/nl/parse", {"query": "test"}),
                ("POST", f"/api/v1/incidents/{fake_id}/approve", {"comment": "no-auth"}),
                ("POST", f"/api/v1/incidents/{fake_id}/reject", {"comment": "no-auth"}),
                ("POST", f"/api/v1/incidents/{fake_id}/execute", None),
            ]
            for method, path, body in unauth_calls:
                resp = await client.request(method, path, json=body)
                assert resp.status_code == 401, f"{method} {path} returned {resp.status_code}"
                assert resp.json()["detail"]["code"] == "AUTH_REQUIRED"


@pytest.mark.asyncio
async def test_alertmanager_webhook_hmac_verification_and_fault_injection_disabled_gate(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """② Alertmanager HMAC (401 missing / 403 invalid / 200 valid) + ENABLE_FAULT_INJECTION=false -> 403."""
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
        enable_fault_injection=False,
    )
    viewer_headers = {"Authorization": f"Bearer {settings.rbac_viewer_token}"}

    payload_bytes = json.dumps(
        {
            "version": "4",
            "status": "firing",
            "alerts": [
                {
                    "status": "firing",
                    "fingerprint": "fp-hmac-test-001",
                    "labels": {"alertname": "PG16PoolExhausted", "severity": "P0", "service": "payment-core"},
                    "annotations": {"summary": "HMAC verified alert"},
                }
            ],
        }
    ).encode("utf-8")
    valid_sig = hmac.new(
        settings.alertmanager_webhook_secret.encode("utf-8"),
        payload_bytes,
        hashlib.sha256,
    ).hexdigest()

    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # 1. Fault injection disabled -> 403 FAULT_INJECTION_DISABLED
            inj_resp = await client.post(
                "/api/v1/incidents/inject",
                headers=viewer_headers,
                json={"fault_type": "db_connection_pool_exhausted"},
            )
            assert inj_resp.status_code == 403
            assert inj_resp.json()["detail"]["code"] == "FAULT_INJECTION_DISABLED"

            # 2. Missing HMAC signature -> 401
            no_sig = await client.post(
                "/api/v1/webhooks/alertmanager",
                content=payload_bytes,
                headers={"Content-Type": "application/json"},
            )
            assert no_sig.status_code == 401
            assert no_sig.json()["detail"]["code"] == "WEBHOOK_SIGNATURE_MISSING"

            # 3. Invalid HMAC signature -> 403 WEBHOOK_SIGNATURE_INVALID
            bad_sig = await client.post(
                "/api/v1/webhooks/alertmanager",
                content=payload_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Alertmanager-Signature": "sha256=0000deadbeef",
                },
            )
            assert bad_sig.status_code == 403
            assert bad_sig.json()["detail"]["code"] == "WEBHOOK_SIGNATURE_INVALID"

            # 4. Valid HMAC-SHA256 signature -> 200 created + dedup
            ok1 = await client.post(
                "/api/v1/webhooks/alertmanager",
                content=payload_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Alertmanager-Signature": f"sha256={valid_sig}",
                },
            )
            ok2 = await client.post(
                "/api/v1/webhooks/alertmanager",
                content=payload_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Alertmanager-Signature": valid_sig,
                },
            )
            assert ok1.status_code == 200 and ok1.json()["results"][0]["action"] == "created"
            assert ok2.status_code == 200 and ok2.json()["results"][0]["action"] == "deduplicated"
            assert ok2.json()["results"][0]["dedup_count"] == 2


@pytest.mark.asyncio
async def test_approval_expiry_409_and_plan_hash_tamper_403(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    """③ Expired approval returns 409 APPROVAL_EXPIRED; tampered action_plan returns 403 PLAN_HASH_MISMATCH."""
    _, session_factory = pg_engine_and_factory
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=False,
        enable_fault_injection=True,
    )
    approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}

    async with LifespanManager(app):
        orch = app.state.orchestrator
        worker = AgentWorker(orch, worker_id="sec-worker", trajectory_consumer=app.state.trajectory_consumer)
        transport = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            # Case A: Expired approval -> 409 APPROVAL_EXPIRED
            inc_exp = await orch.inject_incident(fault_type="db_connection_pool_exhausted")
            await worker.run_until_quiescent()

            async with session_factory() as sess:
                async with sess.begin():
                    await sess.execute(
                        update(RemediationApproval)
                        .where(RemediationApproval.incident_id == inc_exp.id)
                        .values(expires_at=datetime.now(timezone.utc) - timedelta(seconds=60))
                    )

            exp_resp = await client.post(
                f"/api/v1/incidents/{inc_exp.id}/approve",
                headers=approver_headers,
                json={"comment": "Too late"},
            )
            assert exp_resp.status_code == 409
            assert exp_resp.json()["detail"]["code"] == "APPROVAL_EXPIRED"

            # Case B: Tampered action_plan -> 403 PLAN_HASH_MISMATCH + audit trace
            inc_tamper = await orch.inject_incident(fault_type="db_connection_pool_exhausted")
            await worker.run_until_quiescent()

            async with session_factory() as sess:
                async with sess.begin():
                    await sess.execute(
                        update(RemediationApproval)
                        .where(RemediationApproval.incident_id == inc_tamper.id)
                        .values(
                            action_plan={
                                "tool_name": "terminate_idle_in_tx_and_recycle_pool",
                                "target_resource": "tampered-prod-db",
                            }
                        )
                    )

            tamper_resp = await client.post(
                f"/api/v1/incidents/{inc_tamper.id}/approve",
                headers=approver_headers,
                json={"comment": "Should be blocked"},
            )
            assert tamper_resp.status_code == 403
            assert tamper_resp.json()["detail"]["code"] == "PLAN_HASH_MISMATCH"

            async with session_factory() as sess:
                traces = await list_trace_events(sess, inc_tamper.id)
            assert any(t.event_type == "PLAN_HASH_MISMATCH" for t in traces)


def test_production_mode_fail_fast_rejects_default_or_weak_secrets() -> None:
    """④ APP_ENV=production with weak/demo/placeholder secrets MUST fail fast at startup."""
    weak_settings = Settings(
        app_env="production",
        rbac_viewer_token="viewer-demo-token",
        rbac_approver_token="sre-approver-demo-token",
        alertmanager_webhook_secret="dev-alertmanager-webhook-secret",
        database_url="postgresql+asyncpg://aiops:aiops_prod_pwd@postgres:5432/aiops_mesh",
    )
    with pytest.raises(ValueError, match="Production Fail-Fast"):
        weak_settings.validate_production_secrets()

    strong_settings = Settings(
        app_env="production",
        rbac_viewer_token="prod-viewer-token-9f8e7d6c5b4a",
        rbac_approver_token="prod-approver-token-1a2b3c4d5e6f",
        alertmanager_webhook_secret="prod-am-hmac-secret-778899aabbcc",
        database_url="postgresql+asyncpg://aiops:Str0ngProdPgSecret!2026@postgres:5432/aiops_mesh",
    )
    strong_settings.validate_production_secrets()
