import asyncio
import json
import os

from asgi_lifespan import LifespanManager
import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import get_settings
from app.main import create_app


@pytest.mark.asyncio
async def test_api_end_to_end_fail_closed_approval_and_sse_stream(
    pg_engine_and_factory: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    settings = get_settings()
    test_db_url = os.environ.get("TEST_DATABASE_URL", settings.test_database_url)
    test_redis_url = os.environ.get("TEST_REDIS_URL", settings.test_redis_url)

    app = create_app(
        settings=settings,
        database_url=test_db_url,
        redis_url=test_redis_url,
        start_worker=True,
    )

    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            health = await client.get("/api/v1/health")
            assert health.status_code == 200
            assert health.json() == {"status": "ok", "postgres": "ok", "redis": "ok"}

            skills_resp = await client.get("/api/v1/skills")
            assert skills_resp.status_code == 200
            assert len(skills_resp.json()) == 3

            # 1. Inject fault incident
            inject_resp = await client.post(
                "/api/v1/incidents/inject",
                json={"fault_type": "db_connection_pool_exhausted"},
            )
            assert inject_resp.status_code == 201
            incident_id = inject_resp.json()["id"]

            # 2. Wait for background worker to reach PENDING_APPROVAL
            detail = None
            for _ in range(50):
                r = await client.get(f"/api/v1/incidents/{incident_id}")
                assert r.status_code == 200
                detail = r.json()
                if detail["status"] == "PENDING_APPROVAL":
                    break
                await asyncio.sleep(0.05)
            assert detail is not None
            assert detail["status"] == "PENDING_APPROVAL"
            assert detail["approval"]["status"] == "PENDING"

            # 3. Attempt unauthorized /execute (authenticated SRE_APPROVER, but approval still PENDING) -> MUST return 403 Fail-Closed
            approver_headers = {"Authorization": f"Bearer {settings.rbac_approver_token}"}
            exec_resp = await client.post(
                f"/api/v1/incidents/{incident_id}/execute",
                headers=approver_headers,
            )
            assert exec_resp.status_code == 403
            assert exec_resp.json()["detail"]["code"] == "APPROVAL_REQUIRED_FAIL_CLOSED"

            # 4. Open live SSE stream, approve remediation concurrently, collect until complete
            received_events: list[tuple[str, dict]] = []

            async def approve_after_subscribe() -> None:
                await asyncio.sleep(0.15)
                app_resp = await client.post(
                    f"/api/v1/incidents/{incident_id}/approve",
                    headers=approver_headers,
                    json={"operator": "sre-api-test", "comment": "LGTM"},
                )
                assert app_resp.status_code == 200

            async def consume_sse() -> None:
                async with client.stream(
                    "GET", f"/api/v1/incidents/{incident_id}/events"
                ) as sse_resp:
                    assert sse_resp.status_code == 200
                    current_event = "message"
                    async for line in sse_resp.aiter_lines():
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("event:"):
                            current_event = line.split(":", 1)[1].strip()
                        elif line.startswith("data:"):
                            data = json.loads(line.split(":", 1)[1].strip())
                            received_events.append((current_event, data))
                            if current_event == "complete":
                                return

            await asyncio.gather(consume_sse(), approve_after_subscribe())

            trace_types = [d["event_type"] for ev, d in received_events if ev == "trace"]
            assert "ALERT_INJECTED" in trace_types
            assert "EXECUTION_BLOCKED_FAIL_CLOSED" in trace_types
            assert "APPROVAL_GRANTED" in trace_types
            assert "HIGH_RISK_EXECUTED" in trace_types
            assert "TRAJECTORY_ARCHIVED" in trace_types
            assert "INCIDENT_RESOLVED" in trace_types
            assert received_events[-1] == ("complete", {"status": "RESOLVED"})

            # 5. Verify trajectory endpoint & JSONB containment filter
            traj_resp = await client.get(f"/api/v1/trajectories/{incident_id}")
            assert traj_resp.status_code == 200
            assert traj_resp.json()["skill_id"] == "skill.pg16.pool_exhaustion"

            list_resp = await client.get(
                "/api/v1/incidents",
                params={"blackboard_contains": json.dumps({"stage": "RESOLVED"})},
            )
            assert list_resp.status_code == 200
            assert any(item["id"] == incident_id for item in list_resp.json())
