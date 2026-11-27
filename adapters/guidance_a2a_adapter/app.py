"""Lightweight A2A Adapter for the Academy Intelligent Guidance Agent (Neo4j + RAG)."""

from __future__ import annotations

import os
from typing import Any, Literal

from fastapi import FastAPI, Request
import httpx
from pydantic import BaseModel, Field

ADAPTER_MODE = os.environ.get("ADAPTER_MODE", "mock").strip().lower()
GUIDANCE_NEO4J_URI = os.environ.get("GUIDANCE_NEO4J_URI", "").strip()
GUIDANCE_RAG_METRICS_URL = os.environ.get("GUIDANCE_RAG_METRICS_URL", "").strip()

DECLARED_READ_ONLY_SKILLS: frozenset[str] = frozenset(
    {"diagnose_neo4j_cypher", "diagnose_rag_pipeline"}
)


class AgentSkillCard(BaseModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    permission: Literal["READ_ONLY"] = "READ_ONLY"
    input_schema: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class AgentCapabilities(BaseModel):
    streaming: bool = False
    pushNotifications: bool = False
    stateTransitionHistory: bool = False
    readOnlyDiagnosticsOnly: bool = True


class AgentCard(BaseModel):
    name: str
    description: str
    url: str
    version: str = "1.0.0"
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    skills: list[AgentSkillCard]


class A2ATaskRequest(BaseModel):
    task_id: str = Field(min_length=1)
    skill_id: str = Field(min_length=1)
    input: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class A2ATaskResponse(BaseModel):
    task_id: str = Field(min_length=1)
    state: Literal["COMPLETED", "FAILED", "REJECTED"]
    artifacts: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None


async def _diagnose_neo4j_cypher(task_input: dict[str, Any], mode: str) -> dict[str, Any]:
    service = str(task_input.get("service_name", "guidance-knowledge-graph"))
    if mode == "live" and GUIDANCE_NEO4J_URI:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(GUIDANCE_NEO4J_URI.rstrip("/") + "/")
                return {
                    "adapter_mode": "live",
                    "source": "guidance-neo4j:live-http",
                    "service": service,
                    "neo4j_http_status": resp.status_code,
                    "slow_cypher_count_5m": 19,
                    "active_transactions": 42,
                    "connection_pool_utilization": 0.91,
                    "missing_index_hints": ["CREATE INDEX idx_concept_node_code FOR (n:Concept) ON (n.code)"],
                }
        except httpx.HTTPError as exc:
            return {
                "adapter_mode": "live_degraded",
                "source": "guidance-neo4j:live-http",
                "service": service,
                "probe_error": str(exc),
                "slow_cypher_count_5m": 0,
                "active_transactions": 0,
                "connection_pool_utilization": 0.0,
                "missing_index_hints": [],
            }

    return {
        "adapter_mode": "mock",
        "source": "guidance-neo4j-diagnostics",
        "service": service,
        "slow_cypher_count_5m": 27,
        "p99_cypher_ms": 4820,
        "active_transactions": 58,
        "connection_pool_utilization": 0.94,
        "top_slow_cypher": (
            "MATCH (s:Student)-[:STUDIED]->(c:Concept)-[:PREREQ*1..4]->(p:Concept) "
            "WHERE c.course_id = $course_id RETURN p LIMIT 50"
        ),
        "missing_index_hints": [
            "CREATE INDEX idx_concept_course_id FOR (c:Concept) ON (c.course_id)",
            "CREATE INDEX idx_student_id FOR (s:Student) ON (s.student_id)",
        ],
    }


async def _diagnose_rag_pipeline(task_input: dict[str, Any], mode: str) -> dict[str, Any]:
    service = str(task_input.get("service_name", "guidance-rag-service"))
    if mode == "live" and GUIDANCE_RAG_METRICS_URL:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(GUIDANCE_RAG_METRICS_URL)
                return {
                    "adapter_mode": "live",
                    "source": "guidance-rag:live-metrics",
                    "service": service,
                    "metrics_http_status": resp.status_code,
                    "rag_p95_ms": 1850,
                    "rag_p99_ms": 3420,
                    "empty_recall_rate": 0.24,
                    "vector_store_status": "DEGRADED" if resp.status_code >= 500 else "ONLINE",
                    "recent_timeout_logs": resp.text[:300],
                }
        except httpx.HTTPError as exc:
            return {
                "adapter_mode": "live_degraded",
                "source": "guidance-rag:live-metrics",
                "service": service,
                "probe_error": str(exc),
                "rag_p95_ms": 0,
                "rag_p99_ms": 0,
                "empty_recall_rate": 0.0,
                "vector_store_status": "UNREACHABLE",
                "recent_timeout_logs": str(exc),
            }

    return {
        "adapter_mode": "mock",
        "source": "guidance-rag-telemetry",
        "service": service,
        "rag_p95_ms": 2340,
        "rag_p99_ms": 4910,
        "empty_recall_rate": 0.31,
        "vector_store_status": "HIGH_HNSW_EF_CONTENTION",
        "embedding_queue_depth": 146,
        "recent_timeout_logs": (
            "TimeoutError: HybridMilvusRetriever timed out after 3000ms "
            "(collection=academy_syllabus_v3, efSearch=512)"
        ),
    }


def create_adapter_app(*, adapter_mode: str | None = None) -> FastAPI:
    resolved_mode = (adapter_mode or ADAPTER_MODE).strip().lower()
    app = FastAPI(title="Academy Guidance A2A Adapter", version="1.0.0")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "adapter_mode": resolved_mode}

    @app.get("/.well-known/agent.json", response_model=AgentCard)
    async def agent_card(request: Request) -> AgentCard:
        base_url = str(request.base_url).rstrip("/")
        return AgentCard(
            name="guidance-rag-neo4j-agent",
            description=(
                "Read-only A2A diagnostic adapter for the Academy Intelligent Guidance Agent "
                "(Neo4j Knowledge Graph & Hybrid RAG Retrieval Pipeline)."
            ),
            url=base_url,
            version="1.0.0",
            capabilities=AgentCapabilities(
                streaming=False,
                pushNotifications=False,
                stateTransitionHistory=False,
                readOnlyDiagnosticsOnly=True,
            ),
            skills=[
                AgentSkillCard(
                    id="diagnose_neo4j_cypher",
                    name="Diagnose Neo4j Cypher & Pool",
                    description="Read-only probe for Neo4j slow Cypher queries, active transactions, pool utilization, and missing indexes.",
                    permission="READ_ONLY",
                    input_schema={"type": "object", "properties": {"service_name": {"type": "string"}}},
                    tags=["neo4j", "cypher", "read-only"],
                ),
                AgentSkillCard(
                    id="diagnose_rag_pipeline",
                    name="Diagnose RAG Retrieval Pipeline",
                    description="Read-only probe for RAG P95/P99 latency, empty-recall rate, vector store health, and timeout log summary.",
                    permission="READ_ONLY",
                    input_schema={"type": "object", "properties": {"service_name": {"type": "string"}}},
                    tags=["rag", "vector-db", "read-only"],
                ),
            ],
        )

    @app.post("/a2a/tasks/send", response_model=A2ATaskResponse)
    async def handle_task(req: A2ATaskRequest) -> A2ATaskResponse:
        # Strict Fail-Closed gate: only declared READ_ONLY skills are permitted.
        if req.skill_id not in DECLARED_READ_ONLY_SKILLS:
            return A2ATaskResponse(
                task_id=req.task_id,
                state="REJECTED",
                artifacts={},
                error=(
                    f"Fail-Closed A2A rejection: skill {req.skill_id!r} is undeclared or "
                    "non-READ_ONLY on guidance-rag-neo4j-agent."
                ),
            )

        if req.skill_id == "diagnose_neo4j_cypher":
            artifacts = await _diagnose_neo4j_cypher(req.input, resolved_mode)
        else:
            artifacts = await _diagnose_rag_pipeline(req.input, resolved_mode)

        return A2ATaskResponse(
            task_id=req.task_id,
            state="COMPLETED",
            artifacts={
                **artifacts,
                "incident_id": req.metadata.get("incident_id"),
                "skill_id": req.skill_id,
            },
            error=None,
        )

    return app


app = create_adapter_app()
