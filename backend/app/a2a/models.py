"""Standard A2A (Agent-to-Agent) Pydantic schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class AgentSkillCard(BaseModel):
    """Capability descriptor for a single A2A skill."""

    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    permission: Literal["READ_ONLY", "HIGH_RISK_GATED"] = "READ_ONLY"
    input_schema: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class AgentCapabilities(BaseModel):
    """Protocol capabilities advertised in /.well-known/agent.json."""

    streaming: bool = False
    pushNotifications: bool = False
    stateTransitionHistory: bool = True
    readOnlyDiagnosticsOnly: bool = False


class AgentCard(BaseModel):
    """Standard A2A Agent Card served at GET /.well-known/agent.json."""

    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    url: str = Field(min_length=1)
    version: str = Field(default="1.0.0", min_length=1)
    capabilities: AgentCapabilities = Field(default_factory=AgentCapabilities)
    skills: list[AgentSkillCard] = Field(default_factory=list)


class A2ATaskRequest(BaseModel):
    """Standard A2A task delegation payload sent to POST /a2a/tasks/send."""

    task_id: str = Field(min_length=1)
    skill_id: str = Field(min_length=1)
    input: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class A2ATaskResponse(BaseModel):
    """Standard A2A task execution result returned from POST /a2a/tasks/send."""

    task_id: str = Field(min_length=1)
    state: Literal["COMPLETED", "FAILED", "REJECTED"]
    artifacts: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
