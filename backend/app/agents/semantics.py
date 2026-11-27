"""Strongly-typed truthfulness and execution outcome semantics (Phase 6)."""

from __future__ import annotations

from enum import StrEnum


class EvidenceSource(StrEnum):
    """Distinguishes real production telemetry from synthetic mock fixtures."""

    MOCK = "MOCK"
    LIVE = "LIVE"


class ExecutionOutcome(StrEnum):
    """Distinguishes simulated writes, verified real writes, unknown states, and failures."""

    SIMULATED = "SIMULATED"
    VERIFIED = "VERIFIED"
    UNKNOWN = "UNKNOWN"
    FAILED = "FAILED"
