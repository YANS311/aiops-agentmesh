"""Async A2A client with Agent Card caching, Bearer auth, and timeout circuit breaking."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import Any

import httpx
from pydantic import ValidationError

from app.a2a.models import A2ATaskRequest, A2ATaskResponse, AgentCard

logger = logging.getLogger(__name__)


class A2AClientError(RuntimeError):
    """Structured error raised when A2A discovery or task delegation fails."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class A2ADiscoveryResult:
    """Result of discovering an external A2A peer."""

    base_url: str
    status: str
    card: AgentCard | None = None
    error: str | None = None


class A2AClient:
    """Asynchronous A2A protocol client backed by httpx.AsyncClient."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 5.0,
        cache_ttl_seconds: float = 60.0,
        auth_token: str = "",
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.cache_ttl_seconds = cache_ttl_seconds
        self.auth_token = auth_token.strip()
        self.transport = transport
        self._card_cache: dict[str, tuple[float, AgentCard]] = {}

    def clear_cache(self) -> None:
        self._card_cache.clear()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        return headers

    async def discover_agent_card(
        self, base_url: str, *, use_cache: bool = True
    ) -> AgentCard:
        normalized = base_url.rstrip("/")
        now = time.monotonic()
        if use_cache and normalized in self._card_cache:
            expires_at, cached = self._card_cache[normalized]
            if now < expires_at:
                return cached

        url = f"{normalized}/.well-known/agent.json"
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
            ) as client:
                resp = await client.get(url, headers=self._headers())
        except httpx.TimeoutException as exc:
            raise A2AClientError("A2A_TIMEOUT", f"Timed out discovering {url}: {exc}") from exc
        except httpx.HTTPError as exc:
            raise A2AClientError("A2A_UNREACHABLE", f"Failed to reach {url}: {exc}") from exc

        if resp.status_code != 200:
            raise A2AClientError(
                "A2A_HTTP_ERROR",
                f"Agent Card endpoint {url} returned HTTP {resp.status_code}",
            )

        try:
            card = AgentCard.model_validate(resp.json())
        except (ValueError, ValidationError) as exc:
            raise A2AClientError(
                "A2A_INVALID_CARD",
                f"Invalid AgentCard JSON from {url}: {exc}",
            ) from exc

        self._card_cache[normalized] = (now + self.cache_ttl_seconds, card)
        return card

    async def send_task(
        self, base_url: str, request: A2ATaskRequest
    ) -> A2ATaskResponse:
        normalized = base_url.rstrip("/")
        url = f"{normalized}/a2a/tasks/send"
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout_seconds,
                transport=self.transport,
            ) as client:
                resp = await client.post(
                    url,
                    headers=self._headers(),
                    json=request.model_dump(mode="json"),
                )
        except httpx.TimeoutException as exc:
            raise A2AClientError("A2A_TIMEOUT", f"A2A task {request.task_id} timed out: {exc}") from exc
        except httpx.HTTPError as exc:
            raise A2AClientError("A2A_UNREACHABLE", f"A2A peer {url} unreachable: {exc}") from exc

        if resp.status_code != 200:
            raise A2AClientError(
                "A2A_HTTP_ERROR",
                f"A2A peer {url} returned HTTP {resp.status_code}: {resp.text[:240]}",
            )

        try:
            return A2ATaskResponse.model_validate(resp.json())
        except (ValueError, ValidationError) as exc:
            raise A2AClientError(
                "A2A_INVALID_RESPONSE",
                f"Invalid A2ATaskResponse from {url}: {exc}",
            ) from exc

    async def discover_peers(
        self, peer_urls: list[str], *, use_cache: bool = True
    ) -> list[A2ADiscoveryResult]:
        results: list[A2ADiscoveryResult] = []
        for raw_url in peer_urls:
            base = raw_url.strip().rstrip("/")
            if not base:
                continue
            try:
                card = await self.discover_agent_card(base, use_cache=use_cache)
                results.append(A2ADiscoveryResult(base_url=base, status="ONLINE", card=card))
            except A2AClientError as exc:
                logger.warning("A2A peer discovery failed for %s: %s", base, exc.message)
                results.append(
                    A2ADiscoveryResult(base_url=base, status="UNREACHABLE", error=exc.message)
                )
        return results

    async def find_peer_for_skill(
        self, peer_urls: list[str], skill_id: str
    ) -> tuple[str, AgentCard] | None:
        for raw_url in peer_urls:
            base = raw_url.strip().rstrip("/")
            if not base:
                continue
            try:
                card = await self.discover_agent_card(base)
            except A2AClientError:
                continue
            if any(sk.id == skill_id for sk in card.skills):
                return base, card
        return None
