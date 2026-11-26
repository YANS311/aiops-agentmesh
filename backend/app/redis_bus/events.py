import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import redis.asyncio as aioredis
from redis.asyncio.client import PubSub


def incident_events_channel(incident_id: UUID | str) -> str:
    return f"incident:{incident_id}:events"


def incident_blackboard_key(incident_id: UUID | str) -> str:
    return f"incident:{incident_id}:blackboard"


class RedisEventBus:
    """Handles Redis Pub/Sub trace streaming and shared blackboard caching."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    async def publish_trace_event(
        self, incident_id: UUID | str, event_payload: dict[str, Any]
    ) -> int:
        channel = incident_events_channel(incident_id)
        serialized = json.dumps(event_payload, default=str)
        return int(await self.redis.publish(channel, serialized))

    async def set_blackboard_cache(
        self,
        incident_id: UUID | str,
        blackboard: dict[str, Any],
        ttl_seconds: int = 3600,
    ) -> None:
        key = incident_blackboard_key(incident_id)
        await self.redis.set(key, json.dumps(blackboard, default=str), ex=ttl_seconds)

    async def get_blackboard_cache(self, incident_id: UUID | str) -> dict[str, Any] | None:
        key = incident_blackboard_key(incident_id)
        raw = await self.redis.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def subscribe_incident_events(self, incident_id: UUID | str) -> PubSub:
        pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        await pubsub.subscribe(incident_events_channel(incident_id))
        return pubsub

    async def iter_pubsub_events(
        self,
        pubsub: PubSub,
        *,
        keepalive_seconds: float = 10.0,
        stop_event: asyncio.Event | None = None,
    ) -> AsyncIterator[dict[str, Any] | None]:
        """Yield decoded JSON dicts or None on keepalive timeout."""
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            message = await pubsub.get_message(timeout=keepalive_seconds)
            if message is None:
                yield None
                continue
            if message.get("type") == "message":
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                yield json.loads(data)
