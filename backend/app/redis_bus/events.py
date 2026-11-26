import json
from typing import Any
from uuid import UUID

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

GLOBAL_EVENTS_STREAM = "stream:aiops:events"
TRAJECTORY_CONSUMER_GROUP = "cg:trajectory-archiver"


def incident_events_stream(incident_id: UUID | str) -> str:
    return f"stream:incident:{incident_id}:events"


def incident_blackboard_key(incident_id: UUID | str) -> str:
    return f"incident:{incident_id}:blackboard"


class RedisEventBus:
    """Redis 7 Streams event bus (XADD / XGROUP / XREADGROUP / XACK / XAUTOCLAIM) and blackboard cache."""

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self.redis = redis_client

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

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

    async def publish_trace_event(
        self, incident_id: UUID | str, event_payload: dict[str, Any]
    ) -> tuple[str, str]:
        fields = {
            "trace_id": str(event_payload["id"]),
            "incident_id": str(incident_id),
            "event_type": str(event_payload["event_type"]),
            "agent_role": str(event_payload["agent_role"]),
            "risk_tier": str(event_payload["risk_tier"]),
            "data": json.dumps(event_payload, default=str),
        }
        # MULTI/EXEC: both streams receive the event atomically (or neither does).
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xadd(
                incident_events_stream(incident_id),
                fields,
                maxlen=2000,
                approximate=True,
            )
            pipe.xadd(
                GLOBAL_EVENTS_STREAM,
                fields,
                maxlen=10000,
                approximate=True,
            )
            inc_entry_id, global_entry_id = await pipe.execute()
        return str(inc_entry_id), str(global_entry_id)

    async def ensure_consumer_group(
        self, stream_key: str, group_name: str, start_id: str = "0"
    ) -> None:
        try:
            await self.redis.xgroup_create(
                name=stream_key,
                groupname=group_name,
                id=start_id,
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def destroy_consumer_group(self, stream_key: str, group_name: str) -> None:
        try:
            await self.redis.xgroup_destroy(name=stream_key, groupname=group_name)
        except ResponseError:
            pass

    @staticmethod
    def _decode_entries(
        raw_entries: list[tuple[Any, dict[Any, Any]]],
    ) -> list[tuple[str, dict[str, Any]]]:
        parsed: list[tuple[str, dict[str, Any]]] = []
        for entry_id, fields in raw_entries:
            if not fields:
                continue
            raw_data = fields.get("data") or fields.get(b"data")
            if raw_data is None:
                continue
            if isinstance(raw_data, bytes):
                raw_data = raw_data.decode("utf-8")
            eid = entry_id.decode("utf-8") if isinstance(entry_id, bytes) else str(entry_id)
            parsed.append((eid, json.loads(raw_data)))
        return parsed

    async def read_group(
        self,
        stream_key: str,
        group_name: str,
        consumer_name: str,
        *,
        count: int = 20,
        block_ms: int | None = 250,
    ) -> list[tuple[str, dict[str, Any]]]:
        kwargs: dict[str, Any] = {
            "groupname": group_name,
            "consumername": consumer_name,
            "streams": {stream_key: ">"},
            "count": count,
        }
        if block_ms is not None:
            kwargs["block"] = block_ms

        response = await self.redis.xreadgroup(**kwargs)
        if not response:
            return []
        entries: list[tuple[str, dict[str, Any]]] = []
        for _stream, raw_list in response:
            entries.extend(self._decode_entries(raw_list))
        return entries

    async def ack(self, stream_key: str, group_name: str, *entry_ids: str) -> int:
        if not entry_ids:
            return 0
        return int(await self.redis.xack(stream_key, group_name, *entry_ids))

    async def pending_count(self, stream_key: str, group_name: str) -> int:
        info = await self.redis.xpending(stream_key, group_name)
        return int(info["pending"])

    async def autoclaim_pending(
        self,
        stream_key: str,
        group_name: str,
        consumer_name: str,
        *,
        min_idle_ms: int = 5000,
        count: int = 20,
    ) -> list[tuple[str, dict[str, Any]]]:
        res = await self.redis.xautoclaim(
            name=stream_key,
            groupname=group_name,
            consumername=consumer_name,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=count,
        )
        raw_messages = res[1] if isinstance(res, (list, tuple)) and len(res) >= 2 else []
        return self._decode_entries(raw_messages)

    async def list_groups(self, stream_key: str) -> list[dict[str, Any]]:
        try:
            return list(await self.redis.xinfo_groups(stream_key))
        except ResponseError:
            return []
