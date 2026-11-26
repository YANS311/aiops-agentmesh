from contextlib import asynccontextmanager
import logging
from pathlib import Path
import time

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

from app.agents.llm import LLMProvider, build_llm_provider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.reliability import LeaseWatchdog, OutboxRelay
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.api.routes import router as api_router
from app.config import Settings, get_settings
from app.db.base import create_engine_and_sessionmaker, init_db
from app.observability.logging import configure_json_logging
from app.observability.metrics import refresh_db_gauges, render_prometheus_metrics
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry

configure_json_logging()
logger = logging.getLogger(__name__)


def create_app(
    *,
    settings: Settings | None = None,
    database_url: str | None = None,
    redis_url: str | None = None,
    skills_dir: Path | None = None,
    llm_provider: LLMProvider | None = None,
    start_worker: bool = True,
    auto_init_db: bool | None = None,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    resolved_db_url = database_url or resolved_settings.database_url
    resolved_redis_url = redis_url or resolved_settings.redis_url
    resolved_skills_dir = skills_dir or resolved_settings.skills_dir
    should_init_db = (
        resolved_settings.auto_init_db if auto_init_db is None else auto_init_db
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine, session_factory = create_engine_and_sessionmaker(resolved_db_url)
        if should_init_db:
            await init_db(engine)

        redis_client = aioredis.from_url(resolved_redis_url, decode_responses=True)
        redis_bus = RedisEventBus(redis_client)
        skill_registry = SkillRegistry(resolved_skills_dir)
        tool_registry = ToolRegistry()
        resolved_llm = llm_provider or build_llm_provider(resolved_settings)

        outbox_relay = OutboxRelay(
            session_factory=session_factory,
            redis_bus=redis_bus,
            batch_size=resolved_settings.outbox_batch_size,
            poll_interval_seconds=resolved_settings.outbox_poll_interval_seconds,
        )
        trajectory_consumer = TrajectoryStreamConsumer(
            session_factory=session_factory,
            redis_bus=redis_bus,
            consumer_name="archiver-api-1",
            outbox_relay=outbox_relay,
        )
        await trajectory_consumer.initialize()

        orchestrator = AIOpsOrchestrator(
            session_factory=session_factory,
            redis_bus=redis_bus,
            skill_registry=skill_registry,
            tool_registry=tool_registry,
            llm_provider=resolved_llm,
            step_delay_ms=resolved_settings.step_delay_ms,
            trajectory_consumer=trajectory_consumer,
            outbox_relay=outbox_relay,
            lease_seconds=resolved_settings.task_lease_seconds,
        )
        worker = AgentWorker(
            orchestrator,
            worker_id="worker-api-1",
            trajectory_consumer=trajectory_consumer,
        )
        watchdog = LeaseWatchdog(
            orchestrator, interval_seconds=resolved_settings.watchdog_interval_seconds
        )

        app.state.settings = resolved_settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.redis_client = redis_client
        app.state.redis_bus = redis_bus
        app.state.skill_registry = skill_registry
        app.state.tool_registry = tool_registry
        app.state.orchestrator = orchestrator
        app.state.trajectory_consumer = trajectory_consumer
        app.state.outbox_relay = outbox_relay
        app.state.watchdog = watchdog
        app.state.worker = worker
        app.state.shutdown_report = None

        if start_worker:
            outbox_relay.start()
            trajectory_consumer.start()
            watchdog.start()
            worker.start()

        try:
            yield
        finally:
            timeout = resolved_settings.shutdown_timeout_seconds
            started = time.monotonic()
            worker_drained = await worker.stop(timeout=timeout)
            await watchdog.stop()
            outbox_remaining = await outbox_relay.flush(timeout=timeout)
            await trajectory_consumer.stop(timeout=timeout)
            outbox_remaining = await outbox_relay.flush(timeout=min(timeout, 2.0))
            await outbox_relay.stop()

            app.state.shutdown_report = {
                "worker_drained": worker_drained,
                "outbox_pending_remaining": outbox_remaining,
                "duration_seconds": round(time.monotonic() - started, 3),
            }
            logger.info("Graceful shutdown completed: %s", app.state.shutdown_report)

            await redis_client.aclose()
            await engine.dispose()

    app = FastAPI(
        title="AgentMesh AIOps Control Plane",
        version="0.3.2",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:5176",
            "http://localhost:5176",
            "http://127.0.0.1:8088",
            "http://localhost:8088",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router)

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics(request: Request) -> Response:
        await refresh_db_gauges(request.app.state.session_factory)
        body, media_type = render_prometheus_metrics()
        return Response(content=body, media_type=media_type)

    return app


app = create_app()
