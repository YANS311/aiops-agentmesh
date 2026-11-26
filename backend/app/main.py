from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

from app.agents.llm import MockLLMProvider
from app.agents.orchestrator import AgentWorker, AIOpsOrchestrator
from app.agents.tools import ToolRegistry
from app.agents.trajectory_consumer import TrajectoryStreamConsumer
from app.api.routes import router as api_router
from app.config import Settings, get_settings
from app.db.base import create_engine_and_sessionmaker, init_db
from app.redis_bus.events import RedisEventBus
from app.skills.loader import SkillRegistry


def create_app(
    *,
    settings: Settings | None = None,
    database_url: str | None = None,
    redis_url: str | None = None,
    skills_dir: Path | None = None,
    start_worker: bool = True,
) -> FastAPI:
    resolved_settings = settings or get_settings()
    resolved_db_url = database_url or resolved_settings.database_url
    resolved_redis_url = redis_url or resolved_settings.redis_url
    resolved_skills_dir = skills_dir or resolved_settings.skills_dir

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine, session_factory = create_engine_and_sessionmaker(resolved_db_url)
        await init_db(engine)

        redis_client = aioredis.from_url(resolved_redis_url, decode_responses=True)
        redis_bus = RedisEventBus(redis_client)
        skill_registry = SkillRegistry(resolved_skills_dir)
        tool_registry = ToolRegistry()
        llm_provider = MockLLMProvider()

        trajectory_consumer = TrajectoryStreamConsumer(
            session_factory=session_factory,
            redis_bus=redis_bus,
            consumer_name="archiver-api-1",
        )
        await trajectory_consumer.initialize()

        orchestrator = AIOpsOrchestrator(
            session_factory=session_factory,
            redis_bus=redis_bus,
            skill_registry=skill_registry,
            tool_registry=tool_registry,
            llm_provider=llm_provider,
            step_delay_ms=resolved_settings.step_delay_ms,
            trajectory_consumer=trajectory_consumer,
        )
        worker = AgentWorker(
            orchestrator,
            worker_id="worker-api-1",
            trajectory_consumer=trajectory_consumer,
        )

        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.redis_client = redis_client
        app.state.redis_bus = redis_bus
        app.state.skill_registry = skill_registry
        app.state.tool_registry = tool_registry
        app.state.orchestrator = orchestrator
        app.state.trajectory_consumer = trajectory_consumer
        app.state.worker = worker

        if start_worker:
            trajectory_consumer.start()
            worker.start()

        try:
            yield
        finally:
            await worker.stop()
            await trajectory_consumer.stop()
            await redis_client.aclose()
            await engine.dispose()

    app = FastAPI(
        title="AgentMesh AIOps Control Plane",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://127.0.0.1:5176",
            "http://localhost:5176",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(api_router)
    return app


app = create_app()
