from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[2] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql+asyncpg://aiops:aiops_dev_pwd@127.0.0.1:5436/aiops_mesh"
    test_database_url: str = "postgresql+asyncpg://aiops:aiops_dev_pwd@127.0.0.1:5436/aiops_mesh_test"
    redis_url: str = "redis://127.0.0.1:6386/0"
    test_redis_url: str = "redis://127.0.0.1:6386/1"
    backend_host: str = "127.0.0.1"
    backend_port: int = 8016
    llm_provider: str = "mock"
    llm_base_url: str = "https://api.deepseek.com/v1"
    llm_model: str = "deepseek-chat"
    llm_api_key: str = ""
    llm_timeout_seconds: float = 45.0
    step_delay_ms: int = 0
    task_lease_seconds: float = 30.0
    watchdog_interval_seconds: float = 5.0
    outbox_batch_size: int = 100
    outbox_poll_interval_seconds: float = 0.2
    shutdown_timeout_seconds: float = 10.0
    rbac_viewer_token: str = "viewer-demo-token"
    rbac_approver_token: str = "sre-approver-demo-token"
    auto_init_db: bool = False
    skills_dir: Path = Path(__file__).resolve().parents[1] / "skills"


@lru_cache
def get_settings() -> Settings:
    return Settings()
