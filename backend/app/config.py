from functools import lru_cache
from pathlib import Path
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


_WEAK_SECRET_MARKERS = ("demo", "changeme", "placeholder", "aiops_prod_pwd", "aiops_dev_pwd", "dev-")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[2] / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_env: str = "development"
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
    rbac_viewer_token: str = Field(
        default="viewer-demo-token",
        validation_alias=AliasChoices("RBAC_VIEWER_TOKEN", "VIEWER_API_TOKEN"),
    )
    rbac_approver_token: str = Field(
        default="sre-approver-demo-token",
        validation_alias=AliasChoices("RBAC_APPROVER_TOKEN", "SRE_APPROVER_API_TOKEN"),
    )
    alertmanager_webhook_secret: str = "dev-alertmanager-webhook-secret"
    approval_ttl_seconds: int = 900
    enable_fault_injection: bool = False
    auto_init_db: bool = False
    embedded_workers: bool = False
    tool_execution_mode: str = "mock"
    a2a_peer_urls: str = ""
    a2a_auth_token: str = ""
    a2a_timeout_seconds: float = 5.0
    skills_dir: Path = Path(__file__).resolve().parents[1] / "skills"

    def parsed_a2a_peer_urls(self) -> list[str]:
        return [item.strip().rstrip("/") for item in self.a2a_peer_urls.split(",") if item.strip()]

    def validate_production_secrets(self) -> None:
        """Fail-Fast at startup if APP_ENV=production uses missing or weak/placeholder secrets."""
        if self.app_env.strip().lower() != "production":
            return

        checks = {
            "RBAC_VIEWER_TOKEN / VIEWER_API_TOKEN": self.rbac_viewer_token,
            "RBAC_APPROVER_TOKEN / SRE_APPROVER_API_TOKEN": self.rbac_approver_token,
            "ALERTMANAGER_WEBHOOK_SECRET": self.alertmanager_webhook_secret,
            "DATABASE_URL": self.database_url,
        }
        for name, value in checks.items():
            raw = (value or "").strip()
            lowered = raw.lower()
            if not raw or len(raw) < 12 or any(marker in lowered for marker in _WEAK_SECRET_MARKERS):
                raise ValueError(
                    f"Production Fail-Fast: {name} is missing, too short (<12), or contains a weak/demo placeholder."
                )
        if self.rbac_viewer_token == self.rbac_approver_token:
            raise ValueError(
                "Production Fail-Fast: VIEWER and SRE_APPROVER tokens must be distinct."
            )


@lru_cache
def get_settings() -> Settings:
    return Settings()
