from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# External systems that may call the hub's webhook receiver.
SOURCES: tuple[str, ...] = ("pos", "payments", "loyalty")


class Settings(BaseSettings):
    """Hub configuration. Every secret comes from the environment; nothing is hardcoded.

    Copy `.env.example` with fresh random secrets: `python -m irlab.devtools init-env`.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./irlab.db"
    # Create tables at startup (local dev and tests). Docker Compose runs Alembic instead.
    db_auto_create: bool = True
    # Empty → in-process memory idempotency store (single worker only). Compose uses Redis.
    redis_url: str = ""

    # Comma-separated per source. Every entry verifies (rotation window); the first
    # entry is the current one, which the mounted mock providers sign with.
    # Empty → the source is "not configured" and its webhooks are rejected (fail closed).
    pos_webhook_secrets: SecretStr = SecretStr("")
    payments_webhook_secrets: SecretStr = SecretStr("")
    loyalty_webhook_secrets: SecretStr = SecretStr("")

    webhook_tolerance_s: int = 300
    webhook_max_body_bytes: int = 256 * 1024
    # How long a delivery may hold the "processing" claim before another delivery may retry it.
    idempotency_lock_ttl_s: int = 30
    # How long the fast path remembers a processed event id (the DB remembers forever).
    idempotency_done_ttl_s: int = 72 * 3600

    # Mount the three mock providers under /mock/{pos,payments,loyalty} in the hub process.
    mount_mocks: bool = True
    log_level: str = "INFO"
    # json (default) or kv (key=value, easier to read in a terminal).
    log_format: Literal["json", "kv"] = "json"

    # Operator endpoints (replay, discard, reconcile, worker tick) require X-Ops-Key.
    # Empty → those endpoints answer 503 (fail closed), like unconfigured webhook sources.
    ops_api_key: SecretStr = SecretStr("")

    # Provider REST APIs (outbox dispatcher, reconciliation). Empty + MOUNT_MOCKS=true →
    # the in-process mocks under /mock/{name}.
    pos_api_url: str = ""
    payments_api_url: str = ""
    loyalty_api_url: str = ""

    # ---- worker (M2) ----
    # Run the processor/dispatcher loop inside the API process. Compose runs a separate
    # worker container instead (RUN_WORKER=false on the API).
    run_worker: bool = True
    worker_poll_interval_s: float = 0.5
    worker_batch_size: int = 100
    # A claimed inbox event / outbox message whose worker died is retried after this lease.
    claim_lease_s: float = 60
    # Out-of-order events wait this long for the missing sequence before going to the DLQ.
    park_timeout_s: float = 300
    # Unexpected processing errors are retried this many times before dead-lettering.
    processing_max_attempts: int = 5

    # ---- outbound calls (M2) ----
    outbound_timeout_s: float = 2.0
    retry_base_s: float = 0.5
    retry_cap_s: float = 60.0
    retry_max_attempts: int = 6

    # ---- reconciliation and alerts (M3) ----
    # 0 disables the periodic run in the worker (the API endpoint still works).
    reconcile_interval_s: float = 60
    # Orders younger than this are skipped: their webhooks may still be in flight.
    reconcile_grace_s: float = 30
    reconcile_auto_repair: bool = True
    alert_interval_s: float = 15
    alert_webhook_url: str = ""
    alert_dlq_depth: int = 1
    alert_signature_failures: int = 5
    alert_signature_window_s: int = 300
    alert_parked_age_s: int = 60
    alert_open_mismatches: int = 1

    # ---- AI-assisted triage (M4), read-only ----
    triage_provider: Literal["mock", "anthropic"] = "mock"
    anthropic_api_key: SecretStr = SecretStr("")
    triage_model: str = "claude-opus-5-5"

    def webhook_secrets(self, source: str) -> list[str]:
        if source not in SOURCES:
            return []
        raw: SecretStr = getattr(self, f"{source}_webhook_secrets")
        return [s.strip() for s in raw.get_secret_value().split(",") if s.strip()]

    def provider_api_url(self, name: str) -> str:
        return getattr(self, f"{name}_api_url")


@lru_cache
def get_settings() -> Settings:
    return Settings()
