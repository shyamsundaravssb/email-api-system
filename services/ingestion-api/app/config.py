
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    internal_token: str
    generation_api_url: str

    queue_capacity: int
    max_recipients_per_batch: int
    idempotency_ttl_hours: int
    sweeper_after_seconds: int

    log_level: str

    @classmethod
    def from_env(cls):
        database_url = os.getenv("DATABASE_URL")

        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is required")

        return cls(
            database_url=database_url,
            internal_token=os.getenv("INTERNAL_TOKEN", ""),
            generation_api_url=os.getenv(
                "GENERATION_API_URL",
                "http://generation-api:8001"
            ),
            queue_capacity=int(os.getenv("QUEUE_CAPACITY", "1000")),
            max_recipients_per_batch=int(
                os.getenv("MAX_RECIPIENTS_PER_BATCH", "1000")
            ),
            idempotency_ttl_hours=int(
                os.getenv("IDEMPOTENCY_TTL_HOURS", "24")
            ),
            sweeper_after_seconds=int(
                os.getenv("SWEEPER_AFTER_SECONDS", "300")
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
