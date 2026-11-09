
import os
from dataclasses import dataclass
from urllib.parse import quote


def _get_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a valid integer"
        ) from exc

    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")

    return value


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()

    if not value:
        raise RuntimeError(
            f"Required environment variable {name} is missing"
        )

    return value


def _build_database_url() -> str:
    database_url = os.getenv("DATABASE_URL", "").strip()

    if database_url:
        return database_url

    user = quote(_required_env("POSTGRES_USER"), safe="")
    password = quote(_required_env("POSTGRES_PASSWORD"), safe="")
    database = quote(_required_env("POSTGRES_DB"), safe="")

    host = os.getenv("POSTGRES_HOST", "postgres").strip()
    port = os.getenv("POSTGRES_PORT", "5432").strip()

    return (
        f"postgresql://{user}:{password}"
        f"@{host}:{port}/{database}"
    )


def _build_rabbitmq_url() -> str:
    rabbitmq_url = os.getenv("RABBITMQ_URL", "").strip()

    if rabbitmq_url:
        return rabbitmq_url

    user = quote(_required_env("RABBITMQ_USER"), safe="")
    password = quote(_required_env("RABBITMQ_PASSWORD"), safe="")

    host = os.getenv("RABBITMQ_HOST", "rabbitmq").strip()
    port = os.getenv("RABBITMQ_PORT", "5672").strip()
    vhost = quote(os.getenv("RABBITMQ_VHOST", "/"), safe="")

    return (
        f"amqp://{user}:{password}"
        f"@{host}:{port}/{vhost}"
    )


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    internal_token: str

    generation_concurrency: int = 10
    publish_rate_per_second: int = 50
    generation_max_items: int = 1000

    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=_build_database_url(),
            rabbitmq_url=_build_rabbitmq_url(),
            internal_token=_required_env("INTERNAL_TOKEN"),
            generation_concurrency=_get_positive_int(
                "GENERATION_CONCURRENCY", 10
            ),
            publish_rate_per_second=_get_positive_int(
                "PUBLISH_RATE_PER_SECOND", 50
            ),
            generation_max_items=_get_positive_int(
                "GENERATION_MAX_ITEMS", 1000
            ),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )
