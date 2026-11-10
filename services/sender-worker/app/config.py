import os
import logging
from dataclasses import dataclass
from urllib.parse import urlparse


def get_int_env(name: str, default: int) -> int:
    """Read an integer environment variable and validate it."""

    value = os.getenv(name, str(default))

    try:
        result = int(value)
    except ValueError:
        raise ValueError(
            f"Environment variable {name} must be an integer."
        )

    if result <= 0:
        raise ValueError(
            f"Environment variable {name} must be greater than zero."
        )

    return result


def get_bool_env(name: str, default: bool = False) -> bool:
    """Read a boolean environment variable."""

    value = os.getenv(name, str(default)).strip().lower()

    if value in ("true", "1", "yes", "on"):
        return True

    if value in ("false", "0", "no", "off"):
        return False

    raise ValueError(
        f"Environment variable {name} must be true or false."
    )


@dataclass(frozen=True)
class Settings:
    """Central configuration for the sender worker."""

    database_url: str
    rabbitmq_url: str

    prefetch_count: int
    concurrency: int

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_use_tls: bool

    smtp_pool_size: int
    smtp_max_active_connections: int
    smtp_max_emails_per_connection: int

    log_level: str


def load_config() -> Settings:
    """Load and validate configuration from environment variables."""

    database_url = os.getenv("DATABASE_URL", "").strip()
    rabbitmq_url = os.getenv("RABBITMQ_URL", "").strip()

    smtp_host = os.getenv("SMTP_HOST", "mailpit").strip()

    smtp_port = get_int_env("SMTP_PORT", 1025)
    prefetch_count = get_int_env("PREFETCH_COUNT", 50)
    concurrency = get_int_env("CONCURRENCY", 20)

    smtp_pool_size = get_int_env("SMTP_POOL_SIZE", 100)

    smtp_max_active_connections = get_int_env(
        "SMTP_MAX_ACTIVE_CONNECTIONS",
        20,
    )

    smtp_max_emails_per_connection = get_int_env(
        "SMTP_MAX_EMAILS_PER_CONNECTION",
        20,
    )

    smtp_use_tls = get_bool_env("SMTP_USE_TLS", False)

    smtp_user = os.getenv("SMTP_USER", "")
    smtp_password = os.getenv("SMTP_PASSWORD", "")

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if not database_url:
        raise ValueError("DATABASE_URL is required.")

    if not rabbitmq_url:
        raise ValueError("RABBITMQ_URL is required.")

    if not smtp_host:
        raise ValueError("SMTP_HOST cannot be empty.")

    if smtp_port > 65535:
        raise ValueError("SMTP_PORT must not exceed 65535.")

    if smtp_use_tls and smtp_port == 1025:
        raise ValueError(
            "Mailpit development SMTP uses port 1025 without TLS."
        )

    if smtp_max_active_connections > smtp_pool_size:
        raise ValueError(
            "SMTP_MAX_ACTIVE_CONNECTIONS cannot exceed SMTP_POOL_SIZE."
        )

    if log_level not in logging.getLevelNamesMapping():
        raise ValueError(f"Invalid LOG_LEVEL: {log_level}")

    # Basic URL validation.
    database = urlparse(database_url)
    rabbitmq = urlparse(rabbitmq_url)

    if not database.scheme or not database.hostname:
        raise ValueError("DATABASE_URL is not a valid connection URL.")

    if rabbitmq.scheme not in ("amqp", "amqps") or not rabbitmq.hostname:
        raise ValueError("RABBITMQ_URL is not a valid AMQP connection URL.")

    return Settings(
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        prefetch_count=prefetch_count,
        concurrency=concurrency,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        smtp_use_tls=smtp_use_tls,
        smtp_pool_size=smtp_pool_size,
        smtp_max_active_connections=smtp_max_active_connections,
        smtp_max_emails_per_connection=smtp_max_emails_per_connection,
        log_level=log_level,
    )


# Load configuration when the module is imported.
settings = load_config()
