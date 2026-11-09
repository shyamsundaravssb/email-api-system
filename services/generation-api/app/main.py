import logging
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

from fastapi import FastAPI

from app.routes.generate import router as generate_router
from app.routes.health import router as health_router


logger = logging.getLogger(__name__)


def get_positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a valid integer") from exc

    if value <= 0:
        raise RuntimeError(f"{name} must be greater than zero")

    return value


@asynccontextmanager
async def lifespan(app: FastAPI):
    mode = os.getenv("GENERATION_MODE", "standalone").strip().lower()
    internal_token = os.getenv("INTERNAL_TOKEN", "").strip()

    if not internal_token:
        raise RuntimeError("INTERNAL_TOKEN is missing")

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    app.state.mode = mode
    app.state.settings = SimpleNamespace(
        internal_token=internal_token,
        generation_max_items=get_positive_int(
            "GENERATION_MAX_ITEMS", 1000
        ),
    )
    app.state.database = None
    app.state.publisher = None
    app.state.processor = None

    database = None
    publisher = None

    try:
        logger.info("Starting Generation API in %s mode", mode)

        if mode == "standalone":
            logger.info(
                "Standalone mode: PostgreSQL and RabbitMQ connections "
                "are disabled"
            )

        elif mode == "integrated":
            from app.config import Settings
            from app.db import Database
            from app.processor import GenerationProcessor
            from app.publisher import RabbitMQPublisher

            settings = Settings.from_env()
            database = await Database.connect(
                database_url=settings.database_url
            )

            publisher = RabbitMQPublisher(
                rabbitmq_url=settings.rabbitmq_url
            )
            await publisher.connect()

            app.state.settings = settings
            app.state.database = database
            app.state.publisher = publisher
            app.state.processor = GenerationProcessor(
                database=database,
                publisher=publisher,
                settings=settings,
            )

            logger.info("Integrated Generation API started successfully")

        else:
            raise RuntimeError(
                "GENERATION_MODE must be 'standalone' or 'integrated'"
            )

        yield

    finally:
        logger.info("Shutting down Generation API")

        if publisher is not None:
            await publisher.close()

        if database is not None:
            await database.close()


app = FastAPI(
    title="Generation API",
    description="Email rendering and generation service",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(generate_router)
