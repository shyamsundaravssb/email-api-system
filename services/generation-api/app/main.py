import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings
from app.db import Database
from app.processor import GenerationProcessor
from app.publisher import RabbitMQPublisher
from app.routes.generate import router as generate_router
from app.routes.health import router as health_router


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    database = None
    publisher = None

    try:
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
