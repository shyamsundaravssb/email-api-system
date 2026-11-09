
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings
from app.db import create_db_pool, close_db_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application startup and shutdown lifecycle.
    """

    settings = Settings.from_env()
    app.state.settings = settings

    # Initialize PostgreSQL connection pool
    app.state.db_pool = await create_db_pool(
        settings.database_url
    )

    print("Ingestion API started successfully")
    print("PostgreSQL connection pool initialized")

    yield

    # Graceful shutdown
    await close_db_pool(app.state.db_pool)

    print("Ingestion API shutdown completed")


app = FastAPI(
    title="Email API - Ingestion Service",
    description="Bulk email ingestion and validation service",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok"}
