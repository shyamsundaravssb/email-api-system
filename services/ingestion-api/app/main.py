
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import Settings
from app.db import create_db_pool, close_db_pool
from app.routes.batches import router as batches_router
from app.outbox_worker import outbox_worker


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    app.state.settings = settings

    app.state.db_pool = await create_db_pool(
        settings.database_url
    )

    worker_task = asyncio.create_task(
        outbox_worker(
            db_pool=app.state.db_pool,
            settings=settings,
        )
    )

    app.state.outbox_worker_task = worker_task

    print("Ingestion API started successfully")
    print("PostgreSQL connection pool initialized")
    print("Generation outbox worker started")

    try:
        yield
    finally:
        worker_task.cancel()

        try:
            await worker_task
        except asyncio.CancelledError:
            pass

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


app.include_router(batches_router)
