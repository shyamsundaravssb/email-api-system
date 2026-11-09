import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health")
async def health():
    """Check whether the API process is running."""

    return {"status": "ok"}


@router.get("/health/ready")
async def readiness(request: Request):
    """Check readiness according to the configured operating mode."""

    mode = getattr(request.app.state, "mode", "unknown")

    if mode == "standalone":
        return {
            "status": "ready",
            "mode": "standalone",
            "database": "not_required",
            "rabbitmq": "not_required",
        }

    database = getattr(request.app.state, "database", None)
    publisher = getattr(request.app.state, "publisher", None)

    if database is None or publisher is None:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "database": False,
                "rabbitmq": False,
            },
        )

    try:
        database_ready = await database.check()
    except Exception:
        logger.exception("PostgreSQL readiness check failed")
        database_ready = False

    try:
        rabbitmq_ready = await publisher.check()
    except Exception:
        logger.exception("RabbitMQ readiness check failed")
        rabbitmq_ready = False

    ready = database_ready and rabbitmq_ready

    return JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "not_ready",
            "database": database_ready,
            "rabbitmq": rabbitmq_ready,
        },
    )
