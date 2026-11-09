
import asyncio
import logging

from app.generation_handoff import handoff_batch_to_generation
from app.repository import claim_generation_handoff

logger = logging.getLogger(__name__)


async def outbox_worker(db_pool, settings):
    """
    Continuously find and process due generation handoffs.
    """

    logger.info("Generation outbox worker started")

    while True:
        try:
            async with db_pool.acquire() as conn:
                claim = await claim_generation_handoff(
                    conn=conn,
                    lease_seconds=60,
                )

            if claim:
                batch_id = str(claim["batch_id"])

                logger.info(
                    "Processing generation handoff",
                    extra={"batch_id": batch_id},
                )

                await handoff_batch_to_generation(
                    batch_id=batch_id,
                    db_pool=db_pool,
                    settings=settings,
                    existing_claim=claim,
                )

                continue

            await asyncio.sleep(
                settings.generation_handoff_poll_seconds
            )

        except asyncio.CancelledError:
            logger.info("Generation outbox worker stopped")
            raise

        except Exception:
            logger.exception("Generation outbox polling failed")

            await asyncio.sleep(
                settings.generation_handoff_poll_seconds
            )
