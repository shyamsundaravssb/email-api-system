import logging
import uuid

from app.generation_client import GenerationClient
from app.repository import (
    claim_generation_handoff,
    delete_failed_generation_batch,
    get_generation_payload,
    mark_generation_handoff_submitted,
    retry_generation_handoff,
)

logger = logging.getLogger(__name__)


async def handoff_batch_to_generation(
    batch_id: str,
    db_pool,
    settings,
    existing_claim: dict | None = None,
):
    """
    Process a generation handoff.

    If existing_claim is supplied, use that claim.
    Otherwise, claim the batch's outbox record.
    """

    batch_uuid = uuid.UUID(batch_id)

    try:
        # Step 1: Obtain claim ownership.
        claim = existing_claim

        if claim is None:
            async with db_pool.acquire() as conn:
                claim = await claim_generation_handoff(
                    conn=conn,
                    batch_id=batch_uuid,
                    lease_seconds=60,
                )

        if not claim:
            logger.info(
                "Generation handoff not claimable",
                extra={"batch_id": batch_id},
            )
            return

        handoff_id = claim["id"]
        claim_token = claim["claim_token"]
        attempts = claim["attempts"]

        try:
            # Step 2: Retrieve accepted recipients.
            async with db_pool.acquire() as conn:
                payload = await get_generation_payload(
                    conn=conn,
                    batch_id=batch_uuid,
                )

            if not payload or not payload["items"]:
                raise RuntimeError(
                    "No accepted recipients found for handoff"
                )

            # Step 3: Submit to Generation API.
            client = GenerationClient(settings)
            result = await client.submit_batch(payload)

            # Step 4: Mark successful submission.
            async with db_pool.acquire() as conn:
                updated = await mark_generation_handoff_submitted(
                    conn=conn,
                    handoff_id=handoff_id,
                    claim_token=claim_token,
                )

            if not updated:
                logger.warning(
                    "Generation handoff claim was lost before completion",
                    extra={"batch_id": batch_id},
                )
                return

            logger.info(
                "Generation handoff submitted successfully",
                extra={
                    "batch_id": batch_id,
                    "received": result.get("received"),
                    "attempts": attempts,
                },
            )

        except Exception as exc:
            logger.exception(
                "Generation handoff attempt failed",
                extra={
                    "batch_id": batch_id,
                    "attempts": attempts,
                },
            )

            # Step 5: Retry or permanently fail the handoff.
            async with db_pool.acquire() as conn:
                if attempts >= settings.generation_handoff_max_attempts:
                    deleted = await delete_failed_generation_batch(
                        conn=conn,
                        handoff_id=handoff_id,
                        claim_token=claim_token,
                    )

                    if deleted:
                        logger.error(
                            "Generation handoff exhausted all attempts. "
                            "Batch and associated records deleted.",
                            extra={
                                "batch_id": str(deleted["batch_id"]),
                                "attempts": deleted["attempts"],
                                "max_attempts": settings.generation_handoff_max_attempts,
                                "error": str(exc)[:1000],
                            },
                        )
                    else:
                        logger.warning(
                            "Could not delete failed batch; "
                            "generation handoff claim may have expired.",
                            extra={"batch_id": batch_id},
                        )

                else:
                    updated = await retry_generation_handoff(
                        conn=conn,
                        handoff_id=handoff_id,
                        claim_token=claim_token,
                        error=str(exc),
                        attempts=attempts,
                    )

                    if updated:
                        logger.info(
                            "Generation handoff scheduled for retry",
                            extra={
                                "batch_id": batch_id,
                                "attempts": attempts,
                            },
                        )
                    else:
                        logger.warning(
                            "Could not release generation handoff; "
                            "claim may have expired",
                            extra={"batch_id": batch_id},
                        )

    except Exception:
        logger.exception(
            "Generation handoff processor failed",
            extra={"batch_id": batch_id},
        )