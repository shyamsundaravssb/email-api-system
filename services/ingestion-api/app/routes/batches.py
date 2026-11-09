
import logging
import uuid

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    Header,
    HTTPException,
    Request,
)
from fastapi.responses import JSONResponse

from app.auth import get_current_account
from app.idempotency import (
    begin_idempotency_request,
    calculate_request_hash,
    store_successful_response,
    IdempotencyConflict,
    IdempotencyPayloadMismatch,
)
from app.repository import (
    create_batch,
    create_generation_handoff,
)
from app.schemas import (
    BatchCreateErrorResponse,
    BatchCreateRequest,
    BatchCreateResponse,
)
from app.validation import validate_batch
from app.generation_handoff import handoff_batch_to_generation


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/batches",
    tags=["Batches"],
)


def build_error_response(
    error: str,
    total: int,
    results: list[dict],
) -> dict:
    """Validate and serialize a zero-accepted response."""

    return BatchCreateErrorResponse.model_validate({
        "error": error,
        "total": total,
        "results": results,
    }).model_dump(mode="json")


@router.post("")
async def create_email_batch(
    payload: BatchCreateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    account: dict = Depends(get_current_account),
    idempotency_key: str | None = Header(
        default=None,
        alias="Idempotency-Key",
    ),
):
    settings = request.app.state.settings
    pool = request.app.state.db_pool

    request_body = payload.model_dump(mode="json")
    request_hash = calculate_request_hash(request_body)

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():

                # 1. Check idempotency before business validation.
                if idempotency_key:
                    replay = await begin_idempotency_request(
                        conn=conn,
                        account_id=account["id"],
                        idem_key=idempotency_key,
                        request_hash=request_hash,
                    )

                    if replay is not None:
                        response_body = replay["response_body"]
                        response_status = replay["response_status"]
                        replayed = True

                        # Validate stored response before replaying it.
                        response_body = BatchCreateResponse.model_validate(
                            response_body
                        ).model_dump(mode="json")

                    else:
                        replayed = False
                else:
                    replayed = False

                if not (idempotency_key and replayed):

                    # 2. Validate all recipients using this transaction's
                    # existing connection.
                    validation_result = await validate_batch(
                        recipients=payload.recipients,
                        db_pool=pool,
                        max_recipients=settings.max_recipients_per_batch,
                        conn=conn,
                    )

                    # 3. Handle batches exceeding the configured limit.
                    if not validation_result["valid"]:
                        results = [
                            {
                                "position": position,
                                "to_email": recipient.to_email,
                                "status": "rejected",
                                "reason": "too_many_recipients",
                            }
                            for position, recipient in enumerate(
                                payload.recipients,
                                start=1,
                            )
                        ]

                        response_status = 422
                        response_body = build_error_response(
                            "too_many_recipients",
                            len(payload.recipients),
                            results,
                        )

                    else:
                        # 4. Log all-invalid requests without logging
                        # email addresses or recipient variables.
                        if not validation_result["accepted"]:
                            reason_counts = {}

                            for item in validation_result["rejected"]:
                                reason = item["error"]
                                reason_counts[reason] = (
                                    reason_counts.get(reason, 0) + 1
                                )

                            logger.info(
                                "Batch rejected: no valid recipients",
                                extra={
                                    "account_id": str(account["id"]),
                                    "total": len(payload.recipients),
                                    "rejection_reasons": reason_counts,
                                },
                            )

                        # 5. Allocate capacity and create rows only if
                        # at least one recipient can be accepted.
                        batch_result = await create_batch(
                            conn=conn,
                            account_id=account["id"],
                            validation_result=validation_result,
                            queue_capacity=settings.queue_capacity,
                        )

                        if batch_result["accepted"] > 0:
                            response_status = 202

                            # Create the outbox record in the same transaction.
                            await create_generation_handoff(
                                conn=conn,
                                batch_id=uuid.UUID(batch_result["batch_id"]),
                            )

                            response_body = BatchCreateResponse.model_validate(
                                batch_result
                            ).model_dump(mode="json")

                            # 6. Remember only successful 202 responses.
                            if idempotency_key:
                                await store_successful_response(
                                    conn=conn,
                                    account_id=account["id"],
                                    idem_key=idempotency_key,
                                    request_hash=request_hash,
                                    ttl_hours=settings.idempotency_ttl_hours,
                                    batch_id=uuid.UUID(
                                        batch_result["batch_id"]
                                    ),
                                    response_body=response_body,
                                )

                        else:
                            has_server_busy = any(
                                item.get("reason") == "server_busy"
                                for item in batch_result["results"]
                            )

                            if has_server_busy:
                                response_status = 503
                                error = "queue_capacity_exhausted"
                            else:
                                response_status = 422
                                error = "all_recipients_invalid"

                            response_body = build_error_response(
                                error,
                                batch_result["total"],
                                batch_result["results"],
                            )

                        # Schedule Generation API handoff only for newly created batches.
        if response_status == 202 and not replayed:
            batch_id = response_body["batch_id"]

            background_tasks.add_task(
                handoff_batch_to_generation,
                batch_id=batch_id,
                db_pool=pool,
                settings=settings,
            )
    except IdempotencyPayloadMismatch:
        raise HTTPException(
            status_code=422,
            detail=(
                "Idempotency key was already used "
                "with a different request body"
            ),
        )

    except IdempotencyConflict:
        raise HTTPException(
            status_code=409,
            detail=(
                "Request with this idempotency key "
                "is already being processed"
            ),
        )

    headers = {}

    if idempotency_key:
        headers["Idempotency-Replayed"] = str(replayed).lower()

    return JSONResponse(
        status_code=response_status,
        content=response_body,
        headers=headers,
    )
