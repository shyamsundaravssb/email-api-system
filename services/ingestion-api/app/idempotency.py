
import hashlib
import json
import uuid
from typing import Any

from app.repository import create_batch


def calculate_request_hash(payload: dict[str, Any]) -> str:
    """Generate a deterministic SHA-256 hash of the request body."""

    canonical_body = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical_body.encode("utf-8")).hexdigest()


async def acquire_idempotency_lock(
    conn,
    account_id: uuid.UUID,
    idem_key: str,
) -> bool:
    """Acquire a nonblocking transaction-level lock for this key."""

    lock_identity = f"{account_id}:{idem_key}"

    return await conn.fetchval(
        """
        SELECT pg_try_advisory_xact_lock(
            hashtextextended($1, 0)
        )
        """,
        lock_identity,
    )


async def get_idempotency_record(
    conn,
    account_id: uuid.UUID,
    idem_key: str,
) -> dict[str, Any] | None:
    """Fetch an existing idempotency record."""

    row = await conn.fetchrow(
        """
        SELECT
            id,
            request_hash,
            state,
            batch_id,
            response_status,
            response_body,
            created_at,
            expires_at
        FROM idempotency_keys
        WHERE account_id = $1 AND idem_key = $2
        """,
        account_id,
        idem_key,
    )
    return dict(row) if row else None


async def delete_idempotency_record(
    conn,
    account_id: uuid.UUID,
    idem_key: str,
) -> None:
    """Delete an idempotency record."""

    await conn.execute(
        """
        DELETE FROM idempotency_keys
        WHERE account_id = $1 AND idem_key = $2
        """,
        account_id,
        idem_key,
    )


async def begin_idempotency_request(
    conn,
    account_id: uuid.UUID,
    idem_key: str,
    request_hash: str,
) -> dict[str, Any] | None:
    """
    Acquire the key lock and check for an existing result.

    Returns a replay result if a completed matching request exists.
    Returns None when this request may proceed.

    The caller must keep the transaction open until processing finishes.
    """

    acquired = await acquire_idempotency_lock(
        conn,
        account_id,
        idem_key,
    )

    if not acquired:
        raise IdempotencyConflict()

    existing = await get_idempotency_record(
        conn,
        account_id,
        idem_key,
    )

    if existing is None:
        return None

    now = await conn.fetchval("SELECT NOW()")

    if existing["expires_at"] <= now:
        await delete_idempotency_record(conn, account_id, idem_key)
        return None

    if existing["request_hash"] != request_hash:
        raise IdempotencyPayloadMismatch()

    if existing["state"] == "completed":
        return {
            "replayed": True,
            "response_status": existing["response_status"],
            "response_body": normalize_response_body(
                existing["response_body"]
            ),
        }

    # A committed in-progress record can only be recovered after
    # the lock is acquired and the record is at least 60 seconds old.
    age_seconds = await conn.fetchval(
        """
        SELECT EXTRACT(EPOCH FROM (NOW() - $1::timestamptz))
        """,
        existing["created_at"],
    )

    if age_seconds < 60:
        raise IdempotencyConflict()

    await delete_idempotency_record(conn, account_id, idem_key)
    return None


async def store_successful_response(
    conn,
    account_id: uuid.UUID,
    idem_key: str,
    request_hash: str,
    ttl_hours: int,
    batch_id: uuid.UUID,
    response_body: dict[str, Any],
) -> None:
    """
    Store a completed 202 response.

    The record, batch, and email rows are committed together.
    """

    await conn.execute(
        """
        INSERT INTO idempotency_keys (
            id,
            account_id,
            idem_key,
            request_hash,
            state,
            batch_id,
            response_status,
            response_body,
            expires_at
        )
        VALUES (
            $1, $2, $3, $4, 'completed',
            $5, 202, $6::jsonb,
            NOW() + ($7 * INTERVAL '1 hour')
        )
        """,
        uuid.uuid4(),
        account_id,
        idem_key,
        request_hash,
        batch_id,
        json.dumps(response_body),
        ttl_hours,
    )


async def create_batch_for_request(
    conn,
    account_id: uuid.UUID,
    validation_result: dict[str, Any],
    queue_capacity: int,
) -> dict[str, Any]:
    """Delegate batch creation to the repository."""

    return await create_batch(
        conn=conn,
        account_id=account_id,
        validation_result=validation_result,
        queue_capacity=queue_capacity,
    )


def normalize_response_body(body):
    """Convert asyncpg JSONB strings into Python dictionaries."""

    if isinstance(body, str):
        return json.loads(body)
    return body


class IdempotencyConflict(Exception):
    """An idempotency key is currently being processed."""


class IdempotencyPayloadMismatch(Exception):
    """The same idempotency key was used with a different request body."""
