import json
import uuid
from typing import Any

from app.capacity import allocate_capacity


async def create_batch(
    conn,
    account_id: uuid.UUID,
    validation_result: dict[str, Any],
    queue_capacity: int,
) -> dict[str, Any]:
    """
    Allocate capacity and create a batch atomically.

    No batch or email rows are inserted when zero recipients
    can be accepted.
    """

    accepted_candidates = validation_result["accepted"]
    rejected_candidates = validation_result["rejected"]

    total_count = len(accepted_candidates) + len(rejected_candidates)

    # Capacity allocation is skipped when every recipient is invalid.
    if accepted_candidates:
        capacity_result = await allocate_capacity(
            conn=conn,
            valid_recipients=accepted_candidates,
            queue_capacity=queue_capacity,
        )
        accepted = capacity_result["accepted"]
        server_busy = capacity_result["server_busy"]
    else:
        accepted = []
        server_busy = []

    # Combine results and preserve original request order.
    all_candidates = []

    for item in accepted:
        all_candidates.append((item, "accepted", None))

    for item in server_busy:
        all_candidates.append((item, "rejected", "server_busy"))

    for item in rejected_candidates:
        all_candidates.append((item, "rejected", item["error"]))

    all_candidates.sort(key=lambda entry: entry[0]["position"])

    # Zero accepted: return results without creating database rows.
    if not accepted:
        return {
            "batch_id": None,
            "total": total_count,
            "accepted": 0,
            "rejected": total_count,
            "results": [
                {
                    "position": item["position"],
                    "to_email": item["recipient"].to_email,
                    "status": status,
                    "reason": reason,
                }
                for item, status, reason in all_candidates
            ],
        }

    batch_id = uuid.uuid4()

    await conn.execute(
        """
        INSERT INTO batches (
            id,
            account_id,
            total_count,
            accepted_count,
            rejected_count
        )
        VALUES ($1, $2, $3, $4, $5)
        """,
        batch_id,
        account_id,
        total_count,
        len(accepted),
        total_count - len(accepted),
    )

    recipient_results = []

    for item, status, reject_reason in all_candidates:
        recipient = item["recipient"]
        email_id = uuid.uuid4()
        tracking_token = uuid.uuid4()

        await conn.execute(
            """
            INSERT INTO emails (
                id,
                batch_id,
                position,
                template_id,
                to_email,
                to_name,
                variables,
                status,
                reject_reason,
                tracking_token
            )
            VALUES (
                $1, $2, $3, $4, $5,
                $6, $7::jsonb, $8, $9, $10
            )
            """,
            email_id,
            batch_id,
            item["position"],
            recipient.template_id,
            recipient.to_email,
            recipient.to_name,
            json.dumps(recipient.variables or {}),
            status,
            reject_reason,
            tracking_token,
        )

        result = {
            "position": item["position"],
            "to_email": recipient.to_email,
            "status": status,
        }

        if status == "accepted":
            result["email_id"] = str(email_id)
        else:
            result["reason"] = reject_reason

        recipient_results.append(result)

    return {
        "batch_id": str(batch_id),
        "total": total_count,
        "accepted": len(accepted),
        "rejected": total_count - len(accepted),
        "results": recipient_results,
    }



async def get_generation_payload(
    conn,
    batch_id: uuid.UUID,
) -> dict[str, Any] | None:
    """
    Build the internal Generation API payload for a batch.

    Only accepted recipients are included.
    """

    rows = await conn.fetch(
        """
        SELECT
            b.id AS batch_id,
            a.from_email,
            a.from_name,
            e.id AS email_id,
            e.tracking_token,
            e.template_id,
            e.to_email,
            e.to_name,
            e.variables
        FROM batches b
        JOIN accounts a
            ON a.id = b.account_id
        JOIN emails e
            ON e.batch_id = b.id
        WHERE b.id = $1
          AND e.status = 'accepted'
        ORDER BY e.position
        """,
        batch_id,
    )

    if not rows:
        return None

    first = rows[0]

    return {
        "batch_id": str(first["batch_id"]),
        "from_email": first["from_email"],
        "from_name": first["from_name"],
        "items": [
            {
                "email_id": str(row["email_id"]),
                "tracking_token": str(row["tracking_token"]),
                "template_id": row["template_id"],
                "to_email": row["to_email"],
                "to_name": row["to_name"],
                "variables": (
                    json.loads(row["variables"])
                    if isinstance(row["variables"], str)
                    else row["variables"]
                ),
            }
            for row in rows
        ],
    }


async def claim_generation_handoff(
    conn,
    lease_seconds: int = 60,
    batch_id: uuid.UUID | None = None,
) -> dict[str, Any] | None:
    """
    Atomically claim one due generation handoff.

    Expired processing claims can also be reclaimed.
    """

    claim_token = uuid.uuid4()

    async with conn.transaction():
        row = await conn.fetchrow(
            """
            WITH candidate AS (
                SELECT id
                FROM generation_handoffs
                WHERE (
                    (
                        status = 'pending'
                        AND next_retry_at <= NOW()
                    )
                    OR (
                        status = 'processing'
                        AND locked_until <= NOW()
                    )
                )
                AND ($3::uuid IS NULL OR batch_id = $3)
                ORDER BY next_retry_at, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE generation_handoffs AS h
            SET
                status = 'processing',
                claim_token = $1,
                locked_until = NOW() + ($2 * INTERVAL '1 second'),
                attempts = attempts + 1,
                updated_at = NOW()
            FROM candidate
            WHERE h.id = candidate.id
            RETURNING
                h.id,
                h.batch_id,
                h.attempts,
                h.claim_token
            """,
            claim_token,
            lease_seconds,
            batch_id,
        )

    return dict(row) if row else None


async def mark_generation_handoff_submitted(
    conn,
    handoff_id: uuid.UUID,
    claim_token: uuid.UUID,
) -> bool:
    """Mark a claimed handoff as submitted."""

    result = await conn.execute(
        """
        UPDATE generation_handoffs
        SET
            status = 'submitted',
            submitted_at = NOW(),
            claim_token = NULL,
            locked_until = NULL,
            last_error = NULL,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'processing'
          AND claim_token = $2
        """,
        handoff_id,
        claim_token,
    )

    return result == "UPDATE 1"

async def fail_generation_handoff(
    conn,
    handoff_id: uuid.UUID,
    claim_token: uuid.UUID,
    error: str,
) -> bool:
    """Permanently fail a handoff after exhausting retry attempts."""
    result = await conn.execute(
        """
        UPDATE generation_handoffs
        SET
            status = 'failed',
            failed_at = NOW(),
            claim_token = NULL,
            locked_until = NULL,
            last_error = $3,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'processing'
          AND claim_token = $2
        """,
        handoff_id,
        claim_token,
        error[:1000],
    )

    return result == "UPDATE 1"


async def delete_failed_generation_batch(
    conn,
    handoff_id: uuid.UUID,
    claim_token: uuid.UUID,
) -> dict[str, Any] | None:
    """
    Delete a batch and all associated records after terminal
    generation handoff failure.

    Returns batch information if the claim is valid and deletion
    succeeds. Returns None if the claim is no longer owned.
    """

    async with conn.transaction():

        # Verify ownership and lock the handoff row.
        handoff = await conn.fetchrow(
            """
            SELECT id, batch_id, attempts, last_error
            FROM generation_handoffs
            WHERE id = $1
                AND status = 'processing'
                AND claim_token = $2
                AND locked_until > NOW()
            FOR UPDATE
            """,
            handoff_id,
            claim_token,
        )

        if handoff is None:
            return None

        batch_id = handoff["batch_id"]

        # Delete records associated with individual emails.
        await conn.execute(
            """
            DELETE FROM email_attempts
            WHERE email_id IN (
                SELECT id FROM emails WHERE batch_id = $1
            )
            """,
            batch_id,
        )

        await conn.execute(
            """
            DELETE FROM email_events
            WHERE email_id IN (
                SELECT id FROM emails WHERE batch_id = $1
            )
            """,
            batch_id,
        )

        await conn.execute(
            """
            DELETE FROM tracking_links
            WHERE email_id IN (
                SELECT id FROM emails WHERE batch_id = $1
            )
            """,
            batch_id,
        )

        # Delete emails.
        await conn.execute(
            "DELETE FROM emails WHERE batch_id = $1",
            batch_id,
        )

        # Delete batch-dependent records.
        await conn.execute(
            "DELETE FROM generation_handoffs WHERE batch_id = $1",
            batch_id,
        )

        await conn.execute(
            "DELETE FROM idempotency_keys WHERE batch_id = $1",
            batch_id,
        )

        # Finally, delete the batch.
        await conn.execute(
            "DELETE FROM batches WHERE id = $1",
            batch_id,
        )

        return {
            "batch_id": batch_id,
            "attempts": handoff["attempts"],
        }


async def retry_generation_handoff(
    conn,
    handoff_id: uuid.UUID,
    claim_token: uuid.UUID,
    error: str,
    attempts: int,
    max_backoff_seconds: int = 300,
) -> bool:
    """Release a failed handoff and schedule its next retry."""

    backoff_seconds = min(
        15 * (2 ** max(attempts - 1, 0)),
        max_backoff_seconds,
    )

    result = await conn.execute(
        """
        UPDATE generation_handoffs
        SET
            status = 'pending',
            next_retry_at = NOW() + ($3 * INTERVAL '1 second'),
            claim_token = NULL,
            locked_until = NULL,
            last_error = $4,
            updated_at = NOW()
        WHERE id = $1
          AND status = 'processing'
          AND claim_token = $2
        """,
        handoff_id,
        claim_token,
        backoff_seconds,
        error[:1000],
    )

    return result == "UPDATE 1"


async def create_generation_handoff(
    conn,
    batch_id: uuid.UUID,
) -> None:
    """Create an outbox record for a newly accepted batch."""

    await conn.execute(
        """
        INSERT INTO generation_handoffs (batch_id)
        VALUES ($1)
        ON CONFLICT (batch_id) DO NOTHING
        """,
        batch_id,
    )