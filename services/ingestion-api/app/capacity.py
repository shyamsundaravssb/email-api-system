
from typing import Any


LIVE_STATUSES = ("accepted", "queued", "retrying")
CAPACITY_LOCK_ID = 1001


async def allocate_capacity(
    conn,
    valid_recipients: list[dict[str, Any]],
    queue_capacity: int,
) -> dict[str, Any]:
    """
    Allocate available queue capacity to valid recipients.

    IMPORTANT:
    Call this function inside an active database transaction.

    The transaction-level advisory lock serializes capacity
    allocation across concurrent ingestion requests.
    """

    # Serialize all capacity allocation transactions.
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1)",
        CAPACITY_LOCK_ID,
    )

    # Count emails currently occupying queue capacity.
    occupied = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM emails
        WHERE status = ANY($1::text[])
        """,
        list(LIVE_STATUSES),
    )

    available = max(0, queue_capacity - occupied)

    accepted = valid_recipients[:available]
    server_busy = valid_recipients[available:]

    return {
        "occupied": occupied,
        "available": available,
        "accepted": accepted,
        "server_busy": server_busy,
    }
