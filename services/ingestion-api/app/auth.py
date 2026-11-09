
import hashlib

import asyncpg
from fastapi import Header, HTTPException, Request


async def get_current_account(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    """
    Authenticate a marketer using their API key.

    The raw API key is never stored in the database.
    Only its SHA-256 hash is used for lookup.
    """

    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    api_key_hash = hashlib.sha256(
        x_api_key.encode("utf-8")
    ).hexdigest()

    pool: asyncpg.Pool = request.app.state.db_pool

    async with pool.acquire() as connection:
        account = await connection.fetchrow(
            """
            SELECT id, name, from_email, from_name
            FROM accounts
            WHERE api_key_hash = $1
            """,
            api_key_hash,
        )

    if account is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return dict(account)
