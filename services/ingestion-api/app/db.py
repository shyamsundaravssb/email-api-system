
import asyncpg


async def create_db_pool(database_url: str) -> asyncpg.Pool:
    """
    Create an asynchronous PostgreSQL connection pool.
    """
    return await asyncpg.create_pool(
        dsn=database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )


async def close_db_pool(pool: asyncpg.Pool) -> None:
    """
    Gracefully close all database connections.
    """
    if pool is not None:
        await pool.close()
