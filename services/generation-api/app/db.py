import json
import logging
from typing import Any
from uuid import UUID

import asyncpg


logger = logging.getLogger(__name__)


class Database:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(
        cls,
        database_url: str,
        min_size: int = 1,
        max_size: int = 20,
    ) -> "Database":
        """
        Create an asynchronous PostgreSQL connection pool.

        JSON and JSONB values are decoded into Python objects
        so that emails.variables can be used as a dictionary.
        """

        async def initialize_connection(
            connection: asyncpg.Connection,
        ) -> None:
            await connection.set_type_codec(
                "json",
                schema="pg_catalog",
                encoder=json.dumps,
                decoder=json.loads,
            )

            await connection.set_type_codec(
                "jsonb",
                schema="pg_catalog",
                encoder=json.dumps,
                decoder=json.loads,
            )

        pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=min_size,
            max_size=max_size,
            command_timeout=30,
            init=initialize_connection,
        )

        logger.info("PostgreSQL connection pool created")

        return cls(pool)

    async def close(self) -> None:
        """Close all PostgreSQL connections."""

        if self.pool is not None:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")

    async def check(self) -> bool:
        """Check whether PostgreSQL is reachable."""

        try:
            async with self.pool.acquire() as connection:
                await connection.fetchval("SELECT 1")

            return True

        except Exception:
            logger.exception("PostgreSQL health check failed")
            return False

    def acquire(self):
        """
        Return a connection from the pool.

        Usage:
            async with database.acquire() as connection:
                ...
        """

        return self.pool.acquire()

    async def get_email_for_update(
        self,
        connection: asyncpg.Connection,
        email_id: UUID,
    ) -> dict[str, Any] | None:
        """
        Retrieve an email and lock its row for the current
        database transaction.

        The query also retrieves the associated batch and
        account sender information.
        """

        row = await connection.fetchrow(
            """
            SELECT
                e.id AS email_id,
                e.batch_id,
                e.position,
                e.template_id,
                e.to_email,
                e.to_name,
                e.variables,
                e.status,
                e.tracking_token,
                e.message_id,

                b.account_id,

                a.from_email,
                a.from_name

            FROM emails AS e

            JOIN batches AS b
                ON b.id = e.batch_id

            JOIN accounts AS a
                ON a.id = b.account_id

            WHERE e.id = $1

            FOR UPDATE OF e
            """,
            email_id,
        )

        return dict(row) if row else None

    async def get_template(
        self,
        connection: asyncpg.Connection,
        template_id: str,
    ) -> dict[str, Any] | None:
        """Retrieve a template from PostgreSQL."""

        row = await connection.fetchrow(
            """
            SELECT
                template_id,
                subject_template,
                html_template,
                text_template,
                required_variables

            FROM templates

            WHERE template_id = $1
            """,
            template_id,
        )

        return dict(row) if row else None

    async def mark_queued(
        self,
        connection: asyncpg.Connection,
        email_id: UUID,
        message_id: str,
    ) -> bool:
        """
        Mark an accepted email as queued.

        This method must only be called after RabbitMQ has
        confirmed that the message was published.
        """

        result = await connection.execute(
            """
            UPDATE emails

            SET
                status = 'queued',
                message_id = $2,
                queued_at = now(),
                updated_at = now(),
                last_error = NULL

            WHERE id = $1
              AND status = 'accepted'
            """,
            email_id,
            message_id,
        )

        return result == "UPDATE 1"

    async def mark_failed(
        self,
        connection: asyncpg.Connection,
        email_id: UUID,
        error_message: str,
    ) -> bool:
        """
        Mark an accepted email as permanently failed.

        Infrastructure failures should not use this method.
        They should leave the email accepted so that the
        ingestion service can retry it.
        """

        result = await connection.execute(
            """
            UPDATE emails

            SET
                status = 'failed',
                last_error = $2,
                failed_at = now(),
                updated_at = now()

            WHERE id = $1
              AND status = 'accepted'
            """,
            email_id,
            error_message[:2000],
        )

        return result == "UPDATE 1"

    async def get_email_status(
        self,
        email_id: UUID,
    ) -> str | None:
        """Retrieve the current status of an email."""

        async with self.pool.acquire() as connection:
            return await connection.fetchval(
                """
                SELECT status
                FROM emails
                WHERE id = $1
                """,
                email_id,
            )
