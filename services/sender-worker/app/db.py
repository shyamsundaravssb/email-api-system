from typing import Literal

import asyncpg


AttemptOutcome = Literal[
    "success",
    "transient_error",
    "permanent_error",
]

EmailStatus = Literal[
    "sent",
    "retrying",
    "failed",
]


class Database:
    """Handles asynchronous PostgreSQL operations for the sender worker."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Create an asynchronous PostgreSQL connection pool."""

        if self.pool is not None:
            return

        self.pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=1,
            max_size=10,
            command_timeout=10,
        )

    async def close(self) -> None:
        """Close all connections in the pool."""

        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    def _get_pool(self) -> asyncpg.Pool:
        """Return the connection pool or raise an error."""

        if self.pool is None:
            raise RuntimeError(
                "Database is not connected. Call connect() first."
            )

        return self.pool

    async def get_email_status(self, email_id: str) -> str | None:
        """
        Retrieve the current status of an email.

        Returns None if the email does not exist.
        """

        pool = self._get_pool()

        async with pool.acquire() as connection:
            status = await connection.fetchval(
                """
                SELECT status
                FROM emails
                WHERE id = $1::uuid
                """,
                email_id,
            )

        return status

    async def record_attempt(
        self,
        email_id: str,
        attempt_no: int,
        outcome: AttemptOutcome,
        status: EmailStatus,
        smtp_code: int | None = None,
        smtp_response: str | None = None,
        error_message: str | None = None,
    ) -> bool:
        """
        Record an SMTP attempt and update the email status atomically.

        Returns:
            True  - Attempt recorded successfully.
            False - Email was already sent; no changes were made.
        """

        pool = self._get_pool()

        if attempt_no < 1:
            raise ValueError("attempt_no must be at least 1.")

        expected_status = {
            "success": "sent",
            "transient_error": ("retrying", "failed"),
            "permanent_error": "failed",
        }

        allowed_statuses = expected_status[outcome]

        if isinstance(allowed_statuses, str):
            allowed_statuses = (allowed_statuses,)

        if status not in allowed_statuses:
            raise ValueError(
                f"Invalid status '{status}' for outcome '{outcome}'."
            )

        if outcome == "success" and error_message is not None:
            raise ValueError("Successful attempts cannot have an error.")

        if outcome != "success" and not error_message:
            raise ValueError("Failed attempts require an error message.")

        async with pool.acquire() as connection:
            async with connection.transaction():

                # Lock the email row to prevent concurrent updates.
                email = await connection.fetchrow(
                    """
                    SELECT status
                    FROM emails
                    WHERE id = $1::uuid
                    FOR UPDATE
                    """,
                    email_id,
                )

                if email is None:
                    raise LookupError(
                        f"Email not found: {email_id}"
                    )

                # Never overwrite an email that has already been sent.
                if email["status"] == "sent":
                    return False

                # Update the main email record.
                result = await connection.execute(
                    """
                    UPDATE emails
                    SET
                        status = $2,
                        attempts = GREATEST(attempts, $3),
                        last_error = $4,
                        sent_at = CASE
                            WHEN $2 = 'sent' THEN now()
                            ELSE sent_at
                        END,
                        failed_at = CASE
                            WHEN $2 = 'failed' THEN now()
                            ELSE NULL
                        END,
                        updated_at = now()
                    WHERE id = $1::uuid
                    """,
                    email_id,
                    status,
                    attempt_no,
                    error_message,
                )

                if result != "UPDATE 1":
                    raise RuntimeError(
                        f"Failed to update email: {email_id}"
                    )

                # Record the individual SMTP attempt.
                await connection.execute(
                    """
                    INSERT INTO email_attempts (
                        email_id,
                        attempt_no,
                        outcome,
                        smtp_code,
                        smtp_response
                    )
                    VALUES (
                        $1::uuid,
                        $2,
                        $3,
                        $4,
                        $5
                    )
                    """,
                    email_id,
                    attempt_no,
                    outcome,
                    smtp_code,
                    smtp_response,
                )

        return True