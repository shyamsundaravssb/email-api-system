
import asyncio
import os
from pathlib import Path

import asyncpg


ROOT_DIR = Path(__file__).resolve().parent.parent

MIGRATIONS = [
    ("001", ROOT_DIR / "db/init/001_schema.sql"),
    ("002", ROOT_DIR / "db/init/002_seed_accounts.sql"),
    ("003", ROOT_DIR / "db/init/003_seed_templates.sql"),
    ("004", ROOT_DIR / "db/migrations/004_generation_handoffs.sql"),
    ("005", ROOT_DIR / "db/migrations/005_generation_handoffs_failure_state.sql"),
]

ADVISORY_LOCK_ID = 20261109

BASE_TABLES = [
    "accounts", "templates", "batches", "emails",
    "email_attempts", "idempotency_keys",
    "tracking_links", "email_events",
]

REQUIRED_COLUMNS = {
    "accounts": [
        "id", "name", "api_key_hash", "from_email",
        "from_name", "created_at",
    ],
    "templates": [
        "template_id", "subject_template", "html_template",
        "text_template", "required_variables",
        "created_at", "updated_at",
    ],
    "batches": [
        "id", "account_id", "total_count",
        "accepted_count", "rejected_count", "created_at",
    ],
    "emails": [
        "id", "batch_id", "position", "template_id",
        "to_email", "to_name", "variables", "status",
        "reject_reason", "attempts", "last_error",
        "message_id", "tracking_token", "created_at",
        "queued_at", "sent_at", "failed_at", "updated_at",
    ],
    "email_attempts": [
        "id", "email_id", "attempt_no", "outcome",
        "smtp_code", "smtp_response", "attempted_at",
    ],
    "idempotency_keys": [
        "id", "account_id", "idem_key", "request_hash",
        "state", "batch_id", "response_status",
        "response_body", "created_at", "expires_at",
    ],
    "tracking_links": [
        "id", "email_id", "link_token",
        "original_url", "created_at",
    ],
    "email_events": [
        "id", "email_id", "event_type", "link_id",
        "occurred_at", "ip_address", "user_agent", "raw",
    ],
}

REQUIRED_CONSTRAINTS = {
    "accounts_api_key_hash_key": "u",
    "accounts_pkey": "p",
    "templates_pkey": "p",
    "batches_account_id_fkey": "f",
    "batches_pkey": "p",
    "emails_batch_id_fkey": "f",
    "emails_pkey": "p",
    "emails_status_check": "c",
    "emails_tracking_token_key": "u",
    "email_attempts_email_id_fkey": "f",
    "email_attempts_outcome_check": "c",
    "email_attempts_pkey": "p",
    "idempotency_keys_account_id_fkey": "f",
    "idempotency_keys_account_id_idem_key_key": "u",
    "idempotency_keys_batch_id_fkey": "f",
    "idempotency_keys_pkey": "p",
    "idempotency_keys_state_check": "c",
    "tracking_links_email_id_fkey": "f",
    "tracking_links_link_token_key": "u",
    "tracking_links_pkey": "p",
    "email_events_email_id_fkey": "f",
    "email_events_event_type_check": "c",
    "email_events_link_id_fkey": "f",
    "email_events_pkey": "p",
}

REQUIRED_INDEXES = {
    "idx_emails_batch_status",
    "idx_emails_live",
    "idx_emails_message_id",
    "idx_attempts_email",
    "idx_events_email_type",
    "idx_events_type_time",
}


async def table_exists(conn, table):
    return await conn.fetchval(
        "SELECT to_regclass($1) IS NOT NULL",
        f"public.{table}",
    )


async def column_exists(conn, table, column):
    return await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
              AND column_name = $2
        )
        """,
        table,
        column,
    )


async def verify_initial_schema(conn):
    existing = [
        table for table in BASE_TABLES
        if await table_exists(conn, table)
    ]

    if not existing:
        return False

    if len(existing) != len(BASE_TABLES):
        raise RuntimeError(
            "Partial initial schema detected. "
            "Migration 001 cannot be safely baselined."
        )

    for table, columns in REQUIRED_COLUMNS.items():
        for column in columns:
            if not await column_exists(conn, table, column):
                raise RuntimeError(
                    f"Missing column: {table}.{column}"
                )

    constraints = await conn.fetch(
        """
        SELECT conname, contype
        FROM pg_constraint
        WHERE connamespace = 'public'::regnamespace
        """
    )
    actual_constraints = {
        row["conname"]: (
            row["contype"].decode()
            if isinstance(row["contype"], bytes)
            else row["contype"]
        )
        for row in constraints
    }

    for name, expected_type in REQUIRED_CONSTRAINTS.items():
        if actual_constraints.get(name) != expected_type:
            raise RuntimeError(
                f"Missing or incorrect constraint: {name}"
            )

    indexes = await conn.fetch(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        """
    )
    actual_indexes = {row["indexname"] for row in indexes}

    missing_indexes = REQUIRED_INDEXES - actual_indexes

    if missing_indexes:
        raise RuntimeError(
            f"Missing indexes: {sorted(missing_indexes)}"
        )

    print("VERIFIED: Initial schema")
    return True


async def verify_account_seed(conn):
    return await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM accounts
            WHERE name = 'Grasshopper Development'
              AND from_email = 'test@grasshopper.optimoze.com'
              AND from_name = 'Grasshopper'
              AND api_key_hash =
                  encode(sha256('dev-api-key'::bytea), 'hex')
        )
        """
    )


async def verify_template_seed(conn):
    expected = {
        "otp_email": ["name", "otp"],
        "welcome_email": ["name"],
        "password_reset": ["name", "reset_link"],
    }

    rows = await conn.fetch(
        """
        SELECT template_id, required_variables
        FROM templates
        WHERE template_id = ANY($1::text[])
        """,
        list(expected),
    )

    actual = {
        row["template_id"]: list(row["required_variables"])
        for row in rows
    }

    return actual == expected


async def verify_handoff_schema(conn):
    if not await table_exists(conn, "generation_handoffs"):
        return False

    columns = [
        "id", "batch_id", "status", "attempts",
        "next_retry_at", "claim_token", "locked_until",
        "last_error", "created_at", "updated_at",
        "submitted_at",
    ]

    for column in columns:
        if not await column_exists(
            conn, "generation_handoffs", column
        ):
            raise RuntimeError(
                f"Migration 004 incomplete: missing {column}"
            )

    constraints = await conn.fetch(
        """
        SELECT conname, contype
        FROM pg_constraint
        WHERE conrelid = 'public.generation_handoffs'::regclass
        """
    )

    actual = {
        row["conname"]: (
            row["contype"].decode()
            if isinstance(row["contype"], bytes)
            else row["contype"]
        )
        for row in constraints
    }

    expected = {
        "generation_handoffs_pkey": "p",
        "generation_handoffs_batch_id_key": "u",
        "generation_handoffs_batch_id_fkey": "f",
        "generation_handoffs_attempts_check": "c",
        "generation_handoffs_check": "c",
        "generation_handoffs_status_check": "c",
    }

    for name, kind in expected.items():
        if actual.get(name) != kind:
            raise RuntimeError(
                f"Migration 004 constraint missing or incorrect: {name}"
            )

    indexes = await conn.fetch(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'generation_handoffs'
        """
    )

    actual_indexes = {row["indexname"] for row in indexes}

    required_indexes = {
        "idx_generation_handoffs_pending",
        "idx_generation_handoffs_expired",
    }

    if not required_indexes.issubset(actual_indexes):
        raise RuntimeError(
            "Migration 004 indexes are incomplete"
        )

    definition = await conn.fetchval(
        """
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'public.generation_handoffs'::regclass
          AND conname = 'generation_handoffs_status_check'
        """
    )

    required_statuses = [
        "'pending'", "'processing'", "'submitted'"
    ]

    if not all(value in definition for value in required_statuses):
        raise RuntimeError(
            "Migration 004 status constraint is incorrect"
        )

    print("VERIFIED: Generation handoff schema")
    return True


async def verify_failure_state(conn):
    failed_at = await column_exists(
        conn, "generation_handoffs", "failed_at"
    )

    definition = await conn.fetchval(
        """
        SELECT pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'public.generation_handoffs'::regclass
          AND conname = 'generation_handoffs_status_check'
        """
    )

    allows_failed = (
        definition is not None and "'failed'" in definition
    )

    if failed_at and allows_failed:
        print("VERIFIED: Generation handoff failure state")
        return True

    if failed_at or allows_failed:
        raise RuntimeError(
            "Migration 005 is partially applied. "
            "Manual inspection required."
        )

    return False


async def record_migration(conn, version):
    await conn.execute(
        """
        INSERT INTO schema_migrations (version)
        VALUES ($1)
        ON CONFLICT (version) DO NOTHING
        """,
        version,
    )


async def run_migrations():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required"
        )

    conn = await asyncpg.connect(database_url)

    try:
        await conn.execute(
            "SELECT pg_advisory_lock($1)",
            ADVISORY_LOCK_ID,
        )

        try:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

            recorded = await conn.fetch(
                "SELECT version FROM schema_migrations"
            )
            recorded_versions = {
                row["version"] for row in recorded
            }

            known_versions = [version for version, _ in MIGRATIONS]

            unknown = recorded_versions - set(known_versions)

            if unknown:
                raise RuntimeError(
                    f"Unknown migration versions: {sorted(unknown)}"
                )

            # Migration history must be a continuous prefix.
            for index, version in enumerate(known_versions):
                if version in recorded_versions:
                    if not all(
                        previous in recorded_versions
                        for previous in known_versions[:index]
                    ):
                        raise RuntimeError(
                            "Migration history contains a gap. "
                            "Manual inspection required."
                        )

            for version, path in MIGRATIONS:
                if not path.exists():
                    raise FileNotFoundError(path)

                if version in recorded_versions:
                    print(f"SKIPPED: Migration {version}")
                    continue

                # Existing database baseline checks.
                if version == "001":
                    if await verify_initial_schema(conn):
                        async with conn.transaction():
                            await record_migration(conn, version)
                        print("BASELINED: Migration 001")
                        continue

                if version == "002":
                    if await verify_account_seed(conn):
                        async with conn.transaction():
                            await record_migration(conn, version)
                        print("BASELINED: Migration 002")
                        continue

                if version == "003":
                    if await verify_template_seed(conn):
                        async with conn.transaction():
                            await record_migration(conn, version)
                        print("BASELINED: Migration 003")
                        continue

                if version == "004":
                    if await verify_handoff_schema(conn):
                        async with conn.transaction():
                            await record_migration(conn, version)
                        print("BASELINED: Migration 004")
                        continue

                if version == "005":
                    if await verify_failure_state(conn):
                        async with conn.transaction():
                            await record_migration(conn, version)
                        print("BASELINED: Migration 005")
                        continue

                sql = path.read_text(encoding="utf-8")

                async with conn.transaction():
                    await conn.execute(sql)
                    await record_migration(conn, version)

                print(f"APPLIED: Migration {version}")

        finally:
            await conn.execute(
                "SELECT pg_advisory_unlock($1)",
                ADVISORY_LOCK_ID,
            )

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_migrations())
