import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from email.utils import parseaddr
from pathlib import Path

import aio_pika
import asyncpg
from aio_pika import DeliveryMode, Message


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "queue_message.json"

DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
EXCHANGE_NAME = "email.exchange"
ROUTING_KEY = "send"


def load_fixture():
    """Load and validate the sample queue message."""
    if not FIXTURE_PATH.exists():
        raise FileNotFoundError(f"Fixture not found: {FIXTURE_PATH}")

    with FIXTURE_PATH.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    required_fields = (
        "email_id",
        "batch_id",
        "message_id",
        "from_email",
        "from_name",
        "to_email",
        "to_name",
        "subject",
        "html",
        "text",
        "tracking_token",
    )

    missing = [field for field in required_fields if field not in payload]
    if missing:
        raise ValueError(f"Fixture is missing fields: {', '.join(missing)}")

    for field in ("from_email", "to_email"):
        address = payload[field]
        if not isinstance(address, str) or not parseaddr(address)[1] or "@" not in address:
            raise ValueError(f"Invalid email address in fixture: {field}")

    return payload


async def create_batch_and_emails(pool, fixture, count):
    """
    Create one batch and its email records in PostgreSQL.

    The database rows are committed before messages are published.
    """
    batch_id = uuid.uuid4()
    messages = []

    async with pool.acquire() as connection:
        async with connection.transaction():
            account = await connection.fetchrow(
                """
                SELECT id, from_email, from_name
                FROM accounts
                WHERE from_email = $1
                ORDER BY created_at
                LIMIT 1
                """,
                fixture["from_email"],
            )

            if account is None:
                raise RuntimeError(
                    "No development account found for sender address "
                    f"{fixture['from_email']!r}. Check db/init/002_seed_accounts.sql."
                )

            await connection.execute(
                """
                INSERT INTO batches (
                    id,
                    account_id,
                    total_count,
                    accepted_count,
                    rejected_count
                )
                VALUES ($1, $2, $3, $3, 0)
                """,
                batch_id,
                account["id"],
                count,
            )

            for position in range(count):
                email_id = uuid.uuid4()
                tracking_token = uuid.uuid4()

                # Generate a unique Message-ID for each email.
                message_id = (
                    f"<{email_id}@{fixture['from_email'].split('@', 1)[1]}>"
                )

                payload = {
                    "email_id": str(email_id),
                    "batch_id": str(batch_id),
                    "message_id": message_id,
                    "from_email": account["from_email"],
                    "from_name": account["from_name"] or fixture["from_name"],
                    "to_email": fixture["to_email"],
                    "to_name": fixture["to_name"],
                    "subject": fixture["subject"],
                    "html": fixture["html"],
                    "text": fixture["text"],
                    "tracking_token": str(tracking_token),
                }

                await connection.execute(
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
                        message_id,
                        tracking_token,
                        queued_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6,
                        $7::jsonb, 'queued', $8, $9, now()
                    )
                    """,
                    email_id,
                    batch_id,
                    position,
                    "sample_email",
                    payload["to_email"],
                    payload["to_name"],
                    json.dumps({}),
                    message_id,
                    tracking_token,
                )

                messages.append(payload)

    logger.info(
        "Created batch %s with %d queued email records.",
        batch_id,
        count,
    )

    return batch_id, messages


async def publish_messages(messages):
    """Publish persistent messages to RabbitMQ with publisher confirms."""
    if not RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL environment variable is required.")

    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    try:
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        exchange = await channel.get_exchange(EXCHANGE_NAME)

        published = 0

        for payload in messages:
            body = json.dumps(payload).encode("utf-8")

            message = Message(
                body=body,
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=payload["message_id"],
            )

            confirmation = await exchange.publish(
                message,
                routing_key=ROUTING_KEY,
                mandatory=True,
            )

            if confirmation is not None:
                if confirmation.__class__.__name__ in ("Nack", "Reject"):
                    raise RuntimeError(
                        f"RabbitMQ did not confirm message "
                        f"{payload['email_id']}."
                    )

            published += 1
            logger.info(
                "Published %d/%d: email_id=%s",
                published,
                len(messages),
                payload["email_id"],
            )

        return published

    finally:
        await connection.close()


async def run(count):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is required.")

    if not RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL environment variable is required.")

    fixture = load_fixture()
    logger.info("Loaded sample message fixture.")

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=15,
    )

    try:
        batch_id, messages = await create_batch_and_emails(
            pool,
            fixture,
            count,
        )

        published = await publish_messages(messages)

        logger.info("Sample publishing completed.")
        logger.info("Batch ID: %s", batch_id)
        logger.info("Database records created: %d", len(messages))
        logger.info("Messages published: %d", published)

    finally:
        await pool.close()


def main():
    parser = argparse.ArgumentParser(
        description="Create sample email records and publish them to RabbitMQ."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="Number of sample emails to publish (default: 5).",
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("--count must be greater than zero.")

    try:
        asyncio.run(run(args.count))
    except KeyboardInterrupt:
        logger.warning("Publishing interrupted.")
        sys.exit(130)
    except Exception:
        logger.exception("Sample message publishing failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()