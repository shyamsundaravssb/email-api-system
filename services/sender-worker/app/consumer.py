import asyncio
import logging

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from aiosmtplib.errors import SMTPResponseException

from app.config import settings
from app.db import Database
from app.message import EmailMessage
from app.retry import RetryPublisher
from app.smtp_sender import SMTPSender


logger = logging.getLogger(__name__)


class SenderWorker:
    """Consumes email messages and coordinates SMTP and database operations."""

    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.exchange = None

        self.database = Database(settings.database_url)
        self.smtp_sender = SMTPSender(
            pool_size=settings.smtp_pool_size,
            max_active_connections=settings.smtp_max_active_connections,
            max_emails_per_connection=settings.smtp_max_emails_per_connection,
        )

        self.retry_publisher = None

        # Limits simultaneous message processing.
        self.semaphore = asyncio.Semaphore(settings.concurrency)

        self.consumer_tag = None
        self._started = False
        self.active_tasks = set()

    async def start(self):
        """Connect to dependencies and start consuming messages."""

        if self._started:
            return

        logger.info("Starting sender worker...")

        # Connect to PostgreSQL.
        await self.database.connect()
        logger.info("Connected to PostgreSQL.")

        # Connect to SMTP.
        await self.smtp_sender.connect()
        logger.info("Connected to SMTP server.")

        try:
            # Connect robustly to RabbitMQ.
            self.connection = await aio_pika.connect_robust(
                settings.rabbitmq_url
            )

            # Enable publisher confirms.
            self.channel = await self.connection.channel(
                publisher_confirms=True,
                on_return_raises=True,
            )

            # Limit unacknowledged messages.
            await self.channel.set_qos(
                prefetch_count=settings.prefetch_count
            )

            # Use the existing project exchange and queue.
            self.exchange = await self.channel.get_exchange(
                "email.exchange"
            )

            self.queue = await self.channel.get_queue(
                "email.send"
            )

            self.retry_publisher = RetryPublisher(self.exchange)

            # Start manual-acknowledgement consumer.
            self.consumer_tag = await self.queue.consume(
                self._on_message,
                no_ack=False,
            )

            self._started = True

            logger.info(
                "Sender worker is consuming email.send "
                "(prefetch=%s, concurrency=%s).",
                settings.prefetch_count,
                settings.concurrency,
            )

        except Exception:
            await self.close()
            raise

    async def _on_message(
    self,
    message: AbstractIncomingMessage,
):
        """Process one incoming RabbitMQ message."""

        task = asyncio.current_task()

        if task is not None:
            self.active_tasks.add(task)

        try:
            async with self.semaphore:
                await self._process_message(message)

        except Exception:
            logger.exception(
                "Message processing failed. "
                "Leaving message unacknowledged for recovery."
            )

        finally:
            if task is not None:
                self.active_tasks.discard(task)

    async def _process_message(
        self,
        message: AbstractIncomingMessage,
    ):
        """Parse, send, record, retry, or dead-letter one email."""

        # Parse and validate the message.
        try:
            email = EmailMessage.from_payload(
                message.body,
                headers=message.headers,
            )

        except ValueError:
            logger.exception("Invalid email queue message.")

            # A malformed message must not loop forever.
            await self.retry_publisher.publish_dead(message)
            await message.ack()

            return

        logger.info(
            "Processing email_id=%s attempt=%s recipient=%s",
            email.email_id,
            email.attempt,
            email.to_email,
        )

        # Avoid sending an email that has already been sent.
        current_status = await self.database.get_email_status(
            email.email_id
        )

        if current_status == "sent":
            logger.info(
                "Email %s is already sent. Acknowledging duplicate.",
                email.email_id,
            )

            await message.ack()
            return

        if current_status is None:
            logger.error(
                "Email %s does not exist in PostgreSQL. "
                "Sending message to DLQ.",
                email.email_id,
            )

            await self.retry_publisher.publish_dead(message)
            await message.ack()
            return

                # Attempt SMTP delivery.
        try:
            response = await self.smtp_sender.send(email)

        except Exception as exc:
            # Only SMTP sending errors should enter the retry/DLQ flow.
            await self._handle_smtp_failure(
                message=message,
                email=email,
                error=exc,
            )
            return

        # SMTP accepted the email. Record the successful handoff separately.
        smtp_response = str(response[1]) if response else None

        try:
            recorded = await self.database.record_attempt(
                email_id=email.email_id,
                attempt_no=email.attempt,
                outcome="success",
                status="sent",
                smtp_code=250,
                smtp_response=smtp_response,
            )

        except Exception:
            # Do not classify a database failure as an SMTP failure.
            # Leave the RabbitMQ message unacknowledged for recovery.
            logger.exception(
                "SMTP accepted email %s, but recording success in "
                "PostgreSQL failed. Message remains unacknowledged.",
                email.email_id,
            )
            raise

        if recorded:
            logger.info(
                "Email %s successfully accepted by SMTP.",
                email.email_id,
            )

        await message.ack()

    async def _handle_smtp_failure(
        self,
        message: AbstractIncomingMessage,
        email: EmailMessage,
        error: Exception,
    ):
        """Classify an SMTP failure and apply retry/DLQ rules."""

        smtp_code = None

        if isinstance(error, SMTPResponseException):
            smtp_code = error.code

        # SMTP 5xx responses are permanent failures.
        permanent_failure = (
            smtp_code is not None
            and 500 <= smtp_code <= 599
        )

        error_message = str(error) or type(error).__name__

        smtp_response = error_message[:1000]

        if permanent_failure:
            logger.error(
                "Permanent SMTP failure for email %s: %s",
                email.email_id,
                error_message,
            )

            # Publish to DLQ before ACK.
            await self.retry_publisher.publish_dead(message)

            await self.database.record_attempt(
                email_id=email.email_id,
                attempt_no=email.attempt,
                outcome="permanent_error",
                status="failed",
                smtp_code=smtp_code,
                smtp_response=smtp_response,
                error_message=error_message,
            )

            await message.ack()
            return

        # Temporary error on the final attempt.
        if email.attempt >= 3:
            logger.error(
                "Email %s exhausted all retry attempts.",
                email.email_id,
            )

            await self.retry_publisher.publish_dead(message)

            await self.database.record_attempt(
                email_id=email.email_id,
                attempt_no=email.attempt,
                outcome="transient_error",
                status="failed",
                smtp_code=smtp_code,
                smtp_response=smtp_response,
                error_message=error_message,
            )

            await message.ack()
            return

        # Temporary failure: schedule the next attempt.
        next_attempt = email.attempt + 1

        logger.warning(
            "Temporary SMTP failure for email %s. "
            "Scheduling attempt %s.",
            email.email_id,
            next_attempt,
        )

        # Publish to retry queue and wait for confirmation.
        await self.retry_publisher.publish_retry(
            message,
            next_attempt=next_attempt,
        )

        # Record the failure in PostgreSQL.
        await self.database.record_attempt(
            email_id=email.email_id,
            attempt_no=email.attempt,
            outcome="transient_error",
            status="retrying",
            smtp_code=smtp_code,
            smtp_response=smtp_response,
            error_message=error_message,
        )

        # Acknowledge only after publish and DB update.
        await message.ack()

    async def close(self):
        """Stop consumption and close dependencies gracefully."""

        logger.info("Stopping sender worker...")

        # Step 1: Stop receiving new messages.
        if self.queue is not None and self.consumer_tag is not None:
            try:
                await self.queue.cancel(self.consumer_tag)
                logger.info("RabbitMQ consumer cancelled.")
            except Exception:
                logger.exception("Failed to cancel RabbitMQ consumer.")

            self.consumer_tag = None

        # Step 2: Wait for active messages to finish.
        # Keep RabbitMQ, SMTP, and PostgreSQL connections open
        # while these tasks are still processing.
        while self.active_tasks:
            tasks = list(self.active_tasks)

            logger.info(
                "Waiting for %s active message tasks...",
                len(tasks),
            )

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        logger.info("All active message tasks finished.")

        # Step 3: Close RabbitMQ only after processing finishes.
        if self.connection is not None:
            try:
                await self.connection.close()
                logger.info("RabbitMQ connection closed.")
            except Exception:
                logger.exception("Failed to close RabbitMQ connection.")

            self.connection = None
            self.channel = None
            self.queue = None
            self.exchange = None

        # Step 4: Close SMTP connections.
        try:
            await self.smtp_sender.close()
        except Exception:
            logger.exception("Failed to close SMTP connections.")

        # Step 5: Close PostgreSQL pool.
        try:
            await self.database.close()
        except Exception:
            logger.exception("Failed to close PostgreSQL pool.")

        self._started = False

        logger.info("Sender worker stopped.")