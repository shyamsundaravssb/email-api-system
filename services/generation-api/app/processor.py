import asyncio
import logging
import time
from typing import Any
from uuid import UUID, uuid4

from app.config import Settings
from app.db import Database
from app.publisher import RabbitMQPublisher
from app.rendering import (
    TemplateRenderError,
    append_open_tracking_pixel,
    render_email,
    rewrite_tracking_link,
)
from app.schemas import GenerateItem, GenerateRequest


logger = logging.getLogger(__name__)


class PublishRateLimiter:
    """
    Shared process-wide rate limiter.

    It spaces out RabbitMQ publications so concurrent workers
    cannot exceed the configured publication rate.
    """

    def __init__(self, rate_per_second: int):
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be greater than zero")

        self.interval = 1.0 / rate_per_second
        self.next_allowed = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()

        async with self.lock:
            now = loop.time()
            wait_time = max(0.0, self.next_allowed - now)

            if wait_time:
                await asyncio.sleep(wait_time)

            self.next_allowed = max(
                self.next_allowed,
                loop.time(),
            ) + self.interval


class GenerationProcessor:
    """
    Coordinates database retrieval, template rendering,
    and confirmed RabbitMQ publication.
    """

    def __init__(
        self,
        database: Database,
        publisher: RabbitMQPublisher,
        settings: Settings,
    ):
        self.database = database
        self.publisher = publisher
        self.settings = settings

        self.semaphore = asyncio.Semaphore(
            settings.generation_concurrency
        )

        self.rate_limiter = PublishRateLimiter(
            settings.publish_rate_per_second
        )

    async def process_batch(self, request: GenerateRequest) -> None:
        """
        Process a batch with a bounded number of workers.

        The queue is bounded as well, so a large batch does not
        create one asyncio task per recipient.
        """

        start_time = time.perf_counter()
        published_count = 0

        if len(request.items) > self.settings.generation_max_items:
            logger.error(
                "Batch %s contains %s items; configured maximum is %s",
                request.batch_id,
                len(request.items),
                self.settings.generation_max_items,
            )
            return

        worker_count = min(
            self.settings.generation_concurrency,
            len(request.items),
        )

        queue: asyncio.Queue[GenerateItem | None] = asyncio.Queue(
            maxsize=max(1, worker_count * 2)
        )

        async def worker(worker_number: int) -> None:
            nonlocal published_count

            while True:
                item = await queue.get()

                try:
                    if item is None:
                        return

                    async with self.semaphore:
                        result = await self.process_item(
                            request.batch_id,
                            item,
                        )

                        if result:
                            published_count += 1

                except Exception:
                    # An unexpected or infrastructure error must not
                    # mark the email failed. Its transaction rolls back,
                    # leaving the row eligible for a later retry.
                    logger.exception(
                        "Worker %s encountered an error for email %s",
                        worker_number,
                        getattr(item, "email_id", "unknown"),
                    )

                finally:
                    queue.task_done()

        workers = [
            asyncio.create_task(worker(number))
            for number in range(worker_count)
        ]

        try:
            for item in request.items:
                await queue.put(item)

            await queue.join()

        finally:
            for _ in workers:
                await queue.put(None)

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

        elapsed = time.perf_counter() - start_time
        throughput = (
            published_count / elapsed
            if elapsed > 0
            else 0
        )

        logger.info(
            "GENERATION PERFORMANCE | batch_id=%s | "
            "published=%s | elapsed=%.3f seconds | "
            "throughput=%.2f emails/second",
            request.batch_id,
            published_count,
            elapsed,
            throughput,
        )

        logger.info(
            "Finished processing batch %s containing %s requested emails",
            request.batch_id,
            len(request.items),
        )

    async def process_item(
        self,
        batch_id: UUID,
        item: GenerateItem,
    ) -> bool:
        """
        Process one email inside a database transaction.

        The row lock prevents two concurrent generation attempts
        from publishing the same accepted email simultaneously.
        """

        async with self.database.acquire() as connection:
            async with connection.transaction():

                email = await self.database.get_email_for_update(
                    connection,
                    item.email_id,
                )

                if email is None:
                    logger.warning(
                        "Email %s does not exist; skipping",
                        item.email_id,
                    )
                    return False

                if email["batch_id"] != batch_id:
                    logger.warning(
                        "Email %s does not belong to batch %s; skipping",
                        item.email_id,
                        batch_id,
                    )
                    return False

                # Only accepted rows are eligible for generation.
                # queued, sent, failed, and rejected rows are skipped.
                if email["status"] != "accepted":
                    logger.info(
                        "Skipping email %s because its status is %s",
                        item.email_id,
                        email["status"],
                    )
                    return False

                template = await self.database.get_template(
                    connection,
                    email["template_id"],
                )

                if template is None:
                    await self.database.mark_failed(
                        connection,
                        email["email_id"],
                        f"Unknown template: {email['template_id']}",
                    )

                    logger.warning(
                        "Email %s failed because template %s does not exist",
                        item.email_id,
                        email["template_id"],
                    )
                    return False

                variables = email["variables"]

                try:
                    rendered = render_email(
                        subject_template=template["subject_template"],
                        html_template=template["html_template"],
                        text_template=template["text_template"],
                        variables=variables,
                        required_variables=template["required_variables"],
                    )

                except TemplateRenderError as exc:
                    await self.database.mark_failed(
                        connection,
                        email["email_id"],
                        str(exc),
                    )

                    logger.warning(
                        "Email %s permanently failed during rendering: %s",
                        item.email_id,
                        exc,
                    )
                    return False

                # Prepare click and open tracking URLs.
                tracking_base_url = self.settings.track_base_url
                html_content = rendered["html"]

                # One click-tracked link per email, supplied as variables["link"].
                original_link = variables.get("link")

                if isinstance(original_link, str) and original_link.strip():
                    link_token = str(uuid4())
                    click_url = f"{tracking_base_url}/c/{link_token}"

                    html_content, link_rewritten = rewrite_tracking_link(
                        html_content,
                        original_link,
                        click_url,
                    )

                    if link_rewritten:
                        await self.database.create_tracking_link(
                            connection,
                            email["email_id"],
                            link_token,
                            original_link,
                        )
                    else:
                        raise TemplateRenderError(
                            "The link variable was not found in the rendered HTML href"
                        )

                # Add an open-tracking pixel to every email.
                open_url = (
                    f"{tracking_base_url}/o/{email['tracking_token']}"
                )
                html_content = append_open_tracking_pixel(
                    html_content,
                    open_url,
                )

                sender_email = email["from_email"]
                sender_name = email["from_name"]

                if (
                    not isinstance(sender_email, str)
                    or "@" not in sender_email
                    or not sender_email.rsplit("@", 1)[1]
                ):
                    await self.database.mark_failed(
                        connection,
                        email["email_id"],
                        "Account sender email address is invalid",
                    )

                    logger.error(
                        "Email %s has an invalid account sender address",
                        item.email_id,
                    )
                    return False

                sender_domain = sender_email.rsplit("@", 1)[1].lower()

                # Stable across retries. The Sender Worker can use this
                # identifier to detect duplicate publications.
                message_id = (
                    f"<{email['email_id']}@{sender_domain}>"
                )

                payload: dict[str, Any] = {
                    "email_id": str(email["email_id"]),
                    "batch_id": str(email["batch_id"]),
                    "to_email": email["to_email"],
                    "to_name": email["to_name"],
                    "from_email": sender_email,
                    "from_name": sender_name,
                    "subject": rendered["subject"],
                    "html": html_content,
                    "text": rendered["text"],
                    "message_id": message_id,
                    "tracking_token": str(email["tracking_token"]),
                }

                # Apply the shared publication rate limit immediately
                # before publishing to RabbitMQ.
                await self.rate_limiter.wait()

                # This call returns only after publisher confirmation.
                # If it raises, the transaction rolls back and the email
                # remains accepted for a later retry.
                await self.publisher.publish(
                    payload=payload,
                    message_id=message_id,
                )

                updated = await self.database.mark_queued(
                    connection,
                    email["email_id"],
                    message_id,
                )

                if not updated:
                    raise RuntimeError(
                        f"Could not mark email {item.email_id} as queued "
                        "after RabbitMQ confirmed publication"
                    )

                logger.info(
                    "Email %s successfully published and marked queued",
                    item.email_id,
                )
                return True