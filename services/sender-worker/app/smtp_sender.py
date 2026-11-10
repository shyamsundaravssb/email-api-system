import asyncio
import logging
from collections import deque
from email.message import EmailMessage as MIMEEmail
from email.utils import formataddr, formatdate

from aiosmtplib import SMTP

from app.config import settings
from app.message import EmailMessage


logger = logging.getLogger(__name__)


class SMTPSender:
    """Asynchronous SMTP connection pool with concurrent connection creation."""

    def __init__(
        self,
        pool_size: int | None = None,
        max_active_connections: int | None = None,
        max_emails_per_connection: int | None = None,
    ):
        self.pool_size = (
            pool_size if pool_size is not None
            else settings.smtp_pool_size
        )

        self.max_active_connections = (
            max_active_connections
            if max_active_connections is not None
            else settings.smtp_max_active_connections
        )

        self.max_emails_per_connection = (
            max_emails_per_connection
            if max_emails_per_connection is not None
            else settings.smtp_max_emails_per_connection
        )

        if self.pool_size < 1:
            raise ValueError("SMTP pool size must be at least 1.")

        if not 1 <= self.max_active_connections <= self.pool_size:
            raise ValueError(
                "Active SMTP connections must be between 1 and pool size."
            )

        if self.max_emails_per_connection < 1:
            raise ValueError(
                "Maximum emails per connection must be at least 1."
            )

        self._idle_connections: deque[SMTP] = deque()
        self._all_connections: set[SMTP] = set()
        self._connection_counts: dict[SMTP, int] = {}
        self._in_use_connections: set[SMTP] = set()

        # Reservations count connections currently being established.
        self._creating_connections = 0

        # Coordinates pool changes without holding a lock during network I/O.
        self._condition = asyncio.Condition()

        self._active_semaphore = asyncio.Semaphore(
            self.max_active_connections
        )

        self._started = False

        # Performance counters.
        self._max_observed_active_connections = 0
        self._total_connections_created = 0
        self._total_connections_recycled = 0

    async def _open_connection(self) -> SMTP:
        """Establish an SMTP connection without holding the pool lock."""

        smtp = SMTP(
            hostname=settings.smtp_host,
            port=settings.smtp_port,
            username=settings.smtp_user or None,
            password=settings.smtp_password or None,
            use_tls=settings.smtp_use_tls,
            start_tls=False,
            timeout=15,
        )

        try:
            await smtp.connect()
            return smtp

        except Exception:
            smtp.close()
            raise

    async def connect(self) -> None:
        """Initialize the pool with one SMTP connection."""

        async with self._condition:
            if self._started:
                return

            self._creating_connections += 1

        try:
            smtp = await self._open_connection()

        except Exception:
            async with self._condition:
                self._creating_connections -= 1
                self._condition.notify_all()
            raise

        async with self._condition:
            self._creating_connections -= 1

            if not self._started:
                self._started = True
                self._all_connections.add(smtp)
                self._connection_counts[smtp] = 0
                self._idle_connections.append(smtp)
                self._total_connections_created += 1

            else:
                smtp.close()

            self._condition.notify_all()

        logger.info(
            "SMTP pool initialized. Pool size: %s, active limit: %s, "
            "emails per connection: %s",
            self.pool_size,
            self.max_active_connections,
            self.max_emails_per_connection,
        )

    def build_mime_message(self, email: EmailMessage) -> MIMEEmail:
        """Build an email with plain-text and HTML alternatives."""

        message = MIMEEmail()

        message["From"] = formataddr(
            (email.from_name, email.from_email)
        )

        message["To"] = formataddr(
            (email.to_name, email.to_email)
        )

        message["Subject"] = email.subject

        message["Date"] = formatdate(
            localtime=False,
            usegmt=True,
        )

        message["Message-ID"] = self._format_message_id(
            email.message_id
        )

        message.set_content(email.text)

        message.add_alternative(
            email.html,
            subtype="html",
        )

        return message

    @staticmethod
    def _format_message_id(message_id: str) -> str:
        """Ensure Message-ID is enclosed in angle brackets."""

        message_id = message_id.strip()

        if not message_id.startswith("<"):
            message_id = f"<{message_id}>"

        if not message_id.endswith(">"):
            message_id = f"{message_id}>"

        return message_id

    async def _acquire_connection(self) -> SMTP:
        """Acquire an idle connection or create one concurrently."""

        while True:
            create_connection = False

            async with self._condition:
                if not self._started:
                    raise RuntimeError("SMTP sender is shutting down.")

                # Prefer reusing an existing idle connection.
                while self._idle_connections:
                    smtp = self._idle_connections.popleft()

                    if smtp.is_connected:
                        self._in_use_connections.add(smtp)

                        active = len(self._in_use_connections)

                        if active > self._max_observed_active_connections:
                            self._max_observed_active_connections = active

                            logger.info(
                                "SMTP active connections increased: %s/%s",
                                active,
                                self.max_active_connections,
                            )

                        return smtp

                    self._all_connections.discard(smtp)
                    self._connection_counts.pop(smtp, None)

                # Reserve a slot before releasing the condition.
                if (
                    len(self._all_connections)
                    + self._creating_connections
                    < self.pool_size
                ):
                    self._creating_connections += 1
                    create_connection = True

                else:
                    # All connections are busy. Wait for one to return.
                    await self._condition.wait()
                    continue

            if create_connection:
                try:
                    # Important: network connection creation happens
                    # OUTSIDE the condition lock.
                    smtp = await self._open_connection()

                except Exception:
                    async with self._condition:
                        self._creating_connections -= 1
                        self._condition.notify_all()
                    raise

                async with self._condition:
                    self._creating_connections -= 1

                    if not self._started:
                        smtp.close()
                        self._condition.notify_all()
                        raise RuntimeError(
                            "SMTP sender stopped during connection creation."
                        )

                    self._all_connections.add(smtp)
                    self._connection_counts[smtp] = 0
                    self._in_use_connections.add(smtp)

                    self._total_connections_created += 1

                    active = len(self._in_use_connections)

                    if active > self._max_observed_active_connections:
                        self._max_observed_active_connections = active

                        logger.info(
                            "SMTP active connections increased: %s/%s",
                            active,
                            self.max_active_connections,
                        )

                    current_pool = len(self._all_connections)

                    self._condition.notify_all()

                logger.info(
                    "SMTP connection opened. Current pool: %s/%s",
                    current_pool,
                    self.pool_size,
                )

                return smtp

    async def _release_connection(
        self,
        smtp: SMTP,
        recycle: bool,
    ) -> None:
        """Return a connection to the pool or discard it."""

        discard = False

        async with self._condition:
            self._in_use_connections.discard(smtp)

            if (
                recycle
                or not self._started
                or not smtp.is_connected
            ):
                self._all_connections.discard(smtp)
                self._connection_counts.pop(smtp, None)

                if recycle:
                    self._total_connections_recycled += 1

                discard = True

            else:
                self._idle_connections.append(smtp)

            self._condition.notify_all()

        if discard:
            smtp.close()

            logger.info(
                "SMTP connection discarded. Remaining connections: %s",
                len(self._all_connections),
            )

    async def send(self, email: EmailMessage):
        """Send an email using an available SMTP connection."""

        if not self._started:
            raise RuntimeError(
                "SMTP sender is not connected. Call connect() first."
            )

        async with self._active_semaphore:
            if not self._started:
                raise RuntimeError("SMTP sender is shutting down.")

            smtp = await self._acquire_connection()

            recycle = False

            try:
                if not smtp.is_connected:
                    await smtp.connect()

                mime_message = self.build_mime_message(email)

                response = await smtp.send_message(mime_message)

                self._connection_counts[smtp] += 1

                emails_sent = self._connection_counts[smtp]

                if emails_sent >= self.max_emails_per_connection:
                    recycle = True

                    logger.info(
                        "SMTP connection reached its email limit. "
                        "Recycling connection."
                    )

                return response

            except Exception:
                # Preserve the existing retry/DLQ handling.
                recycle = True
                raise

            finally:
                await self._release_connection(
                    smtp,
                    recycle=recycle,
                )

    async def close(self) -> None:
        """Close all SMTP connections gracefully."""

        async with self._condition:
            self._started = False

            connections = list(self._all_connections)

            self._all_connections.clear()
            self._connection_counts.clear()
            self._in_use_connections.clear()
            self._idle_connections.clear()

            self._condition.notify_all()

        for smtp in connections:
            if smtp.is_connected:
                try:
                    await smtp.quit()

                except Exception:
                    smtp.close()

        logger.info(
            "SMTP pool statistics: created=%s, recycled=%s, "
            "maximum simultaneous connections=%s",
            self._total_connections_created,
            self._total_connections_recycled,
            self._max_observed_active_connections,
        )

        logger.info("SMTP connection pool closed.")