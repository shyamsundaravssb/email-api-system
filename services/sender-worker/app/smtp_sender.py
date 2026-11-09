import asyncio
from datetime import datetime, timezone
from email.message import EmailMessage as MIMEEmail
from email.utils import formataddr, formatdate

from aiosmtplib import SMTP

from app.config import settings
from app.message import EmailMessage


class SMTPSender:
    """
    Asynchronous SMTP sender with reusable SMTP connections.

    A small connection pool is used so that the worker does not
    establish a new SMTP connection for every email.
    """

    def __init__(self, pool_size: int = 5):
        self.pool_size = max(1, min(pool_size, settings.concurrency))

        self._connections: asyncio.Queue[SMTP] = asyncio.Queue()
        self._all_connections: list[SMTP] = []
        self._started = False

    async def connect(self) -> None:
        """Create and initialize the SMTP connection pool."""

        if self._started:
            return

        try:
            for _ in range(self.pool_size):
                smtp = SMTP(
                    hostname=settings.smtp_host,
                    port=settings.smtp_port,
                    username=settings.smtp_user or None,
                    password=settings.smtp_password or None,
                    use_tls=settings.smtp_use_tls,
                    start_tls=False,
                    timeout=15,
                )

                await smtp.connect()

                self._all_connections.append(smtp)
                await self._connections.put(smtp)

            self._started = True

        except Exception:
            await self.close()
            raise

    def build_mime_message(
        self,
        email: EmailMessage,
    ) -> MIMEEmail:
        """
        Construct an email with plain-text and HTML alternatives.
        """

        message = MIMEEmail()

        # Standard email headers.
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

        # Plain-text content.
        message.set_content(email.text)

        # HTML alternative.
        message.add_alternative(
            email.html,
            subtype="html",
        )

        return message

    @staticmethod
    def _format_message_id(message_id: str) -> str:
        """Ensure the Message-ID uses angle brackets."""

        message_id = message_id.strip()

        if not message_id.startswith("<"):
            message_id = f"<{message_id}>"

        if not message_id.endswith(">"):
            message_id = f"{message_id}>"

        return message_id

    async def send(self, email: EmailMessage):
        """
        Send an email using an available pooled SMTP connection.

        SMTP errors are allowed to propagate so that the retry
        component can classify and handle them.
        """

        if not self._started:
            raise RuntimeError(
                "SMTP sender is not connected. Call connect() first."
            )

        smtp = await self._connections.get()

        try:
            if not smtp.is_connected:
                await smtp.connect()

            mime_message = self.build_mime_message(email)

            response = await smtp.send_message(mime_message)

            return response

        finally:
            await self._connections.put(smtp)

    async def close(self) -> None:
        """Close all SMTP connections gracefully."""

        connections = list(self._all_connections)

        self._all_connections.clear()
        self._started = False

        for smtp in connections:
            if smtp.is_connected:
                try:
                    await smtp.quit()
                except Exception:
                    smtp.close()

        # Remove references to any queued connections.
        self._connections = asyncio.Queue()