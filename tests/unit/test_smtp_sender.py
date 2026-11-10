import asyncio
import os
import sys
import unittest
from unittest.mock import patch
from pathlib import Path


os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@localhost:5432/test",
)
os.environ.setdefault(
    "RABBITMQ_URL",
    "amqp://test:test@localhost:5672/",
)
os.environ.setdefault("SMTP_HOST", "localhost")
os.environ.setdefault("SMTP_PORT", "1025")
os.environ.setdefault("SMTP_USE_TLS", "false")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SENDER_WORKER_PATH = PROJECT_ROOT / "services" / "sender-worker"

sys.path.insert(0, str(SENDER_WORKER_PATH))

from app.message import EmailMessage
from app.smtp_sender import SMTPSender


class FakeSMTP:
    """Mock SMTP connection for isolated pool testing."""

    instances = []
    active_sends = 0
    maximum_active_sends = 0
    active_connections = 0
    maximum_active_connections = 0
    fail_next_send = False

    def __init__(self, **kwargs):
        self.is_connected = False
        self.close_called = False
        self.quit_called = False
        self.sent_count = 0

        FakeSMTP.instances.append(self)

    async def connect(self):
        await asyncio.sleep(0.02)

        self.is_connected = True
        FakeSMTP.active_connections += 1

        FakeSMTP.maximum_active_connections = max(
            FakeSMTP.maximum_active_connections,
            FakeSMTP.active_connections,
        )

    async def send_message(self, message):
        if FakeSMTP.fail_next_send:
            FakeSMTP.fail_next_send = False
            raise RuntimeError("Simulated SMTP failure")

        FakeSMTP.active_sends += 1

        FakeSMTP.maximum_active_sends = max(
            FakeSMTP.maximum_active_sends,
            FakeSMTP.active_sends,
        )

        try:
            await asyncio.sleep(0.01)
            self.sent_count += 1
            return "250 OK"

        finally:
            FakeSMTP.active_sends -= 1

    async def quit(self):
        self.quit_called = True

        if self.is_connected:
            FakeSMTP.active_connections -= 1

        self.is_connected = False

    def close(self):
        self.close_called = True

        if self.is_connected:
            FakeSMTP.active_connections -= 1

        self.is_connected = False


def create_email(number=1):
    return EmailMessage(
        email_id=f"email-{number}",
        batch_id="test-batch",
        message_id=f"message-{number}@example.com",
        from_email="sender@example.com",
        from_name="Test Sender",
        to_email="recipient@example.com",
        to_name="Test Recipient",
        subject=f"Test Email {number}",
        html="<h1>Hello</h1>",
        text="Hello",
        tracking_token=f"tracking-{number}",
    )


class TestSMTPSender(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        FakeSMTP.instances = []
        FakeSMTP.active_sends = 0
        FakeSMTP.maximum_active_sends = 0
        FakeSMTP.active_connections = 0
        FakeSMTP.maximum_active_connections = 0
        FakeSMTP.fail_next_send = False

        self.smtp_patch = patch(
            "app.smtp_sender.SMTP",
            FakeSMTP,
        )
        self.smtp_patch.start()

    def tearDown(self):
        self.smtp_patch.stop()

    async def test_initialization_creates_one_connection(self):
        sender = SMTPSender(
            pool_size=100,
            max_active_connections=20,
            max_emails_per_connection=20,
        )

        await sender.connect()

        self.assertEqual(len(FakeSMTP.instances), 1)
        self.assertEqual(len(sender._all_connections), 1)

        await sender.close()

    async def test_connection_is_reused(self):
        sender = SMTPSender(
            pool_size=5,
            max_active_connections=1,
            max_emails_per_connection=20,
        )

        await sender.connect()

        await sender.send(create_email(1))
        await sender.send(create_email(2))
        await sender.send(create_email(3))

        self.assertEqual(len(FakeSMTP.instances), 1)
        self.assertEqual(FakeSMTP.instances[0].sent_count, 3)

        await sender.close()

    async def test_connection_recycles_after_email_limit(self):
        sender = SMTPSender(
            pool_size=5,
            max_active_connections=1,
            max_emails_per_connection=2,
        )

        await sender.connect()

        first_connection = FakeSMTP.instances[0]

        await sender.send(create_email(1))
        await sender.send(create_email(2))

        self.assertTrue(first_connection.close_called)
        self.assertEqual(len(sender._all_connections), 0)

        await sender.send(create_email(3))

        self.assertEqual(len(FakeSMTP.instances), 2)

        await sender.close()

    async def test_active_connection_limit(self):
        sender = SMTPSender(
            pool_size=100,
            max_active_connections=2,
            max_emails_per_connection=20,
        )

        await sender.connect()

        await asyncio.gather(
            *[
                sender.send(create_email(i))
                for i in range(1, 11)
            ]
        )

        self.assertGreaterEqual(
            FakeSMTP.maximum_active_sends,
            2,
        )

        self.assertLessEqual(
            FakeSMTP.maximum_active_sends,
            2,
        )

        self.assertEqual(
            sender._max_observed_active_connections,
            2,
        )

        self.assertGreaterEqual(
            FakeSMTP.maximum_active_connections,
            2,
        )

        await sender.close()

        self.assertEqual(FakeSMTP.active_connections, 0)

    async def test_failed_connection_is_discarded(self):
        sender = SMTPSender(
            pool_size=5,
            max_active_connections=1,
            max_emails_per_connection=20,
        )

        await sender.connect()

        first_connection = FakeSMTP.instances[0]
        FakeSMTP.fail_next_send = True

        with self.assertRaises(RuntimeError):
            await sender.send(create_email(1))

        self.assertTrue(first_connection.close_called)

        await sender.send(create_email(2))

        self.assertEqual(len(FakeSMTP.instances), 2)

        await sender.close()

    async def test_shutdown_closes_all_connections(self):
        sender = SMTPSender(
            pool_size=5,
            max_active_connections=2,
            max_emails_per_connection=20,
        )

        await sender.connect()
        await sender.send(create_email(1))

        connections = list(FakeSMTP.instances)

        await sender.close()

        self.assertEqual(len(sender._all_connections), 0)

        for connection in connections:
            self.assertTrue(connection.quit_called)

    async def test_invalid_configuration_is_rejected(self):
        with self.assertRaises(ValueError):
            SMTPSender(
                pool_size=5,
                max_active_connections=10,
                max_emails_per_connection=20,
            )


if __name__ == "__main__":
    unittest.main()