import asyncio
import json
import logging
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message


logger = logging.getLogger(__name__)

EXCHANGE_NAME = "email.exchange"
ROUTING_KEY = "send"


class PublishError(Exception):
    """Raised when a message cannot be confirmed by RabbitMQ."""


class RabbitMQPublisher:
    """
    Publishes persistent email messages to RabbitMQ.

    Messages are sent to:
        Exchange: email.exchange
        Routing key: send
        Queue: email.send

    Publisher confirms are enabled so the caller can update the
    database only after RabbitMQ acknowledges the publication.
    """

    def __init__(self, rabbitmq_url: str):
        self.rabbitmq_url = rabbitmq_url

        self.connection = None
        self.channel = None
        self.exchange = None

        self._connect_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Create a robust RabbitMQ connection and publisher channel."""

        async with self._connect_lock:
            if (
                self.connection is not None
                and not self.connection.is_closed
                and self.channel is not None
                and not self.channel.is_closed
            ):
                return

            try:
                self.connection = await aio_pika.connect_robust(
                    self.rabbitmq_url,
                    client_properties={
                        "connection_name": "generation-api-publisher"
                    },
                )

                self.channel = await self.connection.channel(
                    publisher_confirms=True,
                    on_return_raises=True,
                )

                # Declare the exchange using the same durable settings
                # already configured in infra/rabbitmq/definitions.json.
                self.exchange = await self.channel.declare_exchange(
                    EXCHANGE_NAME,
                    ExchangeType.DIRECT,
                    durable=True,
                    auto_delete=False,
                )

                logger.info(
                    "Connected to RabbitMQ; publisher exchange is ready"
                )

            except Exception as exc:
                await self.close()

                raise PublishError(
                    "Could not connect to RabbitMQ publisher"
                ) from exc

    async def publish(
        self,
        payload: dict[str, Any],
        message_id: str,
    ) -> None:
        """
        Publish one persistent JSON message.

        message_id must be stable across retries. The processor will
        derive it from the database email UUID.
        """

        if not isinstance(payload, dict):
            raise PublishError("Message payload must be a dictionary")

        if not isinstance(message_id, str) or not message_id.strip():
            raise PublishError("A non-empty message_id is required")

        try:
            body = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")

        except (TypeError, ValueError) as exc:
            raise PublishError(
                "Message payload is not valid JSON"
            ) from exc

        await self.connect()

        message = Message(
            body=body,
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message_id,
            headers={
                "schema_version": 1,
                "producer": "generation-api",
                "x-attempt": 1,
            },
        )

        try:
            confirmation = await self.exchange.publish(
                message,
                routing_key=ROUTING_KEY,
                mandatory=True,
            )

            if confirmation is None:
                raise PublishError(
                    "RabbitMQ did not return a publisher confirmation"
                )

            if confirmation.__class__.__name__ in {"Nack", "Reject"}:
                raise PublishError(
                    "RabbitMQ negatively acknowledged the message"
                )

            logger.debug(
                "RabbitMQ confirmed message publication: %s",
                message_id,
            )

        except PublishError:
            raise

        except Exception as exc:
            logger.warning(
                "RabbitMQ publication failed for message %s",
                message_id,
            )

            raise PublishError(
                "RabbitMQ could not confirm message publication"
            ) from exc

    async def check(self) -> bool:
        """Return whether the RabbitMQ connection and channel are usable."""

        return (
            self.connection is not None
            and not self.connection.is_closed
            and self.channel is not None
            and not self.channel.is_closed
            and self.exchange is not None
        )

    async def close(self) -> None:
        """Close the RabbitMQ connection cleanly."""

        connection = self.connection

        self.exchange = None
        self.channel = None
        self.connection = None

        if connection is not None and not connection.is_closed:
            try:
                await connection.close()
            except Exception:
                logger.exception("Error while closing RabbitMQ connection")
