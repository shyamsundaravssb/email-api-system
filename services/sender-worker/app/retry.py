from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractExchange, AbstractIncomingMessage


class RetryPublisher:
    """
    Publishes failed email messages to retry queues or the DLQ.

    RabbitMQ handles retry delays through the configured queue TTLs.
    """

    def __init__(self, exchange: AbstractExchange):
        self.exchange = exchange

    @staticmethod
    def _build_message(
        original: AbstractIncomingMessage,
        attempt: int,
    ) -> Message:
        """
        Create a persistent outgoing message while preserving
        the original email payload and relevant message properties.
        """

        headers = dict(original.headers or {})
        headers["x-attempt"] = attempt

        return Message(
            body=original.body,
            headers=headers,
            content_type=original.content_type or "application/json",
            content_encoding=original.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=original.priority,
            correlation_id=original.correlation_id,
            message_id=original.message_id,
            timestamp=original.timestamp,
            type=original.type,
            app_id=original.app_id,
        )

    async def publish_retry(
        self,
        original: AbstractIncomingMessage,
        next_attempt: int,
    ) -> None:
        """
        Publish a failed message to the appropriate retry queue.

        Attempt 1 failure -> retry.1
        Attempt 2 failure -> retry.2
        """

        if next_attempt == 2:
            routing_key = "retry.1"

        elif next_attempt == 3:
            routing_key = "retry.2"

        else:
            raise ValueError(
                f"Unsupported retry attempt: {next_attempt}"
            )

        outgoing = self._build_message(
            original,
            attempt=next_attempt,
        )

        result = await self.exchange.publish(
            outgoing,
            routing_key=routing_key,
            mandatory=True,
        )

        if result is False:
            raise RuntimeError(
                f"RabbitMQ did not confirm publishing to {routing_key}."
            )

    async def publish_dead(
        self,
        original: AbstractIncomingMessage,
    ) -> None:
        """
        Publish a permanently failed or exhausted message to the DLQ.
        """

        attempt = (original.headers or {}).get("x-attempt", 1)

        outgoing = self._build_message(
            original,
            attempt=attempt,
        )

        result = await self.exchange.publish(
            outgoing,
            routing_key="dead",
            mandatory=True,
        )

        if result is False:
            raise RuntimeError(
                "RabbitMQ did not confirm publishing to the DLQ."
            )