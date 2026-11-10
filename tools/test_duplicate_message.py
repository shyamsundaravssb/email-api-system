import asyncio
import json
import os

import aio_pika
from aio_pika import DeliveryMode, Message


EMAIL_ID = "1ca32971-0ee6-4722-8e7e-2c6580520a6c"

payload = {
    "email_id": EMAIL_ID,
    "batch_id": "a160fa58-2626-45a4-ba56-e8154208de58",
    "message_id": (
        "<1ca32971-0ee6-4722-8e7e-2c6580520a6c"
        "@grasshopper.optimoze.com>"
    ),
    "from_email": "test@grasshopper.optimoze.com",
    "from_name": "Grasshopper",
    "to_email": "test@example.com",
    "to_name": "Integration Test",
    "subject": "Duplicate Message Test",
    "html": "<p>This duplicate should NOT be delivered.</p>",
    "text": "This duplicate should NOT be delivered.",
    "tracking_token": "e3cb03d5-c4e6-427f-a71e-2e791dae4fec",
}


async def main():
    rabbitmq_url = os.environ["RABBITMQ_URL"]

    connection = await aio_pika.connect_robust(rabbitmq_url)

    try:
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        exchange = await channel.get_exchange("email.exchange")

        message = Message(
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=payload["message_id"],
            headers={"x-attempt": 1},
        )

        await exchange.publish(
            message,
            routing_key="send",
            mandatory=True,
        )

        print("Duplicate message published successfully.")
        print("Email ID:", EMAIL_ID)

    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
