import asyncio
from aiosmtpd.smtp import SMTP as SMTPProtocol


class TemporaryFailureHandler:
    async def handle_DATA(self, server, session, envelope):
        print(
            f"Simulated temporary SMTP failure for: "
            f"{envelope.rcpt_tos}"
        )

        return "550 5.1.1 Permanent recipient failure"


async def main():
    from aiosmtpd.controller import Controller

    handler = TemporaryFailureHandler()

    controller = Controller(
        handler,
        hostname="0.0.0.0",
        port=1025,
    )

    controller.start()

    print("Temporary SMTP failure simulator running on port 1025")

    try:
        await asyncio.Event().wait()
    finally:
        controller.stop()


if __name__ == "__main__":
    asyncio.run(main())
