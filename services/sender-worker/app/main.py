import asyncio
import logging
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.config import settings
from app.consumer import SenderWorker


# Shared health state.
health_state = {
    "ready": False,
    "status": "starting",
}


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP health endpoint for Docker Compose."""

    def do_GET(self):
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        ready = health_state["ready"]

        status_code = 200 if ready else 503

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        response = (
            '{"status":"ok"}'
            if ready
            else '{"status":"starting"}'
        )

        self.wfile.write(response.encode("utf-8"))

    def log_message(self, format, *args):
        # Avoid logging every Docker health-check request.
        return


def start_health_server():
    """Start the HTTP health server on port 8002."""

    server = ThreadingHTTPServer(
        ("0.0.0.0", 8002),
        HealthHandler,
    )

    server_thread = threading.Thread(
        target=server.serve_forever,
        name="health-server",
        daemon=True,
    )

    server_thread.start()

    logging.info("Health server started on port 8002.")

    return server


async def run_worker():
    """Start the sender worker and handle graceful shutdown."""

    worker = SenderWorker()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # Register operating-system shutdown signals.
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            shutdown_event.set,
        )

    health_server = start_health_server()

    try:
        logging.info("Initializing sender worker...")

        await worker.start()

        health_state["ready"] = True
        health_state["status"] = "running"

        logging.info("Sender worker is ready.")

        # Keep the application alive while consuming messages.
        await shutdown_event.wait()

    except Exception:
        logging.exception("Sender worker encountered a startup/runtime error.")
        raise

    finally:
        logging.info("Graceful shutdown initiated.")

        health_state["ready"] = False
        health_state["status"] = "stopping"

        # Stop consuming and close worker dependencies.
        await worker.close()

        # Stop the health server.
        await asyncio.to_thread(health_server.shutdown)

        health_server.server_close()

        logging.info("Sender worker shutdown completed.")


def main():
    """Application entry point."""

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s "
            "%(message)s"
        ),
    )

    try:
        asyncio.run(run_worker())

    except KeyboardInterrupt:
        logging.info("Application interrupted.")

    except Exception:
        logging.exception("Sender worker terminated unexpectedly.")
        raise


if __name__ == "__main__":
    main()