
import asyncio
import logging
from typing import Any

import httpx

from app.config import Settings

logger = logging.getLogger(__name__)


class GenerationHandoffError(Exception):
    """Raised when the Generation API handoff ultimately fails."""


class GenerationClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.endpoint = (
            f"{settings.generation_api_url}/internal/generate"
        )

    async def submit_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.settings.internal_token:
            raise GenerationHandoffError(
                "INTERNAL_TOKEN is not configured"
            )

        headers = {
            "X-Internal-Token": self.settings.internal_token,
            "Content-Type": "application/json",
        }

        for attempt in range(1, self.settings.generation_api_max_attempts + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=self.settings.generation_api_timeout_seconds
                ) as client:
                    response = await client.post(
                        self.endpoint,
                        json=payload,
                        headers=headers,
                    )

                if response.status_code == 202:
                    result = response.json()

                    logger.info(
                        "Generation API accepted batch",
                        extra={
                            "batch_id": payload.get("batch_id"),
                            "received": result.get("received"),
                            "attempt": attempt,
                        },
                    )
                    return result

                # Authentication and request validation errors are not
                # expected to succeed by retrying the same request.
                if 400 <= response.status_code < 500:
                    raise GenerationHandoffError(
                        f"Generation API rejected request: "
                        f"HTTP {response.status_code}"
                    )

                logger.warning(
                    "Generation API returned a retryable response",
                    extra={
                        "batch_id": payload.get("batch_id"),
                        "status_code": response.status_code,
                        "attempt": attempt,
                    },
                )

            except (httpx.TimeoutException, httpx.TransportError) as exc:
                logger.warning(
                    "Generation API connection failed",
                    extra={
                        "batch_id": payload.get("batch_id"),
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )

            if attempt < self.settings.generation_api_max_attempts:
                await asyncio.sleep(2 ** (attempt - 1))

        raise GenerationHandoffError(
            f"Generation API handoff failed after "
            f"{self.settings.generation_api_max_attempts} attempts"
        )
