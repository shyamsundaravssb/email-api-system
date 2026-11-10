import json
from dataclasses import dataclass
from email.utils import parseaddr
from typing import Any


REQUIRED_FIELDS = (
    "email_id",
    "batch_id",
    "message_id",
    "from_email",
    "from_name",
    "to_email",
    "to_name",
    "subject",
    "html",
    "text",
    "tracking_token",
)

OPTIONAL_NAME_FIELDS = ("from_name", "to_name")


@dataclass(frozen=True)
class EmailMessage:
    """Structured representation of an email queue message."""

    email_id: str
    batch_id: str
    message_id: str

    from_email: str
    from_name: str

    to_email: str
    to_name: str

    subject: str
    html: str
    text: str

    tracking_token: str

    attempt: int = 1

    @classmethod
    def from_payload(
        cls,
        body: bytes,
        headers: dict[str, Any] | None = None,
    ) -> "EmailMessage":
        """Decode and validate a RabbitMQ email message."""

        # Decode JSON.
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "Message body contains invalid JSON."
            ) from exc

        if not isinstance(payload, dict):
            raise ValueError(
                "Message body must be a JSON object."
            )

        # Check required keys.
        missing_fields = [
            field
            for field in REQUIRED_FIELDS
            if field not in payload
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required message fields: {missing_fields}"
            )

        # Validate all required values.
        for field in REQUIRED_FIELDS:
            value = payload[field]

            # Sender and recipient names may be null.
            if field in OPTIONAL_NAME_FIELDS and value is None:
                continue

            if not isinstance(value, str):
                raise ValueError(
                    f"Field '{field}' must be a string."
                )

            if not value.strip():
                raise ValueError(
                    f"Field '{field}' cannot be empty."
                )

        # Validate sender and recipient email addresses.
        from_email = payload["from_email"]
        to_email = payload["to_email"]

        if (
            parseaddr(from_email)[1] != from_email
            or "@" not in from_email
        ):
            raise ValueError(
                "Invalid sender email address."
            )

        if (
            parseaddr(to_email)[1] != to_email
            or "@" not in to_email
        ):
            raise ValueError(
                "Invalid recipient email address."
            )

        # Read retry count from RabbitMQ headers.
        headers = headers or {}
        attempt_value = headers.get("x-attempt", 1)

        if isinstance(attempt_value, bool) or not isinstance(
            attempt_value, int
        ):
            raise ValueError(
                "RabbitMQ x-attempt header must be an integer."
            )

        if attempt_value < 1:
            raise ValueError(
                "RabbitMQ x-attempt must be at least 1."
            )

        # Normalize optional names for SMTP MIME construction.
        from_name = payload["from_name"] or ""
        to_name = payload["to_name"] or ""

        return cls(
            email_id=payload["email_id"],
            batch_id=payload["batch_id"],
            message_id=payload["message_id"],
            from_email=from_email,
            from_name=from_name,
            to_email=to_email,
            to_name=to_name,
            subject=payload["subject"],
            html=payload["html"],
            text=payload["text"],
            tracking_token=payload["tracking_token"],
            attempt=attempt_value,
        )