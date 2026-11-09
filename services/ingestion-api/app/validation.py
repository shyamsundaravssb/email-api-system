
import re
from typing import Any


EMAIL_PATTERN = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)


def validate_email(email: str) -> str | None:
    """Return an error code if the email format is invalid."""

    if not EMAIL_PATTERN.fullmatch(email):
        return "invalid_email"

    return None


def validate_required_variables(
    required_variables: list[str],
    supplied_variables: dict[str, Any],
) -> list[str]:
    """Return the names of required variables that are missing."""

    return [
        variable
        for variable in required_variables
        if variable not in supplied_variables
        or supplied_variables[variable] is None
        or (
            isinstance(supplied_variables[variable], str)
            and not supplied_variables[variable].strip()
        )
    ]


async def validate_batch(
    recipients: list[Any],
    db_pool,
    max_recipients: int,
    conn=None,
) -> dict[str, Any]:
    """
    Validate all recipients in a batch.

    Template data is fetched in one query.
    Recipient order is preserved.

    Returns accepted and rejected recipient details.
    """

    if len(recipients) > max_recipients:
        return {
            "valid": False,
            "error": "too_many_recipients",
            "accepted": [],
            "rejected": [],
        }

    template_ids = list({
        recipient.template_id for recipient in recipients
    })

    query = """
        SELECT template_id, required_variables
        FROM templates
        WHERE template_id = ANY($1::text[])
    """

    if conn is not None:
        rows = await conn.fetch(query, template_ids)
    else:
        async with db_pool.acquire() as db_conn:
            rows = await db_conn.fetch(query, template_ids)

    templates = {
        row["template_id"]: row["required_variables"]
        for row in rows
    }

    accepted = []
    rejected = []

    for position, recipient in enumerate(recipients, start=1):
        error = validate_email(recipient.to_email)

        if error is None:
            required_variables = templates.get(recipient.template_id)

            if required_variables is None:
                error = "unknown_template"
            else:
                missing = validate_required_variables(
                    required_variables,
                    recipient.variables or {},
                )

                if missing:
                    error = "missing_variables"

        result = {
            "position": position,
            "recipient": recipient,
            "error": error,
        }

        if error:
            rejected.append(result)
        else:
            accepted.append(result)

    return {
        "valid": True,
        "error": None,
        "accepted": accepted,
        "rejected": rejected,
    }
