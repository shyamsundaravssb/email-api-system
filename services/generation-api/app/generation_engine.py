from typing import Any

from app.rendering import TemplateRenderError, render_email
from app.schemas import GenerateRequest


# These are initial local testing templates.
# Later, integrated mode will retrieve templates from PostgreSQL.
TEMPLATES: dict[str, dict[str, Any]] = {
    "welcome_email": {
        "subject_template": "Welcome to {{ company_name }}, {{ name }}!",
        "html_template": """
        <!DOCTYPE html>
        <html>
        <body>
            <h1>Hello {{ name }}!</h1>
            <p>Welcome to {{ company_name }}.</p>
            <p>We are happy to have you with us.</p>
            <p>Thank you,<br>{{ company_name }} Team</p>
        </body>
        </html>
        """,
        "text_template": """
        Hello {{ name }}!

        Welcome to {{ company_name }}.

        We are happy to have you with us.

        Thank you,
        {{ company_name }} Team
        """,
        "required_variables": ["name", "company_name"],
    },

    "password_reset": {
        "subject_template": "Reset your {{ company_name }} password",
        "html_template": """
        <!DOCTYPE html>
        <html>
        <body>
            <h2>Hello {{ name }},</h2>
            <p>We received a request to reset your password.</p>
            <p>
                <a href="{{ reset_link }}">Reset your password</a>
            </p>
            <p>If you did not request this, you can ignore this email.</p>
        </body>
        </html>
        """,
        "text_template": """
        Hello {{ name }},

        We received a request to reset your password.

        Reset your password here:
        {{ reset_link }}

        If you did not request this, you can ignore this email.
        """,
        "required_variables": [
            "name",
            "company_name",
            "reset_link",
        ],
    },

    "email_verification": {
        "subject_template": "Verify your {{ company_name }} email address",
        "html_template": """
        <!DOCTYPE html>
        <html>
        <body>
            <h2>Hello {{ name }},</h2>
            <p>Please verify your email address.</p>
            <p>
                <a href="{{ verification_link }}">
                    Verify email address
                </a>
            </p>
        </body>
        </html>
        """,
        "text_template": """
        Hello {{ name }},

        Please verify your email address:

        {{ verification_link }}
        """,
        "required_variables": [
            "name",
            "company_name",
            "verification_link",
        ],
    },

    "order_confirmation": {
        "subject_template": "Order {{ order_number }} confirmed",
        "html_template": """
        <!DOCTYPE html>
        <html>
        <body>
            <h2>Thank you, {{ name }}!</h2>
            <p>Your order has been confirmed.</p>
            <p>Order number: {{ order_number }}</p>
            <p>Total: {{ total }}</p>
        </body>
        </html>
        """,
        "text_template": """
        Thank you, {{ name }}!

        Your order has been confirmed.

        Order number: {{ order_number }}
        Total: {{ total }}
        """,
        "required_variables": [
            "name",
            "order_number",
            "total",
        ],
    },
}


def generate_preview(
    request: GenerateRequest,
) -> list[dict[str, Any]]:
    """
    Generate personalized email payloads without PostgreSQL
    or RabbitMQ.

    This is intended for independent Generation API testing.
    """

    sender_email = str(request.from_email)

    if "@" not in sender_email:
        raise TemplateRenderError("Invalid sender email address")

    sender_domain = sender_email.rsplit("@", 1)[1].lower()

    if not sender_domain:
        raise TemplateRenderError("Invalid sender email domain")

    generated_emails: list[dict[str, Any]] = []

    for item in request.items:
        template = TEMPLATES.get(item.template_id)

        if template is None:
            raise TemplateRenderError(
                f"Unknown template: {item.template_id}"
            )

        rendered = render_email(
            subject_template=template["subject_template"],
            html_template=template["html_template"],
            text_template=template["text_template"],
            variables=item.variables,
            required_variables=template["required_variables"],
        )

        message_id = (
            f"<{item.email_id}@{sender_domain}>"
        )

        generated_emails.append({
            "email_id": item.email_id,
            "batch_id": request.batch_id,
            "template_id": item.template_id,
            "to_email": str(item.to_email),
            "to_name": item.to_name,
            "from_email": sender_email,
            "from_name": request.from_name,
            "subject": rendered["subject"],
            "html": rendered["html"],
            "text": rendered["text"],
            "message_id": message_id,
            "tracking_token": item.tracking_token,
        })

    return generated_emails