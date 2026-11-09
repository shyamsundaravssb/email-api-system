import logging
import html
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlsplit

from jinja2 import (
    StrictUndefined,
    TemplateError,
)
from jinja2.sandbox import SandboxedEnvironment


logger = logging.getLogger(__name__)


class TemplateRenderError(Exception):
    """Raised when an email template cannot be rendered."""


# Variables that contain URLs must contain absolute HTTP/HTTPS URLs.
URL_VARIABLES = {
    "link",
    "dashboard_url",
    "order_url",
    "offer_url",
    "terms_url",
    "unsubscribe_url",
}


class HTMLToTextParser(HTMLParser):
    """Convert rendered HTML into a readable plain-text body."""

    BLOCK_TAGS = {
        "address", "article", "blockquote", "br", "div",
        "footer", "h1", "h2", "h3", "h4", "header",
        "li", "ol", "p", "section", "table", "tr", "ul",
    }

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts)

        lines = [
            line.strip()
            for line in text.splitlines()
        ]

        # Remove repeated blank lines.
        cleaned = []
        previous_blank = False

        for line in lines:
            is_blank = not line

            if is_blank and previous_blank:
                continue

            cleaned.append(line)
            previous_blank = is_blank

        return "\n".join(cleaned).strip()


class EmailRenderer:
    """
    Render subject, HTML and plain-text email content.

    HTML uses automatic escaping.
    Subject and text use non-HTML rendering.
    Missing variables raise an error instead of silently
    producing incomplete emails.
    """

    def __init__(self):
        self.html_environment = SandboxedEnvironment(
            undefined=StrictUndefined,
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )

        self.text_environment = SandboxedEnvironment(
            undefined=StrictUndefined,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    @staticmethod
    def validate_variables(
        variables: dict[str, Any],
        required_variables: list[str] | tuple[str, ...],
    ) -> None:
        """Check that all required template variables exist."""

        if not isinstance(variables, dict):
            raise TemplateRenderError(
                "Template variables must be a dictionary"
            )

        missing = []

        for variable in required_variables:
            if variable not in variables:
                missing.append(variable)
                continue

            value = variables[variable]

            if value is None:
                missing.append(variable)
            elif isinstance(value, str) and not value.strip():
                missing.append(variable)

        if missing:
            raise TemplateRenderError(
                "Missing required template variables: "
                + ", ".join(missing)
            )

    @staticmethod
    def validate_urls(variables: dict[str, Any]) -> None:
        """
        Validate known URL variables.

        Only absolute HTTP and HTTPS URLs are accepted.
        This prevents javascript: and other unsafe schemes.
        """

        for variable in URL_VARIABLES:
            value = variables.get(variable)

            if value is None:
                continue

            if not isinstance(value, str):
                raise TemplateRenderError(
                    f"{variable} must be a URL string"
                )

            parsed = urlsplit(value)

            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or any(char.isspace() for char in value)
            ):
                raise TemplateRenderError(
                    f"{variable} must be an absolute HTTP or HTTPS URL"
                )

            if parsed.username or parsed.password:
                raise TemplateRenderError(
                    f"{variable} must not contain URL credentials"
                )

    @staticmethod
    def html_to_text(html_content: str) -> str:
        """Generate a plain-text alternative from rendered HTML."""

        parser = HTMLToTextParser()

        try:
            parser.feed(html_content)
            parser.close()
            return parser.get_text()

        except Exception as exc:
            raise TemplateRenderError(
                "Could not generate plain-text email content"
            ) from exc

    def render(
        self,
        subject_template: str,
        html_template: str,
        text_template: str | None,
        variables: dict[str, Any],
        required_variables: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, str]:
        """
        Render one personalised email.

        Returns:
            {
                "subject": "...",
                "html": "...",
                "text": "..."
            }
        """

        required_variables = required_variables or []

        self.validate_variables(
            variables,
            required_variables,
        )

        self.validate_urls(variables)

        try:
            subject = self.text_environment.from_string(
                subject_template
            ).render(**variables).strip()

            html_content = self.html_environment.from_string(
                html_template
            ).render(**variables)

            if text_template and text_template.strip():
                text_content = self.text_environment.from_string(
                    text_template
                ).render(**variables).strip()
            else:
                text_content = self.html_to_text(html_content)

        except TemplateError as exc:
            logger.warning("Email template rendering failed: %s", exc)

            raise TemplateRenderError(
                f"Template rendering failed: {exc}"
            ) from exc

        except (TypeError, ValueError) as exc:
            raise TemplateRenderError(
                f"Invalid template input: {exc}"
            ) from exc

        if not subject:
            raise TemplateRenderError(
                "Rendered email subject cannot be empty"
            )

        return {
            "subject": subject,
            "html": html_content,
            "text": text_content,
        }


# Shared renderer instance for the application.
email_renderer = EmailRenderer()


def render_email(
    subject_template: str,
    html_template: str,
    text_template: str | None,
    variables: dict[str, Any],
    required_variables: list[str] | tuple[str, ...] | None = None,
) -> dict[str, str]:
    """Convenience function used by the generation processor."""

    return email_renderer.render(
        subject_template=subject_template,
        html_template=html_template,
        text_template=text_template,
        variables=variables,
        required_variables=required_variables,
    )


def rewrite_tracking_link(
    html_content: str,
    original_url: str,
    tracking_url: str,
) -> tuple[str, bool]:
    """Replace the matching double-quoted HTML href with a tracking URL."""

    pattern = re.compile(
        r'(?P<prefix>\bhref\s*=\s*")(?P<url>[^"]*)"',
        re.IGNORECASE,
    )

    replaced = False

    def replace_match(match):
        nonlocal replaced

        if html.unescape(match.group("url")) != original_url:
            return match.group(0)

        replaced = True
        return (
            match.group("prefix")
            + html.escape(tracking_url, quote=True)
            + '"'
        )

    return pattern.sub(replace_match, html_content), replaced


def append_open_tracking_pixel(
    html_content: str,
    tracking_url: str,
) -> str:
    """Append a one-pixel open-tracking image to the email HTML."""

    pixel = (
        f'<img src="{html.escape(tracking_url, quote=True)}" '
        'width="1" height="1" alt="" style="display:none;" />'
    )

    if re.search(r"</body\s*>", html_content, re.IGNORECASE):
        return re.sub(
            r"</body\s*>",
            lambda match: pixel + match.group(0),
            html_content,
            count=1,
            flags=re.IGNORECASE,
        )

    return html_content + pixel

