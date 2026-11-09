
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from typing_extensions import Annotated


# -------------------------------------------------------------------
# Request schemas
# -------------------------------------------------------------------

class RecipientRequest(BaseModel):
    """A single recipient in a batch request."""

    model_config = ConfigDict(extra="forbid")

    template_id: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    to_email: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    to_name: str | None = None
    variables: dict[str, Any] = Field(default_factory=dict)


class BatchCreateRequest(BaseModel):
    """Request body for creating an email batch."""

    model_config = ConfigDict(extra="forbid")

    recipients: list[RecipientRequest] = Field(min_length=1)


# -------------------------------------------------------------------
# Recipient result schemas
# -------------------------------------------------------------------

class AcceptedRecipientResult(BaseModel):
    position: int
    to_email: str
    status: Literal["accepted"]
    email_id: str


class RejectedRecipientResult(BaseModel):
    position: int
    to_email: str
    status: Literal["rejected"]
    reason: str


RecipientResult = Annotated[
    AcceptedRecipientResult | RejectedRecipientResult,
    Field(discriminator="status"),
]


# -------------------------------------------------------------------
# Batch response schemas
# -------------------------------------------------------------------

class BatchCreateResponse(BaseModel):
    """Response when at least one recipient is accepted."""

    batch_id: str
    total: int
    accepted: int
    rejected: int
    results: list[RecipientResult]


class BatchCreateErrorResponse(BaseModel):
    """Response when no recipient is accepted."""

    error: Literal[
        "all_recipients_invalid",
        "queue_capacity_exhausted",
        "too_many_recipients",
    ]
    total: int
    results: list[RejectedRecipientResult]


# -------------------------------------------------------------------
# Existing read response schemas
# -------------------------------------------------------------------

class BatchSummaryResponse(BaseModel):
    """Basic batch information."""

    batch_id: str
    status: str
    total: int
    accepted: int
    rejected: int
    created_at: str


class EmailStatusResponse(BaseModel):
    """Individual email status."""

    email_id: str
    batch_id: str
    position: int
    to_email: str
    status: str
    error: str | None = None
    created_at: str
    updated_at: str
