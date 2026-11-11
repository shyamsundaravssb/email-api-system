from typing import Any
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)


class GenerateItem(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    email_id: UUID
    tracking_token: UUID

    template_id: str = Field(
        min_length=1,
        max_length=128,
    )

    to_email: EmailStr

    to_name: str | None = Field(
        default=None,
        max_length=256,
    )

    variables: dict[str, Any] = Field(
        default_factory=dict,
    )

    @field_validator("template_id")
    @classmethod
    def validate_template_id(cls, value: str) -> str:
        if not value:
            raise ValueError("template_id cannot be empty")

        return value


class GenerateRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    batch_id: UUID

    from_email: EmailStr

    from_name: str | None = Field(
        default=None,
        max_length=256,
    )

    items: list[GenerateItem] = Field(
        min_length=1,
        max_length=10000,
    )

    @model_validator(mode="after")
    def validate_unique_items(self):
        email_ids = [item.email_id for item in self.items]
        tracking_tokens = [
            item.tracking_token for item in self.items
        ]

        if len(email_ids) != len(set(email_ids)):
            raise ValueError(
                "Duplicate email_id values are not allowed"
            )

        if len(tracking_tokens) != len(set(tracking_tokens)):
            raise ValueError(
                "Duplicate tracking_token values are not allowed"
            )

        return self


class GenerateResponse(BaseModel):
    batch_id: UUID
    received: int
