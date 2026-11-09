import hmac
import logging

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Header,
    HTTPException,
    Request,
    status,
)

from app.generation_engine import generate_preview
from app.rendering import TemplateRenderError
from app.schemas import (
    GenerateRequest,
    GenerateResponse,
    GeneratePreviewResponse,
)


logger = logging.getLogger(__name__)
router = APIRouter()


def authenticate(
    request: Request,
    internal_token: str | None,
) -> None:
    expected_token = request.app.state.settings.internal_token

    if (
        internal_token is None
        or not hmac.compare_digest(internal_token, expected_token)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal authentication token",
        )


def validate_batch_size(request: Request, payload: GenerateRequest) -> None:
    maximum = request.app.state.settings.generation_max_items

    if len(payload.items) > maximum:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Maximum allowed emails per request is {maximum}",
        )


@router.post(
    "/internal/generate/preview",
    response_model=GeneratePreviewResponse,
    status_code=status.HTTP_200_OK,
)
async def generate_preview_endpoint(
    payload: GenerateRequest,
    request: Request,
    internal_token: str | None = Header(
        default=None,
        alias="X-Internal-Token",
    ),
):
    """Render personalized email content without sending or queueing it."""

    authenticate(request, internal_token)
    validate_batch_size(request, payload)

    try:
        generated_emails = generate_preview(payload)
    except TemplateRenderError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    logger.info(
        "Generated preview for batch %s: %s emails",
        payload.batch_id,
        len(generated_emails),
    )

    return GeneratePreviewResponse(
        batch_id=payload.batch_id,
        received=len(payload.items),
        generated_count=len(generated_emails),
        emails=generated_emails,
    )


@router.post(
    "/internal/generate",
    response_model=GenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def generate(
    payload: GenerateRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    internal_token: str | None = Header(
        default=None,
        alias="X-Internal-Token",
    ),
):
    """Accept a batch for the integrated generation pipeline."""

    authenticate(request, internal_token)
    validate_batch_size(request, payload)

    processor = getattr(request.app.state, "processor", None)

    if processor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Integrated generation is unavailable in standalone mode. "
                "Use /internal/generate/preview for rendering tests."
            ),
        )

    background_tasks.add_task(processor.process_batch, payload)

    logger.info(
        "Accepted generation request for batch %s with %s emails",
        payload.batch_id,
        len(payload.items),
    )

    return GenerateResponse(
        batch_id=payload.batch_id,
        received=len(payload.items),
    )
