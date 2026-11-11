from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from irlab.ingest import WebhookIngestor

router = APIRouter(tags=["webhooks"])


async def read_body_capped(request: Request, limit: int) -> bytes:
    """Read at most limit+1 bytes so an oversized body is detected without buffering all of it."""
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        chunks.append(chunk)
        size += len(chunk)
        if size > limit:
            break
    return b"".join(chunks)[: limit + 1]


@router.post(
    "/webhooks/{source}",
    summary="Receive a signed webhook from the POS, payments or loyalty provider",
    responses={
        200: {"description": "Duplicate delivery: already stored, acknowledged so the sender stops retrying"},
        202: {"description": "New event stored in the inbox"},
        400: {"description": "Malformed headers, JSON or envelope"},
        401: {"description": "Bad signature or timestamp outside tolerance"},
        404: {"description": "Unknown source"},
        409: {"description": "Same event is being stored by a concurrent delivery; retry after Retry-After"},
        413: {"description": "Body larger than WEBHOOK_MAX_BODY_BYTES"},
        503: {"description": "Source not configured, or storage unavailable; retry later"},
    },
)
async def receive_webhook(source: str, request: Request) -> JSONResponse:
    ingestor: WebhookIngestor = request.app.state.ingestor
    # The signature covers the exact bytes on the wire: read them raw, never re-serialise.
    body = await read_body_capped(request, ingestor.max_body_bytes)
    result = await ingestor.ingest(source, request.headers, body)
    return JSONResponse(result.body(), status_code=result.http_status, headers=result.headers)
