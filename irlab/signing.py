"""HMAC-SHA256 webhook signatures, modeled on the Standard Webhooks header convention.

    webhook-id:        evt_…        same as the envelope id, stable across redeliveries
    webhook-timestamp: 1762560000   unix seconds, fresh on every delivery attempt
    webhook-signature: v1,<base64>  space-separated list, so a sender can sign with
                                    old + new secret during a rotation

    signed content = f"{webhook-id}.{webhook-timestamp}." + raw request body

The receiver verifies the *raw bytes* before parsing anything. The timestamp is
inside the signed content, so an attacker cannot replay an old delivery with a
fresh timestamp; the tolerance window bounds how long a captured request is useful.

Both the mock providers (signing) and the hub (verifying) import this module. In
a real deployment each provider ships its own implementation of the same scheme.
"""

import base64
import hashlib
import hmac
import time
from collections.abc import Mapping, Sequence

HEADER_ID = "webhook-id"
HEADER_TIMESTAMP = "webhook-timestamp"
HEADER_SIGNATURE = "webhook-signature"
SCHEME = "v1"
# Bound the work an attacker can cause with a header full of candidate signatures.
MAX_SIGNATURES = 5


class SignatureError(Exception):
    def __init__(self, code: str, http_status: int):
        super().__init__(code)
        self.code = code
        self.http_status = http_status


def compute_signature(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    signed = f"{msg_id}.{timestamp}.".encode() + body
    return base64.b64encode(hmac.new(secret.encode(), signed, hashlib.sha256).digest()).decode()


def sign_headers(
    secrets: str | Sequence[str], msg_id: str, body: bytes, *, timestamp: int | None = None
) -> dict[str, str]:
    """Headers for one delivery attempt. Pass several secrets to dual-sign during rotation."""
    ts = int(time.time()) if timestamp is None else timestamp
    keys = [secrets] if isinstance(secrets, str) else list(secrets)
    signature = " ".join(f"{SCHEME},{compute_signature(k, msg_id, ts, body)}" for k in keys)
    return {HEADER_ID: msg_id, HEADER_TIMESTAMP: str(ts), HEADER_SIGNATURE: signature}


def verify(
    headers: Mapping[str, str],
    body: bytes,
    secrets: Sequence[str],
    *,
    tolerance_s: int,
    now: float | None = None,
) -> str:
    """Return the verified webhook id or raise SignatureError.

    `headers` must support case-insensitive lookup (Starlette `Headers`) or use
    lower-case keys.
    """
    msg_id = headers.get(HEADER_ID)
    ts_raw = headers.get(HEADER_TIMESTAMP)
    sig_header = headers.get(HEADER_SIGNATURE)
    if not (msg_id and ts_raw and sig_header):
        raise SignatureError("missing_signature_headers", 400)
    try:
        ts = int(ts_raw)
    except ValueError:
        raise SignatureError("invalid_timestamp", 400) from None

    now = time.time() if now is None else now
    if abs(now - ts) > tolerance_s:
        raise SignatureError("timestamp_out_of_tolerance", 401)

    candidates = [
        part.split(",", 1)[1].encode("utf-8", errors="replace")
        for part in sig_header.split()[:MAX_SIGNATURES]
        if part.startswith(f"{SCHEME},")
    ]
    if not candidates or not secrets:
        raise SignatureError("invalid_signature", 401)

    for secret in secrets:
        expected = compute_signature(secret, msg_id, ts, body).encode()
        # Constant-time comparison: no early exit that leaks how many bytes matched.
        if any(hmac.compare_digest(expected, candidate) for candidate in candidates):
            return msg_id
    raise SignatureError("invalid_signature", 401)
