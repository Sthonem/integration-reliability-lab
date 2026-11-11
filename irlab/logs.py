"""Structured logging for the `irlab` logger tree.

* JSON lines by default (`LOG_FORMAT=json`), key=value for terminals (`LOG_FORMAT=kv`).
* Call sites attach fields with `extra={...}`.
* Correlation ids live in a context variable and are added to every record, so a
  webhook's event id follows it through processing, the outbox call (sent to the
  provider as `X-Correlation-ID`), the dead-letter queue and the replay.

Secrets never go through `extra`; settings hold them as `SecretStr`.
"""

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

_STANDARD_ATTRS = set(vars(logging.LogRecord("x", 0, "x", 0, "x", None, None))) | {"message", "asctime", "taskName"}

_context: ContextVar[dict[str, Any] | None] = ContextVar("irlab_log_context", default=None)


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Add fields (correlation_id, order_id, outbox_id, ...) to every log record in this block."""
    current = _context.get() or {}
    token = _context.set(current | {k: v for k, v in fields.items() if v is not None})
    try:
        yield
    finally:
        _context.reset(token)


def current_context() -> dict[str, Any]:
    return dict(_context.get() or {})


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    out = dict(_context.get() or {})
    out.update({k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS and v is not None})
    out.pop("_irlab", None)
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        doc.update(_fields(record))
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str, ensure_ascii=False)


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = f"{self.formatTime(record)} {record.levelname} {record.name} msg={record.getMessage()!r}"
        fields = " ".join(f"{k}={v}" for k, v in _fields(record).items())
        line = f"{base} {fields}".rstrip()
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    logger = logging.getLogger("irlab")
    logger.setLevel(level.upper())
    formatter: logging.Formatter = JsonFormatter() if fmt == "json" else KeyValueFormatter()
    for handler in logger.handlers:
        if getattr(handler, "_irlab", False):
            handler.setFormatter(formatter)
            return
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    handler._irlab = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
