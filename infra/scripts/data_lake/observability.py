"""Opt-in Sentry for the scheduled capture runners (cron: jobs, market, SEO, news, monitor).

Off unless ``SENTRY_DSN`` is set and the ``sentry`` extra is installed
(``uv sync --extra sentry``). A failed run (exit code 1) becomes one static
"capture run failed" message tagged with the runner name and exit code; exit code 2
(lock held by an overlapping run, or a blocked lane) is expected and not reported.
An uncaught exception is captured and re-raised. Every event passes the allowlist
:func:`scrub_event`: exception type and stack frame locations only, allowlisted
messages only, no request, user, breadcrumbs, extras or local variables, so source
URLs, payloads and paths never leave the host.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from importlib import import_module
from typing import Any

FAILED_RUN_MESSAGE = "capture run failed"
ALLOWED_MESSAGES = frozenset({FAILED_RUN_MESSAGE})
EXPECTED_EXIT_CODES = frozenset({0, 2})
_TOP_LEVEL_KEYS = frozenset({"event_id", "timestamp", "level", "platform", "logger", "environment", "release", "tags"})
_FRAME_KEYS = ("filename", "module", "function", "lineno", "in_app")


def scrub_event(event: dict[str, Any], hint: object = None) -> dict[str, Any]:
    """Allowlist ``before_send``: keep only fields that cannot carry captured data."""

    clean: dict[str, Any] = {key: value for key, value in event.items() if key in _TOP_LEVEL_KEYS}
    if "exception" in event:
        values = []
        for value in (event["exception"] or {}).get("values") or []:
            frames = ((value.get("stacktrace") or {}).get("frames")) or []
            values.append(
                {
                    "type": value.get("type"),
                    "stacktrace": {"frames": [{k: f[k] for k in _FRAME_KEYS if k in f} for f in frames]},
                }
            )
        clean["exception"] = {"values": values}
    message = event.get("message")
    if message is None and isinstance(event.get("logentry"), dict):
        message = event["logentry"].get("message")
    if message is not None:
        clean["message"] = message if message in ALLOWED_MESSAGES else "[redacted]"
    return clean


def _init_sdk(runner: str) -> Any | None:
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return None
    try:
        sdk = import_module("sentry_sdk")
    except ImportError:
        return None
    sdk.init(
        dsn=dsn,
        environment=os.environ.get("SENTRY_ENVIRONMENT", "local"),
        send_default_pii=False,
        traces_sample_rate=0.0,
        max_breadcrumbs=0,
        before_send=scrub_event,
    )
    sdk.set_tag("runner", runner)
    return sdk


def run_with_reporting(runner: str, main: Callable[[], int]) -> int:
    """Run a capture entrypoint and report a failed run; returns its exit code unchanged."""

    sdk = _init_sdk(runner)
    try:
        code = main()
    except Exception as error:
        if sdk is not None:
            sdk.capture_exception(error)
            sdk.flush(timeout=5)
        raise
    if sdk is not None and code not in EXPECTED_EXIT_CODES:
        sdk.set_tag("exit_code", str(code))
        sdk.capture_message(FAILED_RUN_MESSAGE, level="error")
        sdk.flush(timeout=5)
    return code
