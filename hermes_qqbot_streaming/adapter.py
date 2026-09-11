"""``StreamingQQAdapter`` — Hermes's built-in QQ adapter plus the streaming transport.

The plugin never edits the Hermes tree: it subclasses ``gateway.platforms.qqbot.adapter.QQAdapter``
(so upstream fixes keep flowing) and overrides only the seams where QQ behaves worse than the
generic gateway assumes:

* ``send_stream_frame`` / ``supports_native_streaming`` — C2C typewriter streaming on
  ``/v2/users/{openid}/stream_messages`` (from :mod:`hermes_qqbot_streaming.streaming`);
* ``_api_request`` — a CDN/gateway HTML error page or an empty body is reported as a QQ API error
  instead of surfacing later as ``AttributeError: 'list' object has no attribute 'get'``;
* ``_post_message`` — a passive reply (``msg_id``) that the platform rejects is retried once as a
  proactive message, so a long turn's last message still lands when the reply window expired or the
  per-message reply quota is spent;
* ``_send_chunk`` — rate limits (HTTP 429 / biz 50002) back off exponentially instead of tight-looping;
* ``truncate_message`` — a GFM table is never split across messages.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from gateway.platforms.base import SendResult
from gateway.platforms.qqbot import QQAdapter, check_qq_requirements  # noqa: F401  (re-exported)
from gateway.platforms.qqbot.constants import API_BASE, DEFAULT_API_TIMEOUT

from .chunking import table_safe_truncate
from .streaming import QQStreamMixin

logger = logging.getLogger(__name__)


def _body_snippet(raw: str, limit: int = 200) -> str:
    """One-line preview of a response body for error messages (HTML gateway pages included)."""
    return " ".join(str(raw or "").split())[:limit]


class QQApiError(RuntimeError):
    """QQ REST failure carrying the HTTP status and the platform biz code, so callers can tell a
    transient rejection (retry / fall back) from a permanent one instead of matching message text."""

    def __init__(self, message: str, *, status: int = 0, code: int = 0) -> None:
        super().__init__(message)
        self.status = int(status or 0)
        self.code = int(code or 0)


def builtin_already_streams() -> bool:
    """True when the installed Hermes already streams QQ natively.

    Streaming shipped in-tree in later Hermes releases. Adding our mixin on top of an adapter that
    already inherits it would be a diamond and raise ``TypeError: Cannot create a consistent method
    resolution order`` at import, so the mixin is dropped in that case — the plugin degrades to its
    outbound hardening (tolerant REST errors, proactive-reply fallback, rate-limit backoff,
    table-safe chunking), which is not in stock Hermes.
    """
    return (callable(getattr(QQAdapter, "send_stream_frame", None))
            and bool(getattr(QQAdapter, "SUPPORTS_NATIVE_STREAMING", False)))


_MIXIN_BASES: tuple = () if builtin_already_streams() else (QQStreamMixin,)


class StreamingQQAdapter(*_MIXIN_BASES, QQAdapter):
    """QQ adapter with C2C native streaming and QQ-specific outbound hardening."""

    SUPPORTS_NATIVE_STREAMING = True

    #: Long replies keep GFM tables whole (see :mod:`hermes_qqbot_streaming.chunking`).
    truncate_message = staticmethod(table_safe_truncate)

    _PERMANENT_SEND_ERRORS = ("invalid", "forbidden", "not found")

    def __init__(self, config: Any) -> None:
        super().__init__(config)
        extra = getattr(config, "extra", None) or {}
        # ``platforms.qqbot.extra.streaming: false`` is the escape hatch back to one-shot replies.
        # Each attribute is only defaulted here: an adapter that already streams owns its own state.
        if not hasattr(self, "_streaming_enabled"):
            self._streaming_enabled = bool(extra.get("streaming", True))
        if not hasattr(self, "_stream_states"):
            self._stream_states = {}
        if not hasattr(self, "_stream_disabled_chats"):
            self._stream_disabled_chats = set()

    # ── REST ──

    async def _api_request(
        self, method: str, path: str, body: Optional[Dict[str, Any]] = None,
        timeout: float = DEFAULT_API_TIMEOUT,
    ) -> Dict[str, Any]:
        """QQ REST call whose errors carry ``status``/``code`` and whose body parsing is tolerant.

        The platform (or the CDN in front of it) answers HTML on bad days; ``resp.json()`` would
        raise a bare ValueError there and ``None``/list bodies would blow up as AttributeError one
        call later. Both are reported here, with a snippet, as :class:`QQApiError`."""
        import httpx

        client = self._require_http_client()
        headers = await self._auth_headers()
        try:
            resp = await client.request(method, f"{API_BASE}{path}", headers=headers, json=body, timeout=timeout)
            raw = resp.text or ""
            try:
                data = resp.json()
            except ValueError:
                data = None  # CDN/gateway error page (HTML) or an empty body
            if resp.status_code >= 400:
                payload = data if isinstance(data, dict) else {}
                message = payload.get("message") or _body_snippet(raw) or f"HTTP {resp.status_code}"
                raise QQApiError(
                    f"QQ Bot API error [{resp.status_code}] {path}: {message}",
                    status=resp.status_code,
                    code=int(payload.get("code") or payload.get("err_code") or 0))
            if not isinstance(data, dict):
                raise QQApiError(
                    f"QQ Bot API returned a non-JSON object [{resp.status_code}] {path}: "
                    f"{_body_snippet(raw) or 'empty body'}",
                    status=resp.status_code)
            return data
        except httpx.TimeoutException as exc:
            raise RuntimeError(f"QQ Bot API timeout [{path}]: {exc}") from exc

    async def _post_message(self, path: str, body: Dict[str, Any]) -> SendResult:
        """POST a message body; when QQ rejects the passive reply (``msg_id``), retry without it.

        A passive reply only works inside the inbound message's reply window, and QQ caps how many
        replies one inbound message carries (Tencent's own SDK tracks ~4/hour and switches to
        proactive sends for exactly this reason). Without the fallback a long turn's last message —
        or the only message after a slow tool run — is lost outright. Only a platform-level rejection
        triggers it: a timeout may well have been delivered, and a duplicate is worse than
        re-sending next turn."""
        try:
            return await super()._post_message(path, body)
        except QQApiError as exc:
            if not body.get("msg_id"):
                raise
            logger.info("[%s] Passive reply rejected (%s) — retrying as a proactive message",
                        self._log_tag, exc)
            return await super()._post_message(path, {k: v for k, v in body.items() if k != "msg_id"})

    async def _send_chunk(self, chat_id: str, content: str, reply_to: Optional[str] = None) -> SendResult:
        """Deliver one chunk, retrying transient failures (rate limits back off exponentially)."""
        last_exc: Optional[Exception] = None
        sender = self._text_sender(self._guess_chat_type(chat_id))
        if sender is None:
            return SendResult(success=False, error=f"Unknown chat type for {chat_id}")
        for attempt in range(3):
            try:
                return await sender(chat_id, content, reply_to)
            except Exception as exc:  # noqa: BLE001 — every sender failure is Retryable-or-not
                last_exc = exc
                if any(k in str(exc).lower() for k in self._PERMANENT_SEND_ERRORS + ("bad request",)):
                    break  # permanent — don't retry
                if attempt < 2:
                    delay = 2.0 ** attempt if self._is_rate_limited(exc) else 1.0 * (2 ** attempt)
                    logger.warning("[%s] send retry %d/3 after %.1fs: %s",
                                   self._log_tag, attempt + 1, delay, exc)
                    await asyncio.sleep(delay)

        error_msg = (str(last_exc) or type(last_exc).__name__) if last_exc else "Unknown error"
        logger.error("[%s] Send failed: %s", self._log_tag, error_msg)
        retryable = not any(k in error_msg.lower() for k in self._PERMANENT_SEND_ERRORS)
        return SendResult(success=False, error=error_msg, retryable=retryable)
