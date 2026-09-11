"""QQ Bot C2C native streaming (``POST /v2/users/{openid}/stream_messages``).

QQ's stream API keeps ONE server-side message that is rewritten in place as frames arrive
(``input_mode: "replace"``, every frame carries the whole text so far), so the reply types itself
out in the QQ client instead of landing as one wall of text minutes later. Only C2C (private chat)
has a stream endpoint — group/channel chats report ``supports_native_streaming`` False and keep the
plain send path.

Protocol rules — every one probed against the live platform, not inferred from docs:

* every frame of one stream carries the SAME ``msg_seq`` — that is what makes the frames a single
  message (and keeps them inside the passive-reply quota); only ``index`` advances;
* the ``stream_msg_id`` from the FIRST response must ride on every later frame, else the platform
  answers ``400 / 40054005 消息被去重，请检查请求msgseq``;
* ``index`` must strictly increase (``404 / 40006`` otherwise) and a frame's ``content_raw`` must
  EXTEND the previous one — repainting submitted content is refused with
  ``404 / 40007 已经提交的消息内容不可修改``. Hermes's native frames are not naturally monotonic
  (they carry a tool-progress block below a ``---`` rule that is cleared the moment the reply
  continues), so this mixin keeps them monotonic: it strips the gateway's typing cursor and the
  tool-progress overlay, and when a frame diverges anyway it APPENDS only the part the client has
  not seen instead of reopening a message with text already on screen;
* ``input_state`` 1 = generating, 10 = done (closes the stream; the client stops spinning);
* frames must be >= ~300 ms apart; rate-limit errors (HTTP 429 / biz code 50002) need exponential
  backoff;
* ``msg_id`` must be the inbound message that opened the passive-reply window, so a stream can only
  be sent while that window is alive.

Mixed into an adapter that owns ``_api_request`` / ``_auth_headers`` / ``_last_msg_id`` /
``MAX_MESSAGE_LENGTH`` / ``format_message`` / ``_log_tag``; the gateway's stream consumer drives it
through ``SUPPORTS_NATIVE_STREAMING`` + ``send_stream_frame`` and knows nothing QQ-specific.
"""

from __future__ import annotations

import asyncio
import logging
import os as _os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

STREAM_PATH_TEMPLATE = "/v2/users/{openid}/stream_messages"
STREAM_INPUT_MODE_REPLACE = "replace"
STREAM_STATE_GENERATING = 1
STREAM_STATE_DONE = 10
STREAM_CONTENT_TYPE_MARKDOWN = "markdown"

# Platform throttle: the API rejects/ignores frames faster than this. Frames are cumulative,
# so dropping an intermediate one loses nothing — the next frame carries the whole text.
MIN_FRAME_INTERVAL_SECONDS = 0.32
# The opening frame is what CREATES the message on the client: opening it on a couple of characters
# makes QQ flash an unrenderable empty bubble ("该类型消息不支持查看") before the first real line.
_MIN_OPEN_CHARS = 12
_RATE_LIMIT_HTTP_STATUS = 429
_RATE_LIMIT_BIZ_CODES = {50002}
_RETRY_ATTEMPTS = 3
_RETRY_BASE_DELAY = 1.0
# Errors that mean "this chat will never stream" — retrying only burns the reply window.
_PERMANENT_HTTP_STATUSES = {401, 403, 404}
_PERMANENT_BIZ_CODES = {11253}  # 应用无接口访问权限
_PERMANENT_MESSAGES = ("不支持的调用", "越权", "无权限", "msg_id无效")
# Biz codes for a protocol-state conflict, not a missing capability: a fresh stream fixes them, so
# they must not blacklist the chat the way 11253 does.
#   40006  index 未递增   40007 已提交内容不可修改   40054005 消息被去重
_TRANSIENT_STREAM_BIZ_CODES = {40006, 40007, 40054005}
# The gateway composes native frames as ``<reply text>\n\n---\n<running tool lines>`` and clears the
# tool lines the moment real text arrives (``GatewayStreamConsumer._compose_frame_content``).
_PROGRESS_OVERLAY_SEPARATOR = "\n\n---\n"

# ── The gateway's typing cursor ──
# ``GatewayStreamConsumer._push_update`` appends ``streaming.cursor`` (default " ▉") to every
# INTERIM native frame; only the finalize frame carries none. A trailing suffix that differs
# between consecutive frames makes every frame non-prefix — QQ answers a non-extending frame with
# 404 / 40007 ("已经提交的消息内容不可修改"), and the mixin then seals + reopens a fresh message that
# repeats the whole text ("stacked copies": the reply grows longer every message). A terminal
# typing cursor means nothing inside a QQ stream, so frames are stripped of it before BOTH the
# monotonic comparison and the send.
_BLOCK_CURSOR_CHARS = frozenset(range(0x2580, 0x25A0))  # ▀▁▂▃▄▅▆▇█▉▊▋▌▍▎▏▐░▒▓


def _resolve_frame_cursor() -> str:
    """The configured cursor to strip; ``QQSTREAM_CURSOR_STRIP`` overrides (empty disables)."""
    override = _os.getenv("QQSTREAM_CURSOR_STRIP")
    if override is not None:
        return override
    try:
        from gateway.config import DEFAULT_STREAMING_CURSOR as default_cursor
    except Exception:  # pragma: no cover — Hermes always ships the constant
        return ""
    return default_cursor or ""


_FRAME_CURSOR = _resolve_frame_cursor()


def _strip_frame_cursor(text: str, cursor: str = "") -> str:
    """Drop the gateway's trailing typing cursor from one frame."""
    if not text:
        return text
    cursor = cursor or _FRAME_CURSOR
    if cursor and text.endswith(cursor):
        return text[: -len(cursor)]
    # Cursor unset/unknown: a trailing block element (optionally after a space) is the typing
    # cursor, not a reply ending.
    if ord(text[-1]) in _BLOCK_CURSOR_CHARS:
        end = len(text) - 1
        if end and text[end - 1] == " ":
            end -= 1
        return text[:end]
    return text

def _strip_progress_overlay(text: str) -> str:
    """Drop the gateway's tool-progress overlay from a non-final frame.

    QQ's stream is one message that may only GROW — repainting what the client already shows is
    refused (404 / 40007). The overlay is written, then dropped as soon as the reply continues, so
    leaving it in the frames would force a repaint (and a second bubble). Only a separator followed
    by a non-blank line counts as an overlay, so a reply's own ``---`` rule — surrounded by blank
    lines, as markdown requires — survives untouched.
    """
    cut = text.rfind(_PROGRESS_OVERLAY_SEPARATOR)
    if cut <= 0:
        return text  # no separator, or the frame is the overlay alone
    after = text[cut + len(_PROGRESS_OVERLAY_SEPARATOR):cut + len(_PROGRESS_OVERLAY_SEPARATOR) + 1]
    return text if after == "\n" else text[:cut]


def _stream_head(text: str) -> str:
    """Longest newline-aligned prefix of *text* for a closing frame — never below half the budget,
    so a single long line cannot collapse the head to nothing."""
    cut = text.rfind("\n")
    return text[:cut] if cut >= len(text) // 2 else text


def _undisplayed_tail(shown: str, incoming: str, *, min_overlap: int = 4) -> str:
    """The part of *incoming* the client has not displayed yet.

    ``shown`` is what the live message already displays; ``incoming`` is the text the consumer now
    wants shown. The overlap is the LONGEST suffix of ``shown`` that also prefixes ``incoming``, so
    the appended text is the smallest one that completes the reply. Cutting on a spurious short
    match cannot lose content — the overlap is a suffix of ``shown``, so the result still carries
    every character of ``incoming`` — which is why the floor is small: a short reply prefixed by a
    short interim line overlaps by only a few characters.
    """
    if not shown or incoming.startswith(shown):
        return incoming
    for start in range(min(len(shown), 4000)):
        candidate = shown[start:]
        if len(candidate) >= min_overlap and incoming.startswith(candidate):
            return incoming[len(candidate):]
    return incoming


@dataclass
class StreamState:
    """One live QQ stream for one chat."""

    openid: str
    msg_seq: int
    msg_id: str = ""
    event_id: str = ""
    stream_msg_id: str = ""
    index: int = 0
    last_text: str = ""
    last_sent_at: float = 0.0
    sent_any: bool = False
    turn_key: str = ""
    chat_id: str = ""


class QQStreamMixin:
    """Native streaming for a QQ adapter — C2C only, plain sends everywhere else."""

    SUPPORTS_NATIVE_STREAMING = True

    # ── capability probe ──

    def supports_native_streaming(
        self, chat_type: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """True only for C2C: the QQ open platform exposes ``stream_messages`` for private chats
        alone (group/channel replies stay one-shot). C2C sessions arrive as ``chat_type="dm"``;
        ``"c2c"`` is accepted for direct callers."""
        del metadata
        if not self._stream_capable():
            return False
        return str(chat_type or "").strip().lower() in {"dm", "c2c"}

    def _stream_capable(self) -> bool:
        """Streaming needs markdown (the only content type the endpoint accepts) and the
        ``extra.streaming`` switch; a chat that already answered "not supported" is never retried."""
        return bool(getattr(self, "_markdown_support", False)
                    and getattr(self, "_streaming_enabled", True))

    # ── frame transport ──

    async def send_stream_frame(
        self, text: str, *, finalize: bool = False, chat_id: Optional[str] = None,
        reply_to: Optional[str] = None, **kwargs: Any,
    ) -> bool:
        """Emit one frame of the chat's stream; ``finalize`` sends the closing DONE frame.

        Returns False whenever streaming is unusable for this chat (the consumer then falls back to
        a normal send), True once the frame — or a deliberately skipped duplicate — is through.
        """
        chat_id = str(chat_id or kwargs.get("chat_id") or "")
        states = getattr(self, "_stream_states", None)
        if not chat_id or not self._stream_capable() or states is None:
            return False
        if chat_id in getattr(self, "_stream_disabled_chats", ()):
            return False

        formatted = _strip_frame_cursor(self.format_message(text or ""))
        over_budget = len(formatted) > self.MAX_MESSAGE_LENGTH
        # A frame carrying reply text drops the tool-progress overlay: the overlay is cleared the
        # moment the reply continues, and that repaint is exactly what QQ refuses. The closing frame
        # is passed through verbatim so the client's final text matches what the gateway records.
        body_text = formatted[: self.MAX_MESSAGE_LENGTH]
        if not finalize:
            body_text = _strip_progress_overlay(body_text)[: self.MAX_MESSAGE_LENGTH]
        if not body_text and not finalize:
            # The consumer seeds every turn with an empty frame to show a typing bubble; a QQ stream
            # has no pre-content indicator, so the seed neither opens nor sends anything.
            return True

        turn_key = str(kwargs.get("turn_id") or "")
        state = states.get(chat_id)
        if state is not None and turn_key and state.turn_key != turn_key:
            state = None  # new turn: new msg_seq/index/stream_msg_id
        if state is None:
            if finalize:
                # Nothing was streamed for this reply (short answer that arrived on the closing
                # tick): a lone closing frame renders worse than a plain message, so defer to the
                # gateway's normal send.
                return False
            state = self._open_stream_state(chat_id, turn_key, reply_to)
            if state is None:
                return False
        elif not body_text:
            if not state.sent_any:
                # Nothing was ever shown — close silently and let the normal send deliver.
                states.pop(chat_id, None)
                return False
            body_text = state.last_text  # closing frame must carry content to render as DONE

        if not finalize and not body_text.strip():
            # A whitespace-only frame (the newline the model emits before its first token, plus the
            # gateway's cursor) would create an EMPTY message on the client — QQ answers that with
            # "该类型消息不支持查看". It carries nothing, so it is never sent.
            return True
        if not finalize and not state.sent_any and len(body_text.strip()) < _MIN_OPEN_CHARS:
            # Hold the stream shut until there is a readable amount of text: the opening frame is
            # what creates the message client-side. A reply too short to ever reach the threshold
            # skips streaming and is delivered the normal way (it lands instantly regardless).
            return True

        if not finalize and state.sent_any and (
            time.monotonic() - state.last_sent_at < MIN_FRAME_INTERVAL_SECONDS
        ):
            return True  # throttled: cumulative frames make intermediate drops lossless
        if not finalize and state.sent_any and body_text == state.last_text:
            return True

        if over_budget and finalize:
            # One stream is ONE message that later frames overwrite in place, so the tail past the
            # per-message budget has nowhere to go. Close the stream on the head (the client stops
            # spinning "generating") and report the frame as failed: the gateway then rolls back its
            # delivery flags and sends the complete reply through the normal multi-chunk path.
            # Truncating quietly would drop everything past the budget.
            logger.info(
                "[%s] Reply exceeds the C2C stream budget (%d > %d chars) — closing the stream and "
                "delivering the full text as a normal message",
                self._log_tag, len(formatted), self.MAX_MESSAGE_LENGTH)
            await self._post_stream_frame(state, _stream_head(body_text), finalize=True)
            states.pop(chat_id, None)
            return False

        if state.sent_any and body_text and not body_text.startswith(state.last_text):
            # QQ lets a stream GROW but never repaint (404 / 40007 已经提交的消息内容不可修改), so a frame
            # that does not extend what the client shows cannot be sent as-is. Hermes's frames stop
            # being monotonic when the consumer adopts the authoritative final
            # (``GatewayStreamConsumer._adopt_final_text`` REPLACES the accumulated text, dropping the
            # interim text it streamed before the reply): the client already displays
            # ``state.last_text``, which is that interim text plus a prefix of the final. Appending
            # only the part of the new text the client has not seen keeps the whole reply in ONE
            # message — sealing and reopening a fresh one instead repeats everything already on
            # screen, so the reply arrives twice, the second copy longer than the first.
            tail = _undisplayed_tail(state.last_text, body_text)
            grown = state.last_text + tail
            if len(grown) > self.MAX_MESSAGE_LENGTH:
                # Nothing left to grow into: close the stream on what the client shows and hand the
                # complete reply back to the gateway's normal send path.
                logger.info(
                    "[%s] Diverged frame no longer fits the C2C stream budget (%d > %d chars) — "
                    "closing the stream and delivering the full text as a normal message",
                    self._log_tag, len(grown), self.MAX_MESSAGE_LENGTH)
                await self._post_stream_frame(state, _stream_head(state.last_text), finalize=True)
                states.pop(chat_id, None)
                return False
            logger.debug("[%s] Frame diverged from the streamed text — appending %d new char(s) "
                         "instead of repainting", self._log_tag, len(tail))
            body_text = grown

        if not await self._post_stream_frame(state, body_text, finalize=finalize):
            states.pop(chat_id, None)
            return False
        state.last_text, state.last_sent_at, state.sent_any = body_text, time.monotonic(), True
        if finalize:
            states.pop(chat_id, None)  # the next turn opens a fresh stream
        return True

    def _open_stream_state(
        self, chat_id: str, turn_key: str, reply_to: Optional[str],
    ) -> Optional[StreamState]:
        """Start a stream for this turn; None when there is no passive ``msg_id`` to open with (QQ
        only accepts C2C streams as replies to a recent inbound message)."""
        msg_id = str(reply_to or (getattr(self, "_last_msg_id", {}) or {}).get(chat_id) or "")
        if not msg_id:
            logger.debug("[%s] No inbound msg_id for %s — skipping native streaming", self._log_tag, chat_id)
            return None
        state = StreamState(
            openid=chat_id, chat_id=chat_id, msg_seq=random.randrange(1, 65536),
            msg_id=msg_id, event_id=msg_id, turn_key=turn_key)
        self._stream_states[chat_id] = state
        return state

    async def _post_stream_frame(self, state: StreamState, text: str, *, finalize: bool) -> bool:
        """POST one frame with backoff on rate limits; a permanent rejection disables streaming for
        this chat (the chat keeps working over the normal send path)."""
        opened_stream = state.sent_any
        body: Dict[str, Any] = {
            "input_mode": STREAM_INPUT_MODE_REPLACE,
            "input_state": STREAM_STATE_DONE if finalize else STREAM_STATE_GENERATING,
            "content_type": STREAM_CONTENT_TYPE_MARKDOWN,
            "content_raw": text,
            "event_id": state.event_id,
            "msg_id": state.msg_id,
            "msg_seq": state.msg_seq,
            "index": state.index,
        }
        if state.stream_msg_id:
            body["stream_msg_id"] = state.stream_msg_id

        for attempt in range(_RETRY_ATTEMPTS):
            state.index += 1  # every frame — retries included — takes a fresh index
            body["index"] = state.index
            try:
                data = await self._api_request(
                    "POST", STREAM_PATH_TEMPLATE.format(openid=state.openid), body)
            except Exception as exc:  # noqa: BLE001 — QQ errors are all RuntimeError-ish
                if self._is_rate_limited(exc) and attempt + 1 < _RETRY_ATTEMPTS:
                    delay = _RETRY_BASE_DELAY * (2 ** attempt)
                    logger.debug(
                        "[%s] Stream frame rate limited, retry %d/%d in %.1fs: %s",
                        self._log_tag, attempt + 1, _RETRY_ATTEMPTS, delay, exc)
                    await asyncio.sleep(delay)
                    continue
                if self._is_permanent_stream_error(exc):
                    self._stream_disabled_chats.add(state.chat_id)
                    logger.info(
                        "[%s] QQ streaming unavailable for this chat, using one-shot replies: %s",
                        self._log_tag, exc)
                else:
                    logger.warning("[%s] Stream frame failed: %s", self._log_tag, exc)
                return False
            if not state.stream_msg_id and isinstance(data, dict):
                stream_msg_id = str(data.get("id") or "")
                if stream_msg_id:
                    state.stream_msg_id = stream_msg_id
            if not opened_stream:
                logger.info("[%s] C2C stream opened for %s (msg_seq=%d, %d chars: %r)",
                            self._log_tag, state.chat_id, state.msg_seq, len(text),
                            " ".join(text[:70].split()))
            elif finalize:
                logger.info("[%s] C2C stream closed after %d frame(s) (%d chars)",
                            self._log_tag, state.index, len(text))
            return True
        return False

    # ── error classification ──

    @staticmethod
    def _is_rate_limited(exc: Exception) -> bool:
        status = int(getattr(exc, "status", 0) or 0)
        code = int(getattr(exc, "code", 0) or 0)
        if status == _RATE_LIMIT_HTTP_STATUS or code in _RATE_LIMIT_BIZ_CODES:
            return True
        message = str(exc).lower()
        return "429" in message or "rate limit" in message or "频率" in message

    @staticmethod
    def _is_permanent_stream_error(exc: Exception) -> bool:
        status = int(getattr(exc, "status", 0) or 0)
        code = int(getattr(exc, "code", 0) or 0)
        if code in _TRANSIENT_STREAM_BIZ_CODES:
            return False  # recoverable protocol state (index / repaint / dedup)
        if status in _PERMANENT_HTTP_STATUSES or code in _PERMANENT_BIZ_CODES:
            return True
        return any(marker in str(exc) for marker in _PERMANENT_MESSAGES)
