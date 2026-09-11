"""Invariants of the streaming QQ adapter.

These run against a real Hermes installation (they instantiate the built-in ``QQAdapter``
subclass), so run them with the interpreter that has Hermes installed::

    <hermes>/venv/Scripts/python.exe -m pytest tests/ -q

Every rule here was probed against the live QQ open platform; the HTTP/biz codes in the docstrings
are the platform's own answers, not guesses.
"""

from __future__ import annotations

import asyncio
from unittest import mock

import pytest

pytest.importorskip("gateway.platforms.qqbot", reason="needs a Hermes installation")

from gateway.config import PlatformConfig  # noqa: E402

from hermes_qqbot_streaming.adapter import StreamingQQAdapter  # noqa: E402
from hermes_qqbot_streaming.chunking import table_safe_split, table_safe_truncate  # noqa: E402
from hermes_qqbot_streaming.streaming import (  # noqa: E402
    STREAM_STATE_DONE,
    STREAM_STATE_GENERATING,
    _strip_frame_cursor,
    _strip_progress_overlay,
)

CHAT = "OPENID_CHAT"
MSG_ID = "MSG_IN"


def _adapter(**extra):
    adapter = StreamingQQAdapter(
        PlatformConfig(enabled=True, extra={"app_id": "a", "client_secret": "b", **extra}))
    adapter._last_msg_id[CHAT] = MSG_ID
    return adapter


class _Recorder:
    """Stands in for ``_api_request``: records frame bodies, replays scripted results."""

    def __init__(self, responses=None):
        self.calls = []
        self._responses = list(responses or [])

    async def __call__(self, method, path, body=None, timeout=None):
        self.calls.append((method, path, dict(body or {})))
        if self._responses:
            item = self._responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return {"id": f"stream-{len(self.calls)}"}

    @property
    def bodies(self):
        return [body for _m, _p, body in self.calls]


# --------------------------------------------------------------------------- #
# capability
# --------------------------------------------------------------------------- #

class TestCapability:
    def test_c2c_streams_and_group_does_not(self):
        adapter = _adapter()
        assert adapter.SUPPORTS_NATIVE_STREAMING is True
        assert adapter.supports_native_streaming(chat_type="dm") is True
        assert adapter.supports_native_streaming(chat_type="c2c") is True
        assert adapter.supports_native_streaming(chat_type="group") is False
        assert adapter.supports_native_streaming(chat_type="guild") is False

    def test_escape_hatch_and_markdown_requirement(self):
        assert _adapter(streaming=False).supports_native_streaming(chat_type="dm") is False
        assert _adapter(markdown_support=False).supports_native_streaming(chat_type="dm") is False


# --------------------------------------------------------------------------- #
# frame protocol
# --------------------------------------------------------------------------- #

class TestFrames:
    @pytest.mark.asyncio
    async def test_one_message_many_frames(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("你好", chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        await adapter.send_stream_frame("你好，主人", chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        await adapter.send_stream_frame("你好，主人！", chat_id=CHAT, reply_to=MSG_ID, finalize=True)

        first, second, last = recorder.bodies
        assert [b["index"] for b in (first, second, last)] == [1, 2, 3]
        assert len({b["msg_seq"] for b in (first, second, last)}) == 1
        assert [b["input_state"] for b in (first, second, last)] == [
            STREAM_STATE_GENERATING, STREAM_STATE_GENERATING, STREAM_STATE_DONE]
        assert first["input_mode"] == "replace" and first["content_type"] == "markdown"
        assert first["msg_id"] == MSG_ID and first["event_id"] == MSG_ID
        assert "stream_msg_id" not in first      # the first response id anchors the rest
        assert second["stream_msg_id"] == "stream-1"
        assert recorder.calls[0][1] == f"/v2/users/{CHAT}/stream_messages"
        assert CHAT not in adapter._stream_states  # sealed: the next turn opens a fresh stream

    @pytest.mark.asyncio
    async def test_throttled_frame_is_skipped_but_reported_ok(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("一", chat_id=CHAT, reply_to=MSG_ID)
        assert await adapter.send_stream_frame("一二", chat_id=CHAT, reply_to=MSG_ID) is True
        assert len(recorder.calls) == 1  # cumulative frames: a dropped tick loses nothing

    @pytest.mark.asyncio
    async def test_no_inbound_msg_id_means_no_stream(self):
        adapter = _adapter()
        adapter._last_msg_id.clear()
        adapter._api_request = _Recorder()

        assert await adapter.send_stream_frame("嗨", chat_id=CHAT) is False


# --------------------------------------------------------------------------- #
# monotonic frames (404/40007 "已经提交的消息内容不可修改")
# --------------------------------------------------------------------------- #

class TestMonotonicFrames:
    def test_progress_overlay_is_stripped(self):
        assert _strip_progress_overlay("答案\n\n---\n🔧 terminal: \"ls\"") == "答案"
        assert _strip_progress_overlay("上节\n\n---\n\n下节") == "上节\n\n---\n\n下节"  # a real rule
        assert _strip_progress_overlay("🔧 terminal: \"ls\"") == "🔧 terminal: \"ls\""

    @pytest.mark.asyncio
    async def test_overlay_never_rides_a_frame(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("我来查一下\n\n---\n🔧 terminal: \"ls\"", chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        await adapter.send_stream_frame("我来查一下，结果如下", chat_id=CHAT, reply_to=MSG_ID)

        assert [b["content_raw"] for b in recorder.bodies] == ["我来查一下", "我来查一下，结果如下"]
        assert len({b["msg_seq"] for b in recorder.bodies}) == 1  # still ONE message

    def test_typing_cursor_is_stripped(self):
        assert _strip_frame_cursor("你好 ▉") == "你好"
        assert _strip_frame_cursor("你好 ▉", " ▉") == "你好"
        assert _strip_frame_cursor("你好") == "你好"
        # An unknown/unset cursor still loses its trailing block element.
        assert _strip_frame_cursor("你好 ▌", "") == "你好"

    @pytest.mark.asyncio
    async def test_gateway_typing_cursor_never_reaches_the_stream(self):
        """The gateway appends ``streaming.cursor`` (default " ▉") to every INTERIM native frame.

        Because the suffix sits at the end of each frame and the text only differs before it, a
        frame kept verbatim is not a prefix of its successor — QQ answers the repaint with
        404 / 40007 and the mixin seals + reopens a fresh message per frame, each carrying the
        whole reply again ("stacked copies", the reply growing message by message). The cursor is
        a terminal affordance with no meaning in a QQ stream, so it is stripped.
        """
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        frames = ["你好 ▉", "你好，主人 ▉", "你好，主人！我在查日志 ▉"]
        # The bare cursor alone breaks prefix-stability for every frame after the first.
        assert [frame.startswith(frames[i - 1]) for i, frame in enumerate(frames)][1:] == [False, False]

        for frame in frames:
            await adapter.send_stream_frame(frame, chat_id=CHAT, reply_to=MSG_ID)
            adapter._stream_states[CHAT].last_sent_at = 0.0

        bodies = recorder.bodies
        assert [b["content_raw"] for b in bodies] == ["你好", "你好，主人", "你好，主人！我在查日志"]
        assert "▉" not in "".join(b["content_raw"] for b in bodies)
        assert [b["index"] for b in bodies] == [1, 2, 3]
        assert len({b["msg_seq"] for b in bodies}) == 1  # ONE message, no seal-and-reopen

    @pytest.mark.asyncio
    async def test_diverged_frame_seals_and_opens_a_fresh_stream(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        with mock.patch("hermes_qqbot_streaming.streaming.random.randrange", side_effect=[11, 22]):
            await adapter.send_stream_frame("答案第一部分", chat_id=CHAT, reply_to=MSG_ID)
            adapter._stream_states[CHAT].last_sent_at = 0.0
            assert await adapter.send_stream_frame("完全不同的另一段", chat_id=CHAT, reply_to=MSG_ID) is True

        first, seal, fresh = recorder.bodies
        assert first["msg_seq"] == seal["msg_seq"] == 11
        assert seal["input_state"] == STREAM_STATE_DONE and seal["content_raw"] == "答案第一部分"
        assert fresh["msg_seq"] == 22 and fresh["index"] == 1
        assert "stream_msg_id" not in fresh

    @pytest.mark.asyncio
    async def test_diverged_finalize_pushes_content_then_seals(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        with mock.patch("hermes_qqbot_streaming.streaming.random.randrange", side_effect=[31, 32]):
            await adapter.send_stream_frame("开头", chat_id=CHAT, reply_to=MSG_ID)
            adapter._stream_states[CHAT].last_sent_at = 0.0
            assert await adapter.send_stream_frame("改头换面", chat_id=CHAT, reply_to=MSG_ID, finalize=True) is True

        tail = recorder.bodies[-2:]
        assert [b["input_state"] for b in tail] == [STREAM_STATE_GENERATING, STREAM_STATE_DONE]
        assert tail[0]["content_raw"] == tail[1]["content_raw"] == "改头换面"
        assert len({b["msg_seq"] for b in tail}) == 1


# --------------------------------------------------------------------------- #
# failures
# --------------------------------------------------------------------------- #

class TestFailures:
    @pytest.mark.asyncio
    async def test_rate_limit_retries_with_a_fresh_index(self):
        from hermes_qqbot_streaming.adapter import QQApiError

        adapter = _adapter()
        recorder = _Recorder([QQApiError("busy", status=429), {"id": "s1"}])
        adapter._api_request = recorder

        with mock.patch("hermes_qqbot_streaming.streaming.asyncio.sleep", new=mock.AsyncMock()):
            assert await adapter.send_stream_frame("嗨", chat_id=CHAT, reply_to=MSG_ID) is True
        assert [b["index"] for b in recorder.bodies] == [1, 2]

    @pytest.mark.asyncio
    async def test_permanent_rejection_blacklists_the_chat(self):
        from hermes_qqbot_streaming.adapter import QQApiError

        adapter = _adapter()
        adapter._api_request = _Recorder([QQApiError("不支持的调用", status=404, code=11253)])

        assert await adapter.send_stream_frame("嗨", chat_id=CHAT, reply_to=MSG_ID) is False
        assert CHAT in adapter._stream_disabled_chats

    @pytest.mark.asyncio
    async def test_protocol_state_error_does_not_blacklist_the_chat(self):
        """40007 is recoverable — the next turn must still stream."""
        from hermes_qqbot_streaming.adapter import QQApiError

        adapter = _adapter()
        adapter._api_request = _Recorder([QQApiError("已经提交的消息内容不可修改", status=404, code=40007)])

        assert await adapter.send_stream_frame("嗨", chat_id=CHAT, reply_to=MSG_ID) is False
        assert CHAT not in adapter._stream_disabled_chats

    @pytest.mark.asyncio
    async def test_over_budget_finalize_hands_the_tail_back(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder
        long_answer = "段" * (adapter.MAX_MESSAGE_LENGTH + 500)

        await adapter.send_stream_frame("段" * 10, chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        assert await adapter.send_stream_frame(long_answer, chat_id=CHAT, finalize=True) is False
        assert CHAT not in adapter._stream_states
        assert recorder.bodies[-1]["input_state"] == STREAM_STATE_DONE
        assert len(recorder.bodies[-1]["content_raw"]) <= adapter.MAX_MESSAGE_LENGTH


# --------------------------------------------------------------------------- #
# passive-reply window
# --------------------------------------------------------------------------- #

def _body_text(body) -> str:
    """Text payload of a QQ message body (markdown bodies carry it under ``markdown``)."""
    return body.get("content") or (body.get("markdown") or {}).get("content", "")


class TestPassiveReplyFallback:
    def _api(self, adapter, bodies):
        from hermes_qqbot_streaming.adapter import QQApiError

        async def _call(method, path, body=None, timeout=None):
            bodies.append(dict(body or {}))
            if body and body.get("msg_id"):
                raise QQApiError("msg_id 已过期", status=400, code=40034004)
            return {"id": "m1"}

        adapter._api_request = _call

    @pytest.mark.asyncio
    async def test_rejected_passive_reply_is_retried_proactively(self):
        adapter = _adapter()
        bodies = []
        self._api(adapter, bodies)

        result = await adapter._send_c2c_text(CHAT, "回答", reply_to=MSG_ID)

        assert result.success is True
        assert [b.get("msg_id") for b in bodies] == [MSG_ID, None]
        assert all(_body_text(b) == "回答" for b in bodies)

    @pytest.mark.asyncio
    async def test_timeout_does_not_trigger_a_proactive_resend(self):
        adapter = _adapter()

        async def _call(method, path, body=None, timeout=None):
            raise RuntimeError("QQ Bot API timeout [/v2/users/X/messages]")

        adapter._api_request = _call
        with pytest.raises(RuntimeError):
            await adapter._send_c2c_text(CHAT, "回答", reply_to=MSG_ID)


# --------------------------------------------------------------------------- #
# chunking
# --------------------------------------------------------------------------- #

class TestChunking:
    @staticmethod
    def _table(rows: int) -> str:
        return "\n".join(["| 样品 | 角度 |", "| --- | --- |"]
                         + [f"| 650nm a{i} | {20 + i}.4 |" for i in range(rows)])

    def test_table_is_not_split_across_messages(self):
        import re

        table = self._table(30)
        msg = "预热段。" * 125 + "\n" + table + "\n\n以上。"
        chunks = table_safe_truncate(msg, 800)
        assert len(chunks) > 1
        bodies = [re.sub(r" \(\d+/\d+\)$", "", c) for c in chunks]
        holding = [i for i, c in enumerate(bodies) if "| 650nm a0 |" in c]
        assert len(holding) == 1 and table in bodies[holding[0]]
        assert "".join(bodies).replace("\n", "") == msg.replace("\n", "")

    def test_short_message_is_one_chunk(self):
        assert table_safe_truncate("普通回答", 4096) == ["普通回答"]

    def test_oversized_table_still_splits(self):
        text = "正文\n" + self._table(30)
        inside = text.index("| 650nm a5")
        assert table_safe_split(text, inside, budget=50) == inside
