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
    _undisplayed_tail,
)

CHAT = "OPENID_CHAT"
MSG_ID = "MSG_IN"


@pytest.fixture(autouse=True)
def _prewarmed_turn(monkeypatch):
    """A stream opens on the SECOND frame of a turn (the first is held in case it is the transient
    tool-progress overlay — see QQStreamMixin.send_stream_frame). These tests exercise frames from an
    already-open turn, so the per-turn hold is pre-seeded; the hold itself is covered by
    TestOpening.test_opening_frame_is_held_until_it_is_not_an_overlay."""
    monkeypatch.setattr(StreamingQQAdapter, "_pending_opening_frames",
                        lambda self: {CHAT: ""})


@pytest.fixture(autouse=True)
def _lower_open_threshold(monkeypatch):
    """Two-character frames are the norm in this file; the real opening threshold is a platform
    artefact, covered by test_stream_opens_only_once_there_is_readable_text."""
    import hermes_qqbot_streaming.streaming as streaming_module
    monkeypatch.setattr(streaming_module, "_MIN_OPEN_CHARS", 1)


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
    async def test_whitespace_only_frame_never_opens_a_stream(self):
        """An empty message is what QQ answers with "该类型消息不支持查看"."""
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        assert await adapter.send_stream_frame("\n ▉", chat_id=CHAT, reply_to=MSG_ID) is True
        assert recorder.calls == []

    @pytest.mark.asyncio
    async def test_stream_opens_only_once_there_is_readable_text(self, monkeypatch):
        import hermes_qqbot_streaming.streaming as streaming_module
        monkeypatch.setattr(streaming_module, "_MIN_OPEN_CHARS", 12)

        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        # The opening frame creates the message client-side: a couple of characters makes QQ flash
        # an unrenderable bubble, so the frame is held back.
        assert await adapter.send_stream_frame("好 ▉", chat_id=CHAT, reply_to=MSG_ID) is True
        assert recorder.calls == []

        # Once a readable amount has arrived the stream opens — on that frame, exactly once.
        assert await adapter.send_stream_frame("好，我这就去翻日志看看结果 ▉", chat_id=CHAT,
                                               reply_to=MSG_ID) is True
        assert [b["content_raw"] for b in recorder.bodies] == ["好，我这就去翻日志看看结果"]

    @pytest.mark.asyncio
    async def test_diverged_frame_grows_the_live_message_instead_of_reopening(self):
        """A frame that does not extend the streamed text is APPENDED, never used to open a new
        message: sealing and reopening repeats everything the client already shows, so the reply
        arrives twice (the second copy longer than the first)."""
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        # What the client shows: interim text plus a prefix of the reply.
        await adapter.send_stream_frame("先看一眼日志：最终答案的第一句", chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        # The consumer adopts the authoritative final, which DROPS that interim text.
        assert await adapter.send_stream_frame("最终答案的第一句，还有第二句", chat_id=CHAT,
                                               reply_to=MSG_ID) is True

        bodies = recorder.bodies
        assert len(bodies) == 2                       # still ONE message
        assert len({b["msg_seq"] for b in bodies}) == 1
        assert bodies[1]["content_raw"] == "先看一眼日志：最终答案的第一句，还有第二句"
        # Only the never-displayed part was appended; the shared prefix is not repeated.
        assert bodies[1]["content_raw"].count("最终答案的第一句") == 1

    @pytest.mark.asyncio
    async def test_diverged_finalize_closes_the_same_message(self):
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("查一下：答案开头", chat_id=CHAT, reply_to=MSG_ID)
        adapter._stream_states[CHAT].last_sent_at = 0.0
        assert await adapter.send_stream_frame("答案开头，以及结尾", chat_id=CHAT, reply_to=MSG_ID,
                                               finalize=True) is True

        bodies = recorder.bodies
        assert [b["input_state"] for b in bodies] == [STREAM_STATE_GENERATING, STREAM_STATE_DONE]
        assert bodies[-1]["content_raw"] == "查一下：答案开头，以及结尾"
        assert len({b["msg_seq"] for b in bodies}) == 1
        assert CHAT not in adapter._stream_states  # sealed: the next turn opens a fresh stream

    def test_undisplayed_tail_keeps_only_what_was_never_shown(self):
        assert _undisplayed_tail("评述：答案开头", "答案开头，以及结尾") == "，以及结尾"
        assert _undisplayed_tail("", "全部") == "全部"
        # A text that already extends what is shown needs no tail computed at all.
        assert _undisplayed_tail("已显示", "已显示更多") == "已显示更多"
        # An unrelated rewrite is appended whole rather than truncated to a coincidence.
        assert _undisplayed_tail("完全不同的内容", "另一段话") == "另一段话"


# --------------------------------------------------------------------------- #
# the opening frame
# --------------------------------------------------------------------------- #

class TestOpening:
    """The first frame of a turn is what CREATES the message on the client."""

    @pytest.fixture(autouse=True)
    def _no_prewarm(self, monkeypatch):
        """Defeat the module fixture: these tests exercise the per-turn hold itself, so the mixin's
        real (persistent-per-adapter) store is used."""
        from hermes_qqbot_streaming.streaming import QQStreamMixin
        monkeypatch.setattr(StreamingQQAdapter, "_pending_opening_frames",
                            QQStreamMixin._pending_opening_frames)

    @pytest.mark.asyncio
    async def test_opening_frame_is_held_until_it_is_not_an_overlay(self):
        """A tool that runs before the reply emits text makes the first frame the tool-progress line
        ALONE — a transient overlay Hermes clears the moment real text arrives. QQ stamps the
        message's type at creation, so an overlay-only opening leaves the client flashing
        "该类型消息不支持查看" until real text lands."""
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        # Frame 1: the tool-progress overlay alone — held, nothing created on the client.
        assert await adapter.send_stream_frame('💻 Running L="ls" ▉', chat_id=CHAT,
                                               reply_to=MSG_ID) is True
        assert recorder.calls == []

        # Frame 2: the reply text arrives and the overlay moves below the rule.
        assert await adapter.send_stream_frame('好的，我这就去查日志\n\n---\n💻 Running L="ls" ▉',
                                               chat_id=CHAT, reply_to=MSG_ID) is True
        bodies = recorder.bodies
        assert [b["content_raw"] for b in bodies] == ["好的，我这就去查日志"]
        assert len({b["msg_seq"] for b in bodies}) == 1

    @pytest.mark.asyncio
    async def test_held_frame_alone_defers_to_the_normal_send(self):
        """A reply that never produces a second frame was too short to stream anyway."""
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("短消息", chat_id=CHAT, reply_to=MSG_ID)
        assert await adapter.send_stream_frame("短消息", chat_id=CHAT, reply_to=MSG_ID,
                                               finalize=True) is False
        assert recorder.calls == []

    @pytest.mark.asyncio
    async def test_a_second_frame_that_extends_the_first_opens_on_it(self):
        """With no tool in play both frames are reply text; the second is cumulative."""
        adapter = _adapter()
        recorder = _Recorder()
        adapter._api_request = recorder

        await adapter.send_stream_frame("答案的第一行", chat_id=CHAT, reply_to=MSG_ID)
        assert await adapter.send_stream_frame("答案的第一行，还有第二行", chat_id=CHAT,
                                               reply_to=MSG_ID) is True
        assert [b["content_raw"] for b in recorder.bodies] == ["答案的第一行，还有第二行"]


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
