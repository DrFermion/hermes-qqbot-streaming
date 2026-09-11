"""Table-preserving long-message chunking for QQ.

Mirrors ``BasePlatformAdapter.truncate_message`` — fence-aware splitting that closes and reopens an
orphaned ``` fence between chunks and tags multi-chunk output with ``(1/3)`` — and adds one rule:
a GFM table is never cut down the middle.

A table only renders when its rows arrive in one message; splitting between two ``|…|`` rows strands
the tail as loose text (the header and separator ride the previous message). QQ's own SDK chunker
makes the same call. The rule is bounded: a table too long for one message has to break somewhere,
and a table that starts at a chunk head is left alone (moving the boundary there cannot shrink the
chunk and the caller would loop).
"""

from __future__ import annotations

import bisect
from typing import Callable, List, Optional

_FENCE_CLOSE = "\n```"
_INDICATOR_RESERVE = 10  # room for " (XX/XX)"


def _is_pipe_row(line: str) -> bool:
    stripped = line.strip()
    return len(stripped) >= 2 and stripped.startswith("|") and stripped.endswith("|")


def table_safe_split(text: str, split_at: int, budget: int, len_fn: Optional[Callable[[str], int]] = None) -> int:
    """Nudge a chunk boundary out of a GFM table (see the module docstring)."""
    _len = len_fn or len
    if split_at <= 0 or split_at >= len(text) or budget <= 0:
        return split_at
    lines = text.split("\n")
    starts: List[int] = []
    pos = 0
    for line in lines:
        starts.append(pos)
        pos += len(line) + 1
    # The line the next chunk begins with — splits strip the boundary newline/whitespace.
    tail = split_at
    while tail < len(text) and text[tail] == "\n":
        tail += 1
    if tail >= len(text):
        return split_at
    row = bisect.bisect_right(starts, tail) - 1
    if not _is_pipe_row(lines[row]):
        return split_at
    first = row
    while first > 0 and _is_pipe_row(lines[first - 1]):
        first -= 1
    if first == 0 or first == row:
        return split_at
    last = row
    while last + 1 < len(lines) and _is_pipe_row(lines[last + 1]):
        last += 1
    table = text[starts[first]:starts[last] + len(lines[last])]
    if _len(table) > budget:
        return split_at
    return starts[first]


def table_safe_truncate(content: str, max_length: int = 4096,
                        len_fn: Optional[Callable[[str], int]] = None) -> List[str]:
    """Split a long message, keeping code fences balanced and GFM tables whole."""
    from gateway.platforms.base import _custom_unit_to_cp
    from gateway.platforms.helpers import fence_state_after

    _len = len_fn or len
    if _len(content) <= max_length:
        return [content]
    chunks: List[str] = []
    remaining = content
    carry_lang: Optional[str] = None  # language tag ("" ok) when the previous chunk ended mid-fence
    while remaining:
        prefix = f"```{carry_lang}\n" if carry_lang is not None else ""
        headroom = max_length - _INDICATOR_RESERVE - _len(prefix) - _len(_FENCE_CLOSE)
        if headroom < 1:
            headroom = max(1, max_length // 2)
        if _len(prefix) + _len(remaining) <= max_length - _INDICATOR_RESERVE:
            final_chunk = prefix + remaining
            if carry_lang is not None and fence_state_after(remaining, True, carry_lang)[0]:
                final_chunk += _FENCE_CLOSE
            chunks.append(final_chunk)
            break
        _cp_limit = (_custom_unit_to_cp(remaining, headroom, _len) if _len is not len else headroom)
        region = remaining[:_cp_limit]
        split_at = region.rfind("\n")
        if split_at < _cp_limit // 2:
            split_at = region.rfind(" ")
        if split_at < 1:
            split_at = max(1, _cp_limit)
        split_at = table_safe_split(remaining, split_at, max_length - _INDICATOR_RESERVE, _len)
        # Don't split inside an inline code span: an unpaired backtick breaks MarkdownV2.
        candidate = remaining[:split_at]
        backtick_count = candidate.count("`") - candidate.count("\\`")
        if backtick_count % 2 == 1:
            last_bt = candidate.rfind("`")
            while last_bt > 0 and candidate[last_bt - 1] == "\\":
                last_bt = candidate.rfind("`", 0, last_bt)
            if last_bt > 0:
                safe_split = max(candidate.rfind(" ", 0, last_bt), candidate.rfind("\n", 0, last_bt))
                if safe_split > _cp_limit // 4:
                    split_at = safe_split
        chunk_body = remaining[:split_at]
        remaining = remaining[split_at:].lstrip()
        full_chunk = prefix + chunk_body
        in_code, lang = fence_state_after(chunk_body, carry_lang is not None, carry_lang or "")
        carry_lang = lang if in_code else None
        chunks.append(full_chunk + _FENCE_CLOSE if in_code else full_chunk)
    if len(chunks) > 1:
        chunks = [f"{chunk} ({i + 1}/{len(chunks)})" for i, chunk in enumerate(chunks)]
    return chunks
