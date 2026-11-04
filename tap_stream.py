"""tap_stream.py: incremental parser for the Anthropic Messages SSE stream.

Pure: no I/O, no threads. Feed it the response bytes exactly as they arrive,
split anywhere (mid-line, mid-UTF-8 sequence), and it

  * reassembles the final message in `self.message` (text, thinking and
    tool_use blocks, stop_reason, usage), and
  * returns a list of normalized live events per call, as (name, payload)
    pairs, for the dashboard:

        message_start  {"message": {...}}                     (metadata, empty content)
        block_start    {"index", "type", "block", ["name", "tool_use_id"]}
        block_delta    {"index", "delta_type": text|thinking|input_json, "text"}
        block_stop     {"index", ["input"]}                   (input only for tool_use)
        message_delta  {"stop_reason", "stop_sequence", "usage"}

An upstream `error` event is recorded in `self.error`; `ping` is ignored.
"""

from __future__ import annotations

import codecs
import copy
import json
from typing import Any

Event = tuple  # (name: str, payload: dict)


class MessageStream:
    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._buf = ""                  # text after the last newline
        self._event = ""                # current SSE `event:` field
        self._data: list[str] = []      # current SSE `data:` lines
        self._partial: dict[int, str] = {}  # tool-input JSON being streamed, by block index
        self.message: dict[str, Any] | None = None
        self.error: dict[str, Any] | None = None
        self.finished = False           # saw message_stop

    # -- input ---------------------------------------------------------------

    def feed(self, chunk: bytes) -> list[Event]:
        return self._text(self._decoder.decode(chunk))

    def close(self) -> list[Event]:
        """End of stream: flush a trailing partial line / undispatched event."""
        out = self._text(self._decoder.decode(b"", final=True))
        if self._buf:
            line, self._buf = self._buf, ""
            out += self._line(line.rstrip("\r"))
        out += self._dispatch()
        return out

    # -- SSE framing -----------------------------------------------------------

    def _text(self, text: str) -> list[Event]:
        if not text:
            return []
        lines = (self._buf + text).split("\n")
        self._buf = lines.pop()  # incomplete last line (often "")
        out: list[Event] = []
        for line in lines:
            out += self._line(line[:-1] if line.endswith("\r") else line)
        return out

    def _line(self, line: str) -> list[Event]:
        if line == "":
            return self._dispatch()
        if line.startswith(":"):
            return []  # SSE comment
        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if field == "event":
            self._event = value
        elif field == "data":
            self._data.append(value)
        return []

    def _dispatch(self) -> list[Event]:
        name, data = self._event, self._data
        self._event, self._data = "", []
        if not data:
            return []
        try:
            payload = json.loads("\n".join(data))
        except ValueError:
            return []
        if not isinstance(payload, dict):
            return []
        return self._handle(payload.get("type") or name, payload)

    # -- Messages API semantics ------------------------------------------------

    def _msg(self) -> dict[str, Any]:
        if self.message is None:
            self.message = {"role": "assistant", "content": []}
        self.message.setdefault("content", [])
        return self.message

    def _block(self, index: int) -> dict[str, Any]:
        content = self._msg()["content"]
        while len(content) <= index:
            content.append(None)
        if content[index] is None:
            content[index] = {}
        return content[index]

    def _handle(self, kind: str, p: dict[str, Any]) -> list[Event]:
        if kind == "message_start":
            msg = dict(p.get("message") or {})
            msg["content"] = list(msg.get("content") or [])
            self.message = msg
            return [("message_start", {"message": copy.deepcopy(msg)})]

        if kind == "content_block_start":
            index = int(p.get("index", 0))
            block = dict(p.get("content_block") or {})
            btype = str(block.get("type", ""))
            content = self._msg()["content"]
            while len(content) <= index:
                content.append(None)
            content[index] = block
            event = {"index": index, "type": btype, "block": copy.deepcopy(block)}
            if btype.endswith("tool_use"):
                self._partial[index] = ""
                event["name"] = block.get("name")
                event["tool_use_id"] = block.get("id")
            return [("block_start", event)]

        if kind == "content_block_delta":
            index = int(p.get("index", 0))
            delta = p.get("delta") or {}
            dtype = delta.get("type")
            block = self._block(index)
            if dtype == "text_delta":
                text = delta.get("text", "")
                block["text"] = block.get("text", "") + text
                return [("block_delta", {"index": index, "delta_type": "text", "text": text})]
            if dtype == "thinking_delta":
                text = delta.get("thinking", "")
                block["thinking"] = block.get("thinking", "") + text
                return [("block_delta", {"index": index, "delta_type": "thinking", "text": text})]
            if dtype == "input_json_delta":
                text = delta.get("partial_json", "")
                self._partial[index] = self._partial.get(index, "") + text
                return [("block_delta", {"index": index, "delta_type": "input_json", "text": text})]
            if dtype == "signature_delta":
                block["signature"] = block.get("signature", "") + delta.get("signature", "")
            elif dtype == "citations_delta":
                block.setdefault("citations", []).append(delta.get("citation"))
            return []

        if kind == "content_block_stop":
            index = int(p.get("index", 0))
            event: dict[str, Any] = {"index": index}
            if index in self._partial:
                raw = self._partial.pop(index)
                try:
                    tool_input = json.loads(raw) if raw.strip() else {}
                except ValueError:
                    tool_input = {"_partial_json": raw}
                self._block(index)["input"] = tool_input
                event["input"] = tool_input
            return [("block_stop", event)]

        if kind == "message_delta":
            msg = self._msg()
            for key, value in (p.get("delta") or {}).items():
                msg[key] = value
            msg["usage"] = {**(msg.get("usage") or {}), **(p.get("usage") or {})}
            return [("message_delta", {
                "stop_reason": msg.get("stop_reason"),
                "stop_sequence": msg.get("stop_sequence"),
                "usage": dict(msg["usage"]),
            })]

        if kind == "message_stop":
            self.finished = True
        elif kind == "error":
            err = p.get("error")
            self.error = err if isinstance(err, dict) else {"type": "error", "message": str(err or p)}
        # ping and unknown event types: nothing to do
        return []
