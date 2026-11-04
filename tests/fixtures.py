"""A recorded-style Anthropic Messages SSE stream and the message it reassembles to."""

import json


def sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def delta(index: int, kind: str, **fields) -> str:
    return sse("content_block_delta", {"type": "content_block_delta", "index": index, "delta": {"type": kind, **fields}})


STREAM = "".join([
    sse("message_start", {"type": "message_start", "message": {
        "id": "msg_01", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
        "content": [], "stop_reason": None, "stop_sequence": None,
        "usage": {"input_tokens": 12, "cache_creation_input_tokens": 0,
                  "cache_read_input_tokens": 3400, "output_tokens": 1}}}),
    sse("ping", {"type": "ping"}),
    sse("content_block_start", {"type": "content_block_start", "index": 0,
                                "content_block": {"type": "thinking", "thinking": "", "signature": ""}}),
    delta(0, "thinking_delta", thinking="The user wants "),
    delta(0, "thinking_delta", thinking="a file read. ✓"),
    delta(0, "signature_delta", signature="sig123"),
    sse("content_block_stop", {"type": "content_block_stop", "index": 0}),
    sse("content_block_start", {"type": "content_block_start", "index": 1,
                                "content_block": {"type": "text", "text": ""}}),
    delta(1, "text_delta", text="Let me read "),
    delta(1, "text_delta", text="proxy.py — שלום 🙂"),
    sse("content_block_stop", {"type": "content_block_stop", "index": 1}),
    sse("ping", {"type": "ping"}),
    sse("content_block_start", {"type": "content_block_start", "index": 2,
                                "content_block": {"type": "tool_use", "id": "toolu_01", "name": "Read", "input": {}}}),
    delta(2, "input_json_delta", partial_json=""),
    delta(2, "input_json_delta", partial_json='{"file_'),
    delta(2, "input_json_delta", partial_json='path": "/tmp/pró'),
    delta(2, "input_json_delta", partial_json='xy.py"}'),
    sse("content_block_stop", {"type": "content_block_stop", "index": 2}),
    sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": None},
                          "usage": {"output_tokens": 87}}),
    sse("message_stop", {"type": "message_stop"}),
])

EXPECTED = {
    "id": "msg_01", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
    "content": [
        {"type": "thinking", "thinking": "The user wants a file read. ✓", "signature": "sig123"},
        {"type": "text", "text": "Let me read proxy.py — שלום 🙂"},
        {"type": "tool_use", "id": "toolu_01", "name": "Read", "input": {"file_path": "/tmp/próxy.py"}},
    ],
    "stop_reason": "tool_use", "stop_sequence": None,
    "usage": {"input_tokens": 12, "cache_creation_input_tokens": 0,
              "cache_read_input_tokens": 3400, "output_tokens": 87},
}

ERROR_STREAM = "".join([
    sse("message_start", {"type": "message_start", "message": {
        "id": "msg_02", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
        "content": [], "usage": {"input_tokens": 5, "output_tokens": 1}}}),
    sse("content_block_start", {"type": "content_block_start", "index": 0,
                                "content_block": {"type": "text", "text": ""}}),
    delta(0, "text_delta", text="Partial"),
    sse("error", {"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}),
])


def stream_for(message: dict) -> str:
    """An SSE stream that reassembles to `message` (text, thinking and tool_use blocks)."""
    head = {k: v for k, v in message.items() if k not in ("content", "stop_reason", "usage")}
    usage = message.get("usage") or {"input_tokens": 10, "output_tokens": 5}
    out = [sse("message_start", {"type": "message_start", "message": {
        "type": "message", "role": "assistant", "content": [], **head,
        "usage": {**usage, "output_tokens": 1}}})]
    for i, block in enumerate(message.get("content") or []):
        kind = block["type"]
        start = {"type": kind, "id": block.get("id"), "name": block.get("name"), "input": {}} if kind == "tool_use" \
            else {"type": kind, kind: ""}
        out.append(sse("content_block_start", {"type": "content_block_start", "index": i, "content_block": start}))
        if kind == "text":
            out.append(delta(i, "text_delta", text=block["text"]))
        elif kind == "thinking":
            out.append(delta(i, "thinking_delta", thinking=block["thinking"]))
        elif kind == "tool_use":
            out.append(delta(i, "input_json_delta", partial_json=json.dumps(block["input"])))
        out.append(sse("content_block_stop", {"type": "content_block_stop", "index": i}))
    out.append(sse("message_delta", {"type": "message_delta",
                                     "delta": {"stop_reason": message.get("stop_reason", "end_turn")},
                                     "usage": {"output_tokens": usage.get("output_tokens", 5)}}))
    out.append(sse("message_stop", {"type": "message_stop"}))
    return "".join(out)


SSE_HEADERS = [("content-type", "text/event-stream; charset=utf-8"), ("request-id", "req_1")]


def run(store, body: dict, reply: dict | None = None, headers=(("authorization", "Bearer sk-ant-oat01-SECRET-TOKEN-xyz"),)):
    """Drive one messages exchange through a Store; returns its id."""
    xid = store.begin("POST", "/v1/messages?beta=true", list(headers), json.dumps(body).encode())
    store.response(xid, 200, "OK", SSE_HEADERS)
    store.feed(xid, stream_for(reply or {"content": [{"type": "text", "text": "ok"}]}).encode())
    store.end(xid)
    return xid
