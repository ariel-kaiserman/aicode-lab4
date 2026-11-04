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
