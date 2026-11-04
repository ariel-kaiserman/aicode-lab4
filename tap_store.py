"""tap_store.py: in-memory store of captured exchanges, live fan-out, redaction.

Threading model
---------------
* Relay threads (one per proxied request) call begin / response / feed / end.
  Each call takes a short per-exchange lock, mutates the exchange, and puts the
  resulting live events on an unbounded outbox. Nothing slow happens there:
  no JSON encoding of request bodies, no socket writes.
* One dispatcher thread drains the outbox, serializes each event once, and
  hands the bytes to every subscriber queue with put_nowait. A subscriber
  whose queue is full (a slow or dead browser tab) is dropped.
* Dashboard threads read snapshots (snapshot_json) and subscriber queues.

Every event carries the exchange id and a per-exchange sequence number `seq`;
snapshots carry the exchange's current `seq`. A client that loads a snapshot
while also receiving live events drops any event with seq <= the snapshot's,
so nothing is applied twice.
"""

from __future__ import annotations

import codecs
import json
import os
import queue
import sys
import threading
import traceback
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urlsplit

from tap_stream import MessageStream

SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "set-cookie"}
RAW_CAP = 2 * 1024 * 1024          # bytes of raw response text kept per exchange
BODY_CAP = 32 * 1024 * 1024        # non-streaming bodies buffered for JSON parsing
TRUNCATED = "\n…[truncated: raw response exceeded {:,} bytes; the parsed message is still complete]\n"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# -- error logging (R5) ---------------------------------------------------------

_logged: set = set()
_logged_lock = threading.Lock()


def log_once(exc: BaseException, where: str = "dashboard") -> None:
    """Report an exception to stderr once per (type, source line); never raises."""
    try:
        frames = traceback.extract_tb(exc.__traceback__)
        loc = (frames[-1].filename, frames[-1].lineno) if frames else ("?", 0)
        key = (type(exc).__name__, loc)
        with _logged_lock:
            if key in _logged:
                return
            _logged.add(key)
        print(f"[tap] {where} error (ignored, proxying continues; repeats suppressed): "
              f"{type(exc).__name__}: {exc} at {os.path.basename(loc[0])}:{loc[1]}",
              file=sys.stderr, flush=True)
    except Exception:
        pass


# -- redaction (C5) -------------------------------------------------------------

def redact_value(value: str) -> str:
    # First 12 characters, but never more than a third of a short value.
    return value[: min(12, len(value) // 3)] + "…[redacted]"


def redact_headers(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, value in items:
        key = name.lower()
        if key in SECRET_HEADERS:
            value = redact_value(value)
        out[key] = f"{out[key]}, {value}" if key in out else value
    return out


def classify(method: str, path: str) -> str:
    p = urlsplit(path).path.rstrip("/")
    if p.endswith("/v1/messages/count_tokens"):
        return "count_tokens"
    if method == "POST" and p.endswith("/v1/messages"):
        return "messages"
    return "other"


# -- one request/response pair --------------------------------------------------

class Exchange:
    def __init__(self, xid: int, method: str, path: str, headers: dict[str, str],
                 body: bytes, raw_cap: int) -> None:
        self.id = xid
        self.method = method
        self.path = path
        self.kind = classify(method, path)
        self.started_at = now_iso()
        self.response_at: str | None = None
        self.ended_at: str | None = None
        self.state = "pending"
        self.status: int | None = None
        self.seq = 0
        self.evicted = False
        self.lock = threading.Lock()

        self.request_headers = headers
        self._body = body                     # parsed lazily, off the relay thread
        self._request_json: str | None = None
        self._request_lock = threading.Lock()

        self.response_headers: dict[str, str] = {}
        self.message: dict[str, Any] | None = None
        self.error: dict[str, Any] | None = None
        self.raw_cap = raw_cap
        self.raw_parts: list[str] = []
        self.raw_bytes = 0
        self.raw_truncated = False
        self._raw_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.stream: MessageStream | None = None
        self.body_buf: bytearray | None = None

    def request_json(self) -> str:
        """`{"headers": ..., "body": ...}` as JSON, computed once."""
        with self._request_lock:
            if self._request_json is None:
                body = self._body
                if not body:
                    parsed: Any = None
                else:
                    try:
                        parsed = json.loads(body)
                    except ValueError:
                        parsed = {"_raw": body.decode("utf-8", "replace")}
                self._request_json = json.dumps(
                    {"headers": self.request_headers, "body": parsed}, ensure_ascii=False)
                self._body = b""
            return self._request_json

    def append_raw(self, chunk: bytes) -> None:
        if self.raw_truncated:
            return
        room = self.raw_cap - self.raw_bytes
        part = chunk if len(chunk) <= room else chunk[:room]
        self.raw_bytes += len(part)
        self.raw_parts.append(self._raw_decoder.decode(part))
        if len(part) < len(chunk):
            self.raw_truncated = True
            self.raw_parts.append(TRUNCATED.format(self.raw_cap))

    def _state(self) -> dict[str, Any]:  # caller holds self.lock
        return {
            "id": self.id, "seq": self.seq,
            "started_at": self.started_at, "response_at": self.response_at, "ended_at": self.ended_at,
            "method": self.method, "path": self.path, "kind": self.kind,
            "state": self.state, "status": self.status,
            "response": {
                "headers": self.response_headers,
                "message": self.message,
                "raw": "".join(self.raw_parts),
                "raw_truncated": self.raw_truncated,
                "error": self.error,
            },
        }

    def to_json(self) -> str:
        request = self.request_json()
        with self.lock:
            rest = json.dumps(self._state(), ensure_ascii=False)
        return '{"request": ' + request + ", " + rest[1:]

    def start_json(self) -> str:
        """The exchange_start payload: request filled, response empty, seq 1."""
        start = {
            "id": self.id, "seq": 1,
            "started_at": self.started_at, "response_at": None, "ended_at": None,
            "method": self.method, "path": self.path, "kind": self.kind,
            "state": "pending", "status": None,
            "response": {"headers": {}, "message": None, "raw": "", "raw_truncated": False, "error": None},
        }
        return '{"request": ' + self.request_json() + ", " + json.dumps(start, ensure_ascii=False)[1:]


class Subscriber:
    def __init__(self, maxsize: int) -> None:
        self.queue: queue.Queue[bytes] = queue.Queue(maxsize)
        self.dead = False



class Store:
    def __init__(self, keep: int = 200, raw_cap: int = RAW_CAP, queue_size: int = 1000,
                 meta: dict[str, Any] | None = None) -> None:
        self.keep = max(1, keep)
        self.raw_cap = raw_cap
        self.queue_size = queue_size
        self.meta = meta or {}
        self.session_started_at = now_iso()
        self._lock = threading.Lock()
        self._exchanges: OrderedDict[int, Exchange] = OrderedDict()
        self._next_id = 1
        self._subs: list[Subscriber] = []
        self._outbox: queue.SimpleQueue = queue.SimpleQueue()
        threading.Thread(target=self._dispatch_loop, name="tap-dispatch", daemon=True).start()

    # -- relay side: called from proxy threads, must stay cheap -----------------

    def begin(self, method: str, path: str, headers: Iterable[tuple[str, str]], body: bytes) -> int:
        redacted = redact_headers(headers)
        with self._lock:
            xid = self._next_id
            self._next_id += 1
            ex = Exchange(xid, method, path, redacted, body or b"", self.raw_cap)
            ex.seq = 1
            self._exchanges[xid] = ex
            while len(self._exchanges) > self.keep:
                _, old = self._exchanges.popitem(last=False)
                old.evicted = True
            # Enqueued under the store lock so exchange_start events leave in id order.
            self._outbox.put((ex, "exchange_start", ex.start_json))
        return xid

    def response(self, xid: int | None, status: int, reason: str,
                 headers: Iterable[tuple[str, str]]) -> None:
        ex = self._get(xid)
        if ex is None:
            return
        redacted = redact_headers(headers)
        with ex.lock:
            ex.status = status
            ex.response_at = now_iso()
            ex.state = "streaming"
            ex.response_headers = redacted
            if ex.kind == "messages" and "text/event-stream" in redacted.get("content-type", ""):
                ex.stream = MessageStream()
            else:
                ex.body_buf = bytearray()
            self._emit(ex, "response_start", {
                "status": status, "reason": reason, "headers": redacted,
                "response_at": ex.response_at, "state": ex.state,
            })

    def feed(self, xid: int | None, chunk: bytes) -> None:
        ex = self._get(xid)
        if ex is None or not chunk:
            return
        with ex.lock:
            if ex.ended_at:
                return
            ex.append_raw(chunk)
            if ex.stream is not None:
                for name, payload in ex.stream.feed(chunk):
                    self._emit(ex, name, payload)
                ex.message = ex.stream.message
            elif ex.body_buf is not None:
                if len(ex.body_buf) + len(chunk) > BODY_CAP:
                    ex.body_buf = None
                else:
                    ex.body_buf += chunk

    def end(self, xid: int | None, error: str | None = None, status: int | None = None) -> None:
        ex = self._get(xid)
        if ex is None:
            return
        with ex.lock:
            if ex.ended_at:
                return  # idempotent
            if status is not None:
                ex.status = status
            if ex.stream is not None:
                for name, payload in ex.stream.close():
                    self._emit(ex, name, payload)
                ex.message = ex.stream.message
                if ex.stream.error:
                    err = ex.stream.error
                    ex.error = {"type": err.get("type", "error"), "message": err.get("message", "")}
                elif not ex.stream.finished and error is None:
                    ex.error = {"type": "incomplete", "message": "stream ended before message_stop"}
                ex.stream = None
            elif ex.body_buf is not None:
                self._parse_body(ex, bytes(ex.body_buf))
                ex.body_buf = None
            if error and ex.error is None:
                ex.error = {"type": "proxy_error", "message": error}
            if ex.error is None and ex.status is not None and ex.status >= 400:
                ex.error = {"type": "http_error", "message": f"HTTP {ex.status}"}
            ex.state = "error" if ex.error else "done"
            ex.ended_at = now_iso()
            ex.seq += 1
            # Serialized later by the dispatcher; the exchange no longer changes.
            self._outbox.put((ex, "exchange_end", ex.to_json))

    # -- dashboard side ------------------------------------------------------

    def subscribe(self) -> Subscriber:
        sub = Subscriber(self.queue_size)
        with self._lock:
            self._subs.append(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        sub.dead = True
        with self._lock:
            if sub in self._subs:
                self._subs.remove(sub)

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subs)

    def snapshot_json(self) -> str:
        with self._lock:
            exchanges = list(self._exchanges.values())
        head = json.dumps({"session_started_at": self.session_started_at, "keep": self.keep,
                           "proxy": self.meta}, ensure_ascii=False)
        return head[:-1] + ', "exchanges": [' + ",".join(ex.to_json() for ex in exchanges) + "]}"

    def snapshot(self) -> dict[str, Any]:
        return json.loads(self.snapshot_json())

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until every event published so far has been dispatched (for tests)."""
        done = threading.Event()
        self._outbox.put(done)
        return done.wait(timeout)

    # -- internals -----------------------------------------------------------

    def _get(self, xid: int | None) -> Exchange | None:
        if xid is None:
            return None
        with self._lock:
            return self._exchanges.get(xid)

    def _emit(self, ex: Exchange, name: str, payload: dict[str, Any]) -> None:  # holds ex.lock
        ex.seq += 1
        self._outbox.put((ex, name, {"id": ex.id, "seq": ex.seq, **payload}))

    @staticmethod
    def _parse_body(ex: Exchange, data: bytes) -> None:
        try:
            obj = json.loads(data)
        except ValueError:
            return
        if not isinstance(obj, dict):
            return
        if ex.kind == "messages" and obj.get("type") == "message":
            ex.message = obj
        err = obj.get("error")
        if obj.get("type") == "error" and isinstance(err, dict):
            ex.error = {"type": err.get("type", "error"), "message": err.get("message", "")}

    def _dispatch_loop(self) -> None:
        while True:
            item = self._outbox.get()
            if isinstance(item, threading.Event):
                item.set()
                continue
            try:
                self._dispatch(*item)
            except Exception as e:  # noqa: BLE001
                log_once(e, "dashboard publish")

    def _dispatch(self, ex: Exchange, name: str, payload: Any) -> None:
        if ex.evicted:
            return
        with self._lock:
            subs = list(self._subs)
        if not subs:
            return
        data = payload() if callable(payload) else json.dumps(payload, ensure_ascii=False)
        message = f"event: {name}\ndata: {data}\n\n".encode("utf-8")
        for sub in subs:
            if sub.dead:
                continue
            try:
                sub.queue.put_nowait(message)
            except queue.Full:
                self.unsubscribe(sub)  # R6: slow client; it reloads a snapshot on reconnect
