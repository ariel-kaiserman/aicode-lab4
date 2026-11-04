"""tap_store.py: in-memory store of captured exchanges, live fan-out, redaction.

Threading model
---------------
* Relay threads (one per proxied request) call begin / response / feed / end.
  Each call redacts headers if it has any, stamps the time, and puts one item
  on an unbounded outbox. Nothing else happens there: no parsing, no JSON, no
  socket writes, no waiting (C3, R4).
* One dispatcher thread drains the outbox in order. It owns every exchange's
  state: it parses the SSE stream (tap_stream), parses and analyses request
  bodies (tap_analyze: threads, summaries, search docs), assigns `seq`,
  serializes each live event once and hands the bytes to every subscriber
  queue with put_nowait, hands finished exchanges to the log writer, and
  pushes session stats at most once a second.
* Dashboard threads read snapshots, details, search and stats under the
  per-exchange lock, so they always see a consistent exchange.

Every event carries the exchange id and a per-exchange sequence number `seq`;
snapshots carry the exchange's current `seq`. A client that loads a snapshot
while also receiving live events drops any event with seq <= the snapshot's,
so nothing is applied twice (SPEC 6.1).
"""

from __future__ import annotations

import codecs
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator
from urllib.parse import urlsplit

import tap_analyze
from tap_analyze import Analyzer, Doc, iso_ms
from tap_stream import MessageStream

SECRET_HEADERS = {"authorization", "x-api-key", "cookie", "set-cookie"}
SECRET_BODY_RE = re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}")
RAW_CAP = 2 * 1024 * 1024          # bytes of raw response text kept per exchange
BODY_CAP = 32 * 1024 * 1024        # non-streaming bodies buffered for JSON parsing
TRUNCATED = "\n…[truncated: raw response exceeded {:,} bytes; the parsed message is still complete]\n"
STATS_INTERVAL = 1.0
RATELIMIT_PREFIX = "anthropic-ratelimit-"


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def ms_iso(ms: int | None) -> str | None:
    """Integer epoch milliseconds -> ISO-8601 UTC, exactly (no float rounding)."""
    if ms is None:
        return None
    dt = datetime.fromtimestamp(ms // 1000, timezone.utc).replace(microsecond=(ms % 1000) * 1000)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso() -> str:
    return ms_iso(now_ms()) or ""


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


# -- redaction (C5, SPEC 9) -----------------------------------------------------

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


def redact_body(text: str) -> str:
    """Defence in depth: API keys pasted into a prompt or printed into a tool result."""
    return SECRET_BODY_RE.sub(lambda m: redact_value(m.group(0)), text)


def classify(method: str, path: str) -> str:
    p = urlsplit(path).path.rstrip("/")
    if p.endswith("/v1/messages/count_tokens"):
        return "count_tokens"
    if method == "POST" and p.endswith("/v1/messages"):
        return "messages"
    return "other"


def upstream_info(headers: dict[str, str]) -> dict[str, Any]:
    ratelimit: dict[str, Any] = {}
    for name, value in headers.items():
        if name.startswith(RATELIMIT_PREFIX):
            key = name[len(RATELIMIT_PREFIX):].replace("-", "_")
            ratelimit[key] = int(value) if value.isdigit() else value
    return {"request_id": headers.get("request-id") or headers.get("x-request-id"),
            "ratelimit": ratelimit, "retry_after": headers.get("retry-after")}


# -- one request/response pair --------------------------------------------------

class Exchange:
    """State of one request/response pair. Mutated only by the dispatcher thread."""

    def __init__(self, xid: int, method: str, path: str, headers: dict[str, str],
                 body: bytes, raw_cap: int, started_ms: int) -> None:
        self.id = xid
        self.method = method
        self.path = path
        self.kind = classify(method, path)
        self.state = "pending"
        self.status: int | None = None
        self.seq = 0
        self.ended = False
        self.evicted = False
        self.lock = threading.Lock()
        self.t_start = started_ms
        self.t_response: int | None = None
        self.t_first: int | None = None
        self.t_end: int | None = None

        self.request_headers = headers
        self.request_bytes = len(body)
        self._body: bytes | None = body       # parsed on the dispatcher, then dropped
        self._request_json: str | None = None
        self._request_lock = threading.Lock()

        self.response_headers: dict[str, str] = {}
        self.upstream: dict[str, Any] = {"request_id": None, "ratelimit": {}, "retry_after": None}
        self.response_bytes = 0
        self.message: dict[str, Any] | None = None
        self.error: dict[str, Any] | None = None
        self.raw_cap = raw_cap
        self.raw_parts: list[str] = []
        self.raw_bytes = 0
        self.raw_truncated = False
        self._raw_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.stream: MessageStream | None = None
        self.body_buf: bytearray | None = None

        # analysis (tap_analyze)
        self.thread: dict[str, Any] | None = None
        self.req_summary: dict[str, Any] = {}
        self.tool_results: list[dict] = []
        self.docs: list[Doc] = []

    # -- request body -------------------------------------------------------

    def parse_request(self) -> Any:
        """Redact and parse the request body once. Returns the parsed body; afterwards
        only the JSON text is kept (`request_json`), not the parsed objects."""
        with self._request_lock:
            if self._request_json is not None:
                return json.loads(self._request_json).get("body")
            body, self._body = self._body or b"", None
            parsed: Any = None
            text = ""
            if body:
                text = redact_body(body.decode("utf-8", "replace"))
                try:
                    parsed = json.loads(text)
                except ValueError:
                    parsed = {"_raw": text}
                    text = json.dumps(parsed, ensure_ascii=False)
            self._request_json = ('{"headers": ' + json.dumps(self.request_headers, ensure_ascii=False)
                                  + ', "body": ' + (text or "null") + "}")
            return parsed

    def request_json(self) -> str:
        if self._request_json is None:
            self.parse_request()
        return self._request_json or "{}"

    # -- response -----------------------------------------------------------

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

    def raw(self) -> str:
        if len(self.raw_parts) > 1:
            self.raw_parts = ["".join(self.raw_parts)]
        return self.raw_parts[0] if self.raw_parts else ""

    # -- views (caller holds self.lock) ----------------------------------------

    def timing(self) -> dict[str, Any]:
        t0, t1, tf, te = self.t_start, self.t_response, self.t_first, self.t_end
        out_tokens = ((self.message or {}).get("usage") or {}).get("output_tokens")
        tps = None
        if te is not None and tf is not None and te > tf and isinstance(out_tokens, int) and out_tokens > 0:
            tps = round(out_tokens / ((te - tf) / 1000), 1)
        return {
            "started_at": ms_iso(t0), "response_at": ms_iso(t1),
            "first_token_at": ms_iso(tf), "ended_at": ms_iso(te),
            "ttfb_ms": t1 - t0 if t1 is not None else None,
            "ttft_ms": tf - t0 if tf is not None else None,
            "duration_ms": te - t0 if te is not None else None,
            "output_tps": tps,
        }

    def _common(self) -> dict[str, Any]:
        return {
            "id": self.id, "seq": self.seq, "method": self.method, "path": self.path, "kind": self.kind,
            "state": self.state, "status": self.status, "timing": self.timing(), "thread": self.thread,
            "upstream": self.upstream,
            "sizes": {"request_bytes": self.request_bytes, "response_bytes": self.response_bytes},
        }

    def summary(self) -> dict[str, Any]:
        """SPEC 6.2: everything but the request body, the reply's content and the raw text."""
        s = self._common()
        msg = self.message
        s["response"] = {
            "message": {k: v for k, v in msg.items() if k != "content"} if isinstance(msg, dict) else None,
            "raw_truncated": self.raw_truncated, "error": self.error,
        }
        s["summary"] = {"request": self.req_summary, "response": tap_analyze.response_summary(msg),
                        "tool_results": self.tool_results}
        s["started_at"], s["ended_at"] = s["timing"]["started_at"], s["timing"]["ended_at"]  # v1 aliases
        return s

    def full_state(self) -> dict[str, Any]:
        s = self._common()
        s["response"] = {"headers": self.response_headers, "message": self.message, "raw": self.raw(),
                         "raw_truncated": self.raw_truncated, "error": self.error}
        return s

    def to_json(self) -> str:
        """The full Exchange (SPEC 6) as JSON."""
        request = self.request_json()
        with self.lock:
            rest = json.dumps(self.full_state(), ensure_ascii=False)
        return '{"request": ' + request + ", " + rest[1:]

    @classmethod
    def from_dict(cls, d: dict[str, Any], raw_cap: int = RAW_CAP) -> "Exchange":
        """Rebuild a finished exchange from its log line (for --open)."""
        timing = d.get("timing") or {}
        request = d.get("request") or {}
        response = d.get("response") or {}
        ex = cls(int(d["id"]), d.get("method", ""), d.get("path", ""), request.get("headers") or {},
                 b"", raw_cap, iso_ms(timing.get("started_at")) or 0)
        ex.kind = d.get("kind", ex.kind)
        ex._body = None
        ex._request_json = json.dumps({"headers": request.get("headers") or {}, "body": request.get("body")},
                                      ensure_ascii=False)
        ex.state, ex.status, ex.seq = d.get("state", "done"), d.get("status"), int(d.get("seq") or 0)
        ex.t_response = iso_ms(timing.get("response_at"))
        ex.t_first = iso_ms(timing.get("first_token_at"))
        ex.t_end = iso_ms(timing.get("ended_at"))
        ex.ended = True
        sizes = d.get("sizes") or {}
        ex.request_bytes = sizes.get("request_bytes", 0)
        ex.response_bytes = sizes.get("response_bytes", 0)
        ex.upstream = d.get("upstream") or ex.upstream
        ex.response_headers = response.get("headers") or {}
        ex.message = response.get("message")
        ex.error = response.get("error")
        ex.raw_parts = [response.get("raw") or ""]
        ex.raw_truncated = bool(response.get("raw_truncated"))
        return ex


class Subscriber:
    def __init__(self, maxsize: int) -> None:
        self.queue: queue.Queue[bytes] = queue.Queue(maxsize)
        self.dead = False
        self.resync = False       # dropped for being too slow: tell the page why (R6)


RESYNC = b"event: resync\ndata: {}\n\n"


class Store:
    def __init__(self, keep: int = 200, raw_cap: int = RAW_CAP, queue_size: int = 1000,
                 meta: dict[str, Any] | None = None, pricing: dict | None = None,
                 log: Any = None, mode: str = "live", session: dict | None = None) -> None:
        self.keep = max(1, keep)
        self.raw_cap = raw_cap
        self.queue_size = queue_size
        self.meta = meta or {}
        self.pricing = pricing
        self.log = log                      # tap_log.LogWriter or None
        self.mode = mode                    # live | recorded
        self.session_started_at = (session or {}).get("started_at") or now_iso()
        self.analyzer = Analyzer(pricing)
        self._lock = threading.Lock()
        self._exchanges: OrderedDict[int, Exchange] = OrderedDict()   # retained, in id order
        self._open: dict[int, Exchange] = {}                          # begun, not yet ended
        self._evicted = 0
        self._next_id = 1
        self._subs: list[Subscriber] = []
        self._outbox: queue.SimpleQueue = queue.SimpleQueue()
        self._stats_dirty = False
        self._stats_last = 0.0
        self._stats_sent: str | None = None
        threading.Thread(target=self._dispatch_loop, name="tap-dispatch", daemon=True).start()

    # -- relay side: called from proxy threads, must stay cheap -----------------

    def begin(self, method: str, path: str, headers: Iterable[tuple[str, str]], body: bytes) -> int:
        t = now_ms()
        redacted = redact_headers(headers)      # the secret never enters the store (C5)
        with self._lock:
            xid = self._next_id
            self._next_id += 1
            ex = Exchange(xid, method, path, redacted, body or b"", self.raw_cap, t)
            self._open[xid] = ex
            # Enqueued under the store lock so exchanges start in id order.
            self._outbox.put(("start", ex))
        return xid

    def response(self, xid: int | None, status: int, reason: str,
                 headers: Iterable[tuple[str, str]]) -> None:
        ex = self._get_open(xid)
        if ex is not None:
            self._outbox.put(("response", ex, status, reason, redact_headers(headers), now_ms()))

    def feed(self, xid: int | None, chunk: bytes) -> None:
        if not chunk:
            return
        ex = self._get_open(xid)
        if ex is not None:
            self._outbox.put(("chunk", ex, bytes(chunk), now_ms()))

    def end(self, xid: int | None, error: str | None = None, status: int | None = None,
            source: str | None = None) -> None:
        """Finish an exchange. Idempotent: only the first call counts. `source` is
        "client" when the agent hung up (state `aborted`), else proxy/upstream."""
        ex = self._get_open(xid)
        if ex is not None:
            self._outbox.put(("end", ex, error, status, source, now_ms()))

    def client_gone(self, xid: int | None) -> None:
        """The agent closed its connection while the response was still open (R12)."""
        ex = self._get_open(xid)
        if ex is not None:
            self._outbox.put(("gone", ex, now_ms()))

    # -- loading a recorded session (--open) ---------------------------------

    def load(self, exchanges: Iterable[dict[str, Any]]) -> int:
        n = 0
        for d in sorted(exchanges, key=lambda e: e["id"]):
            self._outbox.put(("load", d))
            n += 1
        self.flush(timeout=max(5.0, n * 0.05))
        with self._lock:
            if self._exchanges:
                self._next_id = max(self._exchanges) + 1
        return n

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

    def _retained(self) -> list[Exchange]:
        with self._lock:
            return list(self._exchanges.values())

    def summaries(self) -> list[dict[str, Any]]:
        out = []
        for ex in self._retained():
            with ex.lock:
                out.append(ex.summary())
        return out

    def session_info(self) -> dict[str, Any]:
        return {"started_at": self.session_started_at, "mode": self.mode, "keep": self.keep,
                "proxy": self.meta, "log": getattr(self.log, "path", None) or self.meta.get("log"),
                "pricing": bool(self.pricing)}

    def snapshot_json(self) -> str:
        """SPEC 6.2: summaries only, plus session stats (without the per-call table)."""
        with self._lock:
            evicted = self._evicted
        summaries = self.summaries()
        head = {"session": self.session_info(), "evicted_before": evicted,
                "stats": tap_analyze.stats(summaries, self.pricing, with_calls=False)}
        body = json.dumps(head, ensure_ascii=False)
        return body[:-1] + ', "exchanges": ' + json.dumps(summaries, ensure_ascii=False) + "}"

    def snapshot(self) -> dict[str, Any]:
        """Parsed snapshot after everything published so far is applied (for tests)."""
        self.flush()
        return json.loads(self.snapshot_json())

    def _find(self, xid: int) -> Exchange | None:
        with self._lock:
            return self._exchanges.get(xid)

    def exchange_json(self, xid: int) -> str | None:
        ex = self._find(xid)
        return ex.to_json() if ex is not None else None

    def exchange(self, xid: int) -> dict[str, Any] | None:
        self.flush()
        text = self.exchange_json(xid)
        return json.loads(text) if text is not None else None

    def raw_text(self, xid: int) -> str | None:
        ex = self._find(xid)
        if ex is None:
            return None
        with ex.lock:
            return ex.raw()

    def search(self, q: str, thread: str | None = None, limit: int = 500) -> dict[str, Any]:
        """Raises tap_analyze.QueryError for a bad query."""
        started = time.perf_counter()
        items = []
        for ex in self._retained():
            with ex.lock:
                items.append((ex.summary(), list(ex.docs)))
        result = tap_analyze.search(items, q, thread=thread, limit=limit)
        result["took_ms"] = round((time.perf_counter() - started) * 1000, 1)
        return result

    def stats(self, with_calls: bool = True) -> dict[str, Any]:
        return tap_analyze.stats(self.summaries(), self.pricing, with_calls=with_calls)

    def export_lines(self) -> Iterator[str]:
        """The log format (SPEC 10.1) for every retained exchange."""
        yield json.dumps({"type": "session", "version": 2, "started_at": self.session_started_at,
                          "upstream": self.meta.get("upstream"), "proxy": self.meta.get("url")},
                         ensure_ascii=False) + "\n"
        for ex in self._retained():
            yield '{"type": "exchange", ' + ex.to_json()[1:] + "\n"

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until every event published so far has been dispatched (for tests)."""
        done = threading.Event()
        self._outbox.put(done)
        return done.wait(timeout)

    # -- dispatcher ------------------------------------------------------------

    def _get_open(self, xid: int | None) -> Exchange | None:
        if xid is None:
            return None
        with self._lock:
            return self._open.get(xid)

    def _dispatch_loop(self) -> None:
        while True:
            wait = None
            if self._stats_dirty:
                wait = max(0.0, self._stats_last + STATS_INTERVAL - time.monotonic())
            try:
                item = self._outbox.get(timeout=wait) if wait is None or wait > 0 else self._outbox.get_nowait()
            except queue.Empty:
                item = None
            if isinstance(item, threading.Event):
                item.set()
            elif item is not None:
                try:
                    getattr(self, "_on_" + item[0])(*item[1:])
                except Exception as e:  # noqa: BLE001
                    log_once(e, "dashboard")
            if self._stats_dirty and time.monotonic() - self._stats_last >= STATS_INTERVAL:
                try:
                    self._push_stats()
                except Exception as e:  # noqa: BLE001
                    log_once(e, "dashboard stats")

    def _send(self, name: str, payload: Any, ex: Exchange | None = None) -> None:
        if ex is not None and ex.evicted:
            return
        with self._lock:
            subs = list(self._subs)
        if not subs:
            return
        data = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
        message = f"event: {name}\ndata: {data}\n\n".encode("utf-8")
        for sub in subs:
            if sub.dead:
                continue
            try:
                sub.queue.put_nowait(message)
            except queue.Full:
                self._drop(sub)

    def _drop(self, sub: Subscriber) -> None:
        """R6: a slow client. Empty its queue so its handler wakes up, sends `resync`
        and closes; the page reloads a snapshot when it reconnects."""
        sub.resync = True
        self.unsubscribe(sub)
        try:
            while True:
                sub.queue.get_nowait()
        except queue.Empty:
            pass
        try:
            sub.queue.put_nowait(RESYNC)     # wakes a handler blocked on the empty queue
        except queue.Full:
            pass

    def _push_stats(self) -> None:
        self._stats_dirty = False
        self._stats_last = time.monotonic()
        if not self.subscriber_count():
            self._stats_sent = None
            return
        data = json.dumps(self.stats(with_calls=False), ensure_ascii=False)
        if data != self._stats_sent:     # only when changed
            self._stats_sent = data
            self._send("stats", data)

    def _add(self, ex: Exchange) -> list[Exchange]:
        evicted = []
        with self._lock:
            self._exchanges[ex.id] = ex
            while len(self._exchanges) > self.keep:
                _, old = self._exchanges.popitem(last=False)
                evicted.append(old)
                self._evicted += 1
        for old in evicted:
            with old.lock:
                old.seq += 1
                payload = {"id": old.id, "seq": old.seq, "logged": self.log is not None}
            self._send("exchange_evicted", payload)
            old.evicted = True
            if old.ended:
                old.docs = []
        return evicted

    def _on_start(self, ex: Exchange) -> None:
        with ex.lock:
            ex.seq = 1
            start = ex.summary()
        self._add(ex)
        self._send("exchange_start", start, ex)
        self._stats_dirty = True
        body = ex.parse_request()
        result = self.analyzer.on_request(ex.id, ex.kind, ex.t_start, body)
        del body
        with ex.lock:
            ex.thread = result["thread"]
            ex.req_summary = result["request"]
            ex.tool_results = result["tool_results"]
            ex.docs = result["docs"]
            if ex.kind != "messages":
                return   # other traffic gets only start / response_start / end (6.1)
            ex.seq += 1
            meta = {"id": ex.id, "seq": ex.seq, "thread": ex.thread,
                    "summary": {"request": ex.req_summary, "tool_results": ex.tool_results}}
        self._send("exchange_meta", meta, ex)

    def _on_response(self, ex: Exchange, status: int, reason: str, headers: dict[str, str], t: int) -> None:
        with ex.lock:
            if ex.ended:
                return
            ex.status = status
            ex.t_response = t
            ex.state = "streaming"
            ex.response_headers = headers
            ex.upstream = upstream_info(headers)
            if ex.kind == "messages" and "text/event-stream" in headers.get("content-type", ""):
                ex.stream = MessageStream()
            else:
                ex.body_buf = bytearray()
            ex.seq += 1
            payload = {"id": ex.id, "seq": ex.seq, "status": status, "reason": reason, "headers": headers,
                       "upstream": ex.upstream, "state": ex.state, "timing": ex.timing()}
        self._send("response_start", payload, ex)
        self._stats_dirty = True

    def _on_chunk(self, ex: Exchange, chunk: bytes, t: int) -> None:
        events, uses = [], []
        with ex.lock:
            if ex.ended:
                return
            ex.response_bytes += len(chunk)
            ex.append_raw(chunk)
            if ex.stream is not None:
                for name, payload in ex.stream.feed(chunk):
                    if name == "block_delta" and ex.t_first is None:
                        ex.t_first = t
                    if name == "block_stop" and payload.get("tool_use_id"):
                        uses.append({"id": payload["tool_use_id"], "name": payload.get("name"),
                                     "input": payload.get("input")})
                    ex.seq += 1
                    events.append((name, {"id": ex.id, "seq": ex.seq, **payload}))
                ex.message = ex.stream.message
            elif ex.body_buf is not None:
                if len(ex.body_buf) + len(chunk) > BODY_CAP:
                    ex.body_buf = None
                else:
                    ex.body_buf += chunk
        for name, payload in events:
            self._send(name, payload, ex)
        for block in uses:
            self.analyzer.on_tool_use(ex.id, block)   # a sub-agent may start before this reply ends

    def _on_gone(self, ex: Exchange, t: int) -> None:
        with ex.lock:
            # A finished (or failed) stream: the agent hanging up now is normal.
            done = ex.ended or (ex.stream is not None and (ex.stream.finished or ex.stream.error))
        if not done:
            self._on_end(ex, "client closed the connection", None, "client", t)

    def _on_end(self, ex: Exchange, error: str | None, status: int | None, source: str | None, t: int) -> None:
        with ex.lock:
            if ex.ended:
                return
            ex.ended = True
            if status is not None:
                ex.status = status
            finished = False
            events = []
            if ex.stream is not None:
                for name, payload in ex.stream.close():
                    ex.seq += 1
                    events.append((name, {"id": ex.id, "seq": ex.seq, **payload}))
                ex.message = ex.stream.message
                finished = ex.stream.finished
                if ex.stream.error:
                    err = ex.stream.error
                    ex.error = {"source": "upstream", "type": err.get("type", "error"),
                                "message": err.get("message", "")}
                elif not finished and error is None:
                    ex.error = {"source": "upstream", "type": "incomplete",
                                "message": "stream ended before message_stop"}
                ex.stream = None
            elif ex.body_buf is not None:
                self._parse_body(ex, bytes(ex.body_buf))
                ex.body_buf = None
            aborted = source == "client" and not finished and not ex.error
            if aborted:
                ex.error = {"source": "client", "type": "aborted", "message": error or "client closed the connection"}
            elif error and ex.error is None and source != "client":
                ex.error = {"source": "proxy", "type": "proxy_error", "message": error}
            if ex.error is None and ex.status is not None and ex.status >= 400:
                ex.error = {"source": "http", "type": "http_error", "message": f"HTTP {ex.status}"}
            ex.state = "aborted" if aborted else "error" if ex.error else "done"
            ex.t_end = t
            ex.seq += 1
            end = ex.summary()
            message = ex.message
        for name, payload in events:
            self._send(name, payload, ex)
        self._send("exchange_end", end, ex)
        docs = self.analyzer.on_end(ex.id, ex.t_end, message)
        with ex.lock:
            if ex.error:
                docs.append(Doc("err", "error", f"{ex.error.get('type', '')}: {ex.error.get('message', '')}"))
            if not ex.evicted:
                ex.docs = ex.docs + docs
        with self._lock:
            self._open.pop(ex.id, None)
        if self.log is not None:
            self.log.write(ex)
        self._stats_dirty = True

    def _on_load(self, d: dict[str, Any]) -> None:
        ex = Exchange.from_dict(d, self.raw_cap)
        self._add(ex)
        result = self.analyzer.on_request(ex.id, ex.kind, ex.t_start, ex.parse_request())
        docs = self.analyzer.on_end(ex.id, ex.t_end if ex.t_end is not None else ex.t_start, ex.message)
        if ex.error:
            docs.append(Doc("err", "error", f"{ex.error.get('type', '')}: {ex.error.get('message', '')}"))
        with ex.lock:
            ex.thread = result["thread"]
            ex.req_summary = result["request"]
            ex.tool_results = result["tool_results"]
            ex.docs = result["docs"] + docs

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
            ex.error = {"source": "upstream", "type": err.get("type", "error"), "message": err.get("message", "")}
