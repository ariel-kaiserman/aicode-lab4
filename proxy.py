#!/usr/bin/env python3
"""proxy.py: the naive agent-traffic proxy for Lab 4.

A reverse proxy that prints every request and response and forwards them
unchanged. Standard library only; nothing to install.

    python3 proxy.py                       # listens on http://localhost:8041
    ANTHROPIC_BASE_URL=http://localhost:8041 claude

Everything Claude Code sends to the API now passes through this program and is
printed to the terminal. The proxy changes nothing: headers go through untouched
(a subscription login needs its bearer token and its `anthropic-beta` header),
and the reply is streamed back byte for byte as it arrives.

While it runs, a live dashboard of the same traffic is served at
http://localhost:8042 (see SPEC.md). It only watches; it never alters traffic.

    python3 proxy.py --port 9000
    python3 proxy.py --upstream https://chatgpt.com/backend-api/codex   # for Codex
    python3 proxy.py --ui-port 9001 --keep 500   # dashboard port, exchanges kept
    python3 proxy.py --quiet                     # don't dump bodies to the terminal
    python3 proxy.py --no-ui                     # no dashboard: the plain proxy
    python3 proxy.py --log                       # also record to ./tap-logs/<start>.jsonl
    python3 proxy.py --log session.jsonl --no-ui # record without serving a page
    python3 proxy.py --open tap-logs/X.jsonl     # reopen a recorded session, read-only
    python3 proxy.py --pricing prices.json       # show estimated cost

The pricing file maps model-id prefixes to USD per million tokens; the longest
matching prefix wins. No prices are built in. `context_window` (tokens) is
optional and drives the context bar:

    {"claude-opus-5": {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0,
                       "context_window": 200000}}
"""

from __future__ import annotations

import argparse
import http.client
import json
import select
import socket
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import tap_analyze
import tap_log
import tap_ui
from tap_store import Store, log_once, now_iso

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we print is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

UPSTREAM = "https://api.anthropic.com"
STORE: Store | None = None   # the dashboard's store; None with --no-ui (and no --log)
QUIET = False                # --quiet: skip the body dump
# The agent hanging up (Esc in Claude Code) shows up as one of these on a write.
CLIENT_GONE = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S")


def tap(method: str, *args: object, **kwargs: object) -> object:
    """Hand an event to the dashboard. Never raises: forwarding comes first."""
    if STORE is None:
        return None
    try:
        return getattr(STORE, method)(*args, **kwargs)
    except Exception as e:  # noqa: BLE001
        log_once(e)
        return None


class ClientWatch:
    """Notices an agent that hangs up while its response is still open (R12).

    A write to a closed socket only fails once the next chunk arrives, which can
    be many seconds into a long silent think. So every 250 ms this thread peeks
    (MSG_PEEK: nothing is consumed) at the sockets of open requests: a readable
    socket that reads as EOF means the agent closed it. It never writes, closes
    or reads anything, so traffic is untouched (C3).
    """

    def __init__(self, interval: float = 0.25) -> None:
        self.interval = interval
        self._socks: dict[socket.socket, int] = {}
        self._lock = threading.Lock()
        threading.Thread(target=self._loop, name="tap-client-watch", daemon=True).start()

    def add(self, sock: socket.socket, xid: int) -> None:
        with self._lock:
            self._socks[sock] = xid

    def remove(self, sock: socket.socket) -> None:
        with self._lock:
            self._socks.pop(sock, None)

    def _loop(self) -> None:
        while True:
            time.sleep(self.interval)
            with self._lock:
                watched = list(self._socks.items())
            if not watched:
                continue
            try:
                readable, _, _ = select.select([s for s, _ in watched], [], [], 0)
            except (OSError, ValueError):
                continue  # a socket closed meanwhile; try again next round
            for sock in readable:
                try:
                    gone = sock.recv(1, socket.MSG_PEEK) == b""
                except (BlockingIOError, InterruptedError):
                    continue
                except OSError:
                    gone = True
                if gone:
                    with self._lock:
                        xid = self._socks.pop(sock, None)
                    tap("client_gone", xid)


WATCH: ClientWatch | None = None


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    _to_client = False   # True while writing to the agent: a failure there is the agent hanging up

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # we print our own lines

    def do_POST(self) -> None:
        self.relay()

    def do_GET(self) -> None:
        self.relay()

    def do_HEAD(self) -> None:
        self.relay()

    def relay(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        xid = tap("begin", self.command, self.path, self.headers.items(), body)
        watch = WATCH if xid is not None else None
        if watch:
            watch.add(self.connection, xid)
        try:
            self.forward(body, xid)
        except BaseException as e:
            if self._to_client and isinstance(e, CLIENT_GONE):
                tap("end", xid, error=f"client closed the connection ({type(e).__name__})", source="client")
            else:
                tap("end", xid, error=f"{type(e).__name__}: {e}")
            raise
        finally:
            if watch:
                watch.remove(self.connection)
        tap("end", xid)  # no-op if forward() already ended it

    def forward(self, body: bytes, xid: object) -> None:
        # 1. Print what the agent sent.
        print(f"\n{'=' * 78}\n[{stamp()}] >>> {self.command} {self.path}", flush=True)
        for name, value in self.headers.items():
            if name.lower() in ("authorization", "x-api-key"):
                value = value[:16] + "..."
            print(f"    {name}: {value}")
        if body and not QUIET:
            try:
                print(json.dumps(json.loads(body), indent=2, ensure_ascii=False))
            except ValueError:
                print(body.decode("utf-8", "replace"))

        # 2. Forward it, headers intact.
        target = urlsplit(UPSTREAM)
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        # Connect with a short timeout (a lost SYN must not stall the agent for
        # minutes); the long timeout is for reading a slow stream.
        upstream = conn_cls(target.netloc, timeout=10)
        try:
            upstream.connect()
        except OSError as e:
            print(f"\n[{stamp()}] !!! cannot reach {UPSTREAM}: {e}", flush=True)
            self.send_error(502, f"proxy: cannot reach {UPSTREAM}: {e}")
            self.close_connection = True
            tap("end", xid, error=f"proxy: cannot reach {UPSTREAM}: {e}", status=502)
            return
        try:
            self.relay_reply(upstream, target.path.rstrip("/") + self.path, body, xid)
        finally:
            upstream.close()

    def relay_reply(self, upstream: http.client.HTTPConnection, path: str, body: bytes, xid: object) -> None:
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        upstream.request(self.command, path, body=body or None, headers=headers)
        reply = upstream.getresponse()

        # 3. Stream the reply back as it arrives, printing it on the way.
        print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        tap("response", xid, reply.status, reply.reason, reply.getheaders())
        self._to_client = True
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command == "HEAD":
            self.close_connection = True
            return
        while True:
            self._to_client = False
            chunk = reply.read1(4096)  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
            if not chunk:
                break
            # Recorded first (a queue put, no parsing), so a chunk the agent never
            # gets because it hung up is still in the partial message.
            tap("feed", xid, chunk)
            self._to_client = True
            self.wfile.write(chunk)
            self.wfile.flush()
            if not QUIET:
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
        self._to_client = False
        self.close_connection = True
        print(f"\n[{stamp()}] --- done", flush=True)


def main() -> None:
    global UPSTREAM, STORE, QUIET, WATCH
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port (default: 8042)")
    parser.add_argument("--no-ui", action="store_true", help="turn the dashboard off entirely")
    parser.add_argument("--keep", type=int, default=200, help="exchanges the dashboard keeps (default: 200)")
    parser.add_argument("--quiet", action="store_true", help="don't print request and response bodies")
    parser.add_argument("--log", nargs="?", const="", default=None, metavar="FILE",
                        help="record every exchange as JSON Lines (default file: ./tap-logs/<start>.jsonl)")
    parser.add_argument("--pricing", metavar="FILE", help="JSON price table for cost estimates (see the docstring)")
    parser.add_argument("--open", metavar="FILE", help="serve the dashboard for a recorded session; no proxying")
    args = parser.parse_args()

    pricing = None
    if args.pricing:
        try:
            with open(args.pricing, encoding="utf-8") as f:
                pricing = tap_analyze.validate_pricing(json.load(f))
        except (OSError, ValueError) as e:
            parser.error(f"--pricing {args.pricing}: {e}")

    if args.open:
        open_recorded(args.open, args.ui_port, pricing, parser)
        return

    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    meta = {"url": f"http://localhost:{args.port}", "upstream": UPSTREAM}
    session = {"started_at": now_iso()}
    log = None
    if args.log is not None:
        path = args.log or tap_log.default_path()
        try:
            log = tap_log.LogWriter(path, {**session, "upstream": UPSTREAM, "proxy": meta["url"]})
        except OSError as e:
            parser.error(f"--log {path}: {e}")
    if not args.no_ui or log:
        store = Store(keep=args.keep, meta=meta, pricing=pricing, log=log, session=session)
        ui_ok = False
        if not args.no_ui:
            try:
                tap_ui.start(store, args.ui_port)
            except OSError as e:
                print(f"dashboard disabled: cannot listen on port {args.ui_port}: {e}", file=sys.stderr)
            else:
                ui_ok = True
                print(f"dashboard at       http://localhost:{args.ui_port}")
        if log:
            print(f"logging to         {log.path}")
        if ui_ok or log:
            STORE = store
            WATCH = ClientWatch()
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if STORE is not None and log is not None:
            # Let exchanges that just ended reach the file before we exit.
            STORE.flush(timeout=2)
            log.flush(timeout=2)


def open_recorded(path: str, ui_port: int, pricing: dict | None, parser: argparse.ArgumentParser) -> None:
    """--open FILE: the dashboard for a recorded session, read-only. No proxy port."""
    try:
        session, exchanges = tap_log.read_log(path)
    except (OSError, ValueError) as e:
        parser.error(f"--open {path}: {e}")
    store = Store(keep=max(1, len(exchanges)), pricing=pricing, mode="recorded", session=session,
                  meta={"url": session.get("proxy"), "upstream": session.get("upstream"), "log": path})
    store.load(exchanges)
    try:
        ui = tap_ui.make_server(store, ui_port)
    except OSError as e:
        parser.error(f"cannot listen on port {ui_port}: {e}")
    print(f"recorded session   {path}  ({len(exchanges)} exchanges)")
    print(f"dashboard at       http://localhost:{ui_port}")
    try:
        ui.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
