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
"""

from __future__ import annotations

import argparse
import http.client
import json
import sys
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import tap_ui
from tap_store import Store, log_once

# Headers that describe one hop of the connection, not the message. They are
# never forwarded. `accept-encoding` is dropped too, so the API answers in plain
# text and what we print is readable.
SKIP = {
    "host", "connection", "keep-alive", "transfer-encoding", "te", "trailers",
    "upgrade", "proxy-authorization", "proxy-authenticate", "accept-encoding",
    "content-length",
}

UPSTREAM = "https://api.anthropic.com"
STORE: Store | None = None   # the dashboard's store; None with --no-ui
QUIET = False                # --quiet: skip the body dump


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


class Tap(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

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
        try:
            self.forward(body, xid)
        except BaseException as e:
            tap("end", xid, error=f"{type(e).__name__}: {e}")
            raise
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
        upstream.sock.settimeout(600)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in SKIP}
        upstream.request(self.command, target.path.rstrip("/") + self.path, body=body or None, headers=headers)
        reply = upstream.getresponse()

        # 3. Stream the reply back as it arrives, printing it on the way.
        print(f"\n[{stamp()}] <<< {reply.status} {reply.reason}", flush=True)
        self.send_response(reply.status, reply.reason)
        for name, value in reply.getheaders():
            if name.lower() not in SKIP and name.lower() != "content-encoding":
                self.send_header(name, value)
        self.send_header("Connection", "close")
        self.end_headers()
        tap("response", xid, reply.status, reply.reason, reply.getheaders())
        if self.command == "HEAD":
            self.close_connection = True
            return
        while chunk := reply.read1(4096):  # read1: pass on each chunk as it lands, don't wait for 4096 bytes
            self.wfile.write(chunk)
            self.wfile.flush()
            if not QUIET:
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
            tap("feed", xid, chunk)  # after the agent has the bytes
        self.close_connection = True
        print(f"\n[{stamp()}] --- done", flush=True)


def main() -> None:
    global UPSTREAM, STORE, QUIET
    parser = argparse.ArgumentParser(description="Print and forward an agent's API traffic.")
    parser.add_argument("--port", type=int, default=8041)
    parser.add_argument("--upstream", default=UPSTREAM, help="where to forward (default: the Anthropic API)")
    parser.add_argument("--ui-port", type=int, default=8042, help="dashboard port (default: 8042)")
    parser.add_argument("--no-ui", action="store_true", help="turn the dashboard off entirely")
    parser.add_argument("--keep", type=int, default=200, help="exchanges the dashboard keeps (default: 200)")
    parser.add_argument("--quiet", action="store_true", help="don't print request and response bodies")
    args = parser.parse_args()
    UPSTREAM = args.upstream.rstrip("/")
    QUIET = args.quiet
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Tap)
    print(f"proxy listening on http://localhost:{args.port}  ->  {UPSTREAM}")
    if not args.no_ui:
        store = Store(keep=args.keep, meta={"url": f"http://localhost:{args.port}", "upstream": UPSTREAM})
        try:
            tap_ui.start(store, args.ui_port)
        except OSError as e:
            print(f"dashboard disabled: cannot listen on port {args.ui_port}: {e}", file=sys.stderr)
        else:
            STORE = store
            print(f"dashboard at       http://localhost:{args.ui_port}")
    print(f"run the agent with:  ANTHROPIC_BASE_URL=http://localhost:{args.port} claude")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
