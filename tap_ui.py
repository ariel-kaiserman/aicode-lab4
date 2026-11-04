"""tap_ui.py: the dashboard's HTTP server.

    GET /                               dashboard.html (with a per-response CSP nonce)
    GET /api/exchanges                  summaries of every retained exchange (SPEC 6.2)
    GET /api/exchanges/{id}             one full exchange
    GET /api/exchanges/{id}/raw         its raw response, as text/plain
    GET /api/search?q=...&thread=...    server-side search (7.8)
    GET /api/stats                      session stats, with the per-call tool table (8.4)
    GET /api/export.jsonl               every retained exchange, in the log format (8.6)
    GET /events                         live events, as Server-Sent Events

Binds to 127.0.0.1 only and rejects any Host header other than
localhost:<port> / 127.0.0.1:<port>, which blocks DNS-rebinding attacks.
"""

from __future__ import annotations

import json
import os
import queue
import re
import secrets
import sys
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from tap_analyze import QueryError
from tap_store import RESYNC, Store, log_once

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
KEEPALIVE_SECONDS = 15
DETAIL_RE = re.compile(r"^/api/exchanges/(\d+)(/raw)?$")
JSON = "application/json; charset=utf-8"
TEXT = "text/plain; charset=utf-8"


class DashboardHandler(BaseHTTPRequestHandler):
    store: Store  # set on the subclass made by make_server
    server_version = "tap-dashboard"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass

    def _host_ok(self) -> bool:
        port = self.server.server_address[1]
        return (self.headers.get("host") or "").lower() in {f"localhost:{port}", f"127.0.0.1:{port}"}

    def do_GET(self) -> None:
        if not self._host_ok():
            self._send(403, b"forbidden: unexpected Host header\n", TEXT)
            return
        url = urlsplit(self.path)
        path, query = url.path, parse_qs(url.query)
        detail = DETAIL_RE.match(path)
        if path in ("/", "/index.html"):
            self._page()
        elif path == "/api/exchanges":
            self._send(200, self.store.snapshot_json().encode("utf-8"), JSON)
        elif detail:
            xid = int(detail.group(1))
            text = self.store.raw_text(xid) if detail.group(2) else self.store.exchange_json(xid)
            if text is None:
                self._json(404, {"error": f"exchange #{xid} is not in memory"})
            else:
                self._send(200, text.encode("utf-8"), TEXT if detail.group(2) else JSON)
        elif path == "/api/search":
            self._search(query)
        elif path == "/api/stats":
            self._json(200, self.store.stats(with_calls=True))
        elif path == "/api/export.jsonl":
            self._export()
        elif path == "/events":
            self._events()
        else:
            self._send(404, b"not found\n", TEXT)

    def _json(self, code: int, obj: object) -> None:
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"), JSON)

    def _send(self, code: int, body: bytes, ctype: str, extra: dict[str, str] | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _search(self, query: dict[str, list[str]]) -> None:
        q = (query.get("q") or [""])[0]
        thread = (query.get("thread") or [""])[0] or None
        try:
            self._json(200, self.store.search(q, thread=thread))
        except QueryError as e:
            self._json(400, {"q": q, "error": str(e)})

    def _export(self) -> None:
        name = "session-" + datetime.now().strftime("%Y-%m-%dT%H-%M-%S") + ".jsonl"
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Connection", "close")
        self.end_headers()
        for line in self.store.export_lines():
            self.wfile.write(line.encode("utf-8"))
        self.close_connection = True

    def _page(self) -> None:
        with open(HTML_PATH, "rb") as f:
            html = f.read()
        # Only the page's own <script> runs: anything injected lacks the nonce.
        nonce = secrets.token_urlsafe(18)
        html = html.replace(b"<script>", f'<script nonce="{nonce}">'.encode(), 1)
        csp = (f"default-src 'none'; script-src 'nonce-{nonce}'; style-src 'unsafe-inline'; "
               "connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; "
               "frame-ancestors 'none'")
        self._send(200, html, "text/html; charset=utf-8",
                   {"Content-Security-Policy": csp, "Referrer-Policy": "no-referrer"})

    def _events(self) -> None:
        sub = self.store.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"retry: 1000\n: connected\n\n")
            self.wfile.flush()
            while not sub.dead:
                try:
                    message = sub.queue.get(timeout=KEEPALIVE_SECONDS)
                except queue.Empty:
                    message = b": keepalive\n\n"
                if sub.dead:
                    break
                self.wfile.write(message)
                self.wfile.flush()
            if sub.resync:
                # Dropped as too slow (R6): say why, then close. The browser
                # reconnects and loads a fresh snapshot.
                self.wfile.write(RESYNC)
                self.wfile.flush()
        except OSError:
            pass  # browser went away
        finally:
            self.store.unsubscribe(sub)
            self.close_connection = True


class DashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address) -> None:  # type: ignore[override]
        exc = sys.exc_info()[1]
        if exc is not None and not isinstance(exc, (ConnectionError, TimeoutError)):
            log_once(exc, "dashboard server")


def make_server(store: Store, port: int, host: str = "127.0.0.1") -> DashboardServer:
    handler = type("BoundDashboardHandler", (DashboardHandler,), {"store": store})
    return DashboardServer((host, port), handler)


def start(store: Store, port: int) -> DashboardServer:
    server = make_server(store, port)
    threading.Thread(target=server.serve_forever, name="tap-ui", daemon=True).start()
    return server
