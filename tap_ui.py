"""tap_ui.py: the dashboard's HTTP server.

    GET /               dashboard.html (with a per-response CSP nonce)
    GET /api/exchanges  snapshot of every retained exchange, as JSON
    GET /events         live events, as Server-Sent Events

Binds to 127.0.0.1 only and rejects any Host header other than
localhost:<port> / 127.0.0.1:<port>, which blocks DNS-rebinding attacks.
"""

from __future__ import annotations

import os
import queue
import secrets
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tap_store import Store, log_once

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
KEEPALIVE_SECONDS = 15


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
            self._send(403, b"forbidden: unexpected Host header\n", "text/plain; charset=utf-8")
            return
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._page()
        elif path == "/api/exchanges":
            self._send(200, self.store.snapshot_json().encode("utf-8"), "application/json; charset=utf-8")
        elif path == "/events":
            self._events()
        else:
            self._send(404, b"not found\n", "text/plain; charset=utf-8")

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
                    break  # dropped as too slow: close, the browser reconnects and resyncs
                self.wfile.write(message)
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
