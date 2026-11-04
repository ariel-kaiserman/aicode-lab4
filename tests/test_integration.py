"""End to end: agent -> proxy -> fake upstream, with the dashboard watching."""

import http.client
import io
import json
import socket
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import proxy
import tap_ui
from tap_store import Store
from tests.fixtures import EXPECTED, STREAM

TOKEN = "Bearer sk-ant-oat01-INTEGRATION-SECRET-abcdefghijklmnop"
BODY = json.dumps({"model": "claude-opus-5-5", "stream": True, "tools": [{"name": "Read"}],
                   "messages": [{"role": "user", "content": "read the proxy"}]}).encode()


class FakeUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen = []
    mode = "stream"          # stream | overloaded | pause (half the stream, a long silence, the rest)
    pause = 2.0

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        FakeUpstream.seen.append((self.path, dict(self.headers), body))
        if FakeUpstream.mode == "overloaded":
            err = json.dumps({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}).encode()
            self.send_response(529)
            self.send_header("content-type", "application/json")
            self.send_header("request-id", "req_overloaded_1")
            self.send_header("content-length", str(len(err)))
            self.end_headers()
            self.wfile.write(err)
            return
        data = STREAM.encode()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream; charset=utf-8")
        self.send_header("request-id", "req_fake_123")
        self.send_header("anthropic-ratelimit-tokens-remaining", "12345")
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            for i in range(0, len(data), 61):
                if FakeUpstream.mode == "pause" and i == 61 * 8:
                    time.sleep(FakeUpstream.pause)
                self.wfile.write(data[i:i + 61])
                self.wfile.flush()
                time.sleep(0.002)
        except OSError:
            pass
        self.close_connection = True


class QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        pass   # the proxy re-raises a client hang-up (as v0 did); keep test output clean


def serve(server):
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        proxy.WATCH = proxy.ClientWatch()

    @classmethod
    def tearDownClass(cls):
        proxy.WATCH = None

    def setUp(self):
        FakeUpstream.mode = "stream"
        self._stdout = sys.stdout
        sys.stdout = io.StringIO()          # the proxy's terminal dump
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstream)
        up_port = serve(self.upstream)
        proxy.UPSTREAM = f"http://127.0.0.1:{up_port}"
        proxy.QUIET = False
        proxy.STORE = self.store = Store()
        self.proxy = QuietServer(("127.0.0.1", 0), proxy.Tap)
        self.proxy_port = serve(self.proxy)
        self.ui = tap_ui.make_server(self.store, 0)
        self.ui_port = serve(self.ui)

    def tearDown(self):
        for s in (self.proxy, self.ui, self.upstream):
            s.shutdown()
            s.server_close()
        proxy.STORE = None
        sys.stdout = self._stdout

    def ui_get(self, path, host=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.ui_port, timeout=5)
        conn.request("GET", path, headers={"Host": host} if host else {})
        return conn.getresponse()

    def call_proxy(self, body):
        conn = http.client.HTTPConnection("127.0.0.1", self.proxy_port, timeout=10)
        conn.request("POST", "/v1/messages?beta=true", body=body,
                     headers={"authorization": TOKEN, "content-type": "application/json"})
        resp = conn.getresponse()
        return resp.status, resp.read()

    def test_traffic_is_forwarded_unchanged_and_shown_live(self):
        events_resp = self.ui_get("/events")
        self.assertEqual(events_resp.status, 200)
        self.assertIn("text/event-stream", events_resp.getheader("content-type"))
        for _ in range(50):                 # wait until the SSE handler has subscribed
            if self.store.subscriber_count():
                break
            time.sleep(0.01)

        body = json.dumps({"model": "claude-opus-5-5", "stream": True,
                           "messages": [{"role": "user", "content": "<script>alert(1)</script>"}]}).encode()
        status, reply = self.call_proxy(body)
        self.assertEqual(status, 200)
        self.assertEqual(reply, STREAM.encode())                     # byte for byte
        self.assertEqual(FakeUpstream.seen[-1][2], body)
        self.assertEqual(FakeUpstream.seen[-1][1].get("authorization"), TOKEN)  # credentials still forwarded

        events = []
        name = None
        while True:
            line = events_resp.fp.readline().decode().rstrip("\n")
            if line.startswith("event: "):
                name = line[7:]
            elif line.startswith("data: "):
                events.append((name, json.loads(line[6:])))
                if name == "exchange_end":
                    break
        names = [n for n, _ in events if n != "stats"]
        self.assertEqual(names[:3], ["exchange_start", "exchange_meta", "response_start"])
        self.assertIn("block_delta", names)
        text = "".join(p["text"] for n, p in events if n == "block_delta" and p["delta_type"] == "text")
        self.assertEqual(text, EXPECTED["content"][1]["text"])
        end = events[-1][1]
        self.assertNotIn("content", end["response"]["message"])           # a summary, not the full exchange
        self.assertEqual(end["summary"]["response"]["tool_calls"][0]["name"], "Read")
        self.assertEqual(end["upstream"]["request_id"], "req_fake_123")
        self.assertEqual(end["upstream"]["ratelimit"]["tokens_remaining"], 12345)
        self.assertIsNotNone(end["timing"]["ttft_ms"])

        snap = self.ui_get("/api/exchanges").read().decode()
        self.assertNotIn("INTEGRATION-SECRET", snap)
        self.assertEqual(json.loads(snap)["exchanges"][0]["state"], "done")
        detail = self.ui_get(f"/api/exchanges/{end['id']}").read().decode()
        self.assertNotIn("INTEGRATION-SECRET", detail)
        self.assertEqual(json.loads(detail)["response"]["message"], EXPECTED)
        raw = self.ui_get(f"/api/exchanges/{end['id']}/raw")
        self.assertIn("text/plain", raw.getheader("content-type"))
        self.assertEqual(raw.read().decode(), STREAM)
        self.assertEqual(self.ui_get("/api/exchanges/999").status, 404)
        self.assertIn(">>> POST /v1/messages?beta=true", sys.stdout.getvalue())   # terminal dump unchanged

    def test_page_is_served_with_csp(self):
        resp = self.ui_get("/")
        html = resp.read().decode()
        self.assertEqual(resp.status, 200)
        csp = resp.getheader("content-security-policy")
        self.assertIn("script-src 'nonce-", csp)
        nonce = csp.split("'nonce-")[1].split("'")[0]
        self.assertIn(f'<script nonce="{nonce}">', html)

    def test_dns_rebinding_host_is_rejected(self):
        self.assertEqual(self.ui_get("/api/exchanges", host="evil.example:%d" % self.ui_port).status, 403)
        self.assertEqual(self.ui_get("/api/exchanges", host="localhost:%d" % self.ui_port).status, 200)

    def snapshot(self):
        self.store.flush()
        return json.loads(self.ui_get("/api/exchanges").read())

    # -- R11: every request produces exactly one Exchange ------------------------

    def test_unreachable_upstream_is_a_502_exchange(self):
        proxy.UPSTREAM = f"http://127.0.0.1:{free_port()}"
        status, _ = self.call_proxy(b"{}")
        self.assertEqual(status, 502)
        exchanges = self.snapshot()["exchanges"]
        self.assertEqual(len(exchanges), 1)
        ex = exchanges[0]
        self.assertEqual((ex["status"], ex["state"]), (502, "error"))
        self.assertIn("cannot reach", ex["response"]["error"]["message"])
        self.assertEqual(ex["response"]["error"]["source"], "proxy")

    def test_upstream_529_is_one_error_exchange(self):
        FakeUpstream.mode = "overloaded"
        status, _ = self.call_proxy(BODY)
        self.assertEqual(status, 529)
        exchanges = self.snapshot()["exchanges"]
        self.assertEqual(len(exchanges), 1)
        ex = exchanges[0]
        self.assertEqual((ex["status"], ex["state"]), (529, "error"))
        self.assertEqual(ex["response"]["error"], {"source": "upstream", "type": "overloaded_error",
                                                   "message": "Overloaded"})
        self.assertEqual(ex["upstream"]["request_id"], "req_overloaded_1")

    def open_and_hang_up(self, after: bytes):
        """Send a request on a raw socket, read until `after` arrives, then close."""
        sock = socket.create_connection(("127.0.0.1", self.proxy_port), timeout=10)
        sock.sendall(b"POST /v1/messages HTTP/1.1\r\nHost: x\r\ncontent-type: application/json\r\n"
                     + f"content-length: {len(BODY)}\r\n\r\n".encode() + BODY)
        seen = b""
        while after not in seen:
            chunk = sock.recv(4096)
            if not chunk:
                break
            seen += chunk
        sock.close()
        return seen

    def wait_for_state(self, state, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            exchanges = self.snapshot()["exchanges"]
            if exchanges and exchanges[0]["state"] == state:
                return exchanges
            time.sleep(0.02)
        return self.snapshot()["exchanges"]

    def test_client_abort_during_a_silence_is_seen_within_a_second(self):
        # R12: the agent hangs up while the model is silent; no write fails yet.
        FakeUpstream.mode, FakeUpstream.pause = "pause", 3.0
        self.open_and_hang_up(b"content_block_delta")
        began = time.monotonic()
        exchanges = self.wait_for_state("aborted", 1.5)
        self.assertLess(time.monotonic() - began, 1.0)
        self.assertEqual(len(exchanges), 1)
        ex = exchanges[0]
        self.assertEqual(ex["state"], "aborted")
        self.assertEqual(ex["response"]["error"]["source"], "client")
        detail = json.loads(self.ui_get(f"/api/exchanges/{ex['id']}").read())
        self.assertEqual(detail["response"]["message"]["content"][0]["type"], "thinking")   # partial message kept

    def test_client_abort_on_write_is_one_aborted_exchange(self):
        FakeUpstream.mode, FakeUpstream.pause = "pause", 0.5
        watch, proxy.WATCH = proxy.WATCH, None        # only the failing write can notice
        try:
            self.open_and_hang_up(b"content_block_delta")
            exchanges = self.wait_for_state("aborted", 3.0)
        finally:
            proxy.WATCH = watch
        self.assertEqual(len(exchanges), 1)
        self.assertEqual(exchanges[0]["state"], "aborted")
        self.assertEqual(exchanges[0]["response"]["error"]["source"], "client")

    # -- v2 endpoints --------------------------------------------------------------

    def test_search_stats_and_export_endpoints(self):
        self.call_proxy(BODY)
        self.store.flush()
        hits = json.loads(self.ui_get("/api/search?q=tool:Read%20pr%C3%B3xy").read())
        self.assertEqual([(h["where"], h["tool"]) for h in hits["hits"]], [("tool_use", "Read")])
        self.assertEqual(hits["exchanges"], [1])
        bad = self.ui_get("/api/search?q=re:(unclosed")
        self.assertEqual(bad.status, 400)
        self.assertIn("error", json.loads(bad.read()))
        stats = json.loads(self.ui_get("/api/stats").read())
        self.assertEqual(stats["totals"]["exchanges"], 1)
        self.assertEqual(stats["calls"][0]["name"], "Read")
        export = self.ui_get("/api/export.jsonl")
        self.assertIn("attachment", export.getheader("content-disposition"))
        lines = [json.loads(line) for line in export.read().decode().splitlines()]
        self.assertEqual([line["type"] for line in lines], ["session", "exchange"])
        self.assertNotIn("INTEGRATION-SECRET", json.dumps(lines))
        self.assertEqual(lines[1]["response"]["message"], EXPECTED)


if __name__ == "__main__":
    unittest.main()
