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


class FakeUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    seen = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length") or 0))
        FakeUpstream.seen.append((self.path, dict(self.headers), body))
        data = STREAM.encode()
        self.send_response(200)
        self.send_header("content-type", "text/event-stream; charset=utf-8")
        self.send_header("Connection", "close")
        self.end_headers()
        for i in range(0, len(data), 61):
            self.wfile.write(data[i:i + 61])
            self.wfile.flush()
            time.sleep(0.002)
        self.close_connection = True


def serve(server):
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server.server_address[1]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self._stdout = sys.stdout
        sys.stdout = io.StringIO()          # the proxy's terminal dump
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), FakeUpstream)
        up_port = serve(self.upstream)
        proxy.UPSTREAM = f"http://127.0.0.1:{up_port}"
        proxy.QUIET = False
        proxy.STORE = self.store = Store()
        self.proxy = ThreadingHTTPServer(("127.0.0.1", 0), proxy.Tap)
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
        names = [n for n, _ in events]
        self.assertEqual(names[:2], ["exchange_start", "response_start"])
        self.assertIn("block_delta", names)
        text = "".join(p["text"] for n, p in events if n == "block_delta" and p["delta_type"] == "text")
        self.assertEqual(text, EXPECTED["content"][1]["text"])
        self.assertEqual(events[-1][1]["response"]["message"], EXPECTED)

        snap = self.ui_get("/api/exchanges").read().decode()
        self.assertNotIn("INTEGRATION-SECRET", snap)
        self.assertEqual(json.loads(snap)["exchanges"][0]["state"], "done")
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

    def test_unreachable_upstream_is_a_502_exchange(self):
        proxy.UPSTREAM = f"http://127.0.0.1:{free_port()}"
        status, _ = self.call_proxy(b"{}")
        self.assertEqual(status, 502)
        ex = json.loads(self.ui_get("/api/exchanges").read())["exchanges"][-1]
        self.assertEqual((ex["status"], ex["state"]), (502, "error"))
        self.assertIn("cannot reach", ex["response"]["error"]["message"])


if __name__ == "__main__":
    unittest.main()
