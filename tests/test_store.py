import json
import threading
import time
import unittest

from tap_store import Store, classify, redact_headers
from tests.fixtures import EXPECTED, STREAM

TOKEN = "Bearer sk-ant-oat01-THIS-IS-THE-SECRET-TOKEN-0123456789"
API_KEY = "sk-ant-api03-ANOTHER-SECRET-VALUE-abcdefghij"
COOKIE = "session=cookie-secret-value-zzzzzzzz"
SET_COOKIE = "sid=response-cookie-secret-yyyyyyyy; Path=/"
SECRETS = ["THIS-IS-THE-SECRET-TOKEN", "ANOTHER-SECRET-VALUE", "cookie-secret-value", "response-cookie-secret"]

REQUEST_HEADERS = [
    ("Authorization", TOKEN), ("X-Api-Key", API_KEY), ("Cookie", COOKIE),
    ("anthropic-beta", "oauth-2025-04-20"), ("content-type", "application/json"),
]
BODY = json.dumps({"model": "claude-opus-5-5", "max_tokens": 100, "stream": True,
                   "messages": [{"role": "user", "content": "<script>alert(1)</script>"}]}).encode()
SSE_HEADERS = [("content-type", "text/event-stream; charset=utf-8"), ("set-cookie", SET_COOKIE)]


def drain(sub):
    out = []
    while not sub.queue.empty():
        out.append(sub.queue.get_nowait().decode())
    return out


def run_exchange(store, chunks=(STREAM.encode(),), status=200, headers=SSE_HEADERS, path="/v1/messages?beta=true"):
    xid = store.begin("POST", path, REQUEST_HEADERS, BODY)
    store.response(xid, status, "OK", headers)
    for chunk in chunks:
        store.feed(xid, chunk)
    store.end(xid)
    return xid


class RedactionTest(unittest.TestCase):
    def test_header_values_are_cut_to_a_prefix(self):
        out = redact_headers(REQUEST_HEADERS)
        self.assertEqual(out["authorization"], TOKEN[:12] + "…[redacted]")
        self.assertEqual(out["anthropic-beta"], "oauth-2025-04-20")
        self.assertTrue(redact_headers([("x-api-key", "short")])["x-api-key"].endswith("…[redacted]"))
        self.assertNotIn("short", redact_headers([("x-api-key", "short")])["x-api-key"])

    def test_credentials_never_reach_snapshot_or_events(self):
        store = Store()
        sub = store.subscribe()
        run_exchange(store)
        self.assertTrue(store.flush())
        everything = store.snapshot_json() + "".join(drain(sub))
        for secret in SECRETS:
            self.assertNotIn(secret, everything)
        self.assertIn("…[redacted]", everything)


class StoreTest(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(classify("POST", "/v1/messages?beta=true"), "messages")
        self.assertEqual(classify("POST", "/v1/messages/count_tokens?beta=true"), "count_tokens")
        self.assertEqual(classify("GET", "/v1/models"), "other")
        self.assertEqual(classify("HEAD", "/"), "other")

    def test_reassembled_exchange_in_snapshot(self):
        store = Store()
        run_exchange(store, chunks=[STREAM.encode()[i:i + 3] for i in range(0, len(STREAM.encode()), 3)])
        ex = store.snapshot()["exchanges"][0]
        self.assertEqual(ex["state"], "done")
        self.assertEqual(ex["status"], 200)
        self.assertEqual(ex["kind"], "messages")
        self.assertEqual(ex["response"]["message"], EXPECTED)
        self.assertEqual(ex["response"]["raw"], STREAM)
        self.assertEqual(ex["request"]["body"]["messages"][0]["content"], "<script>alert(1)</script>")
        self.assertIsNotNone(ex["ended_at"])

    def test_event_sequence_and_seq_numbers(self):
        store = Store()
        sub = store.subscribe()
        xid = run_exchange(store)
        store.flush()
        events = [(m.split("\n")[0][len("event: "):], json.loads(m.split("\n")[1][len("data: "):])) for m in drain(sub)]
        names = [n for n, _ in events]
        self.assertEqual(names[:3], ["exchange_start", "response_start", "message_start"])
        self.assertEqual(names[-1], "exchange_end")
        self.assertEqual([p["seq"] for _, p in events], list(range(1, len(events) + 1)))
        self.assertTrue(all(p["id"] == xid for _, p in events))
        self.assertEqual(events[0][1]["state"], "pending")
        self.assertEqual(events[-1][1]["seq"], store.snapshot()["exchanges"][0]["seq"])

    def test_snapshot_mid_stream_has_matching_seq(self):
        store = Store()
        sub = store.subscribe()
        data = STREAM.encode()
        xid = store.begin("POST", "/v1/messages", REQUEST_HEADERS, BODY)
        store.response(xid, 200, "OK", SSE_HEADERS)
        store.feed(xid, data[: len(data) // 2])
        snap = store.snapshot()["exchanges"][0]
        self.assertEqual(snap["state"], "streaming")
        store.flush()
        self.assertEqual(len(drain(sub)), snap["seq"])  # every event so far is reflected in the snapshot
        store.feed(xid, data[len(data) // 2:])
        store.end(xid)
        self.assertEqual(store.snapshot()["exchanges"][0]["response"]["message"], EXPECTED)

    def test_eviction_past_keep(self):
        store = Store(keep=3)
        ids = [run_exchange(store) for _ in range(5)]
        self.assertEqual([e["id"] for e in store.snapshot()["exchanges"]], ids[2:])
        store.feed(ids[0], b"data: {}\n\n")  # an evicted id is ignored, not an error
        store.end(ids[0])

    def test_slow_subscriber_is_dropped_without_blocking(self):
        store = Store(queue_size=1000)
        slow = store.subscribe()          # never reads
        fast = store.subscribe()          # drained between batches
        received = []

        xid = store.begin("POST", "/v1/messages", REQUEST_HEADERS, BODY)
        store.response(xid, 200, "OK", SSE_HEADERS)
        line = 'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}\n\n'
        elapsed = 0.0
        for _ in range(15):
            began = time.perf_counter()
            for _ in range(100):
                store.feed(xid, line.encode())
            elapsed += time.perf_counter() - began
            self.assertTrue(store.flush())
            received += drain(fast)
        store.end(xid)
        self.assertTrue(store.flush())
        received += drain(fast)

        self.assertLess(elapsed, 1.0)   # publishing never waited on the stuck client
        self.assertTrue(slow.dead)
        self.assertFalse(fast.dead)
        self.assertEqual(store.subscriber_count(), 1)
        self.assertEqual(len(received), 1 + 1 + 1500 + 1)   # start, response_start, deltas, end

    def test_raw_is_capped_but_message_complete(self):
        store = Store(raw_cap=200)
        run_exchange(store)
        resp = store.snapshot()["exchanges"][0]["response"]
        self.assertTrue(resp["raw_truncated"])
        self.assertIn("[truncated", resp["raw"])
        self.assertLess(len(resp["raw"].encode()), 400)
        self.assertEqual(resp["message"], EXPECTED)

    def test_http_error_body(self):
        store = Store()
        body = json.dumps({"type": "error", "error": {"type": "overloaded_error", "message": "Overloaded"}}).encode()
        run_exchange(store, chunks=[body], status=529, headers=[("content-type", "application/json")])
        ex = store.snapshot()["exchanges"][0]
        self.assertEqual(ex["state"], "error")
        self.assertEqual(ex["response"]["error"], {"type": "overloaded_error", "message": "Overloaded"})

    def test_non_streaming_message(self):
        store = Store()
        body = json.dumps(EXPECTED).encode()
        run_exchange(store, chunks=[body[:10], body[10:]], headers=[("content-type", "application/json")])
        ex = store.snapshot()["exchanges"][0]
        self.assertEqual((ex["state"], ex["response"]["message"]), ("done", EXPECTED))

    def test_truncated_stream_is_flagged(self):
        store = Store()
        data = STREAM.encode()
        run_exchange(store, chunks=[data[: len(data) // 2]])
        ex = store.snapshot()["exchanges"][0]
        self.assertEqual(ex["state"], "error")
        self.assertEqual(ex["response"]["error"]["type"], "incomplete")

    def test_end_is_idempotent_and_proxy_error_kept(self):
        store = Store()
        xid = store.begin("POST", "/v1/messages", REQUEST_HEADERS, BODY)
        store.end(xid, error="proxy: cannot reach upstream", status=502)
        store.end(xid)
        ex = store.snapshot()["exchanges"][0]
        self.assertEqual((ex["status"], ex["state"]), (502, "error"))
        self.assertEqual(ex["response"]["error"]["message"], "proxy: cannot reach upstream")

    def test_non_json_request_body(self):
        store = Store()
        xid = store.begin("POST", "/backend-api/codex/responses", [], b"\xffnot json")
        store.end(xid)
        self.assertIn("_raw", store.snapshot()["exchanges"][0]["request"]["body"])

    def test_parallel_exchanges_do_not_mix(self):
        store = Store()
        data = STREAM.encode()

        def worker():
            run_exchange(store, chunks=[data[i:i + 17] for i in range(0, len(data), 17)])
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        exchanges = store.snapshot()["exchanges"]
        self.assertEqual(len(exchanges), 8)
        for ex in exchanges:
            self.assertEqual(ex["response"]["message"], EXPECTED)


if __name__ == "__main__":
    unittest.main()
