"""tap_log: the JSONL session log and reopening it (SPEC 10.1, R17, R18, E1-E3)."""

import json
import os
import stat
import tempfile
import unittest

import tap_log
from tap_store import Store
from tests.fixtures import run
from tests.test_analyze import body, scripted_session, user


class LogTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "session.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, keep=200):
        log = tap_log.LogWriter(self.path, {"started_at": "2026-11-04T13:40:02.000Z", "upstream": "https://x"})
        store = Store(keep=keep, log=log, session={"started_at": "2026-11-04T13:40:02.000Z"})
        return store, log

    def test_round_trip_gives_the_same_snapshot(self):
        store, log = self.record()
        scripted_session(store)
        self.assertTrue(log.flush())
        live = store.snapshot()
        session, exchanges = tap_log.read_log(self.path)
        self.assertEqual((session["version"], session["upstream"]), (2, "https://x"))
        self.assertEqual(len(exchanges), 7)
        reopened = Store(keep=len(exchanges), mode="recorded", session=session)
        reopened.load(exchanges)
        again = reopened.snapshot()
        self.assertEqual(again["exchanges"], live["exchanges"])
        self.assertEqual(again["stats"], live["stats"])
        self.assertEqual(again["session"]["mode"], "recorded")
        self.assertEqual(reopened.stats(), store.stats())       # acceptance item 14
        hits = [{k: v for k, v in s.search("proxy.py").items() if k != "took_ms"} for s in (store, reopened)]
        self.assertEqual(hits[0], hits[1])

    def test_file_is_private_and_redacted(self):
        store, log = self.record()
        key = "sk-ant-api03-" + "K" * 40
        run(store, body([user(f"key {key}")]), headers=[("authorization", "Bearer sk-ant-oat01-HEADER-SECRET-0123456789"),
                                                       ("x-api-key", "sk-ant-api03-XAPI-SECRET-abcdefghijklmn")])
        store.flush()
        self.assertTrue(log.flush())
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)
        with open(self.path, encoding="utf-8") as f:
            text = f.read()
        for secret in ("HEADER-SECRET", "XAPI-SECRET", "K" * 20):
            self.assertNotIn(secret, text)
        self.assertIn("…[redacted]", text)
        lines = [json.loads(line) for line in text.splitlines()]
        self.assertEqual([x["type"] for x in lines], ["session", "exchange"])

    def test_evicted_exchanges_are_still_logged(self):
        store, log = self.record(keep=2)
        open_id = store.begin("POST", "/v1/messages", [], json.dumps(body([user("slow")])).encode())
        for i in range(4):
            run(store, body([user(f"q{i}")]))
        store.end(open_id)                      # evicted from memory before it ended
        store.flush()
        self.assertTrue(log.flush())
        _, exchanges = tap_log.read_log(self.path)
        self.assertEqual(sorted(e["id"] for e in exchanges), [1, 2, 3, 4, 5])
        self.assertEqual(store.snapshot()["evicted_before"], 3)

    def test_existing_file_is_not_overwritten(self):
        with open(self.path, "w") as f:
            f.write("precious\n")
        with self.assertRaises(FileExistsError):
            tap_log.LogWriter(self.path, {})

    def test_default_directory_gets_a_gitignore(self):
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            log = tap_log.LogWriter(tap_log.default_path(), {})
            self.assertTrue(log.path.startswith("tap-logs" + os.sep))
            with open(os.path.join("tap-logs", ".gitignore")) as f:
                self.assertIn("*", f.read().split())
        finally:
            os.chdir(cwd)

    def test_torn_last_line_is_skipped(self):
        store, log = self.record()
        run(store, body([user("a")]))
        store.flush()
        log.flush()
        with open(self.path, "a", encoding="utf-8") as f:
            f.write('{"type": "exchange", "id": 9, "trunc')
        _, exchanges = tap_log.read_log(self.path)
        self.assertEqual([e["id"] for e in exchanges], [1])

    def test_not_a_log(self):
        with open(self.path, "w") as f:
            f.write('{"hello": 1}\n')
        with self.assertRaises(ValueError):
            tap_log.read_log(self.path)


if __name__ == "__main__":
    unittest.main()
