"""tap_analyze: threading (6.3), search (7.8), stats (8.4), summaries (6.2)."""

import json
import unittest

import tap_analyze
from tap_analyze import Analyzer, QueryError, assign_threads, parse_query
from tap_store import Store
from tests.fixtures import run

SYSTEM = [{"type": "text", "text": "You are Claude Code, the main agent.", "cache_control": {"type": "ephemeral"}}]
SUB_SYSTEM = "You are a file-search sub-agent."
TOOLS = [{"name": "Read", "input_schema": {"type": "object"}}, {"name": "Task", "input_schema": {"type": "object"}}]
SUB_TOOLS = [{"name": "Read", "input_schema": {"type": "object"}}]
PROMPT = "Find every TODO comment in the repository and list them with file names."


def user(text, cache=False):
    block = {"type": "text", "text": text}
    if cache:
        block["cache_control"] = {"type": "ephemeral"}
    return {"role": "user", "content": [block]}


def body(messages, system=SYSTEM, tools=TOOLS, model="claude-opus-5-5"):
    out = {"model": model, "max_tokens": 32000, "stream": True, "system": system, "messages": messages}
    if tools:
        out["tools"] = tools
    return out


def usage(inp, out, read=0, write=0):
    return {"input_tokens": inp, "output_tokens": out, "cache_read_input_tokens": read,
            "cache_creation_input_tokens": write}


U1 = user("Read proxy.py and explain it")
R1 = {"model": "claude-opus-5-5", "stop_reason": "tool_use", "usage": usage(100, 20, read=900),
      "content": [{"type": "text", "text": "Let me read it."},
                  {"type": "tool_use", "id": "toolu_read", "name": "Read", "input": {"file_path": "/repo/proxy.py"}}]}
RESULT1 = {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_read",
                                        "content": "proxy.py: the naive agent-traffic proxy\nimport json\n"}]}
R3 = {"model": "claude-opus-5-5", "stop_reason": "tool_use", "usage": usage(50, 40, read=1500, write=200),
      "content": [{"type": "tool_use", "id": "toolu_task", "name": "Task",
                   "input": {"description": "find TODOs", "prompt": PROMPT}}]}
R4 = {"model": "claude-sonnet-5-5", "stop_reason": "end_turn", "usage": usage(300, 60),
      "content": [{"type": "text", "text": "Found 3 TODO comments."}]}
RESULT3 = {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_task",
                                        "content": [{"type": "text", "text": "Found 3 TODO comments."}]}]}


def scripted_session(store):
    """Main agent, a side-call, a Task sub-agent, a follow-up, then a compaction."""
    ids = [
        run(store, body([U1]), R1),                                                        # 1 main
        run(store, body([user("Read proxy.py and explain it")], system="Write a title.", tools=None,
                        model="claude-haiku-5-5"),
            {"model": "claude-haiku-5-5", "content": [{"type": "text", "text": "Proxy walkthrough"}],
             "usage": usage(30, 4)}),                                                      # 2 side
        run(store, body([user("Read proxy.py and explain it", cache=True),
                         {"role": "assistant", "content": R1["content"]}, RESULT1]), R3),   # 3 main, append
        run(store, body([user(PROMPT)], system=SUB_SYSTEM, tools=SUB_TOOLS, model="claude-sonnet-5-5"), R4),  # 4 sub
        run(store, body([U1, {"role": "assistant", "content": R1["content"]}, RESULT1,
                         {"role": "assistant", "content": R3["content"]}, RESULT3]),
            {"model": "claude-opus-5-5", "content": [{"type": "text", "text": "Done."}],
             "usage": usage(10, 5, read=2500)}),                                           # 5 main, append
        run(store, body([user("This session is being continued from a previous conversation. Summary: …")]),
            {"model": "claude-opus-5-5", "content": [{"type": "text", "text": "Continuing."}],
             "usage": usage(400, 8)}),                                                     # 6 main, rewritten
    ]
    xid = store.begin("GET", "/v1/models", [], b"")                                        # 7 other traffic
    store.response(xid, 200, "OK", [("content-type", "application/json")])
    store.feed(xid, b'{"data": []}')
    store.end(xid)
    ids.append(xid)
    store.flush()
    return ids


class ThreadingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = Store()
        cls.ids = scripted_session(cls.store)
        cls.summaries = {s["id"]: s for s in cls.store.snapshot()["exchanges"]}

    def thread(self, xid):
        return self.summaries[xid]["thread"]

    def test_main_append_and_new_from(self):
        t1, t3 = self.thread(1), self.thread(3)
        self.assertEqual((t1["id"], t1["role"], t1["history"], t1["turn"]), ("t1", "main", "new", 1))
        # cache_control moved onto the first message: still the same conversation
        self.assertEqual((t3["id"], t3["history"], t3["prev_id"], t3["turn"]), ("t1", "append", 1, 2))
        self.assertEqual(t3["new_from"], 2)            # U1 and the reply shown in #1 are folded
        self.assertEqual(self.summaries[3]["summary"]["request"]["new_messages"], 1)
        self.assertEqual(self.thread(5)["new_from"], 4)

    def test_side_call(self):
        t2 = self.thread(2)
        self.assertEqual((t2["id"], t2["role"], t2["history"]), ("t2", "side", "new"))

    def test_subagent_is_linked_to_its_task_call(self):
        t4 = self.thread(4)
        self.assertEqual((t4["id"], t4["role"]), ("t3", "subagent"))
        self.assertEqual(t4["spawned_by"], {"exchange_id": 3, "tool_use_id": "toolu_task"})

    def test_compaction_is_a_rewritten_history_in_the_same_thread(self):
        t6 = self.thread(6)
        self.assertEqual((t6["id"], t6["history"], t6["prev_id"], t6["new_from"]), ("t1", "rewritten", 5, 0))

    def test_other_traffic_has_no_thread(self):
        self.assertIsNone(self.thread(7))

    def test_tool_result_client_time(self):
        results = self.summaries[3]["summary"]["tool_results"]
        self.assertEqual([(r["tool_use_id"], r["name"], r["is_error"]) for r in results],
                         [("toolu_read", "Read", False)])
        self.assertIsNotNone(results[0]["client_ms"])
        self.assertGreaterEqual(results[0]["client_ms"], 0)

    def test_same_result_from_a_log(self):
        # R18: recomputing threads from the full recorded exchanges gives the same answer.
        full = [self.store.exchange(i) for i in self.ids]
        replayed = assign_threads(full)
        live = {i: self.thread(i) for i in self.ids if self.thread(i)}
        self.assertEqual(replayed, live)

    def test_parallel_agent_of_the_same_type_is_not_a_rewrite(self):
        a = Analyzer()
        first = a.on_request(1, "messages", 0, body([user("A")]))["thread"]
        a.on_end(1, 100, None)
        cleared = a.on_request(2, "messages", 200, body([user("B")]))["thread"]        # /clear
        busy = a.on_request(3, "messages", 300, body([user("C")]))["thread"]           # #2 still open
        self.assertEqual((first["id"], cleared["id"], cleared["history"]), ("t1", "t1", "rewritten"))
        self.assertEqual((busy["id"], busy["history"], busy["role"]), ("t2", "new", "unknown"))

    def test_rewrite_window_is_ten_minutes(self):
        a = Analyzer()
        a.on_request(1, "messages", 0, body([user("A")]))
        a.on_end(1, 100, None)
        later = a.on_request(2, "messages", 100 + 11 * 60 * 1000, body([user("B")]))["thread"]
        self.assertEqual((later["id"], later["history"]), ("t2", "new"))

    def test_tools_change_keeps_the_thread_and_is_flagged(self):
        a = Analyzer()
        a.on_request(1, "messages", 0, body([U1]))
        nxt = a.on_request(2, "messages", 10, body([U1, {"role": "assistant", "content": "hi"}, user("more")],
                                                   tools=TOOLS + [{"name": "WebFetch"}]))
        self.assertEqual((nxt["thread"]["id"], nxt["thread"]["history"]), ("t1", "append"))
        self.assertTrue(nxt["request"]["tools_changed"])
        self.assertFalse(nxt["request"]["system_changed"])


class SearchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = Store()
        scripted_session(cls.store)

    def search(self, q):
        return self.store.search(q)

    def test_each_hit_is_reported_once_where_it_first_appeared(self):
        result = self.search("explain it")
        self.assertEqual([(h["id"], h["loc"]) for h in result["hits"]], [(1, "m:0:0"), (2, "m:0:0")])
        self.assertEqual(result["exchanges"], [1, 2])

    def test_tool_filter_lists_only_the_calls(self):
        result = self.search("tool:Read proxy.py")
        self.assertEqual([(h["id"], h["loc"], h["where"]) for h in result["hits"]], [(1, "r:1", "tool_use")])
        hit = result["hits"][0]
        self.assertEqual(hit["match"], "proxy.py")
        self.assertTrue(hit["before"].endswith("/repo/"))

    def test_fields(self):
        self.assertEqual(self.search("thread:t3")["exchanges"], [4])
        self.assertEqual(self.search("thread:t3")["hits"], [])
        self.assertEqual(self.search("model:haiku")["exchanges"], [2])
        self.assertEqual(self.search("kind:other")["exchanges"], [7])
        self.assertEqual(self.search("stop:tool_use")["exchanges"], [1, 3])
        self.assertEqual(self.store.search("", thread="t2")["exchanges"], [2])
        self.assertEqual(self.search("is:aborted")["exchanges"], [])

    def test_regex(self):
        result = self.search("re:TODO comments?")
        self.assertEqual(sorted({(h["id"], h["where"]) for h in result["hits"]}),
                         [(3, "tool_use"), (4, "assistant"), (4, "user"), (5, "tool_result")])

    def test_case_insensitive_plain_text_and_phrases(self):
        self.assertEqual(len(self.search("FOUND 3   todo")["hits"]), 2)    # #4's reply and #5's tool_result

    def test_bad_queries_are_errors(self):
        with self.assertRaises(QueryError):
            self.search("re:(unclosed")
        with self.assertRaises(QueryError):
            self.search("is:bogus")

    def test_error_results(self):
        store = Store()
        run(store, body([U1]), R1)
        run(store, body([U1, {"role": "assistant", "content": R1["content"]},
                         {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_read",
                                                       "is_error": True, "content": "No such file"}]}]))
        store.flush()
        result = store.search("is:error")
        self.assertEqual([(h["id"], h["where"], h["tool"]) for h in result["hits"]], [(2, "tool_result", "Read")])
        self.assertEqual(store.search("tool:Read is:error")["exchanges"], [2])
        self.assertEqual(store.search("tool:Bash is:error")["exchanges"], [])

    def test_unknown_field_is_free_text(self):
        q = parse_query("foo:bar tool:Read")
        self.assertEqual((q.text, q.fields), ("foo:bar", {"tool": ["Read"]}))


class StatsTest(unittest.TestCase):
    PRICING = {"claude-opus-5": {"input": 10, "output": 50, "cache_read": 1, "cache_write": 12.5},
               "claude-sonnet": {"input": 3, "output": 15}}

    def stats(self, pricing=None):
        store = Store(pricing=pricing)
        scripted_session(store)
        return store.stats()

    def test_totals_and_cache_hit_rate(self):
        st = self.stats()
        t = st["totals"]
        self.assertEqual((t["exchanges"], t["messages"], t["other"]), (7, 6, 1))
        self.assertEqual(t["cache_read"], 900 + 1500 + 2500)
        self.assertEqual(t["input_tokens"], (100 + 900) + 30 + (50 + 1500 + 200) + 300 + (10 + 2500) + 400)
        self.assertAlmostEqual(t["cache_hit_rate"], round(4900 / t["input_tokens"], 4))
        self.assertIsNone(t["cost"])
        self.assertFalse(st["priced"])
        opus = next(m for m in st["per_model"] if m["model"] == "claude-opus-5-5")
        self.assertEqual(opus["requests"], 4)

    def test_cost_with_a_price_table(self):
        st = self.stats(self.PRICING)
        opus = (100 * 10 + 20 * 50 + 900 * 1) + (50 * 10 + 40 * 50 + 1500 * 1 + 200 * 12.5) \
            + (10 * 10 + 5 * 50 + 2500 * 1) + (400 * 10 + 8 * 50)
        sonnet = 300 * 3 + 60 * 15
        self.assertAlmostEqual(st["totals"]["cost"], (opus + sonnet) / 1e6)
        self.assertEqual(st["unpriced_models"], ["claude-haiku-5-5"])

    def test_threads_and_tools(self):
        st = self.stats()
        threads = {t["id"]: t for t in st["per_thread"]}
        self.assertEqual({k: v["role"] for k, v in threads.items()}, {"t1": "main", "t2": "side", "t3": "subagent"})
        self.assertEqual(threads["t1"]["exchanges"], 4)
        self.assertEqual(threads["t3"]["spawned_by"]["tool_use_id"], "toolu_task")
        calls = {c["id"]: c for c in st["calls"]}
        self.assertEqual((calls["toolu_read"]["exchange"], calls["toolu_read"]["result_exchange"]), (1, 3))
        self.assertEqual(calls["toolu_read"]["result_chars"], len(RESULT1["content"][0]["content"]))
        self.assertIsNotNone(calls["toolu_read"]["client_ms"])
        tools = {t["name"]: t for t in st["per_tool"]}
        self.assertEqual((tools["Read"]["calls"], tools["Task"]["calls"]), (1, 1))
        self.assertEqual([x["id"] for x in st["largest"]][:1], [5])


class SummaryTest(unittest.TestCase):
    def test_no_content_leaks_and_small(self):
        store = Store()
        big = "x" * 2000
        messages = []
        for i in range(20):
            messages.append(user(f"turn {i} {big} SECRET-PROMPT-TEXT"))
            messages.append({"role": "assistant", "content": [
                {"type": "text", "text": big}, {"type": "tool_use", "id": f"toolu_{i:02d}", "name": "Bash",
                                                "input": {"command": "ls " + big}}]})
        messages[-1] = user("last")
        system = [{"type": "text", "text": "S" * 12400}]
        reply = {"model": "claude-opus-5-5", "stop_reason": "tool_use", "usage": usage(41000, 312, read=38900),
                 "content": [{"type": "thinking", "thinking": "THINKING-TEXT " * 50},
                             {"type": "text", "text": "Let me read the proxy first. " + big},
                             {"type": "tool_use", "id": "toolu_zz", "name": "Read", "input": {"file_path": "/r/proxy.py"}}]}
        xid = run(store, body(messages, system=system, tools=[{"name": f"T{i}"} for i in range(14)]), reply)
        summary = store.snapshot()["exchanges"][0]
        text = json.dumps(summary)
        self.assertLess(len(text.encode()), 2048, text)
        for leak in ("SECRET-PROMPT-TEXT", "THINKING-TEXT", "SSSSSSSSSS", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"):
            self.assertNotIn(leak, text)
        self.assertNotIn("body", summary.get("request", {}))
        self.assertNotIn("content", summary["response"]["message"])
        self.assertNotIn("raw", summary["response"])
        s = summary["summary"]
        self.assertEqual((s["request"]["messages"], s["request"]["tools"], s["request"]["system_chars"]), (40, 14, 12400))
        self.assertTrue(s["response"]["preview"].startswith("Let me read the proxy first."))
        self.assertEqual(s["response"]["tool_calls"], [{"id": "toolu_zz", "name": "Read", "arg": "/r/proxy.py"}])
        self.assertEqual(store.exchange(xid)["request"]["body"]["messages"][0]["content"][0]["text"][:6], "turn 0")

    def test_body_scan_redaction(self):
        store = Store()
        key = "sk-ant-api03-" + "Q" * 40
        xid = run(store, body([user(f"my key is {key} please"), {"role": "assistant", "content": "ok"},
                               {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t",
                                                             "content": f"ANTHROPIC_API_KEY={key}\n"}]}]))
        store.flush()
        everything = store.snapshot_json() + store.exchange_json(xid) + json.dumps(store.search("my key"))
        self.assertNotIn("Q" * 20, everything)
        self.assertIn("sk-ant-api03…[redacted]", everything)
        self.assertNotIn("SECRET-TOKEN", everything)       # header redaction still applies


class HelpersTest(unittest.TestCase):
    def test_digest_ignores_cache_control(self):
        self.assertEqual(tap_analyze.digest(user("a", cache=True)), tap_analyze.digest(user("a")))
        self.assertNotEqual(tap_analyze.digest(user("a")), tap_analyze.digest(user("b")))

    def test_key_arg(self):
        self.assertEqual(tap_analyze.key_arg("Bash", {"command": "ls -la\nrm x", "description": "d"}), "ls -la …")
        self.assertEqual(tap_analyze.key_arg("Unknown", {"n": 1, "path": "/x"}), "/x")
        self.assertEqual(tap_analyze.key_arg("TodoWrite", {"todos": [1, 2]}), "2 items")

    def test_pricing_validation(self):
        with self.assertRaises(ValueError):
            tap_analyze.validate_pricing({"claude": {"input": "cheap"}})
        self.assertEqual(tap_analyze.price_for("claude-opus-5-5", {"claude": {"input": 1}, "claude-opus": {"input": 2}}),
                         {"input": 2})


if __name__ == "__main__":
    unittest.main()
