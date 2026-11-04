import random
import unittest

from tap_stream import MessageStream
from tests.fixtures import ERROR_STREAM, EXPECTED, STREAM


def run(chunks):
    parser = MessageStream()
    events = []
    for chunk in chunks:
        events += parser.feed(chunk)
    events += parser.close()
    return parser, events


def split_at(data: bytes, cuts):
    cuts = sorted(set(cuts))
    return [data[a:b] for a, b in zip([0, *cuts], [*cuts, len(data)])]


class StreamParserTest(unittest.TestCase):
    data = STREAM.encode("utf-8")

    def test_whole_stream(self):
        parser, _ = run([self.data])
        self.assertEqual(parser.message, EXPECTED)
        self.assertTrue(parser.finished)
        self.assertIsNone(parser.error)

    def test_one_byte_at_a_time(self):
        # Splits inside every multi-byte UTF-8 sequence and every line.
        parser, _ = run([self.data[i:i + 1] for i in range(len(self.data))])
        self.assertEqual(parser.message, EXPECTED)

    def test_random_split_points(self):
        rng = random.Random(4)
        for _ in range(200):
            cuts = [rng.randrange(1, len(self.data)) for _ in range(rng.randrange(1, 40))]
            parser, _ = run(split_at(self.data, cuts))
            self.assertEqual(parser.message, EXPECTED)

    def test_splits_inside_utf8_sequences(self):
        # Cut right after the first byte of each multi-byte character.
        cuts = [i + 1 for i, b in enumerate(self.data) if b >= 0xC0]
        self.assertGreater(len(cuts), 3)
        parser, _ = run(split_at(self.data, cuts))
        self.assertEqual(parser.message, EXPECTED)

    def test_crlf_line_endings(self):
        parser, _ = run([STREAM.replace("\n", "\r\n").encode()[i:i + 7] for i in range(0, len(self.data) * 2, 7)])
        self.assertEqual(parser.message, EXPECTED)

    def test_missing_final_blank_line_is_flushed_on_close(self):
        parser, _ = run([self.data.rstrip(b"\n")])
        self.assertTrue(parser.finished)
        self.assertEqual(parser.message, EXPECTED)

    def test_live_events(self):
        _, events = run([self.data[i:i + 5] for i in range(0, len(self.data), 5)])
        names = [name for name, _ in events]
        self.assertEqual(names[0], "message_start")
        self.assertEqual(names.count("block_start"), 3)
        self.assertEqual(names.count("block_stop"), 3)
        self.assertEqual(names[-1], "message_delta")
        self.assertNotIn("ping", names)

        by_index = {}
        for name, p in events:
            if name == "block_delta":
                by_index.setdefault((p["index"], p["delta_type"]), []).append(p["text"])
        self.assertEqual("".join(by_index[(0, "thinking")]), EXPECTED["content"][0]["thinking"])
        self.assertEqual("".join(by_index[(1, "text")]), EXPECTED["content"][1]["text"])
        self.assertEqual("".join(by_index[(2, "input_json")]), '{"file_path": "/tmp/próxy.py"}')

        start = next(p for n, p in events if n == "block_start" and p["index"] == 2)
        self.assertEqual((start["type"], start["name"], start["tool_use_id"]), ("tool_use", "Read", "toolu_01"))
        stop = next(p for n, p in events if n == "block_stop" and p["index"] == 2)
        self.assertEqual(stop["input"], {"file_path": "/tmp/próxy.py"})
        last = events[-1][1]
        self.assertEqual(last["stop_reason"], "tool_use")
        self.assertEqual(last["usage"]["output_tokens"], 87)
        self.assertEqual(last["usage"]["cache_read_input_tokens"], 3400)

    def test_message_start_event_is_a_copy(self):
        _, events = run([self.data])
        self.assertEqual(events[0][1]["message"]["content"], [])

    def test_error_event(self):
        parser, _ = run([ERROR_STREAM.encode()])
        self.assertEqual(parser.error, {"type": "overloaded_error", "message": "Overloaded"})
        self.assertFalse(parser.finished)
        self.assertEqual(parser.message["content"], [{"type": "text", "text": "Partial"}])

    def test_bad_tool_json_is_kept_not_lost(self):
        stream = (
            'event: content_block_start\ndata: {"type":"content_block_start","index":0,'
            '"content_block":{"type":"tool_use","id":"t","name":"Bash","input":{}}}\n\n'
            'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,'
            '"delta":{"type":"input_json_delta","partial_json":"{\\"command\\": \\"ls"}}\n\n'
            'event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n'
        )
        parser, _ = run([stream.encode()])
        self.assertEqual(parser.message["content"][0]["input"], {"_partial_json": '{"command": "ls'})

    def test_garbage_is_ignored(self):
        parser, events = run([b": comment\n\ndata: not json\n\nevent: x\n\n", self.data])
        self.assertEqual(parser.message, EXPECTED)
        self.assertEqual(events[0][0], "message_start")


if __name__ == "__main__":
    unittest.main()
