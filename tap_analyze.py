"""tap_analyze.py: pure analysis of captured exchanges (SPEC 6.2, 6.3, 7.8, 8.4).

No I/O, no threads, no clocks: every input is passed in and every result is
returned, so the same captured session always gives the same answer (R18),
live or reopened from a log. The store calls an `Analyzer` from its dispatcher
thread; tests and `--open` call the same code.

    Analyzer.on_request(id, kind, started_ms, body)  -> thread, summary.request, tool_results, search docs
    Analyzer.on_tool_use(id, block)                  (a tool_use block finished streaming)
    Analyzer.on_end(id, ended_ms, message)           -> search docs for the response

    response_summary(message)   summary.response (preview, tool calls, usage)
    search(items, query)        server-side search (7.8)
    stats(summaries, pricing)   session totals (8.4), tool calls (7.12)
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable

SUBAGENT_TOOLS = {"Task", "Agent"}
REWRITE_WINDOW_MS = 10 * 60 * 1000
PREVIEW_CHARS = 120
ARG_CHARS = 100
MAX_LISTED = 50            # tool calls / results listed per summary

# The input field that best describes a call, per Claude Code tool.
KEY_ARG = {
    "Bash": "command", "Read": "file_path", "Edit": "file_path", "MultiEdit": "file_path",
    "Write": "file_path", "NotebookEdit": "notebook_path", "Grep": "pattern", "Glob": "pattern",
    "WebFetch": "url", "WebSearch": "query", "Task": "description", "Agent": "description",
    "Skill": "skill", "TodoWrite": "todos",
}


# -- small helpers ----------------------------------------------------------------

def strip_cache_control(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: strip_cache_control(v) for k, v in value.items() if k != "cache_control"}
    if isinstance(value, list):
        return [strip_cache_control(v) for v in value]
    return value


def digest(value: Any) -> str:
    """Stable short hash of a JSON value, ignoring `cache_control` markers."""
    data = json.dumps(strip_cache_control(value), sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), default=str)
    return hashlib.sha1(data.encode("utf-8", "surrogatepass")).hexdigest()[:16]


def one_line(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[:limit] + "…"


def is_tool_use(block: Any) -> bool:
    return isinstance(block, dict) and str(block.get("type", "")).endswith("tool_use")


def blocks_of(content: Any) -> list:
    if isinstance(content, list):
        return content
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def system_blocks(system: Any) -> list:
    if system is None:
        return []
    if isinstance(system, str):
        return [{"type": "text", "text": system}]
    if isinstance(system, list):
        return system
    return [system]


def result_text(content: Any) -> str:
    """The text of a tool_result's content (a string or a list of blocks)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(b.get("text", "") for b in content
                         if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str))
    return ""


def key_arg(name: str | None, tool_input: Any) -> str:
    if not isinstance(tool_input, dict):
        return ""
    value = tool_input.get(KEY_ARG.get(name or "", ""))
    if value is None:
        value = next((v for v in tool_input.values() if isinstance(v, str)), None)
    if value is None:
        return ""
    if isinstance(value, list):
        return f"{len(value)} item{'' if len(value) == 1 else 's'}"
    text = str(value)
    first = text.split("\n", 1)[0]
    first = first if len(first) <= ARG_CHARS else first[:ARG_CHARS] + "…"
    return first + " …" if first != text and not first.endswith("…") else first


def flat_input(tool_input: Any) -> str:
    """Tool input as searchable text: string values verbatim, the rest as JSON."""
    if isinstance(tool_input, dict):
        return "\n".join(v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
                         for v in tool_input.values())
    return tool_input if isinstance(tool_input, str) else json.dumps(tool_input, ensure_ascii=False)


# -- pricing and context windows (8.4, open question 6) ----------------------------

def price_for(model: str | None, pricing: dict | None) -> dict | None:
    """The entry whose key is the longest prefix of `model`."""
    if not model or not pricing:
        return None
    best = max((k for k in pricing if model.startswith(k)), key=len, default=None)
    return pricing[best] if best is not None else None


def cost_of(usage: dict, price: dict) -> float:
    inp = price.get("input", 0)
    return (usage.get("input_tokens", 0) * inp
            + usage.get("output_tokens", 0) * price.get("output", 0)
            + usage.get("cache_read_input_tokens", 0) * price.get("cache_read", inp)
            + usage.get("cache_creation_input_tokens", 0) * price.get("cache_write", inp)) / 1e6


def validate_pricing(pricing: Any) -> dict:
    if not isinstance(pricing, dict):
        raise ValueError("pricing file must be a JSON object mapping model prefixes to prices")
    for prefix, entry in pricing.items():
        if not isinstance(entry, dict):
            raise ValueError(f"pricing entry {prefix!r} must be an object")
        for key, value in entry.items():
            if key in ("input", "output", "cache_read", "cache_write", "context_window") \
                    and not isinstance(value, (int, float)):
                raise ValueError(f"pricing entry {prefix!r}: {key} must be a number")
    return pricing


# -- request analysis -------------------------------------------------------------

@dataclass
class RequestInfo:
    model: str | None
    messages: list
    msg_hashes: list
    system_hash: str
    tools_hash: str
    n_tools: int
    first_user_texts: list = field(default_factory=list)


def request_info(body: Any) -> RequestInfo | None:
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
        return None
    messages = body["messages"]
    tools = body.get("tools") if isinstance(body.get("tools"), list) else []
    first: list[str] = []
    if messages and isinstance(messages[0], dict) and messages[0].get("role") == "user":
        first = [b["text"].strip() for b in blocks_of(messages[0].get("content"))
                 if isinstance(b, dict) and b.get("type") == "text" and isinstance(b.get("text"), str)]
    return RequestInfo(
        model=body.get("model") if isinstance(body.get("model"), str) else None,
        messages=messages,
        msg_hashes=[digest(m) for m in messages],
        system_hash=digest(body.get("system")),
        tools_hash=digest(tools),
        n_tools=len(tools),
        first_user_texts=first,
    )


def request_summary(body: Any, info: RequestInfo | None) -> dict:
    if not isinstance(body, dict):
        return {}
    thinking = body.get("thinking") if isinstance(body.get("thinking"), dict) else {}
    system = system_blocks(body.get("system"))
    out = {
        "model": body.get("model"),
        "max_tokens": body.get("max_tokens"),
        "stream": bool(body.get("stream")),
        "thinking_budget": thinking.get("budget_tokens"),
        "messages": len(info.messages) if info else None,
        "tools": info.n_tools if info else None,
        "system_chars": sum(len(b.get("text", "")) for b in system if isinstance(b, dict)
                            and isinstance(b.get("text"), str)),
    }
    if info and info.messages:
        out["preview"] = request_preview(info.messages[-1])
    return out


def request_preview(message: Any) -> str:
    """One line for the list when the reply has no text yet: the last user turn."""
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return one_line(content, PREVIEW_CHARS)
    blocks = [b for b in blocks_of(content) if isinstance(b, dict)]
    results = sum(1 for b in blocks if b.get("type") == "tool_result")
    if results:
        return f"{results} tool result{'' if results == 1 else 's'}"
    for b in reversed(blocks):
        text = b.get("text")
        if b.get("type") == "text" and isinstance(text, str) and not re.match(r"\s*<[\w-]+>", text):
            return one_line(text, PREVIEW_CHARS)
    return ""


def response_summary(message: Any) -> dict:
    if not isinstance(message, dict):
        return {"preview": "", "tool_calls": [], "usage": None, "stop_reason": None}
    preview, thinking, calls = "", False, []
    for b in message.get("content") or []:
        if not isinstance(b, dict):
            continue
        if b.get("type") == "text" and not preview and str(b.get("text", "")).strip():
            preview = one_line(b["text"], PREVIEW_CHARS)
        elif b.get("type") == "thinking":
            thinking = True
        elif is_tool_use(b) and len(calls) < MAX_LISTED:
            calls.append({"id": b.get("id"), "name": b.get("name"), "arg": key_arg(b.get("name"), b.get("input"))})
    out = {"preview": preview, "tool_calls": calls, "usage": message.get("usage"),
           "stop_reason": message.get("stop_reason")}
    if thinking:
        out["thinking"] = True
    return out


# -- threading (6.3) --------------------------------------------------------------

class Thread:
    def __init__(self, n: int, info: RequestInfo, role: str, spawned_by: dict | None, first_id: int) -> None:
        self.id = f"t{n}"
        self.role = role
        self.spawned_by = spawned_by
        self.first_id = first_id
        self.model = info.model
        self.system_hash = info.system_hash
        self.tools_hash = info.tools_hash
        self.small = info.n_tools == 0 and len(info.messages) < 3
        self.last_hashes: list = []
        self.last_id = first_id
        self.last_started = 0
        self.last_ended: int | None = None
        self.turns = 0


class Threader:
    """Assigns each messages request to a conversation thread, in id order."""

    def __init__(self) -> None:
        self.threads: dict[str, Thread] = {}
        self.of: dict[int, str] = {}              # exchange id -> thread id
        self._spawns: list[dict] = []             # Task / Agent prompts, in the order seen
        self._spawn_ids: set = set()
        self._has_main = False

    def note_spawn(self, exchange_id: int, tool_use_id: str, prompt: str) -> None:
        if tool_use_id in self._spawn_ids or not prompt.strip():
            return
        self._spawn_ids.add(tool_use_id)
        self._spawns.append({"exchange_id": exchange_id, "tool_use_id": tool_use_id,
                             "prompt": prompt.strip(), "claimed": False})

    def note_end(self, exchange_id: int, ended_ms: int) -> None:
        th = self.threads.get(self.of.get(exchange_id, ""))
        if th is not None and th.last_id == exchange_id:
            th.last_ended = ended_ms

    def _match_spawn(self, info: RequestInfo) -> dict | None:
        texts = info.first_user_texts
        if not texts:
            return None
        joined = "\n".join(texts).strip()
        for sp in self._spawns:
            p = sp["prompt"]
            if not sp["claimed"] and (p == joined or p in texts or (len(p) >= 30 and p in joined)):
                sp["claimed"] = True
                return sp
        return None

    def assign(self, exchange_id: int, started_ms: int, info: RequestInfo) -> dict:
        hashes = info.msg_hashes
        # 1. append: the thread's last request is a prefix of this one. The spec
        # asks for the same model, system and tools; requiring only one of
        # system/tools to match lets "system prompt changed" / "tools changed"
        # (7.2) show up, e.g. when Claude Code loads deferred tools mid-session.
        best, best_key = None, None
        for th in self.threads.values():
            k = len(th.last_hashes)
            if k == 0 or k > len(hashes) or hashes[:k] != th.last_hashes:
                continue
            if th.system_hash != info.system_hash and th.tools_hash != info.tools_hash:
                continue
            exact = th.model == info.model and th.system_hash == info.system_hash and th.tools_hash == info.tools_hash
            key = (k, exact, th.last_started)
            if best_key is None or key > best_key:
                best, best_key = th, key
        history, spawn = "append", None
        if best is None:
            spawn = self._match_spawn(info)
            if spawn is None and info.n_tools:
                # 2. rewritten: same system and tools, active in the last 10 minutes,
                # and not busy with a request of its own (that would be a parallel
                # agent of the same type, not a compaction).
                for th in reversed(list(self.threads.values())):
                    if (th.system_hash == info.system_hash and th.tools_hash == info.tools_hash
                            and started_ms - th.last_started <= REWRITE_WINDOW_MS
                            and th.last_ended is not None and th.last_ended <= started_ms):
                        best, history = th, "rewritten"
                        break
        if best is None:
            # 3. new thread
            if spawn is not None:
                role = "subagent"
            elif info.n_tools == 0 and len(info.messages) < 3:
                role = "side"
            elif info.n_tools and not self._has_main:
                role, self._has_main = "main", True
            else:
                role = "unknown"
            by = {"exchange_id": spawn["exchange_id"], "tool_use_id": spawn["tool_use_id"]} if spawn else None
            best = Thread(len(self.threads) + 1, info, role, by, exchange_id)
            self.threads[best.id] = best
            history, prefix, prev_id = "new", 0, None
            system_changed = tools_changed = False
        else:
            th = best
            prev_id = th.last_id
            if history == "append":
                prefix = len(th.last_hashes)
            else:
                prefix = 0
                for a, b in zip(th.last_hashes, hashes):
                    if a != b:
                        break
                    prefix += 1
            system_changed = th.system_hash != info.system_hash
            tools_changed = th.tools_hash != info.tools_hash
            th.small = th.small and info.n_tools == 0 and len(info.messages) < 3
            if th.role == "side" and not th.small:
                th.role = "unknown"
        th = best
        new_from = prefix
        msgs = info.messages
        # The previous request's reply is the next message here: it was shown there.
        if history == "append" and new_from < len(msgs) and isinstance(msgs[new_from], dict) \
                and msgs[new_from].get("role") == "assistant":
            new_from += 1
        th.turns += 1
        th.last_hashes, th.last_id, th.last_started, th.last_ended = hashes, exchange_id, started_ms, None
        th.model, th.system_hash, th.tools_hash = info.model, info.system_hash, info.tools_hash
        self.of[exchange_id] = th.id
        return {
            "thread": {"id": th.id, "role": th.role, "turn": th.turns, "prev_id": prev_id,
                       "new_from": new_from, "history": history, "spawned_by": th.spawned_by},
            "system_changed": system_changed, "tools_changed": tools_changed,
        }


# -- search documents -------------------------------------------------------------

@dataclass
class Doc:
    """One searchable piece of an exchange, indexed where it first appeared (7.8)."""
    loc: str              # sys:K | m:I:J | m:I:s | r:J | err
    where: str            # system | user | assistant | thinking | tool_use | tool_result | error
    text: str
    tool: str | None = None
    tool_use_id: str | None = None
    is_error: bool = False


def message_docs(messages: list, start: int, names: dict) -> list[Doc]:
    docs: list[Doc] = []
    for i in range(start, len(messages)):
        m = messages[i]
        if not isinstance(m, dict):
            continue
        role = m.get("role") if m.get("role") in ("user", "assistant") else "user"
        content = m.get("content")
        if isinstance(content, str):
            docs.append(Doc(f"m:{i}:s", role, content))
            continue
        for j, b in enumerate(content if isinstance(content, list) else []):
            doc = block_doc(b, f"m:{i}:{j}", role, names)
            if doc:
                docs.append(doc)
    return docs


def block_doc(b: Any, loc: str, role: str, names: dict) -> Doc | None:
    if not isinstance(b, dict):
        return None
    t = b.get("type")
    if t == "text" and isinstance(b.get("text"), str):
        return Doc(loc, role, b["text"])
    if t == "thinking" and isinstance(b.get("thinking"), str):
        return Doc(loc, "thinking", b["thinking"])
    if is_tool_use(b):
        return Doc(loc, "tool_use", flat_input(b.get("input")), tool=b.get("name"), tool_use_id=b.get("id"))
    if t in ("tool_result", "mcp_tool_result"):
        tid = b.get("tool_use_id")
        return Doc(loc, "tool_result", result_text(b.get("content")), tool=names.get(tid),
                   tool_use_id=tid, is_error=bool(b.get("is_error")))
    return None


# -- the incremental analyzer -----------------------------------------------------

class Analyzer:
    def __init__(self, pricing: dict | None = None) -> None:
        self.pricing = pricing
        self.threader = Threader()
        self._uses: dict[str, dict] = {}          # tool_use_id -> {exchange_id, name, ended}
        self._uses_by_ex: dict[int, list] = {}

    def on_request(self, xid: int, kind: str, started_ms: int, body: Any) -> dict:
        """Thread, summary.request, summary.tool_results and search docs for a new request."""
        if kind != "messages":
            return {"thread": None, "request": request_summary(body, None), "tool_results": [], "docs": []}
        info = request_info(body)
        req = request_summary(body, info)
        price = price_for(req.get("model"), self.pricing)
        if price and price.get("context_window"):
            req["context_window"] = price["context_window"]
        if info is None:
            return {"thread": None, "request": req, "tool_results": [], "docs": []}
        assigned = self.threader.assign(xid, started_ms, info)
        thread = assigned["thread"]
        new_from = thread["new_from"]
        req["new_messages"] = len(info.messages) - new_from
        req["system_changed"] = assigned["system_changed"]
        req["tools_changed"] = assigned["tools_changed"]

        names = {b.get("id"): b.get("name") for m in info.messages if isinstance(m, dict)
                 for b in blocks_of(m.get("content")) if is_tool_use(b)}
        results = []
        for m in info.messages[new_from:]:
            for b in blocks_of(m.get("content")) if isinstance(m, dict) else []:
                if isinstance(b, dict) and b.get("type") in ("tool_result", "mcp_tool_result") and len(results) < MAX_LISTED:
                    tid = b.get("tool_use_id")
                    text = result_text(b.get("content"))
                    use = self._uses.get(tid) or {}
                    ended = use.get("ended")
                    results.append({"tool_use_id": tid, "name": names.get(tid) or use.get("name"),
                                    "is_error": bool(b.get("is_error")), "chars": len(text),
                                    "lines": text.count("\n") + 1 if text else 0,
                                    "client_ms": started_ms - ended if ended is not None else None})

        docs = []
        if thread["history"] != "append" or assigned["system_changed"]:
            for k, s in enumerate(system_blocks(body.get("system"))):
                if isinstance(s, dict) and isinstance(s.get("text"), str):
                    docs.append(Doc(f"sys:{k}", "system", s["text"]))
        docs += message_docs(info.messages, new_from, names)
        return {"thread": thread, "request": req, "tool_results": results, "docs": docs}

    def on_tool_use(self, xid: int, block: dict) -> None:
        tid = block.get("id")
        if not tid or tid in self._uses:
            return
        self._uses[tid] = {"exchange_id": xid, "name": block.get("name"), "ended": None}
        self._uses_by_ex.setdefault(xid, []).append(tid)
        tool_input = block.get("input")
        if block.get("name") in SUBAGENT_TOOLS and isinstance(tool_input, dict) \
                and isinstance(tool_input.get("prompt"), str):
            self.threader.note_spawn(xid, tid, tool_input["prompt"])

    def on_end(self, xid: int, ended_ms: int, message: Any) -> list[Doc]:
        content = message.get("content") if isinstance(message, dict) else None
        content = content if isinstance(content, list) else []
        for b in content:
            if is_tool_use(b):
                self.on_tool_use(xid, b)
        for tid in self._uses_by_ex.get(xid, []):
            self._uses[tid]["ended"] = ended_ms
        self.threader.note_end(xid, ended_ms)
        docs = []
        for j, b in enumerate(content):
            doc = block_doc(b, f"r:{j}", "assistant", {})
            if doc:
                docs.append(doc)
        return docs


def assign_threads(exchanges: Iterable[dict]) -> dict[int, dict]:
    """Thread of every messages exchange in a recorded session (full Exchange dicts)."""
    analyzer = Analyzer()
    out = {}
    for ex in sorted(exchanges, key=lambda e: e["id"]):
        timing = ex.get("timing") or {}
        result = analyzer.on_request(ex["id"], ex.get("kind", "other"), iso_ms(timing.get("started_at")) or 0,
                                     (ex.get("request") or {}).get("body"))
        if result["thread"] is not None:
            out[ex["id"]] = result["thread"]
        ended = iso_ms(timing.get("ended_at"))
        if ended is not None:
            analyzer.on_end(ex["id"], ended, (ex.get("response") or {}).get("message"))
    return out


def iso_ms(value: str | None) -> int | None:
    """ISO-8601 UTC timestamp -> integer epoch milliseconds (exact, no float rounding)."""
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (dt - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)


# -- search (7.8) -----------------------------------------------------------------

FIELDS = {"tool", "model", "thread", "is", "stop", "kind"}
IS_VALUES = {"error", "aborted"}
TOKEN_RE = re.compile(r'(\w+):"([^"]*)"|"([^"]*)"|(\S+)')
SNIP_BEFORE, SNIP_MATCH, SNIP_AFTER = 50, 100, 80


class QueryError(ValueError):
    pass


@dataclass
class Query:
    fields: dict
    text: str
    pattern: re.Pattern | None

    @property
    def piece_level(self) -> bool:
        return bool(self.pattern or self.fields.get("tool") or "error" in self.fields.get("is", ()))


def parse_query(q: str) -> Query:
    fields: dict[str, list] = {}
    words: list[str] = []
    for m in TOKEN_RE.finditer(q or ""):
        quoted_field, quoted_value, phrase, word = m.groups()
        if quoted_field and quoted_field.lower() in FIELDS:
            fields.setdefault(quoted_field.lower(), []).append(quoted_value)
            continue
        if phrase is not None:
            words.append(phrase)
            continue
        token = word if word is not None else m.group(0)
        name, sep, value = token.partition(":")
        if sep and value and name.lower() in FIELDS:
            name = name.lower()
            if name == "is" and value.lower() not in IS_VALUES:
                raise QueryError(f"unknown is: value {value!r} (use is:error or is:aborted)")
            fields.setdefault(name, []).append(value.lower() if name == "is" else value)
        else:
            words.append(token)
    text = " ".join(words).strip()
    pattern = None
    if text.startswith("re:"):
        source = text[3:].strip()
        if source:
            try:
                pattern = re.compile(source, re.IGNORECASE)
            except re.error as e:
                raise QueryError(f"invalid regular expression: {e}") from None
    elif text:
        pattern = re.compile(r"\s+".join(re.escape(w) for w in text.split()), re.IGNORECASE)
    return Query(fields, text, pattern)


def _ci_in(values: list, candidate: str | None, exact: bool = True) -> bool:
    if candidate is None:
        return False
    c = candidate.lower()
    return any((v.lower() == c) if exact else (v.lower() in c) for v in values)


def exchange_matches(summary: dict, query: Query) -> bool:
    f = query.fields
    thread = (summary.get("thread") or {}).get("id")
    model = (summary.get("summary") or {}).get("request", {}).get("model") \
        or ((summary.get("response") or {}).get("message") or {}).get("model")
    stop = ((summary.get("response") or {}).get("message") or {}).get("stop_reason")
    if "kind" in f and not _ci_in(f["kind"], summary.get("kind")):
        return False
    if "model" in f and not _ci_in(f["model"], model, exact=False):
        return False
    if "thread" in f and not _ci_in(f["thread"], thread):
        return False
    if "stop" in f and not _ci_in(f["stop"], stop):
        return False
    if "aborted" in f.get("is", ()) and summary.get("state") != "aborted":
        return False
    return True


def doc_matches(doc: Doc, query: Query) -> re.Match | bool | None:
    tools = query.fields.get("tool")
    errors = "error" in query.fields.get("is", ())
    if tools and errors:
        ok = doc.where == "tool_result" and doc.is_error and _ci_in(tools, doc.tool)
    elif tools:
        ok = doc.where == "tool_use" and _ci_in(tools, doc.tool)
    elif errors:
        ok = doc.where == "error" or (doc.where == "tool_result" and doc.is_error)
    else:
        ok = True
    if not ok:
        return None
    if query.pattern is None:
        return True
    return query.pattern.search(doc.text)


def snippet(text: str, match: re.Match | None) -> dict:
    """Context around a match, on one line, as three parts so the page can wrap the match in <mark>."""
    if match is None:
        return {"before": one_line(text, SNIP_MATCH + SNIP_AFTER), "match": "", "after": ""}
    a, b = match.span()
    window = SNIP_BEFORE * 3
    before = " ".join(text[max(0, a - window):a].split())
    if before and text[a - 1:a].isspace():
        before += " "
    if len(before) > SNIP_BEFORE:
        before = "…" + before[-SNIP_BEFORE:]
    elif a > window:
        before = "…" + before
    after = " ".join(text[b:b + SNIP_AFTER * 2].split())
    if after and text[b:b + 1].isspace():
        after = " " + after
    if len(after) > SNIP_AFTER:
        after = after[:SNIP_AFTER] + "…"
    return {"before": before, "match": " ".join(text[a:b].split())[:SNIP_MATCH], "after": after}


def search(items: Iterable[tuple[dict, list]], q: str, thread: str | None = None, limit: int = 500) -> dict:
    """Search (summary, docs) pairs in id order. Raises QueryError for a bad query."""
    query = parse_query(q)
    if thread:
        query.fields.setdefault("thread", []).append(thread)
    ids, hits, truncated = [], [], False
    for summary, docs in items:
        if not exchange_matches(summary, query):
            continue
        if not query.piece_level:
            ids.append(summary["id"])
            continue
        found = False
        for doc in docs or ():
            m = doc_matches(doc, query)
            if not m:
                continue
            found = True
            if len(hits) >= limit:
                truncated = True
                break
            hits.append({"id": summary["id"], "loc": doc.loc, "where": doc.where, "tool": doc.tool,
                         "is_error": doc.is_error, **snippet(doc.text, m if isinstance(m, re.Match) else None)})
        if found:
            ids.append(summary["id"])
    return {"q": q, "fields": query.fields, "text": query.text, "regex": query.text.startswith("re:"),
            "exchanges": ids, "hits": hits, "truncated": truncated}


# -- stats (8.4, 7.11, 7.12) --------------------------------------------------------

def _model(s: dict) -> str | None:
    return ((s.get("summary") or {}).get("request") or {}).get("model") \
        or (((s.get("response") or {}).get("message") or {}).get("model"))


def _usage(s: dict) -> dict:
    return (((s.get("response") or {}).get("message") or {}).get("usage")) or {}


def _tokens(u: dict) -> tuple[int, int, int, int]:
    def n(key: str) -> int:
        v = u.get(key)
        return v if isinstance(v, int) else 0
    return n("input_tokens"), n("cache_read_input_tokens"), n("cache_creation_input_tokens"), n("output_tokens")


def tool_calls(summaries: list[dict]) -> list[dict]:
    """One row per tool_use (7.12), joined with its tool_result in a later request."""
    rows: list[dict] = []
    index: dict[str, dict] = {}
    for s in summaries:
        timing = s.get("timing") or {}
        sm = s.get("summary") or {}
        thread = (s.get("thread") or {}).get("id")
        for tr in sm.get("tool_results") or []:
            row = index.get(tr.get("tool_use_id"))
            if row is None:   # the call itself is no longer retained
                row = {"id": tr.get("tool_use_id"), "name": tr.get("name"), "arg": "", "exchange": None,
                       "time": None, "thread": thread, "client_ms": None}
                index[row["id"]] = row
                rows.append(row)
            row.update({"result_exchange": s["id"], "is_error": tr.get("is_error"),
                        "result_chars": tr.get("chars"), "result_lines": tr.get("lines")})
            started = iso_ms(timing.get("started_at"))
            if row.get("_ended") is not None and started is not None:
                row["client_ms"] = started - row["_ended"]
            elif tr.get("client_ms") is not None:
                row["client_ms"] = tr["client_ms"]
        for tc in (sm.get("response") or {}).get("tool_calls") or []:
            if tc.get("id") in index:
                continue
            row = {"id": tc.get("id"), "name": tc.get("name"), "arg": tc.get("arg", ""), "exchange": s["id"],
                   "time": timing.get("ended_at") or timing.get("started_at"), "thread": thread,
                   "client_ms": None, "result_exchange": None, "is_error": None,
                   "result_chars": None, "result_lines": None, "_ended": iso_ms(timing.get("ended_at"))}
            index[row["id"]] = row
            rows.append(row)
    for row in rows:
        row.pop("_ended", None)
    return rows


def stats(summaries: list[dict], pricing: dict | None = None, with_calls: bool = True) -> dict:
    tot = {"exchanges": len(summaries), "messages": 0, "other": 0, "wall_ms": 0, "input_tokens": 0,
           "uncached_input": 0, "cache_read": 0, "cache_write": 0, "output_tokens": 0,
           "cache_hit_rate": None, "cost": 0.0 if pricing else None, "errors": 0, "aborts": 0,
           "streaming": 0, "tool_calls": 0, "tool_errors": 0}
    models: dict[str, dict] = {}
    threads: dict[str, dict] = {}
    unpriced: set = set()
    first = last = None
    slow, large = [], []
    for s in summaries:
        timing = s.get("timing") or {}
        t0, t1 = iso_ms(timing.get("started_at")), iso_ms(timing.get("ended_at"))
        if t0 is not None:
            first = t0 if first is None else min(first, t0)
            last = max(last or t0, t1 or t0)
        tot["messages" if s.get("kind") == "messages" else "other"] += 1
        state = s.get("state")
        tot["errors"] += state == "error"
        tot["aborts"] += state == "aborted"
        tot["streaming"] += state in ("pending", "streaming")
        uncached, read, write, out = _tokens(_usage(s))
        total_in = uncached + read + write
        tot["input_tokens"] += total_in
        tot["uncached_input"] += uncached
        tot["cache_read"] += read
        tot["cache_write"] += write
        tot["output_tokens"] += out
        model = _model(s)
        cost = None
        if pricing and (uncached or read or write or out):
            price = price_for(model, pricing)
            if price is None:
                unpriced.add(model or "?")
            else:
                cost = cost_of(_usage(s), price)
                tot["cost"] += cost
        thread = s.get("thread") or {}
        if s.get("kind") == "messages" and model:
            m = models.setdefault(model, {"model": model, "requests": 0, "input_tokens": 0, "output_tokens": 0,
                                          "cache_read": 0, "cache_write": 0, "errors": 0, "cost": 0.0 if pricing else None,
                                          "_ttfb": [], "_tps": [], "ids": []})
            m["requests"] += 1
            m["input_tokens"] += total_in
            m["output_tokens"] += out
            m["cache_read"] += read
            m["cache_write"] += write
            m["errors"] += state == "error"
            if cost is not None:
                m["cost"] += cost
            if timing.get("ttfb_ms") is not None:
                m["_ttfb"].append(timing["ttfb_ms"])
            if timing.get("output_tps") is not None:
                m["_tps"].append(timing["output_tps"])
        if thread.get("id"):
            th = threads.setdefault(thread["id"], {"id": thread["id"], "role": thread.get("role"), "turns": 0,
                                                   "exchanges": 0, "input_tokens": 0, "output_tokens": 0,
                                                   "duration_ms": 0, "spawned_by": thread.get("spawned_by"),
                                                   "first_id": s["id"], "last_id": s["id"], "models": [],
                                                   "errors": 0, "_t0": t0, "_t1": t1 or t0})
            th["role"] = thread.get("role") or th["role"]
            th["turns"] = max(th["turns"], thread.get("turn") or 0)
            th["exchanges"] += 1
            th["input_tokens"] += total_in
            th["output_tokens"] += out
            th["errors"] += state in ("error", "aborted")
            th["last_id"] = s["id"]
            if model and model not in th["models"]:
                th["models"].append(model)
            if t0 is not None:
                th["_t0"] = t0 if th["_t0"] is None else min(th["_t0"], t0)
                th["_t1"] = max(th["_t1"] or t0, t1 or t0)
        if timing.get("duration_ms") is not None:
            slow.append({"id": s["id"], "duration_ms": timing["duration_ms"], "ttft_ms": timing.get("ttft_ms"),
                         "model": model, "thread": thread.get("id")})
        if total_in or (s.get("sizes") or {}).get("request_bytes"):
            large.append({"id": s["id"], "input_tokens": total_in,
                          "request_bytes": (s.get("sizes") or {}).get("request_bytes"),
                          "model": model, "thread": thread.get("id")})
    if first is not None:
        tot["wall_ms"] = last - first
    if tot["input_tokens"]:
        tot["cache_hit_rate"] = round(tot["cache_read"] / tot["input_tokens"], 4)
    per_model = []
    for m in models.values():
        ttfb, tps = m.pop("_ttfb"), m.pop("_tps")
        m.pop("ids")
        m["cache_hit_rate"] = round(m["cache_read"] / m["input_tokens"], 4) if m["input_tokens"] else None
        m["mean_ttfb_ms"] = round(sum(ttfb) / len(ttfb)) if ttfb else None
        m["mean_tps"] = round(sum(tps) / len(tps), 1) if tps else None
        per_model.append(m)
    per_thread = []
    for th in threads.values():
        t0, t1 = th.pop("_t0"), th.pop("_t1")
        th["duration_ms"] = (t1 - t0) if t0 is not None and t1 is not None else 0
        per_thread.append(th)
    calls = tool_calls(summaries)
    tools: dict[str, dict] = {}
    for c in calls:
        t = tools.setdefault(c["name"] or "?", {"name": c["name"] or "?", "calls": 0, "errors": 0,
                                                "result_chars": 0, "_client": []})
        t["calls"] += 1
        t["errors"] += bool(c.get("is_error"))
        t["result_chars"] += c.get("result_chars") or 0
        if c.get("client_ms") is not None:
            t["_client"].append(c["client_ms"])
    per_tool = []
    for t in sorted(tools.values(), key=lambda t: (-t["calls"], t["name"])):
        client = t.pop("_client")
        t["median_client_ms"] = round(median(client)) if client else None
        per_tool.append(t)
    tot["tool_calls"] = len(calls)
    tot["tool_errors"] = sum(1 for c in calls if c.get("is_error"))
    if tot["cost"] is not None:
        tot["cost"] = round(tot["cost"], 6)
    for m in per_model:
        if m["cost"] is not None:
            m["cost"] = round(m["cost"], 6)
    out = {
        "totals": tot,
        "per_model": sorted(per_model, key=lambda m: -m["requests"]),
        "per_thread": per_thread,
        "per_tool": per_tool,
        "slowest": sorted(slow, key=lambda x: (-x["duration_ms"], x["id"]))[:10],
        "largest": sorted(large, key=lambda x: (-(x["input_tokens"] or 0), -(x["request_bytes"] or 0), x["id"]))[:10],
        "priced": bool(pricing),
        "unpriced_models": sorted(unpriced),
    }
    if with_calls:
        out["calls"] = calls
    return out
