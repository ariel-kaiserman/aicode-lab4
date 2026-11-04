# SPEC: Live traffic dashboard for `proxy.py` (v2)

> **Status.** v1 (sections marked *v1*) is implemented in commit `c08123e`.
> v2 adds full capture, persistence, and analysis views on top of it. Every v2
> item has a priority: **P1** is the next milestone, **P2** comes after.
> The change list with reasons is in [`SPEC_CHANGES.md`](SPEC_CHANGES.md).

## 1. Goal

While `proxy.py` is running, open a page in the browser that updates live and
shows the conversation between the agent (Claude Code) and the model: what the
agent sends, what the model streams back, and the tool calls and results going
back and forth. It should be well formatted and easy to read, not a JSON dump.

v2 widens the goal from *reading* one exchange to *analysing a whole session*:

1. **Nothing is lost.** Every request that crosses the proxy is recorded,
   including aborted, failed, and non-`messages` calls, with full timing.
   Optionally, the session is written to disk and can be reopened later.
2. **Structure is visible.** Exchanges are grouped into conversations
   (main agent, sub-agents, side-calls), so the user can see who called what.
3. **Questions are quick to answer.** "Where did the agent read `proxy.py`?",
   "How much context did turn 14 send?", "Which tool call failed?", "Why was
   this request slow?" each take one search, one click, or one keystroke.

The terminal output stays as it is. The dashboard is a second view of the same
traffic.

## 2. Non-goals

- Changing, blocking, or replaying traffic. The proxy still forwards bytes
  unchanged.
- Authentication or multi-user access. The dashboard is local only.
- Full support for the Codex / OpenAI Responses format. Section 7.6 covers a
  raw fallback for it.
- Any build step, framework, or third-party dependency.
- Exact billing. Cost figures (8.4) are estimates from a user-supplied price
  table and are labelled as such.

*Removed in v2:* "Saving traffic to disk between runs". It is now an opt-in
feature (section 10).

## 3. Constraints

| # | Constraint | Why |
|---|------------|-----|
| C1 | Python standard library only, same as `proxy.py` | "Nothing to install" stays true |
| C2 | Front end is one self-contained HTML file with inline CSS and vanilla JS, and no CDN. Charts are hand-drawn SVG | Works offline, no build |
| C3 | The dashboard, the log writer, and all analysis must never delay, alter, or break proxied traffic | The agent is the real user of the proxy |
| C4 | Bind to `127.0.0.1` only | Traffic contains prompts, code, and tokens |
| C5 | Credentials are redacted before they reach the dashboard **or the log file** | Neither the browser nor the disk may ever see an API key or bearer token |
| C6 *(v2)* | Analysis (threading, stats, search) is computed in Python, in pure functions | Same reason parsing is: it stays unit-testable, and the browser stays a renderer |

## 4. User experience

```
python3 proxy.py --log
  proxy listening on http://localhost:8041  ->  https://api.anthropic.com
  dashboard at       http://localhost:8042
  logging to         ./tap-logs/2026-11-04T15-40-02.jsonl
  run the agent with:  ANTHROPIC_BASE_URL=http://localhost:8041 claude
```

1. The user starts the proxy and opens `http://localhost:8042`.
2. The user starts `claude` against the proxy and works normally.
3. Every API call shows up on the page within about 100 ms of reaching the
   proxy. The model's reply fills in token by token as it streams.
4. Reloading the page restores everything captured so far in this run, **and
   the same selection, tab, view, and search** (the URL holds that state, 7.9).
5. If the proxy restarts, the page shows "disconnected", reconnects
   automatically, and then shows the new (empty) session.
6. *(v2, P1)* The user can switch between four views with one key:
   **Conversation** (v1 view), **Timeline**, **Stats**, and **Tools** (7.10–7.12).
7. *(v2, P2)* Later, `python3 proxy.py --open tap-logs/….jsonl` opens a
   recorded session read-only in the same page.

## 5. Architecture

```
 agent ──HTTP──▶ :8041 Tap.relay() ──HTTPS──▶ api.anthropic.com
                     │  publish(event)   (non-blocking)
                     ▼
                 Store (in memory, thread-safe)
                     │  dispatcher thread
                     ├──▶ Analyzer: threading, summaries, stats   (v2)
                     ├──▶ LogWriter: append JSONL on exchange end  (v2, optional)
                     ▼  fan-out to subscriber queues
 browser ◀──SSE── :8042 Dashboard server ── GET /                     page
                                         ── GET /api/exchanges        summaries   (v2: summaries only)
                                         ── GET /api/exchanges/{id}   one full exchange (v2)
                                         ── GET /api/search?q=…       (v2)
                                         ── GET /api/stats            (v2)
                                         ── GET /api/export.jsonl     (v2)
                                         ── GET /events               live stream
```

- **Two ports, one process.** *(v1, unchanged.)* The proxy port forwards every
  path to upstream unchanged, so dashboard routes go on their own port.
- **Live transport: Server-Sent Events.** *(v1, unchanged.)*
- **Parsing and analysis happen in Python.** The relay thread only records
  bytes and timestamps. Parsing request bodies, assigning threads, updating
  stats, and writing the log all run on the dispatcher thread or later, never
  on the relay thread (C3).
- *(v2)* **Summaries first, detail on demand.** Every Messages request resends
  the whole conversation, so 200 full exchanges can easily be tens of MB. The
  snapshot carries only small summaries (6.2). The page fetches the full
  exchange when it is opened, and keeps an LRU of the last 20 in memory.

### 5.1 Code layout

| File | Contents |
|------|----------|
| `proxy.py` | Existing relay, plus `publish()` calls and the CLI flags |
| `tap_store.py` | `Store`, `Exchange`, subscriber fan-out, redaction |
| `tap_stream.py` | Incremental parser for the Anthropic Messages SSE stream (pure, no I/O) |
| `tap_analyze.py` *(v2)* | Pure functions: thread assignment, summaries, stats, search, Markdown export |
| `tap_log.py` *(v2)* | JSONL writer (background thread) and reader for `--open` |
| `tap_ui.py` | Dashboard HTTP handler |
| `dashboard.html` | The page, served as-is |
| `tests/` | `unittest` tests for every module above |

## 6. Data model

One **Exchange** is one HTTP request/response pair. Fields new in v2 are
marked `// v2`.

```jsonc
{
  "id": 17,                         // increasing per run
  "seq": 342,                       // per-exchange event counter, see 6.1
  "method": "POST",
  "path": "/v1/messages?beta=true",
  "kind": "messages",               // messages | count_tokens | other
  "state": "streaming",             // pending | streaming | done | error | aborted (v2)
  "status": 200,
  "timing": {                       // v2: all timestamps in one place
    "started_at":     "2026-11-01T15:42:07.120Z",  // request body fully read
    "response_at":    "2026-11-01T15:42:08.010Z",  // upstream status line
    "first_token_at": "2026-11-01T15:42:08.410Z",  // first text/thinking/tool delta
    "ended_at":       "2026-11-01T15:42:11.904Z",  // null while open
    "ttfb_ms": 890, "ttft_ms": 1290, "duration_ms": 4784,
    "output_tps": 88.4              // output_tokens / (ended_at - first_token_at)
  },
  "thread": {                       // v2: see 6.3
    "id": "t2",
    "role": "main",                 // main | subagent | side | unknown
    "turn": 7,                      // position within the thread, 1-based
    "prev_id": 15,                  // previous exchange in the same thread
    "new_from": 22,                 // index of the first message not seen in prev_id
    "history": "append",            // append | rewritten | new
    "spawned_by": null              // {exchange_id, tool_use_id} for sub-agents
  },
  "upstream": {                     // v2: from response headers
    "request_id": "req_…",
    "ratelimit": { "tokens_remaining": 0, "requests_remaining": 0, "reset": "…" },
    "retry_after": null
  },
  "sizes": { "request_bytes": 183204, "response_bytes": 5120 },   // v2
  "request": {
    "headers": { "anthropic-beta": "…", "authorization": "Bearer sk-ant-oat…[redacted]" },
    "body": { /* parsed JSON, or {"_raw": "…"} if not JSON */ }
  },
  "response": {
    "headers": { … },
    "message": {                    // reassembled from the stream
      "id": "msg_…", "model": "…", "role": "assistant",
      "content": [ /* text | thinking | tool_use blocks */ ],
      "stop_reason": "tool_use",
      "usage": { "input_tokens": 0, "output_tokens": 0,
                 "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0 }
    },
    "raw": "event: message_start\ndata: {…}\n\n…",   // capped, see 9
    "raw_truncated": false,         // v2: explicit flag, not only the marker text
    "error": null                   // {source: upstream|http|proxy|client, message}
  }
}
```

- **`aborted`** *(v2, P1)*: the agent closed the connection before the response
  finished (for example the user pressed Esc in Claude Code). The proxy notices
  this as a `BrokenPipeError` / `ConnectionResetError` while writing to the
  client. The partial message is kept, and `error.source` is `client`.
- `timing` replaces the v1 top-level `started_at` / `ended_at`. The snapshot
  keeps the old two fields as aliases for one release.

### 6.1 Live events (`/events`)

Each SSE message has `event:` set to one of the types below and `data:` set to
JSON that always includes `"id"` (the exchange id) and `"seq"`.

**Ordering rule** *(v1 behaviour, now specified)*: `seq` increases by one per
event of an exchange. Snapshots carry the exchange's current `seq`. The page
drops any event whose `seq` is not greater than what it already has, so a
snapshot taken while events are in flight never applies an event twice.

| Event | Payload | When |
|-------|---------|------|
| `exchange_start` | Summary (6.2), request not yet analysed | Request body has been read |
| `exchange_meta` *(v2)* | `thread`, `summary.request` | Dispatcher has parsed the request body |
| `response_start` | `status`, response headers, `upstream` | Upstream status line received |
| `block_start` | `index`, `type`, and `name`/`tool_use_id` for tool_use | `content_block_start` |
| `block_delta` | `index`, `text` (text, thinking, or partial tool-input JSON) | Each `content_block_delta` |
| `block_stop` | `index`, and the final parsed `input` for tool_use | `content_block_stop` |
| `message_delta` | `stop_reason`, `usage` | `message_delta` |
| `exchange_end` | Final summary (not the full exchange, v2) | Stream finished, errored, or aborted |
| `exchange_evicted` *(v2)* | `id`, `logged` (bool) | Dropped from memory past `--keep` |
| `stats` *(v2)* | Session stats (8.4) | At most once per second, only when changed |
| `resync` *(v2)* | none | Sent just before the server drops a slow subscriber (R6), so the page can say why |

For non-streaming or non-`messages` responses, the server sends only
`exchange_start`, `response_start`, and `exchange_end`.

### 6.2 Snapshot and detail endpoints *(v2, P1)*

`GET /api/exchanges` returns
`{"session": {...}, "evicted_before": 0, "exchanges": [Summary, …]}`.
A **Summary** is an Exchange without `request.body`, `response.message.content`,
and `response.raw`, plus:

```jsonc
"summary": {
  "request": { "model": "claude-opus-5-5", "max_tokens": 32000, "stream": true,
               "thinking_budget": 16000, "messages": 41, "tools": 14,
               "system_chars": 12400, "new_messages": 1,
               "system_changed": false, "tools_changed": false },
  "response": { "preview": "Let me read the proxy first.",
                "tool_calls": [{ "id": "toolu_…", "name": "Read", "arg": "…/proxy.py" }],
                "usage": { … }, "stop_reason": "tool_use" },
  "tool_results": [{ "tool_use_id": "toolu_…", "is_error": false, "chars": 5120 }]
}
```

The target is under 2 KB per exchange, so a 200-exchange snapshot stays
under about 400 KB and loads in well under a second.

- `GET /api/exchanges/{id}` returns the full Exchange.
- `GET /api/exchanges/{id}/raw` returns the raw response as `text/plain`.
- `GET /api/exchanges/{id}/transcript.md` *(P2)* returns the exchange's thread
  up to and including this exchange as readable Markdown (8.6).

### 6.3 Conversation threading *(v2, P1)*

Threading moves from the browser (v1 7.2) to `tap_analyze.py`, so the list,
search, stats, timeline, and log all agree, and the rule is unit-tested.

For each `messages` request, hash the system prompt, the tool list, and each
message, ignoring `cache_control`. Then:

1. **append**: an open thread with the same model, system hash, and tools hash
   whose last request's messages are a prefix of this one. Join it.
   `new_from` is the length of that prefix.
2. **rewritten**: no prefix match, but a thread with the same system hash and
   tools hash was active in the last 10 minutes. Join it and mark the history
   as rewritten. This is how context compaction and `/clear` show up. The page
   shows a "⟲ history rewritten" divider.
3. **new**: start a new thread.

Thread **roles**:

- `subagent`: the first user message of the thread equals the `prompt` input of
  an earlier `Task` / `Agent` tool_use. `spawned_by` points at that tool_use,
  and the parent's tool_use card links to the sub-agent thread.
- `side`: a thread with no tools whose only exchange has fewer than 3 messages
  (titles, summaries, classifiers, often on a small model).
- `main`: the first thread with tools that is not a sub-agent.
- `unknown`: anything else.

Thread ids are short and stable (`t1`, `t2`, …) and each gets a fixed colour
used everywhere in the UI.

## 7. Presentation

The page is designed for *reading a conversation* and *analysing a session*,
not for inspecting HTTP.

### 7.1 Layout

```
┌───────────────────────────────┬──────────────────────────────────────────────┐
│ ● live  session 15:40:02  ⏸   │ [Conversation] Timeline  Stats  Tools   ? ⌘K │
│ 🔍 search…              (/)   │  #17  POST /v1/messages   200   4.8s          │
│ [all ▾][model ▾][thread ▾][⚠] │  t2 main · turn 7 · claude-opus-5-5           │
│ 41 exchanges · 1.2M in · 9k $ │  in 41.2k (38.9k cached) · out 312 · 88 tok/s │
│ ─── t2 main ───────────────── │  ttfb 0.9s · ttft 1.3s · stop: tool_use       │
│ #17 15:42:07 opus  ▶ tool_use │  ───────────────────────────────────────────  │
│     🔧 Read …/proxy.py        │  ▸ System prompt (3 blocks, 12.4k chars)      │
│ #16 15:42:01 opus  ✓ end_turn │  ▸ Tools (14): Bash, Read, Edit, …            │
│ ··· 2 other requests          │  ▸ Earlier turns (22 messages)                │
│ ─── t3 subagent ← #12 Task ── │  ── new in this request ────────────────────  │
│ #14 15:41:50 sonnet ✓         │  👤 USER   tool_result (Read) ✓  132 lines ▸  │
│ #13 15:41:44 haiku  side ✓    │  ── response ───────────────────────────────  │
│  …                            │  🤖 ASSISTANT                                 │
│                               │   💭 thinking ▸                               │
│                               │   Let me read the proxy first.▌               │
│                               │   🔧 Read { file_path: "…/proxy.py" }  ⏱ 0.4s │
│                               │  [Conversation | Raw request | Raw response]  │
└───────────────────────────────┴──────────────────────────────────────────────┘
```

- **Left: exchange list**, newest at the top. Each row shows the id, time,
  short model name, a one-line preview (first text of the reply, or the tool
  name and its key argument), and a status badge: pending, streaming
  (animated), stop reason, HTTP error, or aborted.
  - *(v2, P1)* A left border in the thread's colour, and a thin header line
    whenever the thread changes. Option "group by thread" shows one
    collapsible section per thread instead of strict time order.
  - *(v2, P1)* Runs of consecutive non-`messages` rows collapse into one
    "··· N other requests" row that expands on click.
  - *(v2, P1)* Under the filters, a live session total: exchange count, input
    and output tokens, and the estimated cost if a price table is loaded.
- **Right: the selected exchange.** Follow mode works as in v1.
  - *(v2, P1)* **Pause** (⏸, key `p`): stop re-rendering the page while
    capture continues. The list header shows "12 new", and resuming applies
    them in one go. Useful when reading a long reply while the agent keeps
    working.

### 7.2 Showing only what's new

*(v1 behaviour, now driven by `thread.new_from` from 6.3.)* Messages that
already appeared earlier in the thread fold into "▸ Earlier turns (N
messages)". Only the new tail is expanded under "new in this request".

*(v2, P1)* The divider also states what else changed since the previous
request in the thread:

- "system prompt changed" and "tools changed" badges, each with a click-through
  to a line diff (8.5);
- "⟲ history rewritten" when `thread.history` is `rewritten`;
- "new conversation" or "sub-agent of #12" when the thread starts here.

### 7.3 Rendering content blocks

*(v1 table unchanged.)* v2 additions:

| Block | v2 addition |
|-------|-------------|
| `tool_use` | ⏱ **client time**: from the end of this response to the start of the request that carries the matching `tool_result`. This is how long the agent spent running the tool, including permission prompts. Shown once the result arrives |
| `tool_use` (`Task` / `Agent`) | "→ sub-agent t3 (5 exchanges, 18.2k out)" link that jumps to that thread |
| `tool_result` | Size in chars and lines. Error results get a red left border, and the ⚠ filter in the list finds them |
| `text` / `thinking` | Search hits are highlighted (7.8) |

Clicking a `tool_use` card scrolls to its matching `tool_result` in the next
exchange, and the reverse also works. *(v1.)*

### 7.4 Header metadata

For each exchange: method, path, HTTP status, model, `max_tokens`, and token
usage, with cached input tokens shown separately. Errors (HTTP ≥ 400, an
upstream `error` SSE event, the proxy's own 502, or a client abort) show as a
banner with the error message. *(v1.)*

*(v2, P1)* Also: thread id, role, and turn; TTFB, time to first token,
duration, and output tokens per second (live while streaming); the upstream
`request-id` with a copy button (useful for support tickets); and remaining
rate-limit headroom when the headers are present. A context bar shows
input tokens as a fraction of the model's context window, split into cached,
cache-write, and uncached.

### 7.5 Raw tabs

*(v1, unchanged.)* "Raw request" shows pretty-printed JSON of the redacted
headers and body. "Raw response" shows the SSE text exactly as received (up to
the cap). Each tab has a copy button.

*(v2, P2)* A JSON tree view toggle for the raw request (collapsible nodes,
click a node to copy its path such as `messages[3].content[0].text`).

### 7.6 Other traffic

`count_tokens`, `HEAD` probes, `GET /v1/models`, telemetry, and any non-JSON
or non-Anthropic shape (for example Codex via `--upstream`) still appear in the
list as compact grey rows, and show only the Raw tabs.

*(v2 change)* The filter's default is now **all**. Non-`messages` traffic is
kept visible but out of the way by collapsing runs of it (7.1), so nothing that
crosses the proxy is hidden by default.

### 7.7 Visual style

*(v1, unchanged.)* System font for prose, monospace for code; light and dark
themes; roles distinguished by an icon and label, not only colour; readable at
1280 px, list collapses to a top drawer below 900 px.

*(v2)* Thread colours come from a fixed palette of 8 that passes contrast in
both themes. Thread ids are always shown next to the colour, never colour alone.

### 7.8 Search and filters *(v2, P1)*

- A search box at the top of the list (key `/`). Search runs on the server
  (`GET /api/search?q=…&thread=…`) over request messages, system prompts, tool
  inputs, tool results, and reply text.
- Each piece of content is indexed **once, in the exchange where it first
  appeared** (its `new_from` tail, plus its response). Otherwise every hit would
  repeat in every later request of the thread.
- Results list as "#17 · tool_result (Read) · …matched text…". Enter opens the
  exchange, expands the containing block, scrolls to it, and highlights the
  match. `n` / `N` move to the next and previous hit.
- Plain text, case-insensitive by default. `re:` prefix for a regular
  expression. Fields: `tool:Bash`, `model:haiku`, `thread:t3`, `is:error`,
  `is:aborted`, `stop:max_tokens`. Fields can be combined with free text.
- Filter dropdowns (kind, model, thread) and a ⚠ "errors only" toggle sit next
  to the box and write the same query fields, so the box always shows the full
  active filter.

### 7.9 Keyboard and URL state *(v2, P1)*

| Key | Action |
|-----|--------|
| `j` / `k` | Next / previous exchange in the list |
| `[` / `]` | Previous / next exchange **in the same thread** |
| `f` | Toggle follow live |
| `p` | Toggle pause |
| `/` | Focus search · `Esc` clears it |
| `n` / `N` | Next / previous search hit |
| `1` `2` `3` `4` | Conversation / Timeline / Stats / Tools view |
| `r` | Cycle Conversation / Raw request / Raw response tab |
| `e` / `E` | Expand / collapse all blocks in the current exchange |
| `g` | Jump to the matching `tool_use` / `tool_result` of the focused card |
| `?` | Show this table as an overlay |

Shortcuts never fire while typing in an input.

The URL hash holds the view state, for example
`#view=conv&x=17&tab=raw-request&q=tool:Bash`. Reloading restores it, and the
browser Back button undoes selection changes. A link with `x=` turns follow
mode off.

### 7.10 Timeline view *(v2, P2)*

One horizontal lane per thread, time on the x axis. Each exchange is a bar:
a light segment for waiting (start to first token) and a solid segment for
streaming. Gaps between bars in a lane are client time (tool execution and the
user thinking). Bars are coloured by thread, red if errored, hatched if aborted.

```
        15:41:40      15:41:50      15:42:00      15:42:10
 t2 main   ░░███   ░████████            ░░██  ░░░███████▌
 t3 sub ←#12          ░██ ░░███ ░████
 t4 side              █
```

Hover shows id, model, tokens, and durations. Click selects the exchange and
switches to the Conversation view. Wheel zooms, drag pans. A "fit" button
shows the whole session. This answers "were the sub-agents running in
parallel?" and "where did the time go?" at a glance.

### 7.11 Stats view *(v2, P1 for the tiles and tables, P2 for the charts)*

- **Tiles**: exchanges, wall time, total input tokens (with cache-read
  share), output tokens, estimated cost, errors, aborts.
- **Per model**: requests, input, output, cache hit rate, mean TTFB, mean
  tokens per second.
- **Per thread**: role, turns, tokens, duration, spawned by.
- **Context growth chart** (P2): input tokens per turn for the selected thread,
  stacked by cached / cache-write / uncached, with a marker where history was
  rewritten. This shows when the context is about to fill and whether caching
  is working.
- **Slowest 10** and **largest 10** exchanges, each linked.

Every number links to the list filtered to the exchanges behind it.

### 7.12 Tools view *(v2, P1)*

A sortable table of every tool call in the session, one row per `tool_use`:
time, thread, tool name, key argument, client time, result size, ✓/✗.
Above it, a per-tool summary: calls, errors, median client time, total result
chars (which is what those calls added to the context).
Clicking a row opens its exchange with the card focused.

## 8. Behaviour requirements

| ID | Requirement |
|----|-------------|
| R1 | An exchange appears in the list within 100 ms of the proxy finishing reading the request |
| R2 | Text, thinking, and tool-input deltas show up on the page as they stream, not only when the response finishes |
| R3 | Reloading the page shows every retained exchange in its current state, including one still streaming |
| R4 | With 3 browser tabs open **and `--log` on**, the agent sees no measurable added latency. `publish()` never blocks the relay thread |
| R5 | Any exception in parsing, analysis, logging, storing, or publishing is caught and logged to stderr once, and forwarding continues unaffected |
| R6 | A slow or dead dashboard client is dropped when its queue exceeds 1000 events. It gets a `resync` event first if possible. Its next reconnect loads a fresh snapshot |
| R7 | Terminal output from the existing proxy is unchanged, unless `--quiet` is given |
| R8 | `--no-ui` turns the dashboard off entirely, so the proxy behaves exactly like v0. `--no-ui --log` records without serving a page |
| R9 | Gzip and other content-encodings are never seen, because the proxy already drops `accept-encoding`. The parser assumes UTF-8 text |
| R10 | Parallel requests (sub-agents, side-calls) are tracked independently and never mixed up |
| R11 *(v2)* | **Every** request that reaches the proxy produces an Exchange, including ones that fail before upstream answers and ones the client aborts. There is no code path where a request is forwarded but not recorded |
| R12 *(v2)* | A client disconnect mid-stream sets `state: aborted` within 1 s and keeps the partial message |
| R13 *(v2)* | The snapshot for 200 retained exchanges is under 1 MB and the page is interactive within 1 s of load |
| R14 *(v2)* | Opening an exchange whose detail is not cached renders within 300 ms on localhost |
| R15 *(v2)* | Search over 200 exchanges returns in under 300 ms |
| R16 *(v2)* | While a response streams at full speed, the page stays responsive: rendering is batched once per animation frame, and the list re-renders only changed rows |
| R17 *(v2)* | With `--log`, every finished exchange is in the file within 1 s of `exchange_end`, and an exchange evicted from memory is never lost from the log |
| R18 *(v2)* | Thread assignment is deterministic: the same captured session always yields the same thread ids and roles, live or reopened from a log |

### 8.4 Session stats *(v2)*

`tap_analyze.stats(exchanges)` is a pure function returning the totals behind
7.11: per model, per thread, per tool, plus session totals. The server keeps it
up to date incrementally and pushes it as the `stats` event.

**Cost** is computed only if `--pricing FILE` is given: a JSON map of model id
prefix to per-million-token prices for input, output, cache read, and cache
write. Without it, the page shows tokens only. No prices are hard-coded,
because they change and a wrong number is worse than none.

### 8.5 Diffs *(v2, P2)*

- Within a thread, "system prompt changed" and "tools changed" open a line
  diff against the previous request in that thread.
- Any two exchanges can be compared: shift-click a second row, then "Compare"
  shows a side-by-side diff of system, tools, and messages, with unchanged
  messages folded.
- Diffing is a simple line-based LCS in JS; no library.

### 8.6 Export *(v2)*

- **P1**: "Download session" (`/api/export.jsonl`) writes all retained
  exchanges in the log format (10.1).
- **P2**: "Copy as Markdown" on an exchange or a thread: a readable transcript
  with `## User` / `## Assistant` headings, tool calls as fenced JSON, and long
  tool results cut to 40 lines. Meant for lab reports and bug reports.
- Exports are always built from the redacted store, so C5 holds.

## 9. Security and limits

- **Redaction:** before storing, replace the values of `authorization`,
  `x-api-key`, `cookie`, and `set-cookie` with their first 12 characters (and
  never more than a third of the value) plus `…[redacted]`. The full value
  never enters the `Store`, the log file, or any export.
- *(v2)* **Body scan:** also redact strings in request bodies that match
  `sk-ant-[A-Za-z0-9_-]{20,}`, in case the agent pastes a key into a prompt or
  a tool result. This is defence in depth, not a guarantee, and the page says
  so in a footnote.
- **XSS:** *(v1, unchanged)* every piece of captured text is inserted with
  `textContent` or is escaped first. Search highlighting wraps matches in
  `<mark>` elements built with the DOM API, never by string concatenation.
- **Bind and Host check:** *(v1, unchanged.)*
- **Log files** *(v2)*: created with mode `0600`. The default directory
  `./tap-logs/` gets a `.gitignore` containing `*` on first use, so recorded
  prompts and code are not committed by accident.
- **Memory:** the store keeps the most recent 200 exchanges (`--keep N`).
  Each exchange's `raw` response is capped at 2 MB, with a truncation marker
  after the cap. Parsed messages are always complete. *(v2)* When an exchange
  is evicted, the list shows "N older exchanges not in memory" at the bottom,
  with "(in log file)" when `--log` is on.

## 10. CLI

```
python3 proxy.py [--port 8041] [--upstream URL]
                 [--ui-port 8042] [--no-ui] [--keep 200] [--quiet]
                 [--log [FILE]] [--pricing FILE]                     # v2, P1
python3 proxy.py --open FILE [--ui-port 8042]                        # v2, P2
```

- `--log` with no value writes `./tap-logs/<session-start>.jsonl`.
- `--open FILE` serves the dashboard for a recorded session. The proxy port is
  not opened. The page shows "recorded session" instead of "● live".

### 10.1 Log format *(v2)*

JSON Lines, UTF-8. The first line is
`{"type": "session", "version": 2, "started_at": …, "upstream": …}`. Each
following line is `{"type": "exchange", …full Exchange…}`, written once at
`exchange_end`. A crash loses at most the exchanges that were still open. The
writer runs on its own thread with an unbounded queue, so a slow disk can only
delay the log, never the relay.

## 11. Testing and acceptance

**Unit tests** (`python3 -m unittest`, stdlib only). v1 tests stay. v2 adds:

- `tap_analyze` threading, from recorded request bodies: append, rewritten
  (compaction), new; sub-agent detection through a `Task` tool_use; side-call
  detection; same result live and from a log (R18).
- `tap_analyze.stats`: totals, cache hit rate, cost with and without a price
  table.
- Search: a hit is reported once (in the exchange where the text first
  appeared), field filters, `re:` queries, invalid regex returns an error, not
  a 500.
- Summaries: no `request.body`, message content, or raw text leaks into a
  summary; size under 2 KB for a 40-message request.
- Abort: a client that disconnects mid-stream yields `state: aborted` and a
  partial message (R12).
- Log: round trip write then `--open` gives an identical snapshot; file mode
  is `0600`; a credential never appears in the file; body-scan redaction.
- Recording completeness (R11): requests that fail with a connection error,
  an upstream 529, and a client abort each produce exactly one Exchange.

**Manual acceptance checklist** (v1 items 1–8 stay), plus:

9. Ask the agent something that makes it spawn a sub-agent. The list shows a
   new thread marked as a sub-agent of the right `Task` call, and the Task card
   links to it.
10. Press Esc in Claude Code mid-reply. The exchange shows as aborted with the
    partial text.
11. Search `tool:Read proxy.py`. Exactly the Read calls on that file are
    listed, and Enter jumps to the card.
12. Press `j`, `k`, `[`, `]`, `3`, `?`. Each does what 7.9 says. Copy the URL,
    open it in a new tab, and the same exchange and tab are shown.
13. Pause during a long reply, wait, resume. Nothing is missing.
14. Run with `--log`, restart with `--open` on that file. The Stats view shows
    the same numbers as before.
15. Run `/compact` in Claude Code. The next request shows "⟲ history
    rewritten" in the same thread.

## 12. Open questions

1. ~~Persistence~~ *Resolved in v2:* opt-in `--log`, see 10.1. Still open:
   should logging be **on by default**? It fits "track everything" but writes
   prompts and code to disk without being asked. Proposed: keep it opt-in.
2. **One port vs two:** *(v1, unchanged)* revisit if two ports feel clunky.
3. **Codex format:** *(v1, unchanged)* dedicated renderer, or raw fallback?
4. ~~Conversation threading~~ *Resolved in v2:* threads with roles (6.3), a
   thread-coloured list, and timeline lanes (7.10).
5. *(v2)* **Memory for long sessions:** every request repeats its history, so
   200 exchanges hold the same messages many times. Should the store keep each
   distinct message once (content-addressed by the hashes in 6.3) and rebuild
   bodies on demand? It would cut memory by roughly 10× on long sessions, at
   the cost of more complex code.
6. *(v2)* **Context window sizes** for the context bar (7.4): read them from a
   small built-in table, from `--pricing FILE`, or leave the bar out when the
   model is unknown?
