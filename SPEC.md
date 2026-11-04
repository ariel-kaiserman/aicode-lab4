# SPEC: Live traffic dashboard for `proxy.py`

## 1. Goal

While `proxy.py` is running, open a page in the browser that updates live and
shows the conversation between the agent (Claude Code) and the model: what the
agent sends, what the model streams back, and the tool calls and results going
back and forth. It should be well formatted and easy to read, not a JSON dump.

The terminal output stays as it is. The dashboard is a second view of the same
traffic.

## 2. Non-goals

- Changing, blocking, or replaying traffic. The proxy still forwards bytes
  unchanged.
- Saving traffic to disk between runs (see Open Questions).
- Authentication or multi-user access. The dashboard is local only.
- Full support for the Codex / OpenAI Responses format. Section 7.6 covers a
  raw fallback for it.
- Any build step, framework, or third-party dependency.

## 3. Constraints

| # | Constraint | Why |
|---|------------|-----|
| C1 | Python standard library only, same as `proxy.py` | "Nothing to install" stays true |
| C2 | Front end is one self-contained HTML file with inline CSS and vanilla JS, and no CDN | Works offline, no build |
| C3 | The dashboard must never delay, alter, or break proxied traffic | The agent is the real user of the proxy |
| C4 | Bind to `127.0.0.1` only | Traffic contains prompts, code, and tokens |
| C5 | Credentials are redacted before they reach the dashboard | The browser must never see an API key or bearer token |

## 4. User experience

```
python3 proxy.py
  proxy listening on http://localhost:8041  ->  https://api.anthropic.com
  dashboard at       http://localhost:8042
  run the agent with:  ANTHROPIC_BASE_URL=http://localhost:8041 claude
```

1. The user starts the proxy and opens `http://localhost:8042`.
2. The user starts `claude` against the proxy and works normally.
3. Every API call shows up on the page within about 100 ms of reaching the
   proxy. The model's reply fills in token by token as it streams.
4. Reloading the page restores everything captured so far in this run.
5. If the proxy restarts, the page shows "disconnected", reconnects
   automatically, and then shows the new (empty) session.

## 5. Architecture

```
 agent ──HTTP──▶ :8041 Tap.relay() ──HTTPS──▶ api.anthropic.com
                     │  publish(event)   (non-blocking)
                     ▼
                 Store (in memory, thread-safe)
                     │  fan-out to subscriber queues
                     ▼
 browser ◀──SSE── :8042 Dashboard server ── GET /  (index.html)
                                         ── GET /api/exchanges (snapshot)
                                         ── GET /events        (live stream)
```

- **Two ports, one process.** The proxy port forwards every path to upstream
  unchanged, so dashboard routes go on their own port (`--ui-port`, default
  `8042`) and can never collide with an API path. Both servers are
  `ThreadingHTTPServer`s. The UI server runs in a daemon thread.
- **Live transport: Server-Sent Events.** Data only flows one way (server to
  browser), SSE works with the stdlib, and `EventSource` reconnects on its own.
- **Parsing happens in Python.** The proxy parses the model's SSE stream as it
  passes through and publishes *normalized* events. The browser only renders
  them. This keeps the parser unit-testable with `unittest`.

### 5.1 Code layout

| File | Contents |
|------|----------|
| `proxy.py` | Existing relay, plus `publish()` calls and the new CLI flags |
| `tap_store.py` | `Store`, `Exchange`, subscriber fan-out, redaction |
| `tap_stream.py` | Incremental parser for the Anthropic Messages SSE stream (pure, no I/O) |
| `tap_ui.py` | Dashboard HTTP handler (`/`, `/api/exchanges`, `/events`) |
| `dashboard.html` | The page, served as-is |
| `tests/` | `unittest` tests for the parser, store, and redaction |

Putting everything in `proxy.py` is also acceptable if it stays readable. The
module boundaries above matter more than the file count.

## 6. Data model

One **Exchange** is one HTTP request/response pair.

```jsonc
{
  "id": 17,                         // increasing per run
  "started_at": "2026-11-01T15:42:07.120Z",
  "ended_at":   "2026-11-01T15:42:11.904Z",  // null while streaming
  "method": "POST",
  "path": "/v1/messages?beta=true",
  "kind": "messages",               // messages | count_tokens | other
  "state": "streaming",             // pending | streaming | done | error
  "status": 200,
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
    "error": null
  }
}
```

### 6.1 Live events (`/events`)

Each SSE message has `event:` set to one of the types below and `data:` set to
JSON that always includes `"id"` (the exchange id).

| Event | Payload | When |
|-------|---------|------|
| `exchange_start` | full Exchange (request filled, response empty) | Request body has been read |
| `response_start` | `status`, response headers | Upstream status line received |
| `block_start` | `index`, `type`, and `name`/`tool_use_id` for tool_use | `content_block_start` |
| `block_delta` | `index`, `text` (text, thinking, or partial tool-input JSON) | Each `content_block_delta` |
| `block_stop` | `index`, and the final parsed `input` for tool_use | `content_block_stop` |
| `message_delta` | `stop_reason`, `usage` | `message_delta` |
| `exchange_end` | full final Exchange | Stream finished or errored |

For non-streaming or non-`messages` responses, the server sends only
`exchange_start`, `response_start`, and `exchange_end`.

`GET /api/exchanges` returns `{"session_started_at": …, "exchanges": [ … ]}`
with every retained Exchange in its current state. On connect or reconnect, the
page loads this snapshot and then applies live events. Events for an `id` the
page hasn't seen yet trigger a refetch of the snapshot.

## 7. Presentation

The page is designed for *reading a conversation*, not for inspecting HTTP.

### 7.1 Layout

```
┌───────────────────────────────┬──────────────────────────────────────────────┐
│ ● live   session 15:40:02     │  #17  POST /v1/messages   200   4.8s          │
│ [filter: all | messages ▾]    │  claude-opus-5-5 · in 41.2k (38.9k cached)    │
│                               │  · out 312 · stop: tool_use                   │
│ #17 15:42:07 opus  ▶ tool_use │  ───────────────────────────────────────────  │
│     "Let me read proxy.py"    │  ▸ System prompt (3 blocks, 12.4k chars)      │
│ #16 15:42:01 opus  ✓ end_turn │  ▸ Tools (14): Bash, Read, Edit, …            │
│ #15 15:41:58 count_tokens     │  ▸ Earlier turns (22 messages)                │
│ #14 15:41:50 haiku ✓          │  ── new in this request ────────────────────  │
│  …                            │  👤 USER   tool_result (Read) ✓  132 lines ▸  │
│                               │  ── response ───────────────────────────────  │
│                               │  🤖 ASSISTANT                                 │
│                               │   💭 thinking ▸                               │
│                               │   Let me read the proxy first.▌               │
│                               │   🔧 Read { file_path: "…/proxy.py" }         │
│                               │  [Conversation | Raw request | Raw response]  │
└───────────────────────────────┴──────────────────────────────────────────────┘
```

- **Left: exchange list**, newest at the top. Each row shows the id, time,
  short model name, a one-line preview (first text of the reply, or the tool
  name), and a status badge: pending, streaming (animated), stop reason, or
  HTTP error.
- **Right: the selected exchange.** By default the newest exchange is selected
  and **follow mode** is on: the view jumps to each new exchange and
  auto-scrolls while it streams. Clicking an older row or scrolling up turns
  follow mode off. A "⤓ follow live" button turns it back on.

### 7.2 Showing only what's new

Each Messages API request resends the whole conversation, so showing it in full
every time is unreadable. The page compares each request's `messages` array
with the previous `messages` request **from the same conversation** (same
`system` prompt and model, and the earlier array is a prefix of the new one):

- Messages that already appeared earlier fold into one collapsed line,
  "▸ Earlier turns (N messages)".
- Only the new tail is shown expanded, under a "new in this request" divider.
- If there's no matching earlier request (first turn, a sub-agent, a
  side-call to a smaller model), everything is shown and the exchange is
  tagged *new conversation*.

### 7.3 Rendering content blocks

| Block | Rendering |
|-------|-----------|
| `system` | Collapsed by default, with block count and size. Expanded, each block is shown as preformatted text, with `cache_control` shown as a small 📌 badge |
| `tools` | Collapsed list of names. Click a name to see its description and `input_schema`, pretty-printed |
| user `text` | Chat bubble, text wrapped, newlines kept. `<system-reminder>`-style XML-tagged sections fold into labeled collapsibles |
| assistant `text` | Chat bubble. Light Markdown: code fences, inline code, bold, lists. No HTML is ever passed through (see 9) |
| `thinking` | Collapsed "💭 thinking" with character count. Expands to dimmed italic text. `redacted_thinking` shows as a placeholder |
| `tool_use` | Card: 🔧 tool name, the key argument inline (`command` for Bash, `file_path` for Read/Edit/Write, `pattern` for Grep), full input as pretty JSON in a collapsible. While streaming, shows partial JSON as it arrives |
| `tool_result` | Card linked to its `tool_use` by `tool_use_id`, showing the tool's name. ✓ or ✗ (`is_error`). Content over 20 lines is collapsed to the first 8, with "show all N lines" |
| `image` / `document` | Placeholder chip with media type and size. Images are not rendered |
| unknown | Pretty JSON, so nothing is ever hidden |

Clicking a `tool_use` card scrolls to its matching `tool_result` in the next
exchange, and the reverse also works.

### 7.4 Header metadata

For each exchange: method, path, HTTP status, duration (time to first byte and
total), model, `max_tokens`, and token usage, with cached input tokens shown
separately. Errors (HTTP ≥ 400, an upstream `error` SSE event, or the proxy's
own 502) show as a red banner with the error message.

### 7.5 Raw tabs

"Raw request" shows pretty-printed JSON of the redacted headers and body.
"Raw response" shows the SSE text exactly as received (up to the cap). Each tab
has a copy button. These are the escape hatch when the formatted view gets
something wrong.

### 7.6 Other traffic

`count_tokens`, `HEAD` probes, `GET /v1/models`, telemetry, and any non-JSON
or non-Anthropic shape (for example Codex via `--upstream`) still appear in the
list as compact grey rows, and show only the Raw tabs. A filter dropdown
switches between *all* and *messages only*. The default is *messages only*.

### 7.7 Visual style

- System font for prose, monospace for code, JSON, and tool input.
- Light and dark themes, following `prefers-color-scheme`.
- Roles are distinguished by more than color: an icon and a label on every
  bubble.
- Readable at 1280 px wide. Below 900 px, the list collapses to a top drawer.

## 8. Behaviour requirements

| ID | Requirement |
|----|-------------|
| R1 | An exchange appears in the list within 100 ms of the proxy finishing reading the request |
| R2 | Text, thinking, and tool-input deltas show up on the page as they stream, not only when the response finishes |
| R3 | Reloading the page shows every retained exchange in its current state, including one still streaming |
| R4 | With 3 browser tabs open, the agent sees no measurable added latency. `publish()` never blocks the relay thread |
| R5 | Any exception in parsing, storing, or publishing is caught and logged to stderr once, and forwarding continues unaffected |
| R6 | A slow or dead dashboard client is dropped when its queue exceeds 1000 events. Its next reconnect loads a fresh snapshot |
| R7 | Terminal output from the existing proxy is unchanged, unless `--quiet` is given (new flag, suppresses the body dump) |
| R8 | `--no-ui` turns the dashboard off entirely, so the proxy behaves exactly like today |
| R9 | Gzip and other content-encodings are never seen, because the proxy already drops `accept-encoding`. The parser assumes UTF-8 text |
| R10 | Parallel requests (sub-agents, side-calls) are tracked independently and never mixed up |

## 9. Security and limits

- **Redaction:** before storing, replace the values of `authorization`,
  `x-api-key`, `cookie`, and `set-cookie` with their first 12 characters plus
  `…[redacted]`. The full value never enters the `Store`.
- **XSS:** every piece of captured text is inserted with `textContent` or is
  escaped first. The Markdown renderer works on escaped text and emits only a
  fixed whitelist of tags. Model output and tool results are untrusted input.
- **Bind:** both servers bind to `127.0.0.1`. The UI server rejects requests
  whose `Host` header isn't `localhost:<ui-port>` or `127.0.0.1:<ui-port>`, to
  block DNS rebinding.
- **Memory:** the store keeps the most recent 200 exchanges (`--keep N`).
  Each exchange's `raw` response is capped at 2 MB, with a truncation marker
  after the cap. Parsed messages are always complete.

## 10. CLI

```
python3 proxy.py [--port 8041] [--upstream URL]
                 [--ui-port 8042] [--no-ui] [--keep 200] [--quiet]
```

## 11. Testing and acceptance

**Unit tests** (`python3 -m unittest`, stdlib only):

- `tap_stream`: feed a recorded Messages SSE stream split at arbitrary byte
  boundaries, including mid-UTF-8 sequences and mid-line splits. The
  reassembled message must match the expected JSON. Cover text, thinking,
  tool_use with `input_json_delta`, `error` events, and `ping`.
- Redaction: credentials never appear in the snapshot JSON.
- Store: eviction past `--keep`, and that a full subscriber queue is dropped
  without blocking `publish()`.

**Manual acceptance checklist:**

1. Run `python3 proxy.py`, open `:8042`, then run
   `ANTHROPIC_BASE_URL=http://localhost:8041 claude` and ask it to read a file
   and edit it.
2. The page shows the user prompt, the assistant streaming text, the
   `tool_use` card for Read, and then the matching `tool_result` in the next
   exchange, all live.
3. Earlier turns fold. Only the new messages are expanded.
4. Reload mid-stream: the page restores and keeps streaming.
5. Kill and restart the proxy: the page shows "disconnected", then reconnects.
6. `grep` the page source (view-source and the `/api/exchanges` output) for the
   API key or token. It must not appear.
7. A prompt containing `<script>alert(1)</script>` renders as text.
8. `--no-ui` gives terminal behaviour identical to the original `proxy.py`.

## 12. Open questions

1. **Persistence:** should exchanges also be appended to a `.jsonl` file so a
   session can be reopened later? (Proposed: not in v1. Add `--log FILE` later.)
2. **One port vs two:** serving the UI under a reserved prefix such as
   `/_tap/` on `:8041` would mean one URL instead of two. Two ports were chosen
   to keep the proxy's path space untouched. Revisit if two ports feel clunky.
3. **Codex format:** is it worth a dedicated renderer for the OpenAI Responses
   stream, or is the raw fallback enough for this lab?
4. **Conversation threading:** should related exchanges (main agent vs
   sub-agents) be grouped into separate lanes rather than one flat list?
