# SPEC changes: v1 → v2

v1 (commit `c08123e`) answers "what is happening in *this* exchange?" well.
Reading the spec against the code shows where it falls short for *analysing a
whole session*:

- Traffic can be lost or hidden. Aborted requests have no state, evicted
  exchanges disappear, nothing is saved, and non-`messages` traffic is filtered
  out by default.
- There is no structure. Main agent, sub-agents, and side-calls are one flat
  list, and threading lives only in the browser.
- There are no answers across exchanges: no search, no totals, no timing
  breakdown, and no tool overview.
- It will not scale. The snapshot sends every full request body, and each one
  repeats the whole conversation.
- Navigation is mouse-only, and state is lost on reload.

The changes below fix these. **P1** is the next milestone; **P2** comes after.

## A. Track everything

| # | Change | Spec section | Pri | Why |
|---|--------|--------------|-----|-----|
| A1 | New `aborted` state for client disconnects, partial message kept, `error.source` field | 6, R12 | P1 | Pressing Esc in Claude Code is common. Today the `BrokenPipeError` from `wfile.write` lands in the generic `except BaseException` in `relay()`, so a user cancel looks the same as an upstream failure. The chunk that failed to write is also never fed to the store |
| A2 | R11: every forwarded request produces an Exchange, with tests for connection errors, 529s, and aborts | 8, 11 | P1 | Turns "track everything" into a testable requirement |
| A3 | `timing` object: `response_at`, `first_token_at`, TTFB, TTFT, duration, output tok/s | 6, 7.4 | P1 | v1 shows "time to first byte" in 7.4 but its data model has no field for it. Slowness can be in the network, the queue, or generation, and these numbers tell them apart |
| A4 | `upstream` object: `request-id`, rate-limit headers, `retry-after` | 6, 7.4 | P1 | The request id is what you need for a bug report. Rate-limit headroom explains throttling and retries |
| A5 | `sizes` (request/response bytes), explicit `raw_truncated` flag | 6 | P1 | Shows how big requests get. The flag means the UI doesn't have to parse the marker text |
| A6 | Opt-in `--log [FILE]` JSONL, written off the relay thread; `--open FILE` to reopen | 10, 10.1, R17 | P1 / P2 | Resolves open question 1. Sessions can be analysed after the fact and evicted exchanges are kept |
| A7 | Default filter changes from *messages only* to *all*, with runs of other traffic collapsed | 7.1, 7.6 | P1 | Hiding traffic by default goes against "track everything". Collapsing keeps the list readable |
| A8 | `exchange_evicted` event and an "N older exchanges not in memory" marker | 6.1, 9 | P1 | Today evicted exchanges vanish silently |
| A9 | `resync` event before dropping a slow subscriber | 6.1, R6 | P1 | The page can say *why* it reloaded instead of just flickering |
| A10 | `seq` ordering rule written into the spec | 6.1 | — | The code already does this (`tap_store.py`), but the spec didn't say so. A reimplementation could double-apply events |

## B. Structure: conversation threads

| # | Change | Spec section | Pri | Why |
|---|--------|--------------|-----|-----|
| B1 | Thread assignment moves from JS to `tap_analyze.py`, with a precise rule (append / rewritten / new) | 6.3, C6 | P1 | The list, search, stats, timeline, and log must agree. A pure Python function can be unit-tested (R18) |
| B2 | Thread roles: main, subagent (linked to the spawning `Task` call), side | 6.3, 7.3 | P1 | Resolves open question 4. "Which sub-agent did this?" is the most common question in multi-agent sessions |
| B3 | "History rewritten" detection | 6.3, 7.2 | P1 | v1 tags a compacted conversation as a *new conversation*, which is wrong and confusing |
| B4 | Thread colour bar in the list, group-by-thread option, `[` / `]` to move within a thread | 7.1, 7.9 | P1 | Lets you follow one agent through interleaved parallel traffic |
| B5 | "System prompt changed" / "tools changed" badges | 7.2 | P1 | `<system-reminder>` and tool-list changes between turns are invisible in v1 |

## C. Analysis features

| # | Change | Spec section | Pri | Why |
|---|--------|--------------|-----|-----|
| C1 | Server-side full-text search with field filters (`tool:`, `model:`, `thread:`, `is:error`, `re:`) | 7.8 | P1 | Lets you find "where did it read X?" without clicking through 50 exchanges. Each hit is indexed once, where the text first appeared, so the results aren't flooded with repeats |
| C2 | Tools view: every tool call with client time, result size, ✓/✗, plus per-tool totals | 7.12 | P1 | Shows what the agent actually did, and which tools fill the context |
| C3 | Tool **client time** on each `tool_use` card | 7.3 | P1 | A free measurement: the gap between the response and the next request is tool time plus permission prompts. It shows where wall time goes when the model isn't the bottleneck |
| C4 | Stats view: tiles, per-model and per-thread tables, slowest and largest exchanges | 7.11, 8.4 | P1 | Session-level totals: cache hit rate, tokens, errors |
| C5 | Cost estimate only from `--pricing FILE`, never hard-coded | 8.4 | P1 | Prices change. A confidently wrong number is worse than none |
| C6 | Context bar in the header and a context-growth chart per thread | 7.4, 7.11 | P1 / P2 | Shows how close the agent is to the context limit and whether caching works |
| C7 | Timeline view with one lane per thread, wait vs stream segments | 7.10 | P2 | Answers "was this parallel?" and "where did the time go?" visually |
| C8 | Diffs: within a thread and between any two exchanges | 8.5 | P2 | Useful for prompt engineering: what exactly changed between these two calls? |
| C9 | Export: session JSONL (P1), Markdown transcript (P2) | 8.6 | P1 / P2 | Useful for lab write-ups and bug reports. Built from the redacted store |
| C10 | JSON tree view with copyable paths in the raw tab | 7.5 | P2 | Large raw bodies are hard to navigate as flat text |

## D. Smoother to use

| # | Change | Spec section | Pri | Why |
|---|--------|--------------|-----|-----|
| D1 | Keyboard shortcuts (`j/k`, `[/]`, `/`, `n/N`, `1–4`, `f`, `p`, `e`, `g`, `?`) | 7.9 | P1 | v1 only handles PageUp/ArrowUp/Home for leaving follow mode |
| D2 | URL hash keeps view, selection, tab, and query; Back button works | 7.9 | P1 | Reload restores what you were looking at, and an exchange can be linked |
| D3 | Pause rendering while capture continues | 7.1 | P1 | Lets you read a long reply while the agent keeps working, without it jumping |
| D4 | Summaries-only snapshot plus on-demand detail with a 20-entry LRU | 5, 6.2, R13, R14 | P1 | Likely the biggest speed win. v1's snapshot carries every full body (each repeats the whole conversation), so reload slows down as the session grows |
| D5 | `exchange_end` carries a summary, not the full exchange | 6.1 | P1 | Same reason: avoids pushing MBs to every tab after each turn |
| D6 | Performance requirements R13–R16 | 8 | P1 | "Smooth" becomes something you can check |

## E. Security

| # | Change | Spec section | Pri | Why |
|---|--------|--------------|-----|-----|
| E1 | Redaction covers the log file and exports, not only the Store | C5, 9 | P1 | Persistence opens a new place for keys to leak |
| E2 | Body scan for `sk-ant-…` strings | 9 | P1 | Keys pasted into prompts or printed by `env` in a Bash result would otherwise be stored |
| E3 | Log files `0600`, auto `.gitignore` in `tap-logs/` | 9 | P1 | Keeps recorded code and prompts from being committed or read by other users |
| E4 | Search highlights built with the DOM API | 9 | P1 | Highlighting is a classic place for an XSS bug to slip in |

## F. Housekeeping

- Header note says v1 is implemented and v2 is additive. Every v2 item is
  marked and given a priority.
- New modules `tap_analyze.py` and `tap_log.py` in the layout (5.1).
- Open questions: 1 and 4 resolved; new 5 (dedupe repeated messages in
  memory) and 6 (where context-window sizes come from).
- Non-goals: "no persistence" removed; "exact billing" added.
- Acceptance checklist items 9–15 added for the new behaviour.

## Decisions I made that you may want to change

1. **Logging is opt-in**, even though the brief is "track everything". Writing
   prompts and code to disk without being asked felt wrong. Switching the
   default is a one-line change to 10 and open question 1.
2. **Analysis runs in Python, not JS** (C6). It means more server endpoints,
   but it keeps the testing story from v1, and search over 200 full bodies in
   the browser would need all of them downloaded, which D4 removes.
3. **Timeline, diffs, and Markdown export are P2.** They are the most code for
   the least day-to-day value compared with search, threads, and the Tools view.
