"""tap_log.py: the opt-in session log (SPEC 10.1).

JSON Lines, UTF-8. The first line describes the session:

    {"type": "session", "version": 2, "started_at": ..., "upstream": ..., "proxy": ...}

and every following line is one finished exchange, written once at
exchange_end, already redacted by the store:

    {"type": "exchange", ...full Exchange...}

The writer runs on its own thread with an unbounded queue, so a slow disk can
only delay the log, never the relay (C3). Files are created with mode 0600, and
the default directory gets a `.gitignore` containing `*` (SPEC 9).
"""

from __future__ import annotations

import json
import os
import queue
import threading
from datetime import datetime
from typing import Any

from tap_store import log_once

DEFAULT_DIR = "tap-logs"
VERSION = 2


def default_path(directory: str = DEFAULT_DIR) -> str:
    return os.path.join(directory, datetime.now().strftime("%Y-%m-%dT%H-%M-%S") + ".jsonl")


def _prepare_dir(path: str) -> None:
    directory = os.path.dirname(path) or "."
    fresh = not os.path.isdir(directory)
    os.makedirs(directory, exist_ok=True)
    if os.path.basename(os.path.abspath(directory)) == DEFAULT_DIR:
        ignore = os.path.join(directory, ".gitignore")
        if fresh or not os.path.exists(ignore):
            with open(ignore, "w", encoding="utf-8") as f:
                f.write("# recorded prompts and code: never commit these\n*\n")


class LogWriter:
    def __init__(self, path: str, session: dict[str, Any]) -> None:
        _prepare_dir(path)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            raise FileExistsError(f"log file already exists and is not empty: {path}")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        os.fchmod(fd, 0o600)               # also if the file existed (empty) with wider bits
        self.path = path
        self._file = os.fdopen(fd, "a", encoding="utf-8")
        self._file.write(json.dumps({"type": "session", "version": VERSION, **session}, ensure_ascii=False) + "\n")
        self._file.flush()
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self.written = 0
        threading.Thread(target=self._loop, name="tap-log", daemon=True).start()

    def write(self, exchange: Any) -> None:
        """Queue a finished exchange (anything with to_json(), or a JSON object string)."""
        self._queue.put(exchange)

    def flush(self, timeout: float = 5.0) -> bool:
        done = threading.Event()
        self._queue.put(done)
        return done.wait(timeout)

    def _loop(self) -> None:
        while True:
            item = self._queue.get()
            if isinstance(item, threading.Event):
                item.set()
                continue
            try:
                text = item if isinstance(item, str) else item.to_json()
                self._file.write('{"type": "exchange", ' + text.lstrip()[1:] + "\n")
                self._file.flush()          # R17: on disk within a second of exchange_end
                self.written += 1
            except Exception as e:  # noqa: BLE001
                log_once(e, "log writer")


def read_log(path: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """(session line, exchanges) from a log file. A torn last line (crash) is skipped."""
    session: dict[str, Any] = {}
    exchanges: dict[int, dict[str, Any]] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError:
                continue
            if not isinstance(obj, dict):
                continue
            kind = obj.pop("type", None)
            if kind == "session" and not session:
                session = obj
            elif kind == "exchange" and isinstance(obj.get("id"), int):
                exchanges[obj["id"]] = obj
    if not session:
        raise ValueError(f"{path}: not a proxy.py session log (no session line)")
    return session, [exchanges[k] for k in sorted(exchanges)]
