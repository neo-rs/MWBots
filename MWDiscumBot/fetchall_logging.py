"""Minimal logging for fetchall (MWDiscumBot). Writes to console and fetchall log file."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parent
_LOGS_DIR = _ROOT / "logs"
_FETCHALL_LOG_PATH = _LOGS_DIR / "Botlogs" / "fetchalllogs.json"


def _ensure_dir(p: Path) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _append_json_line(path: Path, entry: dict) -> None:
    _ensure_dir(path)
    try:
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def log_fetchall(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    print(f"[FETCHALL] {msg}", flush=True)
    entry = {"level": "INFO", "tag": "FETCHALL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)


def log_warn(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    print(f"[FETCHALL] [WARN] {msg}", flush=True)
    entry = {"level": "WARN", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)


def log_info(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    print(f"[FETCHALL] [INFO] {msg}", flush=True)
    entry = {"level": "INFO", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)
