from __future__ import annotations

import json
import logging
import os
import platform
import re as _re
import sys
import threading
import time
import builtins as _builtins
import importlib
from pathlib import Path
from typing import Any, Dict, List, Optional

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_LOGS_DIR = _BOT_DIR / "logs"

_BOT_LOG_PATH = _LOGS_DIR / "Botlogs" / "datamanagerbotlogs.json"
_SYSTEM_LOG_PATH = _CONFIG_DIR / "systemlogs.json"
_TRACE_LOG_PATH = _LOGS_DIR / "decision_traces.jsonl"

_CONSOLE_LOCK = threading.RLock()
_VERBOSE_CONSOLE: bool = True

# ---------------- Discum-style console colors ----------------
try:
    _colorama = importlib.import_module("colorama")
    _cinit = getattr(_colorama, "init", lambda **kwargs: None)
    _F = getattr(_colorama, "Fore", None)
    _S = getattr(_colorama, "Style", None)
    if callable(_cinit):
        _cinit(autoreset=True)
    if _F is None or _S is None:
        raise ImportError("colorama missing symbols")
except Exception:
    class _Dummy:
        def __getattr__(self, k):
            return ""
    _F = _S = _Dummy()

_ANSI_ESC = "\x1b["


def _colorize_line(text: str) -> str:
    if _ANSI_ESC in text:
        return text
    s = text
    s = _re.sub(r"^\[INFO\]", f"{_F.GREEN}[INFO]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[WARN(?:ING)?\]", f"{_F.YELLOW}[WARN]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[ERROR\]", f"{_F.RED}[ERROR]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[DEBUG\]", f"{_F.WHITE}[DEBUG]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[FETCHALL\]", f"{_F.CYAN}[FETCHALL]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[FORWARD\]", f"{_F.MAGENTA}[FORWARD]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[GLOBAL\]", f"{_F.BLUE}[GLOBAL]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[FILTER\]", f"{_F.YELLOW}[FILTER]{_S.RESET_ALL}", s)
    s = _re.sub(
        r"(?P<prefix>\s|^)#([a-z0-9\-_]+)",
        lambda m: f"{m.group('prefix')}{_F.BLUE}#{m.group(2)}{_S.RESET_ALL}",
        s,
        flags=_re.IGNORECASE,
    )
    s = _re.sub(
        r"(?P<prefix>\s|^)@([A-Za-z0-9_][A-Za-z0-9_\.\-]{1,30})",
        lambda m: f"{m.group('prefix')}{_F.MAGENTA}@{m.group(2)}{_S.RESET_ALL}",
        s,
    )
    return s


def _print_colorized(*args, **kwargs):
    try:
        with _CONSOLE_LOCK:
            if not args:
                return _builtins.print(*args, **kwargs)
            text = " ".join(str(a) for a in args)
            text = (
                text.replace("→", "->")
                .replace("←", "<-")
                .replace("↔", "<->")
                .replace("•", "*")
                .replace("✓", "[OK]")
                .replace("✗", "[X]")
            )
            try:
                _builtins.print(_colorize_line(text), **kwargs)
            except UnicodeEncodeError:
                safe_text = text.encode("ascii", errors="replace").decode("ascii")
                _builtins.print(_colorize_line(safe_text), **kwargs)
    except Exception:
        _builtins.print(*args, **kwargs)


# Intercept raw prints in this module to apply colors to legacy lines
print = _print_colorized  # type: ignore


def _ensure_parent_dir(p: Path) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _append_json_line(path: Path, entry: Dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    try:
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def write_bot_log(entry: Dict[str, Any]) -> None:
    _append_json_line(_BOT_LOG_PATH, entry)


def write_trace_log(entry: Dict[str, Any]) -> None:
    """Write a per-message decision trace (JSONL) for debugging routing/classification."""
    e = dict(entry or {})
    e.setdefault("event", "decision_trace")
    _append_json_line(_TRACE_LOG_PATH, e)


def write_system_log(entry: Dict[str, Any]) -> None:
    _ensure_parent_dir(_SYSTEM_LOG_PATH)
    try:
        entry = dict(entry)
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        logs: List[Dict[str, Any]] = []
        try:
            if _SYSTEM_LOG_PATH.exists():
                with open(_SYSTEM_LOG_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        logs = loaded
        except Exception:
            logs = []
        logs.append(entry)
        logs = logs[-500:]
        tmp = Path(str(_SYSTEM_LOG_PATH) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
        try:
            os.replace(str(tmp), str(_SYSTEM_LOG_PATH))
        except Exception:
            with open(_SYSTEM_LOG_PATH, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def setup_console_logging(*, verbose: bool) -> None:
    global _VERBOSE_CONSOLE
    _VERBOSE_CONSOLE = bool(verbose)
    # Reset console so ANSI segments color only tagged parts (DiscumBot style)
    if platform.system().lower().startswith("win"):
        try:
            os.system("color 07")
        except Exception:
            pass
        try:
            if sys.stdout.encoding != "utf-8":
                import codecs

                sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
                sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
        except Exception:
            pass

    # Keep discord.py internal logs quiet so our console output stays consistent
    try:
        logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler(sys.stdout)], force=True)
    except Exception:
        try:
            logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler(sys.stdout)])
        except Exception:
            pass
    # Prevent discord.py from attaching its own additional handlers (avoids duplicate log lines).
    # Also suppress noisy rate-limit warnings; we handle send throttling + file logs ourselves.
    for logger_name in ("discord", "discord.client", "discord.gateway", "discord.http"):
        try:
            lg = logging.getLogger(logger_name)
            lg.handlers.clear()
            lg.setLevel(logging.ERROR)
            lg.propagate = False
        except Exception:
            continue


def startup_banner(lines: List[str], *, bot_name: str = "MWDataManagerBot") -> None:
    bar = "=" * 55
    with _CONSOLE_LOCK:
        _builtins.print(_F.WHITE + bar + _S.RESET_ALL)
        _builtins.print(f"{_F.GREEN}[START]{_S.RESET_ALL} {_F.WHITE}{bot_name}{_S.RESET_ALL}")
        for line in lines:
            _builtins.print(f"{_F.WHITE}{line}{_S.RESET_ALL}")
        _builtins.print(_F.WHITE + bar + _S.RESET_ALL + "\n")


def log_debug(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    # Discum-style: only show debug when verbose-ish (caller can still write file logs)
    if _VERBOSE_CONSOLE:
        print(f"{_F.WHITE}[DEBUG]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    entry = {"level": "DEBUG", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_info(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    print(f"{_F.GREEN}[INFO]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    entry = {"level": "INFO", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_warn(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    print(f"{_F.YELLOW}[WARN]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    entry = {"level": "WARN", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_error(msg: str, *, error: Optional[BaseException] = None, event: Optional[str] = None, **fields: Any) -> None:
    if error is not None:
        msg = f"{msg} ({type(error).__name__}: {error})"
    print(f"{_F.RED}[ERROR]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    entry: Dict[str, Any] = {"level": "ERROR", "message": msg}
    if event:
        entry["event"] = event
    if error is not None:
        entry["error_type"] = type(error).__name__
        entry["error_message"] = str(error)
    if fields:
        entry.update(fields)
    write_bot_log(entry)
    if error is not None:
        write_system_log(entry)


def _tag_print(tag: str, color: str, msg: str) -> None:
    try:
        with _CONSOLE_LOCK:
            _builtins.print(f"{color}[{tag}]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    except Exception:
        try:
            print(f"[{tag}] {msg}", flush=True)
        except Exception:
            pass


def log_forward(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    _tag_print("FORWARD", _F.MAGENTA, msg)
    entry = {"level": "INFO", "tag": "FORWARD", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_fetchall(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    _tag_print("FETCHALL", _F.CYAN, msg)
    entry = {"level": "INFO", "tag": "FETCHALL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_global(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    _tag_print("GLOBAL", _F.BLUE, msg)
    entry = {"level": "INFO", "tag": "GLOBAL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_filter(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    _tag_print("FILTER", _F.YELLOW, msg)
    entry = {"level": "INFO", "tag": "FILTER", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    write_bot_log(entry)


def log_smartfilter(tag: str, decision: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Structured terminal + file logging for smartfilter decisions.

    Example console line:
      [SMARTFILTER:PROFITABLE_FLIP:TRIGGER] {"reason":"roi_pass", ...}
    """
    details = dict(details or {})
    try:
        payload = json.dumps(details, ensure_ascii=False, default=str)
    except Exception:
        payload = "{}"

    _tag_print(f"SMARTFILTER:{tag}:{decision}", _F.YELLOW, payload)

    entry: Dict[str, Any] = {
        "level": "INFO",
        "tag": "SMARTFILTER",
        "smartfilter_tag": str(tag or ""),
        "decision": str(decision or ""),
        "details": details,
    }
    write_bot_log(entry)
    try:
        write_trace_log({**entry, "event": "smartfilter"})
    except Exception:
        pass

