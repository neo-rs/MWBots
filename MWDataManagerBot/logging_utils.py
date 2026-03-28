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

# Rotation guards for JSONL logs (bots must not append unbounded files).
try:
    _JSONL_MAX_LINES = max(1, int(float(os.getenv("JSONL_LOG_MAX_LINES", "1000") or "1000")))
except Exception:
    _JSONL_MAX_LINES = 1000

try:
    _JSONL_TRIM_CHECK_EVERY_WRITES = max(1, int(float(os.getenv("JSONL_LOG_TRIM_CHECK_EVERY_WRITES", "50") or "50")))
except Exception:
    _JSONL_TRIM_CHECK_EVERY_WRITES = 50

_bot_log_write_count = 0
_trace_log_write_count = 0

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

def _tail_jsonl_lines_bytes(path: Path, max_lines: int) -> tuple[List[str], bool]:
    """Efficiently grab the last `max_lines` JSONL lines by reading from EOF in chunks.

    Returns (lines, truncated) where `truncated` means the file had more than `max_lines` lines.
    """
    max_lines = max(1, int(max_lines or 1))
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size <= 0:
                return ([], False)

            chunk_size = 64 * 1024
            pos = file_size
            data = b""
            nl_count = 0

            while pos > 0 and nl_count <= max_lines:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size)
                data = chunk + data
                nl_count = data.count(b"\n")
                if pos <= 0:
                    break

            lines = data.splitlines()
            truncated = nl_count > max_lines
            if len(lines) > max_lines:
                lines = lines[-max_lines:]
            return ([ln.decode("utf-8", errors="ignore") for ln in lines], truncated)
    except Exception:
        return ([], False)

def _trim_jsonl_file_to_last_lines(path: Path, max_lines: int) -> None:
    """Hard cap a JSONL file to its last `max_lines` entries."""
    try:
        lines, truncated = _tail_jsonl_lines_bytes(path, max_lines)
        if not truncated or not lines:
            return
        tmp = Path(str(path) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for ln in lines:
                f.write(ln)
                f.write("\n")
        os.replace(str(tmp), str(path))
    except Exception:
        pass


def write_bot_log(entry: Dict[str, Any]) -> None:
    global _bot_log_write_count
    _append_json_line(_BOT_LOG_PATH, entry)
    _bot_log_write_count += 1
    if _bot_log_write_count % _JSONL_TRIM_CHECK_EVERY_WRITES != 0:
        return
    try:
        if _BOT_LOG_PATH.exists():
            _trim_jsonl_file_to_last_lines(_BOT_LOG_PATH, _JSONL_MAX_LINES)
    except Exception:
        pass


def write_trace_log(entry: Dict[str, Any]) -> None:
    """Write a per-message decision trace (JSONL) for debugging routing/classification."""
    e = dict(entry or {})
    e.setdefault("event", "decision_trace")
    global _trace_log_write_count
    _append_json_line(_TRACE_LOG_PATH, e)
    _trace_log_write_count += 1
    if _trace_log_write_count % _JSONL_TRIM_CHECK_EVERY_WRITES != 0:
        return
    try:
        if _TRACE_LOG_PATH.exists():
            _trim_jsonl_file_to_last_lines(_TRACE_LOG_PATH, _JSONL_MAX_LINES)
    except Exception:
        pass


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


EXPLAIN_BAR = "=" * 78


def _cfg_verbose() -> bool:
    try:
        import settings_store as _cfg

        return bool(getattr(_cfg, "VERBOSE", False))
    except Exception:
        return bool(_VERBOSE_CONSOLE)


def _print_explainable_lines(lines: List[str], *, accent: str = "") -> None:
    color = accent or _F.CYAN
    try:
        with _CONSOLE_LOCK:
            for line in lines:
                if line == EXPLAIN_BAR:
                    _builtins.print(f"{color}{line}{_S.RESET_ALL}", flush=True)
                else:
                    _builtins.print(f"{_F.WHITE}{line}{_S.RESET_ALL}", flush=True)
    except Exception:
        for line in lines:
            try:
                _builtins.print(line, flush=True)
            except Exception:
                pass


def _truncate_val(v: Any, max_len: int = 140) -> str:
    try:
        s = str(v).replace("\n", " ").strip()
    except Exception:
        s = ""
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _human_bullets_from_details(details: Dict[str, Any], *, max_items: int = 14) -> List[str]:
    """Flat, operator-readable lines from smartfilter/global context dicts."""
    skip_keys = {
        "matches",
        "upcoming_explain",
        "classifier",
        "monitored_keywords",
        "marketplace_matches",
        "resell_indicator_matches",
        "amazon_match",
    }
    bullets: List[str] = []
    mm = details.get("marketplace_matches")
    if isinstance(mm, list) and mm:
        bullets.append(f"- marketplace_matches: {', '.join(str(x) for x in mm[:6])}")
    am = details.get("amazon_match")
    if am:
        bullets.append(f"- amazon_match: {_truncate_val(am, 130)}")
    for k, v in sorted(details.items(), key=lambda kv: str(kv[0])):
        if k in skip_keys or v is None or v is False:
            continue
        if isinstance(v, (dict, list)) and k not in ("reason", "stage"):
            continue
        bullets.append(f"- {k}: {_truncate_val(v, 160)}")
        if len(bullets) >= max_items:
            break
    if not bullets:
        bullets.append("- (no extra context fields)")
    return bullets


def _eli5_smartfilter(tag: str, decision: str, details: Dict[str, Any]) -> str:
    reason = str(details.get("reason") or "")
    tag_u = str(tag or "").upper()
    dec_u = str(decision or "").upper()
    if tag_u == "AMAZON_PROFITABLE_LEAD" and dec_u == "TRIGGER":
        return (
            "Amazon-related content with a product link and a resale marketplace link; "
            "global rules steer this toward the Amazon profitable-leads bucket instead of generic flip channels."
        )
    if tag_u == "AMAZON_PROFITABLE_LEAD" and dec_u == "SKIP":
        if "amz_price_errors_monitor_template" in reason:
            return (
                "This post matches the high-volume AMZ Price Errors / Divine monitor template; "
                "the global profitable-leads shortcut is skipped so routing follows the main classifier (typically regular Amazon)."
            )
        return f"Global profitable-leads trigger did not apply ({reason or 'gating'})."
    if tag_u == "FLIP_CHANNELS" and dec_u == "SKIP":
        if "amazon_content_excluded" in reason:
            return (
                "Amazon-only message: profitable-flip and lunchmoney-flip channels are not evaluated here "
                "(Amazon paths are handled separately)."
            )
        return f"Flip-channel evaluation skipped ({reason or 'see context'})."
    if tag_u == "UPCOMING" and dec_u == "TRIGGER":
        ue = details.get("upcoming_explain")
        if isinstance(ue, dict):
            ind = ue.get("matched_future_indicators") or []
            if isinstance(ind, list) and ind:
                return f"Upcoming / future drop language matched ({', '.join(str(x) for x in ind[:4])})."
        return "Timestamp or future-release wording matched; message is treated as UPCOMING."
    if tag_u == "UPCOMING" and dec_u == "SKIP":
        ue = details.get("upcoming_explain")
        if isinstance(ue, dict) and ue.get("reason"):
            return f"Upcoming classifier skipped: {ue.get('reason')}."
        return "Upcoming classifier skipped (see verbose trace for detail)."
    if tag_u in ("PROFITABLE_FLIP", "LUNCHMONEY_FLIP") and dec_u == "TRIGGER":
        return f"Flip-shaped message met thresholds for {tag_u} routing."
    if tag_u in ("PROFITABLE_FLIP", "LUNCHMONEY_FLIP") and dec_u in ("BLOCK", "SKIP", "FALLBACK"):
        return f"{tag_u}: {dec_u.lower()} ({reason or 'rule gate'})."
    if tag_u == "PRICE_ERROR" and dec_u == "TRIGGER":
        return "Price-error / glitch wording matched; routed to the price-error destination."
    return f"{tag_u} — {dec_u}: {reason or 'see human summary and optional verbose trace'}."


def log_smartfilter(tag: str, decision: str, details: Optional[Dict[str, Any]] = None) -> None:
    """
    Explainable smartfilter logging (Canonical_SOP_with_Explainable_Logging.md):
    console shows ELI5 + human bullets; raw JSON only when verbose.
    File / JSONL traces keep full structured payloads.
    """
    details = dict(details or {})
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

    lines: List[str] = [
        EXPLAIN_BAR,
        f"MWDataManagerBot / SMARTFILTER  |  {tag}  |  {decision}",
        EXPLAIN_BAR,
        "1) ELI5 SUMMARY",
        _eli5_smartfilter(tag, decision, details),
        "",
        "2) HUMAN DECISION SUMMARY",
    ]
    lines.extend(_human_bullets_from_details(details))
    lines.append("")
    lines.append("3) RULES THAT FIRED")
    lines.append(f"- Layer: global_triggers / classifier smartfilter")
    lines.append(f"- Tag: {tag}  |  Outcome: {decision}")
    rk = details.get("reason") or details.get("stage")
    if rk:
        lines.append(f"- Reason key: {rk}")
    if _cfg_verbose():
        lines.append("")
        lines.append("4) RAW FLAGS / TRACE (verbose)")
        try:
            payload = json.dumps(details, ensure_ascii=False, default=str)
            if len(payload) > 3500:
                payload = payload[:3497] + "..."
            lines.append(payload)
        except Exception:
            lines.append("(trace serialization failed)")
    lines.append(EXPLAIN_BAR)
    _print_explainable_lines(lines)


def _classifier_why_for_tag(tag: str, trace: Optional[Dict[str, Any]]) -> str:
    if not trace:
        return ""
    try:
        matches = (trace.get("classifier") or {}).get("matches") or {}
        tag_s = str(tag or "")
        if tag_s == "AMAZON":
            amazon = str(matches.get("amazon") or "").strip()
            if amazon:
                return f"amazon_match={_truncate_val(amazon, 100)}"
        if tag_s == "MONITORED_KEYWORD":
            kws = matches.get("monitored_keywords") or []
            if isinstance(kws, list) and kws:
                return "kw=" + ",".join(str(k) for k in kws[:4])
        if tag_s == "AFFILIATED_LINKS":
            dom = str(matches.get("affiliate_domain") or "").strip()
            reason = str(matches.get("affiliate_reason") or "").strip()
            if dom:
                return f"domain={dom}"
            if reason:
                return reason
    except Exception:
        pass
    return ""


def log_explainable_forward_summary(
    *,
    message_id: int,
    source_channel_id: int,
    source_group: str,
    dest_traces: List[Dict[str, Any]],
    stop_after_first: bool,
    content_preview: str = "",
    forwarded_count: int = 0,
    trace: Optional[Dict[str, Any]] = None,
    flow_label: str = "LIVE FORWARD",
    simulation: bool = False,
) -> None:
    """
    One consolidated routing block per handled message (actual Discord send, not dry-run unless flagged).
    """
    lines: List[str] = [
        EXPLAIN_BAR,
        f"MWDataManagerBot / {flow_label}",
        EXPLAIN_BAR,
        "1) MESSAGE INFO",
        f"- message_id: {message_id}",
        f"- source_channel_id: {source_channel_id}",
        f"- source_group: {source_group}",
    ]
    if content_preview:
        lines.append(f"- content_preview: {_truncate_val(content_preview, 200)}")
    lines.append("")

    sent = sum(1 for t in dest_traces if (t.get("decision") or {}).get("action") == "sent")
    skipped = sum(1 for t in dest_traces if (t.get("decision") or {}).get("action") == "skip")
    errors = sum(1 for t in dest_traces if (t.get("decision") or {}).get("action") == "error")

    if simulation:
        mode = "DRY SEND PREVIEW (simulation — not posted)"
    elif forwarded_count <= 0 and not dest_traces:
        mode = "BLOCKED / NO ROUTE"
    elif forwarded_count <= 0:
        mode = "BLOCKED / NO ROUTE (all legs skipped or failed)"
    elif sent == 1:
        mode = "SINGLE ROUTE"
    else:
        mode = "MULTI ROUTE"

    lines.append("2) ELI5 SUMMARY")
    if forwarded_count > 0:
        lines.append(f"- Bottom line: {mode}; posted to {sent} destination(s).")
    else:
        lines.append(f"- Bottom line: {mode}.")
    if stop_after_first and sent > 0:
        lines.append("- Stop-after-first: remaining planned routes were not attempted after the first send.")
    lines.append("")

    lines.append("3) HUMAN DECISION SUMMARY")
    lines.append("- Winning: destinations below reflect classifier + global triggers + route map.")
    if skipped:
        lines.append(f"- Skipped legs: {skipped} (dedupe, throttle, or invalid destination).")
    if errors:
        lines.append(f"- Errors: {errors} send failure(s); see trace log.")
    lines.append("")

    lines.append("4) DESTINATION DECISION (ordered)")
    if not dest_traces:
        lines.append("- (no routing legs recorded)")
    for i, leg in enumerate(dest_traces, start=1):
        tag = str(leg.get("tag") or "")
        db = int(leg.get("dest_before") or 0)
        da = int(leg.get("dest_after") or db)
        dec = leg.get("decision") or {}
        action = str(dec.get("action") or "")
        mapped = db > 0 and da > 0 and db != da
        why = _classifier_why_for_tag(tag, trace)
        extra = f" | {why}" if why else ""
        if action == "sent":
            sim_note = " (simulated)" if simulation else ""
            lines.append(
                f"- Leg {i}: SENT  tag={tag}  dest_id={da}  route_map_applied={'yes' if mapped else 'no'}{extra}{sim_note}"
            )
        elif action == "skip":
            age = dec.get("age_seconds")
            age_s = f"  age_s={age}" if age is not None else ""
            lines.append(
                f"- Leg {i}: SKIPPED  tag={tag}  dest_id={da}  reason={dec.get('reason', 'unknown')}{age_s}"
            )
        elif action == "error":
            lines.append(f"- Leg {i}: ERROR  tag={tag}  dest_id={da}  error={_truncate_val(dec.get('error'), 120)}")
        else:
            lines.append(f"- Leg {i}: {action or 'unknown'}  tag={tag}  dest_id={da}{extra}")

    lines.append("")
    lines.append("5) FAILURE HINTS")
    if forwarded_count > 0:
        lines.append("- None (at least one send succeeded).")
    else:
        hints: List[str] = []
        for leg in dest_traces:
            dec = leg.get("decision") or {}
            if dec.get("action") == "skip":
                hints.append(f"skip:{dec.get('reason')}")
            if dec.get("action") == "error":
                hints.append(f"error:{dec.get('error_type') or 'send'}")
        lines.append("- " + ("; ".join(hints) if hints else "No matching destination or all legs skipped — check classifier trace / UNCLASSIFIED flow."))

    if _cfg_verbose() and trace:
        lines.append("")
        lines.append("6) RAW FLAGS / TRACE (verbose)")
        try:
            slim = {
                "classifier": trace.get("classifier"),
                "dispatch_link_types": trace.get("dispatch_link_types"),
                "stop_after_first": trace.get("stop_after_first"),
            }
            payload = json.dumps(slim, ensure_ascii=False, default=str)
            if len(payload) > 3000:
                payload = payload[:2997] + "..."
            lines.append(payload)
        except Exception:
            lines.append("(trace slice failed)")

    lines.append(EXPLAIN_BAR)
    _print_explainable_lines(lines)

    # Structured JSON line for journal parsers / Botlogs
    try:
        write_bot_log(
            {
                "level": "INFO",
                "tag": "ROUTING_SUMMARY",
                "message": f"msg={message_id} ch={source_channel_id} forwarded={forwarded_count} mode={mode}",
                "message_id": int(message_id),
                "source_channel_id": int(source_channel_id),
                "source_group": str(source_group),
                "forwarded_count": int(forwarded_count),
                "route_mode": mode,
                "stop_after_first": bool(stop_after_first),
                "dest_traces": dest_traces,
                "simulation": bool(simulation),
            }
        )
    except Exception:
        pass


def log_explainable_major_clearance_send(
    *,
    variant: str,
    message_id: int,
    source_channel_id: int,
    dest_channel_id: int,
    route_map_applied: bool,
) -> None:
    lines = [
        EXPLAIN_BAR,
        "MWDataManagerBot / MAJOR_CLEARANCE",
        EXPLAIN_BAR,
        "1) MESSAGE INFO",
        f"- message_id: {message_id}",
        f"- source_channel_id: {source_channel_id}",
        "",
        "2) ELI5 SUMMARY",
        f"- Home Depot–style clearance embed ({variant}); forwarded to major-clearance destination.",
        "",
        "3) DESTINATION DECISION",
        f"- SENT to dest_id={dest_channel_id}  route_map_applied={'yes' if route_map_applied else 'no'}",
        "",
        EXPLAIN_BAR,
    ]
    _print_explainable_lines(lines)
    try:
        write_bot_log(
            {
                "level": "INFO",
                "tag": "FORWARD",
                "message": f"major-clearance {variant} msg={message_id} -> {dest_channel_id}",
                "variant": variant,
                "message_id": int(message_id),
                "source_channel_id": int(source_channel_id),
                "dest_channel_id": int(dest_channel_id),
            }
        )
    except Exception:
        pass

