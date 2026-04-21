"""Minimal logging for fetchall (MWDiscumBot). Writes to console and fetchall log file.

Every **printed** line begins with `[FETCHALL]` so RSAdminBot journal splitting can route MWDiscumBot
stdout to `discumbot_fetch` (`_journal_line_is_mwdiscumbot_fetchall` checks `"[fetchall]" in line.lower()`).
Multi-line messages repeat the prefix on each line so continuation lines are not mis-classified as D2D.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence, Union

_ROOT = Path(__file__).resolve().parent
_LOGS_DIR = _ROOT / "logs"
_FETCHALL_LOG_PATH = _LOGS_DIR / "Botlogs" / "fetchalllogs.json"


def _ensure_dir(p: Path) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _strip_redundant_fetchall_prefix(msg: str) -> str:
    """Call sites sometimes prefix with '[FETCHALL] '; logger prints [FETCHALL] once per line."""
    m = str(msg or "")
    if m.startswith("[FETCHALL] "):
        return m[len("[FETCHALL] ") :].lstrip()
    return m


def _print_fetchall_journal_lines(body: str, *, level_mid: str = "") -> None:
    """
    Print one or more lines to stdout; each line starts with `[FETCHALL]` for journal routing.
    level_mid: ' [WARN] ', ' [INFO] ', or '' (default info-style body after tag).
    """
    body = _strip_redundant_fetchall_prefix(body)
    lines = str(body or "").splitlines()
    if not lines:
        lines = [""]
    mid = level_mid if level_mid else " "
    for raw in lines:
        ln = raw.rstrip("\r")
        print(f"[FETCHALL]{mid}{ln}", flush=True)


def fmt_discord_channel(cid: Any) -> str:
    """Discord-journal-friendly channel/category snowflake (clickable as <#id> in Discord)."""
    try:
        x = int(cid)
    except Exception:
        return str(cid)
    return f"<#{x}>" if x > 0 else str(cid)


def fmt_discord_channel_list(ids: Union[None, Iterable[Any]], *, limit: int = 60) -> str:
    """Comma-separated <#id> mentions for Discord logs."""
    if ids is None:
        return ""
    if isinstance(ids, (str, bytes)):
        return fmt_discord_channel(ids)
    seq: Sequence[Any]
    if isinstance(ids, dict):
        seq = list(ids.keys())
    elif isinstance(ids, (set, frozenset)):
        seq = sorted(ids)
    else:
        seq = list(ids)
    out: list[str] = []
    for x in seq[: max(0, int(limit))]:
        out.append(fmt_discord_channel(x))
    if len(seq) > int(limit):
        out.append(f"...(+{len(seq) - int(limit)} more)")
    return ", ".join(out)


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
    msg = _strip_redundant_fetchall_prefix(msg)
    _print_fetchall_journal_lines(msg, level_mid=" ")
    entry = {"level": "INFO", "tag": "FETCHALL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)


def log_warn(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    msg = _strip_redundant_fetchall_prefix(msg)
    _print_fetchall_journal_lines(msg, level_mid=" [WARN] ")
    entry = {"level": "WARN", "tag": "FETCHALL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)


def log_info(msg: str, *, event: Optional[str] = None, **fields: Any) -> None:
    msg = _strip_redundant_fetchall_prefix(msg)
    _print_fetchall_journal_lines(msg, level_mid=" [INFO] ")
    entry = {"level": "INFO", "tag": "FETCHALL", "message": msg}
    if event:
        entry["event"] = event
    if fields:
        entry.update(fields)
    _append_json_line(_FETCHALL_LOG_PATH, entry)


def log_fetchall_settings_snapshot() -> None:
    """
    One structured block after fetchall_config.init(): effective fetchall-related settings
    (compare local vs Oracle journal to spot drift).
    """
    try:
        import fetchall_config as fc
    except Exception as e:
        log_warn(f"settings snapshot skipped (fetchall_config import failed: {e})")
        return
    try:
        cfg_path = Path(fc.__file__).resolve().parent / "config" / "settings.json"
        path_note = str(cfg_path)
    except Exception:
        path_note = "?"

    def _yn(b: bool) -> str:
        return "yes" if b else "no"

    try:
        dg = sorted(int(x) for x in (getattr(fc, "DESTINATION_GUILD_IDS", set()) or set()) if int(x) > 0)
    except Exception:
        dg = []
    try:
        sc = sorted(int(x) for x in (getattr(fc, "FETCHALL_STARTUP_CLEAR_CATEGORY_IDS", set()) or set()) if int(x) > 0)
    except Exception:
        sc = []
    try:
        dcat = int(getattr(fc, "FETCHALL_DEFAULT_DEST_CATEGORY_ID", 0) or 0)
    except Exception:
        dcat = 0

    lines = [
        f"effective fetchall settings (from fetchall_config.init → {path_note})",
        f"  destination_guild_ids (numeric guild snowflakes): {dg if dg else '(none)'}",
        f"  fetchall_default_destination_category_id: {fmt_discord_channel(dcat) if dcat else 0}",
        f"  fetchall_max_messages_per_channel: {int(getattr(fc, 'FETCHALL_MAX_MESSAGES_PER_CHANNEL', 0) or 0)}",
        f"  fetchsync_initial_backfill_limit: {int(getattr(fc, 'FETCHSYNC_INITIAL_BACKFILL_LIMIT', 0) or 0)}",
        f"  fetchsync_min_content_chars: {int(getattr(fc, 'FETCHSYNC_MIN_CONTENT_CHARS', 0) or 0)}",
        f"  fetchsync_auto_poll_seconds: {int(getattr(fc, 'FETCHSYNC_AUTO_POLL_SECONDS', 0) or 0)}",
        f"  send_min_interval_seconds: {float(getattr(fc, 'SEND_MIN_INTERVAL_SECONDS', 0.0) or 0.0)}",
        f"  use_webhooks_for_forwarding: {_yn(bool(getattr(fc, 'USE_WEBHOOKS_FOR_FORWARDING', False)))}",
        f"  forward_attachments_as_files: {_yn(bool(getattr(fc, 'FORWARD_ATTACHMENTS_AS_FILES', True)))}",
        f"  forward_attachments_max_files: {int(getattr(fc, 'FORWARD_ATTACHMENTS_MAX_FILES', 0) or 0)}",
        f"  forward_attachments_max_bytes: {int(getattr(fc, 'FORWARD_ATTACHMENTS_MAX_BYTES', 0) or 0)}",
        f"  fetchall_runtime_mappings_reset_on_startup: {_yn(bool(getattr(fc, 'FETCHALL_RUNTIME_MAPPINGS_RESET_ON_STARTUP', False)))}",
        f"  fetchall_startup_clear_enabled: {_yn(bool(getattr(fc, 'FETCHALL_STARTUP_CLEAR_ENABLED', False)))}",
        f"  fetchall_startup_clear_category_ids: {fmt_discord_channel_list(sc) if sc else '(none)'}",
        f"  fetchall_startup_clear_only_mirror_channels: {_yn(bool(getattr(fc, 'FETCHALL_STARTUP_CLEAR_ONLY_MIRROR_CHANNELS', True)))}",
        f"  fetchall_startup_clear_all_channels: {_yn(bool(getattr(fc, 'FETCHALL_STARTUP_CLEAR_ALL_CHANNELS', False)))}",
        f"  fetchall_startup_clear_delay_seconds: {int(getattr(fc, 'FETCHALL_STARTUP_CLEAR_DELAY_SECONDS', 0) or 0)}",
        f"  fetchsync_only_recent_message_days: {int(getattr(fc, 'FETCHSYNC_ONLY_RECENT_MESSAGE_DAYS', 0) or 0)}",
        f"  fetchall_only_channels_with_recent_activity_days: {int(getattr(fc, 'FETCHALL_ONLY_CHANNELS_WITH_RECENT_ACTIVITY_DAYS', 0) or 0)}",
        f"  fetchmirror_require_status_emoji_prefix: {_yn(bool(getattr(fc, 'FETCHMIRROR_REQUIRE_STATUS_EMOJI_PREFIX', False)))}",
        f"  fetch_auto_sequence_sleep_seconds: {float(getattr(fc, 'FETCH_AUTO_SEQUENCE_SLEEP_SECONDS', 1.0) or 1.0)}",
        f"  fetchall_auto_prune_inactive: {_yn(bool(getattr(fc, 'FETCHALL_AUTO_PRUNE_INACTIVE', True)))}",
        f"  fetchall_inactive_prune_days: {float(getattr(fc, 'FETCHALL_INACTIVE_PRUNE_DAYS', 2.0) or 2.0)}",
    ]
    log_fetchall("\n".join(lines), event="settings_snapshot")
