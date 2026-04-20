"""Config used only by fetchall (MWDiscumBot). Loads from config/settings.json."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Set

_ROOT = Path(__file__).resolve().parent
_CONFIG_DIR = _ROOT / "config"
_SETTINGS_PATH = _CONFIG_DIR / "settings.json"

# Only what fetchall.py uses
DESTINATION_GUILD_IDS: Set[int] = set()
FETCHALL_DEFAULT_DEST_CATEGORY_ID: int = 0
FETCHALL_MAX_MESSAGES_PER_CHANNEL: int = 400
FETCHSYNC_INITIAL_BACKFILL_LIMIT: int = 20
FETCHSYNC_MIN_CONTENT_CHARS: int = 25
FETCHSYNC_AUTO_POLL_SECONDS: int = 0
SEND_MIN_INTERVAL_SECONDS: float = 0.0
USE_WEBHOOKS_FOR_FORWARDING: bool = False
FORWARD_ATTACHMENTS_AS_FILES: bool = True
FORWARD_ATTACHMENTS_MAX_FILES: int = 10
FORWARD_ATTACHMENTS_MAX_BYTES: int = 7_500_000

# When True, on MWDiscumBot ready (before startup clear / fetchsync), replace fetchall_mappings.runtime.json
# with {"guilds": []} so merged mappings are base-only until the bot writes runtime again.
# Clears stale partial overlay rows and fetchsync cursors (last_seen_message_id_by_channel); next fetchsync may
# re-backfill recent pages for non-empty mirrors — use with fetchall_startup_clear or accept possible overlap.
FETCHALL_RUNTIME_MAPPINGS_RESET_ON_STARTUP: bool = False

# Startup clear (optional): remove channels in fetchall categories at bot ready, before fetchsync runs
FETCHALL_STARTUP_CLEAR_ENABLED: bool = False
FETCHALL_STARTUP_CLEAR_CATEGORY_IDS: Set[int] = set()
# True = delete only channels with topic MIRROR: or "separator for". False = delete ALL channels in category (full clear, e.g. Daily Upcoming Drops).
FETCHALL_STARTUP_CLEAR_ONLY_MIRROR_CHANNELS: bool = True
# True = clear ALL channels in target categories (full clear first, then fetchall repopulates). Use for "Daily Upcoming Drops" style.
FETCHALL_STARTUP_CLEAR_ALL_CHANNELS: bool = False
FETCHALL_STARTUP_CLEAR_DELAY_SECONDS: int = 0

# Optional filters (0 = disabled): only fetch/sync recent content
# fetchsync_only_recent_message_days: skip messages older than N days when mirroring
FETCHSYNC_ONLY_RECENT_MESSAGE_DAYS: int = 0
# fetchall_only_channels_with_recent_activity_days: only create/sync channels that have at least one message in the last N days
FETCHALL_ONLY_CHANNELS_WITH_RECENT_ACTIVITY_DAYS: int = 0

# When True, only mirror channels whose names start with a status/calendar emoji (🟢🟡🔴🟠📅🗓️).
# Per-mapping exception list (fetchall_mappings*.json field status_emoji_prefix_exempt_channel_ids) still mirrors
# those source channel ids even when their names use other leading emoji (e.g. 🏬 lanes).
# Mirrors for excluded sources may be pruned on the next fetchall (prune sees them as "not in selected set").
FETCHMIRROR_REQUIRE_STATUS_EMOJI_PREFIX: bool = False

# Pause between fetchall→fetchsync in auto sequence (matches manual !fetchcycle and auto-poller).
FETCH_AUTO_SEQUENCE_SLEEP_SECONDS: float = 1.0


def _parse_int_set(values: Any) -> Set[int]:
    out: Set[int] = set()
    if values is None:
        return out
    if isinstance(values, (int, float)):
        try:
            v = int(values)
            if v > 0:
                out.add(v)
        except Exception:
            pass
        return out
    if isinstance(values, str):
        for p in values.replace("\n", ",").split(","):
            p = p.strip()
            if not p:
                continue
            try:
                v = int(p)
                if v > 0:
                    out.add(v)
            except Exception:
                continue
        return out
    if isinstance(values, (list, tuple, set)):
        for item in values:
            out |= _parse_int_set(item)
        return out
    return out


def _get_int(d: Dict[str, Any], key: str, default: int = 0) -> int:
    try:
        v = d.get(key, default)
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def load_fetchall_settings() -> Dict[str, Any]:
    """Load settings from MWDiscumBot/config/settings.json."""
    try:
        if not _SETTINGS_PATH.exists():
            return {}
        with open(_SETTINGS_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def init(settings: Dict[str, Any]) -> None:
    """Apply settings dict to module-level config (fetchall only)."""
    global DESTINATION_GUILD_IDS, FETCHALL_DEFAULT_DEST_CATEGORY_ID
    global FETCHALL_MAX_MESSAGES_PER_CHANNEL, FETCHSYNC_INITIAL_BACKFILL_LIMIT
    global FETCHSYNC_MIN_CONTENT_CHARS, FETCHSYNC_AUTO_POLL_SECONDS
    global SEND_MIN_INTERVAL_SECONDS, USE_WEBHOOKS_FOR_FORWARDING
    global FORWARD_ATTACHMENTS_AS_FILES, FORWARD_ATTACHMENTS_MAX_FILES, FORWARD_ATTACHMENTS_MAX_BYTES
    global FETCHALL_RUNTIME_MAPPINGS_RESET_ON_STARTUP
    global FETCHALL_STARTUP_CLEAR_ENABLED, FETCHALL_STARTUP_CLEAR_CATEGORY_IDS
    global FETCHALL_STARTUP_CLEAR_ONLY_MIRROR_CHANNELS, FETCHALL_STARTUP_CLEAR_ALL_CHANNELS, FETCHALL_STARTUP_CLEAR_DELAY_SECONDS
    global FETCHSYNC_ONLY_RECENT_MESSAGE_DAYS, FETCHALL_ONLY_CHANNELS_WITH_RECENT_ACTIVITY_DAYS
    global FETCHMIRROR_REQUIRE_STATUS_EMOJI_PREFIX, FETCH_AUTO_SEQUENCE_SLEEP_SECONDS

    DESTINATION_GUILD_IDS = _parse_int_set(settings.get("destination_guild_ids"))
    FETCHALL_DEFAULT_DEST_CATEGORY_ID = _get_int(settings, "fetchall_default_destination_category_id", 0)
    FETCHALL_MAX_MESSAGES_PER_CHANNEL = _get_int(settings, "fetchall_max_messages_per_channel", 400)
    FETCHSYNC_INITIAL_BACKFILL_LIMIT = _get_int(settings, "fetchsync_initial_backfill_limit", 20)
    FETCHSYNC_MIN_CONTENT_CHARS = _get_int(settings, "fetchsync_min_content_chars", 25)
    if FETCHSYNC_MIN_CONTENT_CHARS < 0:
        FETCHSYNC_MIN_CONTENT_CHARS = 0
    if FETCHSYNC_MIN_CONTENT_CHARS > 500:
        FETCHSYNC_MIN_CONTENT_CHARS = 500
    try:
        SEND_MIN_INTERVAL_SECONDS = float(settings.get("send_min_interval_seconds", 0.0) or 0.0)
        if SEND_MIN_INTERVAL_SECONDS < 0:
            SEND_MIN_INTERVAL_SECONDS = 0.0
    except Exception:
        SEND_MIN_INTERVAL_SECONDS = 0.0
    USE_WEBHOOKS_FOR_FORWARDING = bool(settings.get("use_webhooks_for_forwarding", False))
    FORWARD_ATTACHMENTS_AS_FILES = bool(settings.get("forward_attachments_as_files", True))
    FORWARD_ATTACHMENTS_MAX_FILES = _get_int(settings, "forward_attachments_max_files", 10)
    FORWARD_ATTACHMENTS_MAX_BYTES = _get_int(settings, "forward_attachments_max_bytes", 7_500_000)
    if FORWARD_ATTACHMENTS_MAX_FILES < 0:
        FORWARD_ATTACHMENTS_MAX_FILES = 0
    if FORWARD_ATTACHMENTS_MAX_FILES > 10:
        FORWARD_ATTACHMENTS_MAX_FILES = 10
    if FORWARD_ATTACHMENTS_MAX_BYTES < 0:
        FORWARD_ATTACHMENTS_MAX_BYTES = 0
    FETCHSYNC_AUTO_POLL_SECONDS = _get_int(settings, "fetchsync_auto_poll_seconds", 0)
    FETCHALL_RUNTIME_MAPPINGS_RESET_ON_STARTUP = bool(settings.get("fetchall_runtime_mappings_reset_on_startup", False))
    FETCHALL_STARTUP_CLEAR_ENABLED = bool(settings.get("fetchall_startup_clear_enabled", False))
    FETCHALL_STARTUP_CLEAR_CATEGORY_IDS = _parse_int_set(settings.get("fetchall_startup_clear_category_ids"))
    FETCHALL_STARTUP_CLEAR_ONLY_MIRROR_CHANNELS = bool(settings.get("fetchall_startup_clear_only_mirror_channels", True))
    FETCHALL_STARTUP_CLEAR_ALL_CHANNELS = bool(settings.get("fetchall_startup_clear_all_channels", False))
    FETCHALL_STARTUP_CLEAR_DELAY_SECONDS = _get_int(settings, "fetchall_startup_clear_delay_seconds", 0)
    FETCHSYNC_ONLY_RECENT_MESSAGE_DAYS = _get_int(settings, "fetchsync_only_recent_message_days", 0)
    FETCHALL_ONLY_CHANNELS_WITH_RECENT_ACTIVITY_DAYS = _get_int(settings, "fetchall_only_channels_with_recent_activity_days", 0)
    FETCHMIRROR_REQUIRE_STATUS_EMOJI_PREFIX = bool(settings.get("fetchmirror_require_status_emoji_prefix", False))
    try:
        FETCH_AUTO_SEQUENCE_SLEEP_SECONDS = float(settings.get("fetch_auto_sequence_sleep_seconds", 1.0) or 1.0)
        if FETCH_AUTO_SEQUENCE_SLEEP_SECONDS < 0:
            FETCH_AUTO_SEQUENCE_SLEEP_SECONDS = 0.0
        if FETCH_AUTO_SEQUENCE_SLEEP_SECONDS > 30:
            FETCH_AUTO_SEQUENCE_SLEEP_SECONDS = 30.0
    except Exception:
        FETCH_AUTO_SEQUENCE_SLEEP_SECONDS = 1.0
