from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set

VERBOSE: bool = True

DESTINATION_GUILD_IDS: Set[int] = set()
SOURCE_GUILD_IDS: Set[int] = set()

SMART_SOURCE_CHANNELS_ONLINE: Set[int] = set()
SMART_SOURCE_CHANNELS_INSTORE: Set[int] = set()
SMART_SOURCE_CHANNELS_CLEARANCE: Set[int] = set()
SMART_SOURCE_CHANNELS_MISC: Set[int] = set()
SMART_SOURCE_CHANNELS: Set[int] = set()

# Monitoring options (so you don't need to list every mirror channel id)
MONITOR_ALL_DESTINATION_CHANNELS: bool = False
MONITOR_CATEGORY_IDS: Set[int] = set()
MONITOR_WEBHOOK_MESSAGES_ONLY: bool = True

# Outbound send throttling (prevents Discord 429 spam)
SEND_MIN_INTERVAL_SECONDS: float = 0.0

# Routing maps (mirrorworld channel routing)
MIRRORWORLD_ROUTE_ONLINE: Dict[int, int] = {}
MIRRORWORLD_ROUTE_INSTORE: Dict[int, int] = {}

# DEFAULT fallback toggle (prevents spam into DEFAULT)
ENABLE_DEFAULT_FALLBACK: bool = False

# Raw-link behavior (ported from legacy)
ENABLE_RAW_LINK_UNWRAP: bool = True

# Local destinations
SMARTFILTER_AMAZON_CHANNEL_ID: int = 0
SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID: int = 0
SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID: int = 0
SMARTFILTER_UPCOMING_CHANNEL_ID: int = 0
SMARTFILTER_INSTORE_LEADS_CHANNEL_ID: int = 0
SMARTFILTER_MAJOR_STORES_CHANNEL_ID: int = 0
SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID: int = 0
SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID: int = 0
SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID: int = 0
SMARTFILTER_INSTORE_CARDS_CHANNEL_ID: int = 0
SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID: int = 0
SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID: int = 0
SMARTFILTER_DEFAULT_CHANNEL_ID: int = 0
SMARTFILTER_UNCLASSIFIED_CHANNEL_ID: int = 0

# Global trigger destinations
SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID: int = 0
SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID: int = 0
SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID: int = 0
SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID: int = 0

# Runtime knobs
RECENT_TTL_SECONDS: int = 10
GLOBAL_DUPLICATE_TTL_SECONDS: int = 60 * 5
MONITOR_EMBED_TTL_SECONDS: int = 60 * 5
LINK_TRACKING_TTL_SECONDS: int = 24 * 60 * 60

FALLBACK_CHANNEL_ID: int = 0

# Fetch-all defaults (kept even if fetchall is disabled)
FETCHALL_DEFAULT_DEST_CATEGORY_ID: int = 0
FETCHALL_MAX_MESSAGES_PER_CHANNEL: int = 400
FETCHSYNC_INITIAL_BACKFILL_LIMIT: int = 20
FETCHSYNC_MIN_CONTENT_CHARS: int = 25
# Safety default: OFF. Enable explicitly in config/settings.json.
FETCHSYNC_AUTO_POLL_SECONDS: int = 0

EDIT_COOLDOWN_SECONDS: int = 30


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
        pieces = values.replace("\n", ",").split(",")
        for p in pieces:
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
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def init(settings: Dict[str, Any]) -> None:
    """
    Initialize module-level configuration values from the loaded settings dict.

    This keeps the migrated code close to the original datamanagerbot structure
    (which relied on module-level constants), without importing `neonxt.*`.
    """
    global VERBOSE
    global DESTINATION_GUILD_IDS, SOURCE_GUILD_IDS
    global SMART_SOURCE_CHANNELS_ONLINE, SMART_SOURCE_CHANNELS_INSTORE, SMART_SOURCE_CHANNELS_CLEARANCE, SMART_SOURCE_CHANNELS_MISC, SMART_SOURCE_CHANNELS
    global MONITOR_ALL_DESTINATION_CHANNELS, MONITOR_CATEGORY_IDS, MONITOR_WEBHOOK_MESSAGES_ONLY
    global SEND_MIN_INTERVAL_SECONDS
    global MIRRORWORLD_ROUTE_ONLINE, MIRRORWORLD_ROUTE_INSTORE
    global ENABLE_DEFAULT_FALLBACK
    global ENABLE_RAW_LINK_UNWRAP
    global SMARTFILTER_AMAZON_CHANNEL_ID, SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID
    global SMARTFILTER_UPCOMING_CHANNEL_ID, SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, SMARTFILTER_MAJOR_STORES_CHANNEL_ID
    global SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID
    global SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID
    global SMARTFILTER_DEFAULT_CHANNEL_ID, SMARTFILTER_UNCLASSIFIED_CHANNEL_ID
    global SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID
    global SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID
    global RECENT_TTL_SECONDS, GLOBAL_DUPLICATE_TTL_SECONDS, MONITOR_EMBED_TTL_SECONDS, LINK_TRACKING_TTL_SECONDS
    global FALLBACK_CHANNEL_ID
    global FETCHALL_DEFAULT_DEST_CATEGORY_ID, FETCHALL_MAX_MESSAGES_PER_CHANNEL
    global FETCHSYNC_INITIAL_BACKFILL_LIMIT, FETCHSYNC_MIN_CONTENT_CHARS, FETCHSYNC_AUTO_POLL_SECONDS
    global EDIT_COOLDOWN_SECONDS

    VERBOSE = bool(settings.get("verbose", True))

    DESTINATION_GUILD_IDS = _parse_int_set(settings.get("destination_guild_ids"))
    SOURCE_GUILD_IDS = _parse_int_set(settings.get("source_guild_ids"))

    SMART_SOURCE_CHANNELS_ONLINE = _parse_int_set(settings.get("source_channel_ids_online"))
    SMART_SOURCE_CHANNELS_INSTORE = _parse_int_set(settings.get("source_channel_ids_instore"))
    SMART_SOURCE_CHANNELS_CLEARANCE = _parse_int_set(settings.get("source_channel_ids_clearance"))
    SMART_SOURCE_CHANNELS_MISC = _parse_int_set(settings.get("source_channel_ids_misc"))
    SMART_SOURCE_CHANNELS = set()
    SMART_SOURCE_CHANNELS |= SMART_SOURCE_CHANNELS_ONLINE
    SMART_SOURCE_CHANNELS |= SMART_SOURCE_CHANNELS_INSTORE
    SMART_SOURCE_CHANNELS |= SMART_SOURCE_CHANNELS_CLEARANCE
    SMART_SOURCE_CHANNELS |= SMART_SOURCE_CHANNELS_MISC

    # Optional monitoring shortcuts
    MONITOR_ALL_DESTINATION_CHANNELS = bool(settings.get("monitor_all_destination_channels", False))
    MONITOR_CATEGORY_IDS = _parse_int_set(settings.get("monitor_category_ids"))
    MONITOR_WEBHOOK_MESSAGES_ONLY = bool(settings.get("monitor_webhook_messages_only", True))

    try:
        SEND_MIN_INTERVAL_SECONDS = float(settings.get("send_min_interval_seconds", 0.0) or 0.0)
        if SEND_MIN_INTERVAL_SECONDS < 0:
            SEND_MIN_INTERVAL_SECONDS = 0.0
    except Exception:
        SEND_MIN_INTERVAL_SECONDS = 0.0

    ENABLE_DEFAULT_FALLBACK = bool(settings.get("enable_default_fallback", False))

    def _parse_route_map(val: Any) -> Dict[int, int]:
        out: Dict[int, int] = {}
        if isinstance(val, dict):
            for k, v in val.items():
                try:
                    sk = int(str(k).strip())
                    sv = int(str(v).strip())
                    if sk > 0 and sv > 0:
                        out[sk] = sv
                except Exception:
                    continue
        elif isinstance(val, str) and val.strip():
            # Accept "a:b,c:d" form
            for pair in val.split(","):
                if ":" not in pair:
                    continue
                a, b = pair.split(":", 1)
                try:
                    sk = int(a.strip())
                    sv = int(b.strip())
                    if sk > 0 and sv > 0:
                        out[sk] = sv
                except Exception:
                    continue
        return out

    MIRRORWORLD_ROUTE_ONLINE = _parse_route_map(settings.get("mirrorworld_route_online"))
    MIRRORWORLD_ROUTE_INSTORE = _parse_route_map(settings.get("mirrorworld_route_instore"))

    ENABLE_RAW_LINK_UNWRAP = bool(settings.get("enable_raw_link_unwrap", True))

    dests = settings.get("smartfilter_destinations") if isinstance(settings.get("smartfilter_destinations"), dict) else {}
    SMARTFILTER_AMAZON_CHANNEL_ID = _get_int(dests, "AMAZON", 0)
    SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID = _get_int(dests, "AMAZON_FALLBACK", 0)
    SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID = _get_int(dests, "AFFILIATED_LINKS", 0)
    SMARTFILTER_UPCOMING_CHANNEL_ID = _get_int(dests, "UPCOMING", 0)
    SMARTFILTER_INSTORE_LEADS_CHANNEL_ID = _get_int(dests, "INSTORE_LEADS", 0)
    SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID = _get_int(dests, "INSTORE_SEASONAL", 0)
    SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID = _get_int(dests, "INSTORE_SNEAKERS", 0)
    SMARTFILTER_INSTORE_CARDS_CHANNEL_ID = _get_int(dests, "INSTORE_CARDS", 0)
    SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID = _get_int(dests, "INSTORE_THEATRE", 0)
    SMARTFILTER_MAJOR_STORES_CHANNEL_ID = _get_int(dests, "MAJOR_STORES", 0)
    SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID = _get_int(dests, "DISCOUNTED_STORES", 0)
    SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID = _get_int(dests, "MONITORED_KEYWORD", 0)
    SMARTFILTER_DEFAULT_CHANNEL_ID = _get_int(dests, "DEFAULT", 0)
    SMARTFILTER_UNCLASSIFIED_CHANNEL_ID = _get_int(dests, "UNCLASSIFIED", 0)

    gd = settings.get("global_trigger_destinations") if isinstance(settings.get("global_trigger_destinations"), dict) else {}
    SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID = _get_int(gd, "PRICE_ERROR", 0)
    SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID = _get_int(gd, "PROFITABLE_FLIP", 0)
    SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID = _get_int(gd, "LUNCHMONEY_FLIP", 0)
    SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID = _get_int(gd, "AMAZON_PROFITABLE_LEADS", 0)

    RECENT_TTL_SECONDS = _get_int(settings, "recent_ttl_seconds", 10)
    GLOBAL_DUPLICATE_TTL_SECONDS = _get_int(settings, "global_duplicate_ttl_seconds", 60 * 5)
    MONITOR_EMBED_TTL_SECONDS = _get_int(settings, "monitor_embed_ttl_seconds", 60 * 5)
    LINK_TRACKING_TTL_SECONDS = _get_int(settings, "link_tracking_ttl_seconds", 24 * 60 * 60)

    try:
        FALLBACK_CHANNEL_ID = int(settings.get("fallback_channel_id") or 0)
    except Exception:
        FALLBACK_CHANNEL_ID = 0

    FETCHALL_DEFAULT_DEST_CATEGORY_ID = _get_int(settings, "fetchall_default_destination_category_id", 0)
    FETCHALL_MAX_MESSAGES_PER_CHANNEL = _get_int(settings, "fetchall_max_messages_per_channel", 400)
    FETCHSYNC_INITIAL_BACKFILL_LIMIT = _get_int(settings, "fetchsync_initial_backfill_limit", 20)
    FETCHSYNC_MIN_CONTENT_CHARS = _get_int(settings, "fetchsync_min_content_chars", 25)
    # Default OFF unless explicitly enabled (prevents surprise background mirroring + channel creation).
    FETCHSYNC_AUTO_POLL_SECONDS = _get_int(settings, "fetchsync_auto_poll_seconds", 0)
    EDIT_COOLDOWN_SECONDS = _get_int(settings, "edit_cooldown_seconds", 30)


def is_destination_guild(guild_id: Optional[int]) -> bool:
    if not guild_id or not DESTINATION_GUILD_IDS:
        return False
    try:
        return int(guild_id) in DESTINATION_GUILD_IDS
    except Exception:
        return False


def is_monitored_source_channel(channel_id: Optional[int], *, category_id: Optional[int] = None) -> bool:
    """
    Monitor policy (destination guild only; guild check is done elsewhere):
    - If explicit channel ids are configured, use them (strict allowlist).
    - Otherwise, allow either:
      - `monitor_category_ids`: monitor any channel within those categories
      - `monitor_all_destination_channels`: monitor every channel in destination guild(s)
    """
    if not channel_id:
        return False
    try:
        cid = int(channel_id)
    except Exception:
        return False

    if SMART_SOURCE_CHANNELS:
        return cid in SMART_SOURCE_CHANNELS

    try:
        if category_id and MONITOR_CATEGORY_IDS and int(category_id) in MONITOR_CATEGORY_IDS:
            return True
    except Exception:
        pass

    return bool(MONITOR_ALL_DESTINATION_CHANNELS)

