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

# Outbound: send forwarded/mirrored messages via channel webhooks (for clean identity).
USE_WEBHOOKS_FOR_FORWARDING: bool = False

# Outbound: re-upload Discord attachments as real files (vs embed image URLs).
FORWARD_ATTACHMENTS_AS_FILES: bool = True
FORWARD_ATTACHMENTS_MAX_FILES: int = 10
# Default to ~7.5MB to stay under common 8MB limits.
FORWARD_ATTACHMENTS_MAX_BYTES: int = 7_500_000

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
SMARTFILTER_AMZ_DEALS_CHANNEL_ID: int = 0
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
SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID: int = 0

# Home Depot "total inventory" definitive embeds: single source channel → single destination (not MAJOR_CLEARANCE pairing).
HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID: int = 0
HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID: int = 0

# Runtime knobs
RECENT_TTL_SECONDS: int = 10
GLOBAL_DUPLICATE_TTL_SECONDS: int = 60 * 5
MONITOR_EMBED_TTL_SECONDS: int = 60 * 5
LINK_TRACKING_TTL_SECONDS: int = 24 * 60 * 60
FORWARD_ON_EDIT: bool = False
SHORT_EMBED_CHAR_THRESHOLD: int = 50
SHORT_EMBED_RETRY_DELAY_SECONDS: float = 5.0
SHORT_EMBED_MAX_WAIT_SECONDS: float = 35.0

FALLBACK_CHANNEL_ID: int = 0

EDIT_COOLDOWN_SECONDS: int = 30
MAJOR_CLEARANCE_PAIR_TTL_SECONDS: int = 180
MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT: bool = False

# Optional: explicit allowlist of source channels that should use the major-clearance paired-flow.
# If empty, the bot falls back to `source_channel_ids_clearance`.
MAJOR_CLEARANCE_SOURCE_CHANNEL_IDS: Set[int] = set()

# Debug reactions (opt-in)
DEBUG_REACTIONS_ENABLED: bool = False
DEBUG_REACTIONS_ALLOW_CHANNEL_IDS: Set[int] = set()
DEBUG_REACTIONS_EMOJI_ALLOWED: str = "✅"
DEBUG_REACTIONS_EMOJI_BLOCKED: str = "❌"


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
    global USE_WEBHOOKS_FOR_FORWARDING
    global FORWARD_ATTACHMENTS_AS_FILES, FORWARD_ATTACHMENTS_MAX_FILES, FORWARD_ATTACHMENTS_MAX_BYTES
    global SEND_MIN_INTERVAL_SECONDS
    global MIRRORWORLD_ROUTE_ONLINE, MIRRORWORLD_ROUTE_INSTORE
    global ENABLE_DEFAULT_FALLBACK
    global ENABLE_RAW_LINK_UNWRAP
    global SMARTFILTER_AMAZON_CHANNEL_ID, SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, SMARTFILTER_AMZ_DEALS_CHANNEL_ID, SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID
    global SMARTFILTER_UPCOMING_CHANNEL_ID, SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, SMARTFILTER_MAJOR_STORES_CHANNEL_ID
    global SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID
    global SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID
    global SMARTFILTER_DEFAULT_CHANNEL_ID, SMARTFILTER_UNCLASSIFIED_CHANNEL_ID
    global SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID
    global SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID
    global SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID
    global HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID, HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID
    global RECENT_TTL_SECONDS, GLOBAL_DUPLICATE_TTL_SECONDS, MONITOR_EMBED_TTL_SECONDS, LINK_TRACKING_TTL_SECONDS
    global FORWARD_ON_EDIT
    global SHORT_EMBED_CHAR_THRESHOLD, SHORT_EMBED_RETRY_DELAY_SECONDS, SHORT_EMBED_MAX_WAIT_SECONDS
    global FALLBACK_CHANNEL_ID
    global EDIT_COOLDOWN_SECONDS
    global MAJOR_CLEARANCE_PAIR_TTL_SECONDS, MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT
    global MAJOR_CLEARANCE_SOURCE_CHANNEL_IDS
    global DEBUG_REACTIONS_ENABLED, DEBUG_REACTIONS_ALLOW_CHANNEL_IDS
    global DEBUG_REACTIONS_EMOJI_ALLOWED, DEBUG_REACTIONS_EMOJI_BLOCKED

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
    SMARTFILTER_AMZ_DEALS_CHANNEL_ID = _get_int(dests, "AMZ_DEALS", 0)
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
    SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID = _get_int(gd, "MAJOR_CLEARANCE", 0)

    HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID = _get_int(settings, "hd_total_inventory_source_channel_id", 0)
    HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID = _get_int(settings, "hd_total_inventory_destination_channel_id", 0)

    RECENT_TTL_SECONDS = _get_int(settings, "recent_ttl_seconds", 10)
    GLOBAL_DUPLICATE_TTL_SECONDS = _get_int(settings, "global_duplicate_ttl_seconds", 60 * 5)
    MONITOR_EMBED_TTL_SECONDS = _get_int(settings, "monitor_embed_ttl_seconds", 60 * 5)
    LINK_TRACKING_TTL_SECONDS = _get_int(settings, "link_tracking_ttl_seconds", 24 * 60 * 60)
    FORWARD_ON_EDIT = bool(settings.get("forward_on_edit", False))
    SHORT_EMBED_CHAR_THRESHOLD = _get_int(settings, "short_embed_char_threshold", 50)
    if SHORT_EMBED_CHAR_THRESHOLD < 0:
        SHORT_EMBED_CHAR_THRESHOLD = 0
    try:
        SHORT_EMBED_RETRY_DELAY_SECONDS = float(settings.get("short_embed_retry_delay_seconds", 5.0) or 5.0)
    except Exception:
        SHORT_EMBED_RETRY_DELAY_SECONDS = 5.0
    if SHORT_EMBED_RETRY_DELAY_SECONDS < 0:
        SHORT_EMBED_RETRY_DELAY_SECONDS = 0.0
    try:
        SHORT_EMBED_MAX_WAIT_SECONDS = float(settings.get("short_embed_max_wait_seconds", 35.0) or 35.0)
    except Exception:
        SHORT_EMBED_MAX_WAIT_SECONDS = 35.0
    if SHORT_EMBED_MAX_WAIT_SECONDS < 0:
        SHORT_EMBED_MAX_WAIT_SECONDS = 0.0

    try:
        FALLBACK_CHANNEL_ID = int(settings.get("fallback_channel_id") or 0)
    except Exception:
        FALLBACK_CHANNEL_ID = 0

    EDIT_COOLDOWN_SECONDS = _get_int(settings, "edit_cooldown_seconds", 30)
    MAJOR_CLEARANCE_PAIR_TTL_SECONDS = _get_int(settings, "major_clearance_pair_ttl_seconds", 180)
    if MAJOR_CLEARANCE_PAIR_TTL_SECONDS < 10:
        MAJOR_CLEARANCE_PAIR_TTL_SECONDS = 10
    MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT = bool(settings.get("major_clearance_send_single_on_timeout", False))
    MAJOR_CLEARANCE_SOURCE_CHANNEL_IDS = _parse_int_set(settings.get("major_clearance_source_channel_ids"))

    # Debug reactions (disabled by default).
    dbg = settings.get("debug_reactions") if isinstance(settings.get("debug_reactions"), dict) else {}
    DEBUG_REACTIONS_ENABLED = bool(dbg.get("enabled", False))
    DEBUG_REACTIONS_ALLOW_CHANNEL_IDS = _parse_int_set(dbg.get("allow_channel_ids"))
    DEBUG_REACTIONS_EMOJI_ALLOWED = str(dbg.get("emoji_allowed", "✅") or "✅")
    DEBUG_REACTIONS_EMOJI_BLOCKED = str(dbg.get("emoji_blocked", "❌") or "❌")


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

