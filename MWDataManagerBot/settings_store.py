from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set, Tuple

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

# Runtime data capture: record one sample URL per host (last-seen wins).
LINK_HOST_SAMPLES_ENABLED: bool = False
LINK_HOST_SAMPLES_PATH: str = ""
LINK_HOST_SAMPLES_MAX_HOSTS: int = 2000

# Optional: universal resolver fallback (local `universal_link_resolver_v2_ready/universal_link_resolver.py`)
# for classification-only URL expansion (never changes forwarded message text).
UNIVERSAL_RESOLVER_FALLBACK_ENABLED: bool = False
UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS: int = 12
UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH: int = 10
UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE: int = 2
UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS: int = 15 * 60
UNIVERSAL_RESOLVER_FALLBACK_INCLUDE_KNOWN_WRAPPERS: bool = False
UNIVERSAL_RESOLVER_FALLBACK_WHEN_NO_AMAZON_HINT: bool = True

# Affiliate: skip routing bare URL-only posts (no embeds, no attachments) to AFFILIATED_LINKS.
AFFILIATE_SKIP_LINK_ONLY_MESSAGES: bool = True
AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS: int = 80
# AFFILIATED_LINKS send gates (evaluated only when dispatch includes AFFILIATED_LINKS).
AFFILIATED_LINKS_DEDUPE_ENABLED: bool = False
AFFILIATED_LINKS_DEDUPE_DOMAINS: Tuple[str, ...] = ("mavely.app.link",)
# When True (default), duplicate skip applies to every non-Discord external URL in the message.
# When False, only hosts listed in AFFILIATED_LINKS_DEDUPE_DOMAINS are tracked (legacy mavely-only behavior).
AFFILIATED_LINKS_DEDUPE_ALL_EXTERNAL_URLS: bool = True
AFFILIATED_LINKS_DEDUPE_TTL_SECONDS: int = 24 * 60 * 60
AFFILIATED_LINKS_REQUIRE_IMAGE: bool = False
AFFILIATED_LINKS_REQUIRE_EXTERNAL_HTTP_LINK: bool = False

# Forwarding: if message content is URL-only and we are sending embeds, drop content (no duplicate top line).
STRIP_URL_ONLY_CONTENT_WHEN_EMBEDS: bool = True

# Local destinations
SMARTFILTER_AMAZON_CHANNEL_ID: int = 0
SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID: int = 0
# Canonical "conversational deals" destination (`smartfilter_destinations.CONVERSATIONAL_DEALS`).
SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID: int = 0
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
# When > 0, HD_TOTAL_INVENTORY only routes if "Total Inventory" parses to this value or higher.
HD_TOTAL_INVENTORY_MIN_TOTAL: int = 0
# Optional: after a successful forward to the HD_TOTAL_INVENTORY destination, delete the original source message.
HD_TOTAL_INVENTORY_DELETE_SOURCE_ON_SUCCESS: bool = False

# Runtime knobs
RECENT_TTL_SECONDS: int = 10
GLOBAL_DUPLICATE_TTL_SECONDS: int = 60 * 5
MONITOR_EMBED_TTL_SECONDS: int = 60 * 5
LINK_TRACKING_TTL_SECONDS: int = 24 * 60 * 60
FORWARD_ON_EDIT: bool = False
SHORT_EMBED_CHAR_THRESHOLD: int = 50
SHORT_EMBED_RETRY_DELAY_SECONDS: float = 5.0
SHORT_EMBED_MAX_WAIT_SECONDS: float = 35.0
# PRICE_ERROR / short-embed: minimum "core" body length unless URL / $ / %-off signals are present.
PRICE_ERROR_MIN_SUBSTANCE_CHARS: int = 52
# When True, PRICE_ERROR also requires deal signals (link/money/%/ASIN) so text-only \"price error\" memes do not route.
PRICE_ERROR_REQUIRES_DEAL_SUBSTANCE_SIGNALS: bool = True
# When True, PRICE_ERROR requires at least one http(s) URL in the flattened body (embeds + content).
# Blocks keyword hits like \"glitch\" with only $ amounts and an image attachment (no product/checkout link).
PRICE_ERROR_REQUIRES_HTTP_URL: bool = True

FALLBACK_CHANNEL_ID: int = 0

EDIT_COOLDOWN_SECONDS: int = 30
MAJOR_CLEARANCE_PAIR_TTL_SECONDS: int = 180
MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT: bool = False
# When True, definitive Home Depot "total inventory only" embeds do not post alone to MAJOR_CLEARANCE;
# they wait for a follow-up with per-store stock lines (or timeout behavior per MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT).
MAJOR_CLEARANCE_REQUIRE_FOLLOWUP_FOR_DEFINITIVE_HD: bool = True
# When True, format-based instore clearance monitor embeds (any major retailer / merchant URL) classify as
# major-clearance monitors — same path as HD definitive / Tempo HD (suppress MAJOR_STORES; MAJOR_CLEARANCE elsewhere).
INSTORE_CLEARANCE_MONITOR_EMBEDS_MAJOR_CLEARANCE: bool = True

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
    global AFFILIATE_SKIP_LINK_ONLY_MESSAGES
    global AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS
    global AFFILIATED_LINKS_DEDUPE_ENABLED, AFFILIATED_LINKS_DEDUPE_DOMAINS
    global AFFILIATED_LINKS_DEDUPE_ALL_EXTERNAL_URLS, AFFILIATED_LINKS_DEDUPE_TTL_SECONDS
    global AFFILIATED_LINKS_REQUIRE_IMAGE, AFFILIATED_LINKS_REQUIRE_EXTERNAL_HTTP_LINK
    global STRIP_URL_ONLY_CONTENT_WHEN_EMBEDS
    global SMARTFILTER_AMAZON_CHANNEL_ID, SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID, SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID
    global SMARTFILTER_UPCOMING_CHANNEL_ID, SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, SMARTFILTER_MAJOR_STORES_CHANNEL_ID
    global SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID
    global SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID
    global SMARTFILTER_DEFAULT_CHANNEL_ID, SMARTFILTER_UNCLASSIFIED_CHANNEL_ID
    global SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID
    global SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID
    global SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID
    global HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID, HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID, HD_TOTAL_INVENTORY_MIN_TOTAL
    global HD_TOTAL_INVENTORY_DELETE_SOURCE_ON_SUCCESS
    global RECENT_TTL_SECONDS, GLOBAL_DUPLICATE_TTL_SECONDS, MONITOR_EMBED_TTL_SECONDS, LINK_TRACKING_TTL_SECONDS
    global FORWARD_ON_EDIT
    global SHORT_EMBED_CHAR_THRESHOLD, SHORT_EMBED_RETRY_DELAY_SECONDS, SHORT_EMBED_MAX_WAIT_SECONDS
    global PRICE_ERROR_MIN_SUBSTANCE_CHARS, PRICE_ERROR_REQUIRES_DEAL_SUBSTANCE_SIGNALS, PRICE_ERROR_REQUIRES_HTTP_URL
    global FALLBACK_CHANNEL_ID
    global EDIT_COOLDOWN_SECONDS
    global MAJOR_CLEARANCE_PAIR_TTL_SECONDS, MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT
    global MAJOR_CLEARANCE_REQUIRE_FOLLOWUP_FOR_DEFINITIVE_HD, INSTORE_CLEARANCE_MONITOR_EMBEDS_MAJOR_CLEARANCE
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

    # Runtime link host samples (JSON map; one sample per host, last-seen wins).
    LINK_HOST_SAMPLES_ENABLED = bool(settings.get("link_host_samples_enabled", False))
    LINK_HOST_SAMPLES_PATH = str(settings.get("link_host_samples_path", "") or "").strip()
    LINK_HOST_SAMPLES_MAX_HOSTS = _get_int(settings, "link_host_samples_max_hosts", 2000)
    if LINK_HOST_SAMPLES_MAX_HOSTS < 50:
        LINK_HOST_SAMPLES_MAX_HOSTS = 50
    if LINK_HOST_SAMPLES_MAX_HOSTS > 20000:
        LINK_HOST_SAMPLES_MAX_HOSTS = 20000

    UNIVERSAL_RESOLVER_FALLBACK_ENABLED = bool(settings.get("universal_resolver_fallback_enabled", False))
    UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS = _get_int(settings, "universal_resolver_fallback_timeout_seconds", 12)
    if UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS < 3:
        UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS = 3
    if UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS > 60:
        UNIVERSAL_RESOLVER_FALLBACK_TIMEOUT_SECONDS = 60
    UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH = _get_int(settings, "universal_resolver_fallback_max_depth", 10)
    if UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH < 3:
        UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH = 3
    if UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH > 20:
        UNIVERSAL_RESOLVER_FALLBACK_MAX_DEPTH = 20
    UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE = _get_int(settings, "universal_resolver_fallback_max_urls_per_message", 2)
    if UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE < 1:
        UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE = 1
    if UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE > 6:
        UNIVERSAL_RESOLVER_FALLBACK_MAX_URLS_PER_MESSAGE = 6
    UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS = _get_int(settings, "universal_resolver_fallback_cache_ttl_seconds", 15 * 60)
    if UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS < 60:
        UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS = 60
    if UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS > 24 * 60 * 60:
        UNIVERSAL_RESOLVER_FALLBACK_CACHE_TTL_SECONDS = 24 * 60 * 60
    UNIVERSAL_RESOLVER_FALLBACK_INCLUDE_KNOWN_WRAPPERS = bool(
        settings.get("universal_resolver_fallback_include_known_wrappers", False)
    )
    UNIVERSAL_RESOLVER_FALLBACK_WHEN_NO_AMAZON_HINT = bool(
        settings.get("universal_resolver_fallback_when_no_amazon_hint", True)
    )

    AFFILIATE_SKIP_LINK_ONLY_MESSAGES = bool(settings.get("affiliate_skip_link_only_messages", True))
    AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS = _get_int(settings, "affiliated_links_min_substance_chars", 80)
    if AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS < 12:
        AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS = 12
    if AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS > 600:
        AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS = 600
    STRIP_URL_ONLY_CONTENT_WHEN_EMBEDS = bool(settings.get("strip_url_only_message_content_when_embeds", True))

    AFFILIATED_LINKS_DEDUPE_ENABLED = bool(settings.get("affiliated_links_dedupe_enabled", False))
    _dd = settings.get("affiliated_links_dedupe_domains")
    if isinstance(_dd, list) and _dd:
        AFFILIATED_LINKS_DEDUPE_DOMAINS = tuple(
            str(x).lower().strip() for x in _dd if str(x).strip()
        )
    elif isinstance(_dd, str) and _dd.strip():
        AFFILIATED_LINKS_DEDUPE_DOMAINS = tuple(
            p.strip().lower() for p in _dd.split(",") if p.strip()
        )
    else:
        AFFILIATED_LINKS_DEDUPE_DOMAINS = ("mavely.app.link",)
    AFFILIATED_LINKS_DEDUPE_ALL_EXTERNAL_URLS = bool(
        settings.get("affiliated_links_dedupe_all_external_urls", True)
    )
    AFFILIATED_LINKS_DEDUPE_TTL_SECONDS = _get_int(settings, "affiliated_links_dedupe_ttl_seconds", 24 * 60 * 60)
    if AFFILIATED_LINKS_DEDUPE_TTL_SECONDS < 60:
        AFFILIATED_LINKS_DEDUPE_TTL_SECONDS = 60
    if AFFILIATED_LINKS_DEDUPE_TTL_SECONDS > 14 * 24 * 60 * 60:
        AFFILIATED_LINKS_DEDUPE_TTL_SECONDS = 14 * 24 * 60 * 60
    AFFILIATED_LINKS_REQUIRE_IMAGE = bool(settings.get("affiliated_links_require_image", False))
    AFFILIATED_LINKS_REQUIRE_EXTERNAL_HTTP_LINK = bool(
        settings.get("affiliated_links_require_external_http_link", False)
    )

    dests = settings.get("smartfilter_destinations") if isinstance(settings.get("smartfilter_destinations"), dict) else {}
    SMARTFILTER_AMAZON_CHANNEL_ID = _get_int(dests, "AMAZON", 0)
    SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID = _get_int(dests, "AMAZON_FALLBACK", 0)
    # Canonical key only: CONVERSATIONAL_DEALS (no legacy fallback).
    SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID = _get_int(dests, "CONVERSATIONAL_DEALS", 0)
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
    HD_TOTAL_INVENTORY_MIN_TOTAL = _get_int(settings, "hd_total_inventory_min_total", 0)
    if HD_TOTAL_INVENTORY_MIN_TOTAL < 0:
        HD_TOTAL_INVENTORY_MIN_TOTAL = 0
    HD_TOTAL_INVENTORY_DELETE_SOURCE_ON_SUCCESS = bool(settings.get("hd_total_inventory_delete_source_on_success", False))

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

    PRICE_ERROR_MIN_SUBSTANCE_CHARS = _get_int(settings, "price_error_min_substance_chars", 52)
    if PRICE_ERROR_MIN_SUBSTANCE_CHARS < 12:
        PRICE_ERROR_MIN_SUBSTANCE_CHARS = 12
    if PRICE_ERROR_MIN_SUBSTANCE_CHARS > 500:
        PRICE_ERROR_MIN_SUBSTANCE_CHARS = 500

    PRICE_ERROR_REQUIRES_DEAL_SUBSTANCE_SIGNALS = bool(
        settings.get("price_error_requires_deal_substance_signals", True)
    )
    PRICE_ERROR_REQUIRES_HTTP_URL = bool(settings.get("price_error_requires_http_url", True))

    try:
        FALLBACK_CHANNEL_ID = int(settings.get("fallback_channel_id") or 0)
    except Exception:
        FALLBACK_CHANNEL_ID = 0

    EDIT_COOLDOWN_SECONDS = _get_int(settings, "edit_cooldown_seconds", 30)
    MAJOR_CLEARANCE_PAIR_TTL_SECONDS = _get_int(settings, "major_clearance_pair_ttl_seconds", 180)
    if MAJOR_CLEARANCE_PAIR_TTL_SECONDS < 10:
        MAJOR_CLEARANCE_PAIR_TTL_SECONDS = 10
    MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT = bool(settings.get("major_clearance_send_single_on_timeout", False))
    MAJOR_CLEARANCE_REQUIRE_FOLLOWUP_FOR_DEFINITIVE_HD = bool(
        settings.get("major_clearance_require_followup_for_definitive_hd", True)
    )
    INSTORE_CLEARANCE_MONITOR_EMBEDS_MAJOR_CLEARANCE = bool(
        settings.get("instore_clearance_monitor_embeds_major_clearance", True)
    )
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

