from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import settings_store as cfg
from keywords import check_keyword_match, load_keyword_channel_overrides, scan_keywords
from logging_utils import log_smartfilter
from patterns import (
    ALL_STORE_PATTERN,
    AMAZON_ASIN_PATTERN,
    CONVERSATIONAL_DEALS_AMAZON_PHRASE_PATTERN,
    CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN,
    AMAZON_LINK_PATTERN,
    CONVERSATIONAL_DEALS_RETAIL_PATTERN,
    AMAZON_PROFITABLE_INDICATOR_PATTERN,
    is_amazon_deal_complicated_monitor_blob,
    is_amz_price_errors_monitor_blob,
    is_divine_helper_price_monitor_blob,
    is_flipflip_restock_monitor_blob,
    is_ringinthedeals_flipfluence_deal_blob,
    is_flipfluence_rerouter_product_card_blob,
    is_mention_format_noise_blob,
    is_amz_deals_affiliate_bridge_blob,
    allow_amz_deals_despite_complicated_monitor,
    instore_sneakers_bucket_active,
    instore_apparel_suppresses_sneakers_bucket,
    instore_explicit_footwear_intent,
    should_skip_amazon_profitable_leads_monitor_blob,
    is_simple_amazon_profitable_lead_blob,
    passes_deal_substance_gate,
    affiliate_should_suppress_affiliated_links,
    CARDS_PATTERN,
    DISCOUNTED_STORE_PATTERN,
    INSTORE_KEYWORDS,
    LABEL_PATTERN,
    MAJOR_STORE_PATTERN,
    PRICE_ERROR_PATTERN,
    PROFITABLE_FLIP_PATTERN,
    SEASONAL_PATTERN,
    SNEAKERS_PATTERN,
    STORE_DOMAINS,
    WOOT_DEALS_PATTERN,
    TIMESTAMP_PATTERN,
)
from utils import (
    collect_embed_strings,
    extract_urls_from_text,
    is_discord_media_url,
    matches_instore_theatre,
    merge_text_and_embed_strings_for_classifier,
)


# Strong toy / figure signals — suppress INSTORE_CARDS when TCG heuristics misfire (eBay URLs, embeds).
_TOY_FIGURE_SUPPRESS_CARDS_PATTERN = re.compile(
    r"\b("
    r"jakks\s*pacific|world\s+of\s+nintendo|"
    r"2\.5\s*[\"\u201c\u201d]\s*figures?\b|2\.5\s*inch\s*figures?\b|"
    r"action\s*figures?\b|vinyl\s*figures?\b"
    r")\b",
    re.IGNORECASE,
)

# Stronger TCG context than bare substrings (avoids "selection" → select, ".m570." already fixed in patterns).
_INSTORE_CARDS_CONTEXT_PATTERN = re.compile(
    r"("
    r"pokemon|magic\s*the\s*gathering|mtg|yugioh|one\s*piece\s*card|dragon\s*ball\s*(super\s*)?(tcg|card)?|"
    r"flesh\s*and\s*blood|fab\s*tcg|lorcana|digimon\s*card|tcg\b|ccg\b|"
    r"booster\s*(pack|box|case)\b|etb\b|elite\s*trainer\s*box|starter\s*deck|"
    r"\bslab\b|psa\s*\d+|bgs\s*\d+|cgc\s*\d+|graded\s*card|"
    r"rookie\s*card|autograph|auto\s*card|"
    r"topps\b|panini\b|upper\s*deck|bowman\b|donruss\b|\bprizm\b|\bselect\b|\boptic\b|\bmosaic\b|"
    r"case\s*break|sealed\s*(box|pack|case)"
    r")",
    re.IGNORECASE,
)

_FIELD_DELIMITER_PATTERN = r"[:\-/]"
_INSTORE_PRIMARY_FIELD_PATTERNS = [
    re.compile(rf"\bretail(?:\s+price)?\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"\bresell(?:\s+price)?\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"\b(?:where|location)\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
]
_INSTORE_OPTIONAL_FIELD_PATTERNS = [
    re.compile(rf"product\s*title\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"link\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"sku\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
]


def determine_source_group(channel_id: Optional[int]) -> str:
    if channel_id is None:
        return "unknown"
    if int(channel_id) in cfg.SMART_SOURCE_CHANNELS_INSTORE:
        return "instore"
    if int(channel_id) in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()):
        return "clearance"
    if int(channel_id) in cfg.SMART_SOURCE_CHANNELS_ONLINE:
        return "online"
    if int(channel_id) in cfg.SMART_SOURCE_CHANNELS_MISC:
        return "misc"
    if int(channel_id) in cfg.SMART_SOURCE_CHANNELS:
        return "configured"
    return "unknown"


def has_instore_required_fields(text: str) -> bool:
    """
    Detect in-store / flip lead shape. Discord embeds often split labels and values
    (e.g. field name 'Retail' + value '$12.99'), so strict 'Retail:' on one line fails.
    """
    if not text or not str(text).strip():
        return False
    raw = str(text)
    tl = raw.lower()
    # Legacy: Retail:/Resell:/Where: on one line each
    if all(pattern.search(raw) for pattern in _INSTORE_PRIMARY_FIELD_PATTERNS):
        return True
    has_retail = bool(
        re.search(rf"\bretail(?:\s+price)?\s*{_FIELD_DELIMITER_PATTERN}", tl)
        or re.search(r"\bretail(?:\s+price)?\b", tl)
    )
    has_resell = bool(
        re.search(rf"\bresell(?:\s+price)?\s*{_FIELD_DELIMITER_PATTERN}", tl)
        or re.search(r"\bresell\b", tl)
    )
    has_where = bool(
        re.search(rf"\bwhere\s*{_FIELD_DELIMITER_PATTERN}", tl)
        or re.search(r"\blocation\s*:", tl)
        or re.search(rf"\bstores?\s*{_FIELD_DELIMITER_PATTERN}", tl)
    )
    price_tokens = re.findall(r"\$\s*[\d,]+(?:\.\d{2})?(?:\s*[-–]\s*\$?\s*[\d,]+(?:\.\d{2})?)?", tl)
    n_prices = len(price_tokens)
    if has_retail and has_resell and has_where and n_prices >= 1:
        return True
    # Embed-style: label tokens + multiple dollar amounts + at least one known retailer
    if n_prices >= 2 and ALL_STORE_PATTERN.search(tl) and (has_retail or has_resell):
        return True
    if n_prices >= 2 and ALL_STORE_PATTERN.search(tl) and (has_where or bool(MAJOR_STORE_PATTERN.search(tl) or DISCOUNTED_STORE_PATTERN.search(tl))):
        return True
    return False


def _instore_source_obvious_lead(text: str) -> bool:
    """Thin embeds from source_channel_ids_instore only (not clearance)."""
    if not text or len(text.strip()) < 8:
        return False
    tl = text.lower()
    if len(re.findall(r"\$\s*[\d,]+(?:\.\d{1,2})?", tl)) >= 2:
        return True
    if ALL_STORE_PATTERN.search(tl) and re.search(r"\$\s*[\d,]", tl):
        return True
    return False


def store_category_from_location(where_location: str) -> Optional[str]:
    if not where_location:
        return None
    normalized = where_location.lower()
    if MAJOR_STORE_PATTERN.search(normalized):
        return "major"
    if DISCOUNTED_STORE_PATTERN.search(normalized):
        return "discounted"
    return None


def is_definitive_major_clearance_embed(text: str) -> bool:
    """
    Home Depot monitor embed shape for MAJOR_CLEARANCE (clearance source channels only in routing).
    Single source of truth shared with live_forwarder major-clearance flow.
    """
    tl = (text or "").lower()
    if not tl:
        return False
    has_title = ("home depot store clearance deals" in tl) and ("new item" in tl)
    has_inventory = bool(re.search(r"\btotal\s*inventory\b", tl))
    has_internet_number = bool(re.search(r"\binternet\s*number\b", tl))
    has_price_shape = bool(
        re.search(r"\bprice\b", tl)
        and re.search(r"\boriginal\s*price\b", tl)
        and (re.search(r"\bpercentage\s*off\b", tl) or re.search(r"\bdollar\s*off\b", tl))
    )
    return bool(has_title and has_inventory and has_internet_number and has_price_shape)


_HD_TOTAL_INVENTORY_VALUE_PATTERN = re.compile(
    r"\btotal\s*inventory\b[^0-9]{0,160}(\d{1,7})\b",
    re.IGNORECASE,
)


def parse_major_clearance_total_inventory(text: str) -> Optional[int]:
    """Best-effort parse of Total Inventory count from flattened embed/text. None if not found."""
    if not (text or "").strip():
        return None
    m = _HD_TOTAL_INVENTORY_VALUE_PATTERN.search(str(text))
    if not m:
        return None
    try:
        v = int(m.group(1))
        return v if v >= 0 else None
    except Exception:
        return None


def qualifies_hd_total_inventory_route(
    *,
    source_channel_id: Optional[int],
    pe_check_blob: str,
    trace: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    True when HD 1:1 source/dest are configured, source matches, embed is definitive HD clearance,
    and optional min total inventory (HD_TOTAL_INVENTORY_MIN_TOTAL) is satisfied.
    """
    hd_inv_src = int(getattr(cfg, "HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID", 0) or 0)
    hd_inv_dest = int(getattr(cfg, "HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID", 0) or 0)
    if hd_inv_src <= 0 or hd_inv_dest <= 0:
        return False
    if int(source_channel_id or 0) != hd_inv_src:
        return False
    if not is_definitive_major_clearance_embed(pe_check_blob):
        return False
    min_total = int(getattr(cfg, "HD_TOTAL_INVENTORY_MIN_TOTAL", 0) or 0)
    parsed = parse_major_clearance_total_inventory(pe_check_blob)
    if trace is not None:
        try:
            tr = trace.setdefault("classifier", {}).setdefault("matches", {})
            tr["hd_total_inventory_min_total"] = int(min_total)
            tr["hd_total_inventory_parsed"] = parsed
        except Exception:
            pass
    if min_total <= 0:
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["hd_total_inventory"] = True
            except Exception:
                pass
        return True
    ok = parsed is not None and parsed >= min_total
    if trace is not None:
        try:
            tr = trace.setdefault("classifier", {}).setdefault("matches", {})
            tr["hd_total_inventory_passes_min"] = bool(ok)
            if not ok:
                tr["hd_total_inventory_blocked_min"] = True
            else:
                tr["hd_total_inventory"] = True
        except Exception:
            pass
    return bool(ok)


def is_tempo_monitors_major_clearance_candidate(text: str) -> bool:
    """
    TempoMonitors-style **Home Depot** stock embed (MSRP / As low as + SKU or UPC + Tempo footer).

    Walmart/Target/other Tempo feeds share the same MSRP/As-low/UPC shape; they must NOT use the
    major-clearance / HD-clearance bucket. Gate the Tempo branch on explicit Home Depot wording
    (footer copy like "Home Depot Finds" / "Home Depot Leads"). Definitive HD clearance embeds are
    still accepted via is_definitive_major_clearance_embed below.
    """
    tl = (text or "").lower()
    if not tl:
        return False
    has_msrp = "msrp" in tl
    has_as_low = ("as low as" in tl) or ("as-low-as" in tl)
    has_upc = "upc" in tl and bool(re.search(r"\b\d{11,14}\b", tl))
    has_sku = "sku" in tl and bool(re.search(r"\bsku\b[^\n]{0,50}\b\d{6,}\b", tl))
    has_tempo = "tempomonitors.com" in tl or "powered by tempomonitors" in tl
    if has_msrp and has_as_low and (has_upc or has_sku) and has_tempo:
        compact = re.sub(r"\s+", "", tl)
        if "homedepot" not in compact and "home depot" not in tl:
            return is_definitive_major_clearance_embed(text)
        return True
    return is_definitive_major_clearance_embed(text)


def is_major_clearance_monitor_embed_blob(text: str) -> bool:
    """True for definitive HD clearance embeds OR Tempo **Home Depot** stock-monitor embed shape."""
    if not (text or "").strip():
        return False
    if is_definitive_major_clearance_embed(text):
        return True
    return is_tempo_monitors_major_clearance_candidate(text)


_DISCORD_MESSAGE_LINK_ONLY = re.compile(
    r"^\s*https?://(?:www\.|(?:(?:ptb|canary)\.)?)discord(?:app)?\.com/channels/\d+/\d+/\d+\s*$",
    re.IGNORECASE,
)


def is_major_clearance_followup_blob(
    text_to_check: str,
    *,
    message_content: str = "",
    embeds: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    """
    Second-message shapes for major-clearance pairing (Tempo stock lists, nationwide stock embeds,
    Deal Soldier jump links). Canonical with live_forwarder follow-up gate.
    Not for primary monitor embeds (those use is_major_clearance_monitor_embed_blob).
    """
    if is_major_clearance_monitor_embed_blob(text_to_check or ""):
        return False
    tl = (text_to_check or "").lower()
    if not tl:
        return False
    try:
        embed_extra = " ".join(collect_embed_strings(embeds or [])).lower()
    except Exception:
        embed_extra = ""
    tl_all = (tl + " " + embed_extra).strip().lower()
    if not tl_all:
        return False

    c = (message_content or "").strip()
    if c and re.fullmatch(r"[\d\s\-\.]{8,}", c):
        return False
    if re.search(r"\bretail\s*[:\-]|\bresell\s*[:\-]|\bwhere\s*[:\-]|\blocation\s*[:\-]", tl_all):
        return False

    if c and _DISCORD_MESSAGE_LINK_ONLY.match(c):
        return True

    hints = (
        "would look for this",
        "would look for these",
        "lots of stock",
        "lots of additional stores",
    )
    if any(h in tl_all for h in hints):
        return True

    if "nationwide stock check" in tl_all:
        return True
    if ("stores on sale" in tl_all or "percentage of stores on sale" in tl_all) and (
        "nationwide" in tl_all or "across the nation" in tl_all or "units on sale" in tl_all
    ):
        return True
    if "units on sale" in tl_all and "across the nation" in tl_all:
        return True

    if re.search(r"\btons of stock\s*@", tl_all):
        return True

    if re.search(r"\bstock\s*@\s*\$?\s*\d+", tl_all):
        return True
    return False


def classify_instore_destination(
    text_to_check: str,
    where_location: str,
    store_category: Optional[str],
    is_instore_source: bool,
    instore_required: bool,
    instore_context: bool,
    trace: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[int, str]]:
    """
    Canonical instore classification logic (single source of truth).
    Returns (channel_id, tag) for the first matching instore classification, or None.
    Order: Seasonal -> Cards -> Sneakers -> Theatre -> Major Stores -> Discounted Stores -> INSTORE_LEADS
    """
    if not (is_instore_source and instore_required and instore_context):
        return None
    
    # Check patterns once
    seasonal_hit = bool(SEASONAL_PATTERN.search(text_to_check or ""))
    sneakers_pattern_hit = bool(SNEAKERS_PATTERN.search(text_to_check or ""))
    sneakers_hit = bool(instore_sneakers_bucket_active(text_to_check or ""))
    cards_match = CARDS_PATTERN.search(text_to_check or "")
    card_context_hit = bool(_INSTORE_CARDS_CONTEXT_PATTERN.search(text_to_check or ""))
    cards_hit = bool(cards_match) and card_context_hit
    toy_figure_suppress = bool(_TOY_FIGURE_SUPPRESS_CARDS_PATTERN.search(text_to_check or ""))
    if toy_figure_suppress:
        cards_hit = False
    theatre_hit = bool(matches_instore_theatre(text_to_check or "", where_location))
    major_hit = bool(MAJOR_STORE_PATTERN.search(text_to_check or "") or store_category == "major")
    discounted_hit = bool(DISCOUNTED_STORE_PATTERN.search(text_to_check or "") or store_category == "discounted")
    # Keep store traits mutually exclusive when both store-list regexes hit the same blob (e.g. Where lists
    # Target + Five Below). Dispatch order still evaluates MAJOR before DISCOUNTED, so without a tie-break
    # we'd incorrectly zero major whenever any discount banner token appeared anywhere in the message.
    if major_hit and discounted_hit:
        if store_category == "discounted":
            major_hit = False
        else:
            # store_category "major", unknown, or unset → prefer MAJOR_STORES (typical national retail lists).
            discounted_hit = False
    elif discounted_hit:
        major_hit = False
    elif major_hit:
        discounted_hit = False
    
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {}).update(
                {
                    "instore_seasonal": seasonal_hit,
                    "instore_sneakers_pattern_hit": sneakers_pattern_hit,
                    "instore_sneakers_apparel_suppress": bool(
                        sneakers_pattern_hit
                        and instore_apparel_suppresses_sneakers_bucket(text_to_check or "")
                    ),
                    "instore_sneakers_footwear_intent": bool(
                        sneakers_pattern_hit and instore_explicit_footwear_intent(text_to_check or "")
                    ),
                    "instore_sneakers": sneakers_hit,
                    "instore_cards": cards_hit,
                    "instore_cards_pattern_span": (
                        {"start": cards_match.start(), "end": cards_match.end(), "text": cards_match.group(0)[:200]}
                        if cards_match
                        else None
                    ),
                    "instore_cards_context_hit": card_context_hit,
                    "instore_cards_toy_figure_suppress": toy_figure_suppress,
                    "instore_theatre": theatre_hit,
                    "major_store": major_hit,
                    "discounted_store": discounted_hit,
                }
            )
        except Exception:
            pass
    
    # Classification order (priority-based)
    if seasonal_hit and cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, "INSTORE_SEASONAL"
    if cards_hit and cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, "INSTORE_CARDS"
    if sneakers_hit and cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, "INSTORE_SNEAKERS"
    if theatre_hit and cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, "INSTORE_THEATRE"
    # HD clearance monitors + Tempo stock embeds: never MAJOR_STORES / DISCOUNTED / INSTORE_LEADS.
    # live_forwarder major-clearance pairing + sends own these on configured source channels.
    if int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0) > 0 and is_major_clearance_monitor_embed_blob(
        text_to_check or ""
    ):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})[
                    "major_clearance_monitor_suppress_instore_buckets"
                ] = True
            except Exception:
                pass
        return None
    if major_hit and cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID:
        return cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID, "MAJOR_STORES"
    if discounted_hit and cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID:
        return cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, "DISCOUNTED_STORES"
    if cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "INSTORE_LEADS"
    
    return None


def is_truly_upcoming_explain(text: str) -> Dict[str, Any]:
    """
    Explainable UPCOMING validator (single source of truth).
    Returns dict with:
      - reason
      - hard_exclusion_hit
      - has_future_indicator
      - matched_future_indicators
    """
    text_lower = (text or "").lower()

    hard_exclusion_patterns = [
        r"price\s+drop",
        r"returns?\s+till",
        r"returns?\s+until",
        r"discount\s+till",
        r"discount\s+until",
        r"clearance\s+till",
        r"clearance\s+until",
        r"offer\s+ends?",
        r"avg\s+30",
        r"avg\s+365",
        r"average\s+30",
        r"average\s+365",
        r"released\s+on\s+\d",
        r"dropped\s+to",
        r"lowest\s+ever\s+drop",
        # Restock/monitor alerts: not "upcoming" (future drop), so exclude from UPCOMING channel
        r"item\s+restocked",
        r"item\s+restock\b",
        r"\brestocked\b",
        r"restock\s+alert",
        r"back\s+in\s+stock",
        r"in\s+stock\s+now",
    ]
    for pat in hard_exclusion_patterns:
        try:
            if re.search(pat, text_lower, re.IGNORECASE):
                return {
                    "reason": "hard_exclusion",
                    "hard_exclusion_hit": pat,
                    "has_future_indicator": False,
                    "matched_future_indicators": [],
                }
        except Exception:
            continue

    future_indicator_patterns = [
        r"coming\s+soon",
        r"drops?\s+(on|at|in|tomorrow)",
        r"releasing?\s+(on|at|in|tomorrow)",
        r"launches?\s+(on|at|in|tomorrow)",
        r"goes?\s+live",
        r"go\s+live",
        r"available\s+(on|at|tomorrow)",
        r"starts?\s+(on|at|tomorrow)",
        r"pre[- ]?order",
        r"next\s+(week|month)",
        r"tomorrow",
        r"<t:\d+:[a-zA-Z]>",
        r"release\s+date",
        r"time\s*:\s*\d{1,2}(?::\d{2})?\s*(am|pm)",
        r"but\s+(drops?|releases?|launches?)\s+(on|at|in|tomorrow)",
        r"overseas\s+but\s+(drops?|releases?|launches?)",
        r"release\s+type",
        r"\braffle\b",
        r"\beql\b",
    ]
    matched: List[str] = []
    for pat in future_indicator_patterns:
        if len(matched) >= 5:
            break
        try:
            if re.search(pat, text_lower, re.IGNORECASE):
                matched.append(pat)
        except Exception:
            continue
    if not matched:
        return {
            "reason": "missing_future_indicator",
            "hard_exclusion_hit": None,
            "has_future_indicator": False,
            "matched_future_indicators": [],
        }
    return {
        "reason": "future_indicator_present",
        "hard_exclusion_hit": None,
        "has_future_indicator": True,
        "matched_future_indicators": matched,
    }


def is_truly_upcoming(text: str) -> bool:
    """Boolean wrapper for UPCOMING validator."""
    try:
        return bool(is_truly_upcoming_explain(text or "").get("has_future_indicator"))
    except Exception:
        return False


def _store_domain_pattern() -> re.Pattern[str]:
    domains = list(STORE_DOMAINS.keys())
    return re.compile(r"https?://[^\s]*(" + "|".join([re.escape(d) for d in domains]) + r")[^\s]*", re.IGNORECASE)


_STORE_DOMAIN_PATTERN = _store_domain_pattern()

# Never route “New Deal Found!” style banners to the conversational AMZ_DEALS bucket.
_NEW_DEAL_FOUND_PATTERN = re.compile(r"\bnew\s+deal\s+found\b", re.IGNORECASE)


def _primary_store_label_from_blob(text_blob: str) -> str:
    """
    Best-effort "primary store" detector based on the earliest store-domain URL in the blob.
    This prevents misrouting when embeds include multiple comp links (e.g. eBay/Amazon/Walmart)
    but the main product link is a different store (e.g. homedepot.com).
    """
    lb = (text_blob or "").lower()
    best_idx: Optional[int] = None
    best_store = ""
    for dom, store in (STORE_DOMAINS or {}).items():
        try:
            idx = lb.find(str(dom).lower())
        except Exception:
            idx = -1
        if idx < 0:
            continue
        if best_idx is None or idx < best_idx:
            best_idx = idx
            best_store = str(store or "").strip().lower()
    return best_store


def _store_label_present_in_blob(text_blob: str, store_label: str) -> bool:
    """True if any known store-domain mapped to `store_label` exists in the blob."""
    tl = (text_blob or "").lower()
    target = str(store_label or "").strip().lower()
    if not tl or not target:
        return False
    for dom, store in (STORE_DOMAINS or {}).items():
        try:
            if str(store).strip().lower() != target:
                continue
            if str(dom).strip().lower() in tl:
                return True
        except Exception:
            continue
    return False


def _is_amazon_primary(text_blob: str) -> bool:
    # If Woot appears anywhere, treat Amazon as NOT primary even if amzn.to exists.
    if _store_label_present_in_blob(text_blob, "woot") or WOOT_DEALS_PATTERN.search(text_blob or ""):
        return False
    primary = _primary_store_label_from_blob(text_blob)
    # If we can detect a primary store and it's not amazon, do NOT treat "Amazon" comp links as AMAZON.
    if primary and primary != "amazon":
        return False
    # If no store-domain detected, fall back to existing amazon-match behavior.
    return True


def _looks_like_conversational_amazon_deal(
    text_blob: str,
    *,
    source_group: str,
    source_channel_id: Optional[int] = None,
    trace: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Conversational online deals → AMZ_DEALS (settings key; not Amazon-only).

    - Amazon-style phrases (clip coupon, promo stack, …): still require Amazon to be the
      primary store when URLs make another retailer primary.
    - Retail / Instacart glitch phrasing: allowed with **no** product link (Walmart-on-Instacart, etc.).
    """

    def _skip(reason: str) -> bool:
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["amz_deals_conversational_skip"] = reason
            except Exception:
                pass
        return False

    if not text_blob:
        return _skip("empty_blob")
    if source_group != "online":
        return _skip("not_online_source_group")
    # Extra safety: only treat as conversational-amazon when the channel is explicitly
    # configured in source_channel_ids_online.
    try:
        if source_channel_id is not None and int(source_channel_id) not in getattr(cfg, "SMART_SOURCE_CHANNELS_ONLINE", set()):
            return _skip("not_configured_online_channel")
    except Exception:
        return _skip("channel_id_error")
    # "New Deal Found! ... GRAB IT HERE" templates are affiliate-banner style posts (often galaxydeals redirect)
    # and should never land in conversational AMZ_DEALS.
    if _AFFILIATE_GRAB_TEMPLATE.search(text_blob or ""):
        return _skip("grab_it_here_template")
    if _NEW_DEAL_FOUND_PATTERN.search(text_blob) and not allow_amz_deals_despite_complicated_monitor(text_blob):
        return _skip("new_deal_found_banner")
    # FlipFluence rerouter product cards are high-volume embeds that look like "normal product copy",
    # not a conversational deal post.
    if is_flipfluence_rerouter_product_card_blob(text_blob):
        return _skip("flipfluence_rerouter_product_card")
    # These templates almost always include at least one explicit price token.
    if "$" not in text_blob:
        return _skip("no_dollar_sign")
    # Exclude stock-monitor / clearance-feed style embeds that can include "Amazon" in comps.
    if re.search(
        r"(store\s+clearance\s+deals|clearance\s+deals?\s*-\s*new\s+item|"
        r"\binternet\s+number\b|\bpercentage\s+off\b|\bdollar\s+off\b|\btotal\s+inventory\b)",
        text_blob,
        re.IGNORECASE,
    ):
        return _skip("clearance_monitor_shape")

    # In-store arbitrage template (Retail / Resell / Where) — not the AMZ_DEALS "promo stack" bucket.
    if (
        re.search(r"\bretail\s*[:\-]", text_blob, re.IGNORECASE)
        and re.search(r"\bresell\s*[:\-]", text_blob, re.IGNORECASE)
        and re.search(r"\b(where|location)\s*[:\-]", text_blob, re.IGNORECASE)
    ):
        return _skip("instore_style_retail_resell_where")

    # Online flip-template cards: When / Retail / Resell / Quick Links (eBay...) — not CONVERSATIONAL_DEALS.
    # These are structured arbitrage recommendations, not a "conversational deal" post.
    if (
        re.search(r"(?im)^\s*when\b", text_blob)
        and re.search(r"(?im)^\s*retail\b", text_blob)
        and re.search(r"(?im)^\s*resell\b", text_blob)
        and (re.search(r"(?im)^\s*quick\s+links?\b", text_blob) or re.search(r"\bebay\b", text_blob, re.IGNORECASE))
    ):
        return _skip("flip_template_when_retail_resell")

    # Pointer posts (Discord jump link + tiny body) or AMZ Price Errors style snippets.
    if re.search(
        r"https?://(?:(?:www|ptb|canary)\.)?discord(?:app)?\.com/channels/\d+/\d+/\d+",
        text_blob,
        re.IGNORECASE,
    ):
        condensed = re.sub(r"\s+", " ", (text_blob or "").strip())
        short_pointer = len(condensed) <= 520
        price_errors_pointer = bool(
            re.search(r"@?amazon\s+price\s+errors?\b", text_blob, re.IGNORECASE)
            or re.search(r"\bprice\s+errors?\b", text_blob, re.IGNORECASE)
        )
        if short_pointer or price_errors_pointer:
            return _skip("discord_jump_link_pointer")

    if is_amazon_deal_complicated_monitor_blob(text_blob) and not allow_amz_deals_despite_complicated_monitor(text_blob):
        return _skip("teaser_or_xx_price_monitor")
    if is_amz_price_errors_monitor_blob(text_blob):
        return _skip("amz_price_errors_monitor_template")
    if is_divine_helper_price_monitor_blob(text_blob):
        return _skip("divine_helper_monitor")
    if is_flipflip_restock_monitor_blob(text_blob):
        return _skip("flipflip_restock_monitor")
    if is_ringinthedeals_flipfluence_deal_blob(text_blob):
        return _skip("ringinthedeals_flipfluence_template")

    amazon_conv = bool(CONVERSATIONAL_DEALS_AMAZON_PHRASE_PATTERN.search(text_blob))
    retail_conv = bool(CONVERSATIONAL_DEALS_RETAIL_PATTERN.search(text_blob))
    if not amazon_conv and not retail_conv:
        return _skip("no_conversational_phrase")

    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {}).update(
                {
                    "conversational_amazon_phrase": amazon_conv,
                    "conversational_retail_phrase": retail_conv,
                }
            )
        except Exception:
            pass

    # Grocery / Instacart-style monitors: do not require an http link or Amazon-primary URL logic.
    if retail_conv:
        return True

    # Amazon conversational phrases: avoid Woot / “comps” where another store is primary.
    if amazon_conv:
        # Require a strict deal signal; weak phrases like "shipped and sold by amazon" are too noisy.
        if not CONVERSATIONAL_DEALS_STRICT_SIGNAL_PATTERN.search(text_blob or ""):
            return _skip("missing_strict_conversational_signal")
        if not _is_amazon_primary(text_blob):
            return _skip("amazon_not_primary_store")
        return True

    return False


_AFFILIATE_GRAB_TEMPLATE = re.compile(r"\bnew\s+deal\s+found\b[\s\S]{0,300}\bgrab\s+it\s+here\b", re.IGNORECASE)


def _affiliate_has_non_discord_url(blob: str) -> bool:
    """True when blob includes at least one non-Discord-media http(s) URL."""
    urls = extract_urls_from_text(blob or "")
    for u in urls:
        if not u:
            continue
        if is_discord_media_url(u):
            continue
        return True
    return False


_AFFILIATE_LINK_ONLY_LINE_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)


def _affiliate_is_link_only_noise(
    *,
    message_content: str,
    embeds: Optional[List[Dict[str, Any]]],
    attachments: Optional[List[Dict[str, Any]]],
) -> bool:
    """
    True when the user-visible message body is only bare URL line(s) and there is no embed/attachment payload.
    These are not useful "affiliated leads" forwards (often link-drops / collection pages).
    """
    if embeds:
        for e in embeds:
            if isinstance(e, dict) and e:
                return False
    if attachments:
        for a in attachments:
            if isinstance(a, dict) and a:
                return False
    body = (message_content or "").strip()
    if not body:
        return True
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    if not lines:
        return True
    return all(bool(_AFFILIATE_LINK_ONLY_LINE_RE.match(ln)) for ln in lines)


def _affiliate_skip_link_only_route(
    *,
    message_content: str,
    embeds: Optional[List[Dict[str, Any]]],
    attachments: Optional[List[Dict[str, Any]]],
    trace: Optional[Dict[str, Any]],
) -> bool:
    """If True, caller must not route to AFFILIATED_LINKS for this message."""
    if not bool(getattr(cfg, "AFFILIATE_SKIP_LINK_ONLY_MESSAGES", True)):
        return False
    if not _affiliate_is_link_only_noise(
        message_content=message_content or "",
        embeds=embeds,
        attachments=attachments,
    ):
        return False
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = "link_only_body"
        except Exception:
            pass
    return True


def select_target_channel_id(
    text_to_check: str,
    attachments: List[Dict[str, Any]],
    keywords_list: List[str] | None = None,
    source_channel_id: Optional[int] = None,
    trace: Optional[Dict[str, Any]] = None,
    *,
    message_content: str = "",
    embeds: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Tuple[int, str]]:
    """Single-target classifier (used as fallback if multi-type detection returns nothing)."""
    source_group = determine_source_group(source_channel_id)
    # Instore smart-filters (major/discounted/theatre/etc.) only for source_channel_ids_instore — not clearance.
    is_instore_source = source_group == "instore"
    instore_required = has_instore_required_fields(text_to_check)
    if is_instore_source and not instore_required:
        instore_required = _instore_source_obvious_lead(text_to_check or "")
    normalized_lower = (text_to_check or "").lower()

    instore_context = False
    if is_instore_source:
        instore_context = True
    elif any(k in normalized_lower for k in INSTORE_KEYWORDS):
        instore_context = True
    elif LABEL_PATTERN.search(text_to_check or ""):
        instore_context = True
    elif ALL_STORE_PATTERN.search(text_to_check or ""):
        instore_context = True
    if not instore_required:
        instore_context = False

    # Where / location (tolerate markdown bold around colons)
    where_match = re.search(
        r"(?:where|location)\s*\*?\s*[:\-]\s*\*?\s*([^\n]+)", text_to_check or "", re.IGNORECASE
    )
    where_location = where_match.group(1).strip() if where_match else ""
    store_category = store_category_from_location(where_location)
    text_blob = text_to_check or ""
    pe_sel = text_blob
    if embeds:
        try:
            pe_sel = (text_blob + "\n" + " ".join(collect_embed_strings(embeds))).strip()
        except Exception:
            pe_sel = text_blob

    hd_inv_dest = int(getattr(cfg, "HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID", 0) or 0)
    if qualifies_hd_total_inventory_route(
        source_channel_id=source_channel_id, pe_check_blob=pe_sel, trace=trace
    ):
        return hd_inv_dest, "HD_TOTAL_INVENTORY"

    # CLEARANCE routing policy:
    # Clearance source channels should not participate in general routing (PRICE_ERROR/flips/etc).
    # They are handled exclusively via the definitive "Home Depot store clearance deals" embed
    # (MAJOR_CLEARANCE destination).
    if source_group == "clearance":
        mc_id = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
        if mc_id > 0 and is_major_clearance_monitor_embed_blob(pe_sel):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["definitive_major_clearance"] = bool(
                        is_definitive_major_clearance_embed(pe_sel)
                    )
                    trace.setdefault("classifier", {}).setdefault("matches", {})["tempo_major_clearance_candidate"] = bool(
                        is_tempo_monitors_major_clearance_candidate(pe_sel)
                    )
                except Exception:
                    pass
            return mc_id, "MAJOR_CLEARANCE"
        return None

    mc_id = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
    skip_amazon = bool(source_group == "clearance")
    if mc_id > 0 and is_instore_source and (
        is_major_clearance_monitor_embed_blob(pe_sel)
        or is_major_clearance_followup_blob(pe_sel, message_content=message_content or "", embeds=embeds)
    ):
        skip_amazon = True
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["skip_amazon_for_clearance"] = bool(skip_amazon)
        except Exception:
            pass

    if trace is not None:
        try:
            c = trace.setdefault("classifier", {})
            c.update(
                {
                    "source_group": source_group,
                    "is_instore_source": bool(is_instore_source),
                    "instore_required_fields": bool(instore_required),
                    "instore_context": bool(instore_context),
                    "where_location": where_location,
                    "store_category": store_category,
                }
            )
        except Exception:
            pass

    if is_mention_format_noise_blob(text_blob, attachments):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["mention_format_skip"] = True
            except Exception:
                pass
        return None

    # 0) PRICE_ERROR / glitched (online-only; same gate as global_triggers).
    # Exclude rigid AMZ Price Errors monitor templates (Amazon Sold / eBay Avg / flip lines) — not true "glitch" leads.
    _pe_min = int(getattr(cfg, "PRICE_ERROR_MIN_SUBSTANCE_CHARS", 52) or 52)
    if (
        cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID
        and source_group != "instore"
        and not is_amz_price_errors_monitor_blob(text_blob)
        and PRICE_ERROR_PATTERN.search(text_blob)
    ):
        if not passes_deal_substance_gate(text_blob, min_core_chars=_pe_min):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})[
                        "price_error_substance_gate"
                    ] = "blocked_thin_placeholder"
                except Exception:
                    pass
        else:
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["price_error"] = True
                except Exception:
                    pass
            return cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"

    # Woot is an online affiliate merchant (computers.woot.com, etc.). It is not an in-store lead bucket.
    # `_is_amazon_primary` already suppresses AMAZON when Woot is present (amzn.to comps).
    if _store_label_present_in_blob(text_blob, "woot") or WOOT_DEALS_PATTERN.search(text_blob or ""):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["primary_store"] = "woot"
            except Exception:
                pass

    # 1) AMAZON (strict) – profitable flips → AMAZON_PROFITABLE_LEAD
    amazon_match = AMAZON_LINK_PATTERN.search(text_blob) if not skip_amazon else None
    if amazon_match and (cfg.SMARTFILTER_AMAZON_CHANNEL_ID or cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID):
        matched = amazon_match.group(0).lower()
        if ("amazon." in matched or "amzn.to" in matched or "a.co" in matched or matched.startswith("b0")) and _is_amazon_primary(text_blob):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["amazon"] = matched[:200]
                except Exception:
                    pass
            is_profitable = bool(
                PROFITABLE_FLIP_PATTERN.search(text_blob) or AMAZON_PROFITABLE_INDICATOR_PATTERN.search(text_blob)
            )
            skip_profitable_leads = bool(should_skip_amazon_profitable_leads_monitor_blob(text_blob))
            complicated_monitor = bool(is_amazon_deal_complicated_monitor_blob(text_blob))
            simple_profitable = bool(not is_profitable and is_simple_amazon_profitable_lead_blob(text_blob))
            profitable_for_leads_bucket = bool(
                (is_profitable or simple_profitable) and not complicated_monitor
            )
            if trace is not None:
                try:
                    m = trace.setdefault("classifier", {}).setdefault("matches", {})
                    if skip_profitable_leads:
                        m["amz_price_errors_monitor_template"] = bool(
                            is_amz_price_errors_monitor_blob(text_blob)
                        )
                        m["ringinthedeals_deal_feed_template"] = bool(
                            is_ringinthedeals_flipfluence_deal_blob(text_blob)
                        )
                    if complicated_monitor:
                        m["amazon_complicated_monitor_template"] = True
                    if simple_profitable:
                        m["simple_amazon_profitable_lead"] = True
                except Exception:
                    pass
            if (
                profitable_for_leads_bucket
                and cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID
                and not skip_profitable_leads
            ):
                # Canonical tag string (matches global_triggers.py and manual picker)
                return cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "AMAZON_PROFITABLE_LEAD"
            if cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
                return cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"
            if cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID:
                # Last-resort for strict Amazon when AMAZON bucket isn't configured.
                return cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "AMAZON_FALLBACK"

    # FlipFlip Walmart stock monitors: excluded from profitable-leads; Walmart is usually "primary" so the
    # strict Amazon branch above is skipped — still send to the generic AMAZON channel (ops bucket).
    if (
        (not skip_amazon)
        and cfg.SMARTFILTER_AMAZON_CHANNEL_ID
        and is_flipflip_restock_monitor_blob(text_blob)
    ):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["flipflip_monitor_amazon_channel"] = True
            except Exception:
                pass
        return cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"

    # 2) MONITORED_KEYWORD
    keyword_hit = bool(keywords_list and check_keyword_match(text_blob, keywords_list, trace=trace))
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keyword"] = bool(keyword_hit)
        except Exception:
            pass
    if keyword_hit and cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID:
        # Optional per-keyword extra channel routing (configured via /keywordchannel).
        matched_kws: List[str] = []
        try:
            if trace is not None:
                mk = (trace.get("classifier", {}) or {}).get("matches", {}).get("monitored_keywords")
                if isinstance(mk, list):
                    matched_kws = [str(x) for x in mk if str(x).strip()]
        except Exception:
            matched_kws = []
        if not matched_kws:
            try:
                matched_kws = scan_keywords(text_blob, keywords_list)
            except Exception:
                matched_kws = []
        try:
            overrides = load_keyword_channel_overrides()
        except Exception:
            overrides = {}
        chosen = 0
        chosen_kw = ""
        for kw in matched_kws:
            cid = int(overrides.get(str(kw).strip().lower(), 0) or 0)
            if cid > 0 and cid != int(cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID or 0):
                chosen = cid
                chosen_kw = str(kw)
                break
        if chosen > 0:
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keyword_override"] = {
                        "keyword": chosen_kw,
                        "channel_id": chosen,
                    }
                except Exception:
                    pass
            return chosen, "MONITORED_KEYWORD"
        return cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID, "MONITORED_KEYWORD"

    # 3-6) INSTORE categories (canonical classification)
    instore_result = classify_instore_destination(
        text_to_check=text_blob,
        where_location=where_location,
        store_category=store_category,
        is_instore_source=is_instore_source,
        instore_required=instore_required,
        instore_context=instore_context,
        trace=trace,
    )
    if instore_result:
        return instore_result
    if mc_id > 0 and is_instore_source and is_major_clearance_monitor_embed_blob(pe_sel):
        return mc_id, "MAJOR_CLEARANCE"
    if mc_id > 0 and is_instore_source and is_major_clearance_followup_blob(
        pe_sel, message_content=message_content or "", embeds=embeds
    ):
        return mc_id, "MAJOR_CLEARANCE"

    # Conversational Amazon deal bucket — evaluated *after* instore/keyword so Ross-style online posts
    # do not get mis-bucketed as AMZ_DEALS.
    if (
        not skip_amazon
        and source_group == "online"
        and cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID
        and _looks_like_conversational_amazon_deal(
            pe_sel,
            source_group=source_group,
            source_channel_id=source_channel_id,
            trace=trace,
        )
    ):
        return cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID, "CONVERSATIONAL_DEALS"

    # 9) UPCOMING (online only; explainable)
    if cfg.SMARTFILTER_UPCOMING_CHANNEL_ID and (source_group == "online") and TIMESTAMP_PATTERN.search(text_to_check or ""):
        upcoming_explain = is_truly_upcoming_explain(text_to_check or "")
        if trace is not None:
            try:
                trace.setdefault("classifier", {})["upcoming_explain"] = upcoming_explain
            except Exception:
                pass
        if upcoming_explain.get("has_future_indicator"):
            log_smartfilter("UPCOMING", "TRIGGER", {**(trace.get("classifier", {}) if trace else {}), **upcoming_explain})
            return cfg.SMARTFILTER_UPCOMING_CHANNEL_ID, "UPCOMING"
        if cfg.VERBOSE:
            log_smartfilter("UPCOMING", "SKIP", {**(trace.get("classifier", {}) if trace else {}), **upcoming_explain})

    # 10) AFFILIATED_LINKS / other stores (online only — not clearance or instore sources)
    if (source_group == "online") and cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID:
        att_text = " ".join([str(a.get("url", "")) for a in (attachments or []) if isinstance(a, dict)])
        blob = (text_to_check or "") + " " + att_text
        # Hard suppressions: keep AFFILIATED_LINKS focused (exclude flip templates / pokemon / comics / stubs).
        try:
            sup = affiliate_should_suppress_affiliated_links(blob, min_core_chars=int(getattr(cfg, "AFFILIATED_LINKS_MIN_SUBSTANCE_CHARS", 80) or 80))
        except Exception:
            sup = affiliate_should_suppress_affiliated_links(blob, min_core_chars=80)
        if sup:
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = sup
                except Exception:
                    pass
        if not sup:
            if trace is not None:
                try:
                    m = _STORE_DOMAIN_PATTERN.search(blob) if ("http" in blob) else None
                    dom = m.group(1).lower() if m else ""
                    mavely = "mavely.app" in blob.lower()
                    trace.setdefault("classifier", {}).setdefault("matches", {}).update(
                        {
                            "affiliate_http": bool("http" in blob),
                            "affiliate_domain": dom,
                            "affiliate_mavely": bool(mavely),
                        }
                    )
                except Exception:
                    pass
            if _AFFILIATE_GRAB_TEMPLATE.search(blob or ""):
                if trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = "grab_it_here_template"
                    except Exception:
                        pass
            elif "http" in blob and (_STORE_DOMAIN_PATTERN.search(blob) or "mavely.app" in blob.lower()):
                if trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_reason"] = "store_domain_or_mavely"
                    except Exception:
                        pass
                if not _affiliate_skip_link_only_route(
                    message_content=message_content or "",
                    embeds=embeds,
                    attachments=attachments,
                    trace=trace,
                ):
                    return cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"
            if "http" in blob:
                if trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_reason"] = "http_present"
                    except Exception:
                        pass
                # Avoid routing image-only posts (Discord CDN) into affiliate bucket.
                if _affiliate_has_non_discord_url(blob):
                    if not _affiliate_skip_link_only_route(
                        message_content=message_content or "",
                        embeds=embeds,
                        attachments=attachments,
                        trace=trace,
                    ):
                        return cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"
                if trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = "discord_media_only"
                    except Exception:
                        pass

    # 11) DEFAULT fallback
    if (not skip_amazon) and AMAZON_ASIN_PATTERN.search(text_to_check or "") and cfg.SMARTFILTER_AMAZON_CHANNEL_ID and _is_amazon_primary(text_blob):
        return cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"
    # Avoid false positives from text like "Links: Amazon" (must include an actual Amazon URL/ASIN).
    if (not skip_amazon) and cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID and AMAZON_LINK_PATTERN.search(text_blob) and _is_amazon_primary(text_blob):
        return cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "AMAZON_FALLBACK"
    if cfg.SMARTFILTER_DEFAULT_CHANNEL_ID and bool(getattr(cfg, "ENABLE_DEFAULT_FALLBACK", False)):
        return cfg.SMARTFILTER_DEFAULT_CHANNEL_ID, "DEFAULT"
    return None


def detect_all_link_types(
    text_to_check: str,
    attachments: List[Dict[str, Any]],
    keywords_list: List[str] | None = None,
    embeds: List[Dict[str, Any]] | None = None,
    source_channel_id: Optional[int] = None,
    trace: Optional[Dict[str, Any]] = None,
    *,
    message_content: str = "",
) -> List[Tuple[int, str]]:
    """Multi-type classifier used by live forwarder for multi-destination routing."""
    results: List[Tuple[int, str]] = []
    source_group = determine_source_group(source_channel_id)
    # Instore smart-filters (major/discounted/theatre/etc.) only for source_channel_ids_instore — not clearance.
    is_instore_source = source_group == "instore"
    instore_required = has_instore_required_fields(text_to_check)
    if is_instore_source and not instore_required:
        instore_required = _instore_source_obvious_lead(text_to_check or "")
    normalized_lower = (text_to_check or "").lower()

    instore_context = False
    if is_instore_source:
        instore_context = True
    elif any(k in normalized_lower for k in INSTORE_KEYWORDS):
        instore_context = True
    elif LABEL_PATTERN.search(text_to_check or ""):
        instore_context = True
    elif ALL_STORE_PATTERN.search(text_to_check or ""):
        instore_context = True
    if not instore_required:
        instore_context = False

    # Where / location (tolerate markdown bold around colons)
    where_match = re.search(
        r"(?:where|location)\s*\*?\s*[:\-]\s*\*?\s*([^\n]+)", text_to_check or "", re.IGNORECASE
    )
    where_location = where_match.group(1).strip() if where_match else ""
    store_category = store_category_from_location(where_location)
    text_blob = text_to_check or ""
    pe_check_blob = merge_text_and_embed_strings_for_classifier(text_to_check or "", embeds)

    hd_inv_dest = int(getattr(cfg, "HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID", 0) or 0)
    if qualifies_hd_total_inventory_route(
        source_channel_id=source_channel_id, pe_check_blob=pe_check_blob, trace=trace
    ):
        return [(hd_inv_dest, "HD_TOTAL_INVENTORY")]

    mc_id = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
    skip_amazon = bool(source_group == "clearance")
    if mc_id > 0 and is_instore_source and (
        is_major_clearance_monitor_embed_blob(pe_check_blob)
        or is_major_clearance_followup_blob(
            pe_check_blob, message_content=message_content or "", embeds=embeds
        )
    ):
        skip_amazon = True
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["skip_amazon_for_clearance"] = bool(skip_amazon)
        except Exception:
            pass

    # CLEARANCE routing policy:
    # Clearance source channels should not participate in general routing.
    # Definitive HD clearance embeds + Tempo stock-monitor embeds route to MAJOR_CLEARANCE only.
    if source_group == "clearance":
        if mc_id > 0 and is_major_clearance_monitor_embed_blob(pe_check_blob):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["definitive_major_clearance"] = bool(
                        is_definitive_major_clearance_embed(pe_check_blob)
                    )
                    trace.setdefault("classifier", {}).setdefault("matches", {})["tempo_major_clearance_candidate"] = bool(
                        is_tempo_monitors_major_clearance_candidate(pe_check_blob)
                    )
                except Exception:
                    pass
            return [(mc_id, "MAJOR_CLEARANCE")]
        return []

    if trace is not None:
        try:
            c = trace.setdefault("classifier", {})
            c.update(
                {
                    "source_group": source_group,
                    "is_instore_source": bool(is_instore_source),
                    "instore_required_fields": bool(instore_required),
                    "instore_context": bool(instore_context),
                    "where_location": where_location,
                    "store_category": store_category,
                }
            )
        except Exception:
            pass

    if is_mention_format_noise_blob(pe_check_blob, attachments):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["mention_format_skip"] = True
            except Exception:
                pass
        return []

    # PRICE_ERROR / glitched (add early for order_link_types priority; online-only + exclude AMZ monitor templates)
    _pe_min_m = int(getattr(cfg, "PRICE_ERROR_MIN_SUBSTANCE_CHARS", 52) or 52)
    if (
        cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID
        and source_group != "instore"
        and not is_amz_price_errors_monitor_blob(pe_check_blob)
        and PRICE_ERROR_PATTERN.search(pe_check_blob)
    ):
        if not passes_deal_substance_gate(pe_check_blob, min_core_chars=_pe_min_m):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})[
                        "price_error_substance_gate"
                    ] = "blocked_thin_placeholder"
                except Exception:
                    pass
        else:
            results.append((cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"))
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["price_error"] = True
                except Exception:
                    pass

    # Woot is online / affiliate-shaped; do not short-circuit the multi-route classifier into INSTORE_LEADS.
    if _store_label_present_in_blob(text_blob, "woot") or WOOT_DEALS_PATTERN.search(text_blob or ""):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["primary_store"] = "woot"
            except Exception:
                pass

    amazon_detected = False
    amazon_match = AMAZON_LINK_PATTERN.search(text_blob) if not skip_amazon else None
    if amazon_match and (cfg.SMARTFILTER_AMAZON_CHANNEL_ID or cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID):
        matched = amazon_match.group(0).lower()
        if ("amazon." in matched or "amzn.to" in matched or "a.co" in matched or matched.startswith("b0")) and _is_amazon_primary(text_blob):
            amazon_detected = True
            is_profitable = bool(
                PROFITABLE_FLIP_PATTERN.search(text_blob) or AMAZON_PROFITABLE_INDICATOR_PATTERN.search(text_blob)
            )
            skip_profitable_leads = bool(should_skip_amazon_profitable_leads_monitor_blob(text_blob))
            complicated_monitor = bool(is_amazon_deal_complicated_monitor_blob(text_blob))
            simple_profitable = bool(not is_profitable and is_simple_amazon_profitable_lead_blob(text_blob))
            profitable_for_leads_bucket = bool(
                (is_profitable or simple_profitable) and not complicated_monitor
            )
            force_amz_deals = bool(
                source_group == "online"
                and cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID
                and is_amz_deals_affiliate_bridge_blob(text_blob)
            )
            if trace is not None:
                try:
                    m = trace.setdefault("classifier", {}).setdefault("matches", {})
                    if skip_profitable_leads:
                        m["amz_price_errors_monitor_template"] = bool(
                            is_amz_price_errors_monitor_blob(text_blob)
                        )
                        m["ringinthedeals_deal_feed_template"] = bool(
                            is_ringinthedeals_flipfluence_deal_blob(text_blob)
                        )
                    if complicated_monitor:
                        m["amazon_complicated_monitor_template"] = True
                    if simple_profitable:
                        m["simple_amazon_profitable_lead"] = True
                    if force_amz_deals:
                        m["amz_deals_affiliate_bridge"] = True
                except Exception:
                    pass
            if (
                profitable_for_leads_bucket
                and cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID
                and not skip_profitable_leads
                and not force_amz_deals
            ):
                # Canonical tag string (matches global_triggers.py and manual picker)
                results.append((cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "AMAZON_PROFITABLE_LEAD"))
            elif force_amz_deals:
                results.append((cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID, "CONVERSATIONAL_DEALS"))
            elif cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
                results.append((cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"))
            elif cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID:
                # Last-resort for strict Amazon when AMAZON bucket isn't configured.
                results.append((cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "AMAZON_FALLBACK"))
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["amazon"] = matched[:200]
                except Exception:
                    pass

    if (
        (not skip_amazon)
        and cfg.SMARTFILTER_AMAZON_CHANNEL_ID
        and is_flipflip_restock_monitor_blob(text_blob)
        and not amazon_detected
    ):
        results.append((cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"))

    keyword_hit = bool(keywords_list and check_keyword_match(text_blob, keywords_list, trace=trace))
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keyword"] = bool(keyword_hit)
        except Exception:
            pass
    if keyword_hit and cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID:
        base_cid = int(cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID or 0)
        if base_cid > 0:
            results.append((base_cid, "MONITORED_KEYWORD"))

        # Extra per-keyword channels (in addition to the default monitored keyword channel).
        matched_kws: List[str] = []
        try:
            if trace is not None:
                mk = (trace.get("classifier", {}) or {}).get("matches", {}).get("monitored_keywords")
                if isinstance(mk, list):
                    matched_kws = [str(x) for x in mk if str(x).strip()]
        except Exception:
            matched_kws = []
        if not matched_kws:
            try:
                matched_kws = scan_keywords(text_blob, keywords_list)
            except Exception:
                matched_kws = []
        try:
            overrides = load_keyword_channel_overrides()
        except Exception:
            overrides = {}
        routed = []
        for kw in matched_kws[:25]:
            cid = int(overrides.get(str(kw).strip().lower(), 0) or 0)
            if cid > 0 and cid != base_cid:
                results.append((cid, "MONITORED_KEYWORD"))
                routed.append({"keyword": str(kw), "channel_id": int(cid)})
        if trace is not None and routed:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keyword_routed_channels"] = routed[:10]
            except Exception:
                pass

    # INSTORE categories (canonical classification)
    instore_selection = classify_instore_destination(
        text_to_check=text_to_check or "",
        where_location=where_location,
        store_category=store_category,
        is_instore_source=is_instore_source,
        instore_required=instore_required,
        instore_context=instore_context,
        trace=trace,
    )
    if instore_selection:
        results.append(instore_selection)

    if (
        mc_id > 0
        and is_instore_source
        and is_major_clearance_monitor_embed_blob(pe_check_blob)
        and not instore_selection
    ):
        results.append((mc_id, "MAJOR_CLEARANCE"))

    if (
        mc_id > 0
        and is_instore_source
        and is_major_clearance_followup_blob(
            pe_check_blob, message_content=message_content or "", embeds=embeds
        )
        and not any(tag == "MAJOR_CLEARANCE" for _, tag in results)
    ):
        results.append((mc_id, "MAJOR_CLEARANCE"))

    # Conversational Amazon deal bucket — after instore classification so Retail/Resell/Where leads
    # route to instore buckets first; skips inside _looks_like_conversational_amazon_deal gate noise.
    if (
        (not skip_amazon)
        and source_group == "online"
        and cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID
        and not amazon_detected
        and _looks_like_conversational_amazon_deal(
            pe_check_blob,
            source_group=source_group,
            source_channel_id=source_channel_id,
            trace=trace,
        )
    ):
        results.append((cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID, "CONVERSATIONAL_DEALS"))

    if cfg.SMARTFILTER_UPCOMING_CHANNEL_ID and (source_group == "online") and TIMESTAMP_PATTERN.search(text_to_check or ""):
        upcoming_explain = is_truly_upcoming_explain(text_to_check or "")
        if trace is not None:
            try:
                trace.setdefault("classifier", {})["upcoming_explain"] = upcoming_explain
            except Exception:
                pass
        if upcoming_explain.get("has_future_indicator"):
            results.append((cfg.SMARTFILTER_UPCOMING_CHANNEL_ID, "UPCOMING"))
            log_smartfilter("UPCOMING", "TRIGGER", {**(trace.get("classifier", {}) if trace else {}), **upcoming_explain})
        elif cfg.VERBOSE:
            log_smartfilter("UPCOMING", "SKIP", {**(trace.get("classifier", {}) if trace else {}), **upcoming_explain})

    # Affiliate links only if not instore-classified and no Amazon hard match
    if not any(
        tag.startswith("INSTORE")
        or tag in {"MAJOR_STORES", "DISCOUNTED_STORES", "MAJOR_CLEARANCE", "HD_TOTAL_INVENTORY"}
        for _, tag in results
    ):
        if cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID and (source_group == "online"):
            att_text = " ".join([str(a.get("url", "")) for a in (attachments or []) if isinstance(a, dict)])
            blob = (text_to_check or "") + " " + att_text
            if _AFFILIATE_GRAB_TEMPLATE.search(blob or ""):
                if trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = "grab_it_here_template"
                    except Exception:
                        pass
            elif "http" in blob:
                if trace is not None:
                    try:
                        m = _STORE_DOMAIN_PATTERN.search(blob)
                        dom = m.group(1).lower() if m else ""
                        mavely = "mavely.app" in blob.lower()
                        trace.setdefault("classifier", {}).setdefault("matches", {}).update(
                            {
                                "affiliate_http": True,
                                "affiliate_domain": dom,
                                "affiliate_mavely": bool(mavely),
                                "affiliate_reason": "http_present",
                            }
                        )
                    except Exception:
                        pass
                # Avoid routing image-only posts (Discord CDN) into affiliate bucket.
                if _affiliate_has_non_discord_url(blob):
                    if not _affiliate_skip_link_only_route(
                        message_content=message_content or "",
                        embeds=embeds,
                        attachments=attachments,
                        trace=trace,
                    ):
                        results.append((cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"))
                elif trace is not None:
                    try:
                        trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_skip"] = "discord_media_only"
                    except Exception:
                        pass

    # If Amazon detected, suppress other store destinations (keep PRICE_ERROR as it can co-exist)
    if any(tag in ("AMAZON", "AMAZON_PROFITABLE_LEAD", "AMAZON_FALLBACK", "CONVERSATIONAL_DEALS") for _, tag in results):
        results = [
            (cid, tag)
            for cid, tag in results
            if tag in ("AMAZON", "AMAZON_PROFITABLE_LEAD", "AMAZON_FALLBACK", "CONVERSATIONAL_DEALS", "PRICE_ERROR")
        ]

    # DEFAULT fallback if nothing
    if not results:
        if amazon_detected and cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID:
            results.append((cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "AMAZON_FALLBACK"))
        elif cfg.SMARTFILTER_DEFAULT_CHANNEL_ID and bool(getattr(cfg, "ENABLE_DEFAULT_FALLBACK", False)):
            results.append((cfg.SMARTFILTER_DEFAULT_CHANNEL_ID, "DEFAULT"))
    if trace is not None:
        try:
            trace.setdefault("classifier", {})["all_link_types"] = results
        except Exception:
            pass
    return results


def order_link_types(link_types: List[Tuple[int, str]]) -> Tuple[List[Tuple[int, str]], bool]:
    """Dispatch order helper (kept compatible with monolith shape)."""
    if not link_types:
        return [], False
    # Maintain original behavior: if PRICE_ERROR is present, treat it as primary and others as fallback.
    tag_set = {tag for _, tag in link_types}
    primary = list(link_types)
    fallback: List[Tuple[int, str]] = []
    if "PRICE_ERROR" in tag_set:
        primary = [(cid, tag) for cid, tag in link_types if tag == "PRICE_ERROR"]
        fallback = [(cid, tag) for cid, tag in link_types if tag != "PRICE_ERROR"]
    ordered = primary + fallback
    stop_after_first = bool("PRICE_ERROR" in tag_set)
    return ordered, stop_after_first

