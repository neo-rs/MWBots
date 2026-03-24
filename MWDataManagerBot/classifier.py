from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import settings_store as cfg
from keywords import check_keyword_match, load_keyword_channel_overrides, scan_keywords
from logging_utils import log_debug, log_smartfilter
from patterns import (
    ALL_STORE_PATTERN,
    AMAZON_ASIN_PATTERN,
    AMAZON_CONVERSATIONAL_DEAL_PATTERN,
    AMAZON_LINK_PATTERN,
    RETAIL_CONVERSATIONAL_DEAL_PATTERN,
    AMAZON_PROFITABLE_INDICATOR_PATTERN,
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
from utils import matches_instore_theatre


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
    Order: Seasonal -> Sneakers -> Cards -> Theatre -> Major Stores -> Discounted Stores -> INSTORE_LEADS
    """
    if not (is_instore_source and instore_required and instore_context):
        return None
    
    # Check patterns once
    seasonal_hit = bool(SEASONAL_PATTERN.search(text_to_check or ""))
    sneakers_hit = bool(SNEAKERS_PATTERN.search(text_to_check or ""))
    cards_match = CARDS_PATTERN.search(text_to_check or "")
    # `CARDS_PATTERN` contains generic "card" terms, so it can match phrases like
    # "bottom left of the card" (toy listings / packaging) which are not TCG cards.
    # Require at least one stronger trading-card signal when we route to INSTORE_CARDS.
    card_context_hit = bool(
        re.search(
            r"("
            r"pokemon|magic\s*the\s*gathering|mtg|yugioh|one\s*piece\s*card|dragon\s*ball\s*(super\s*)?(tcg|card)?|"
            r"flesh\s*and\s*blood|fab\s*tcg|lorcana|digimon\s*card|tcg|ccg|"
            r"booster\s*(pack|box|case)?|etb|elite\s*trainer\s*box|starter\s*deck|"
            r"slab|psa\s*\d+|bgs\s*\d+|cgc\s*\d+|graded\s*card|"
            r"rookie\s*card|autograph|auto\s*card|"
            r"topps|panini|upper\s*deck|bowman|donruss|prizm|select|optic|mosaic|"
            r"case\s*break|sealed\s*(box|pack|case)"
            r")",
            text_to_check or "",
            re.IGNORECASE,
        )
    )
    cards_hit = bool(cards_match) and card_context_hit
    theatre_hit = bool(matches_instore_theatre(text_to_check or "", where_location))
    major_hit = bool(MAJOR_STORE_PATTERN.search(text_to_check or "") or store_category == "major")
    discounted_hit = bool(DISCOUNTED_STORE_PATTERN.search(text_to_check or "") or store_category == "discounted")
    # Keep store traits mutually exclusive to avoid "major + discounted" overlap leakage.
    # Prefer explicit discounted detection when both patterns happen to match.
    if discounted_hit:
        major_hit = False
    elif major_hit:
        discounted_hit = False
    
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {}).update(
                {
                    "instore_seasonal": seasonal_hit,
                    "instore_sneakers": sneakers_hit,
                    "instore_cards": cards_hit,
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
    if sneakers_hit and cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, "INSTORE_SNEAKERS"
    if cards_hit and cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, "INSTORE_CARDS"
    if theatre_hit and cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID:
        return cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, "INSTORE_THEATRE"
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
    if _NEW_DEAL_FOUND_PATTERN.search(text_blob):
        return _skip("new_deal_found_banner")
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

    amazon_conv = bool(AMAZON_CONVERSATIONAL_DEAL_PATTERN.search(text_blob))
    retail_conv = bool(RETAIL_CONVERSATIONAL_DEAL_PATTERN.search(text_blob))
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
        if not _is_amazon_primary(text_blob):
            return _skip("amazon_not_primary_store")
        return True

    return False


def select_target_channel_id(
    text_to_check: str,
    attachments: List[Dict[str, Any]],
    keywords_list: List[str] | None = None,
    source_channel_id: Optional[int] = None,
    trace: Optional[Dict[str, Any]] = None,
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

    skip_amazon = bool(source_group == "clearance")
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

    # 0) PRICE_ERROR / glitched (high priority, any store)
    if cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID and PRICE_ERROR_PATTERN.search(text_blob):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["price_error"] = True
            except Exception:
                pass
        return cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"

    # Definitive Home Depot clearance monitor embed → MAJOR_CLEARANCE (global_trigger_destinations).
    mc_id = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
    if mc_id > 0 and source_group == "clearance" and is_definitive_major_clearance_embed(text_blob):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["definitive_major_clearance"] = True
            except Exception:
                pass
        return mc_id, "MAJOR_CLEARANCE"

    # WOOT: Woot deals can include affiliate Amazon tracking (amzn.to links),
    # causing them to be detected as Amazon. Route to INSTORE_LEADS instead.
    if (
        _store_label_present_in_blob(text_blob, "woot")
        or WOOT_DEALS_PATTERN.search(text_blob or "")
    ) and cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID:
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["primary_store"] = "woot"
            except Exception:
                pass
        return cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "INSTORE_LEADS"

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
            if is_profitable and cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID:
                # Canonical tag string (matches global_triggers.py and manual picker)
                return cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "AMAZON_PROFITABLE_LEAD"
            if cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
                return cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"
            if cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID:
                # Last-resort for strict Amazon when AMAZON bucket isn't configured.
                return cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "AMAZON_FALLBACK"

    # Conversational Amazon deal bucket (deal templates without explicit amazon.com/amzn.to).
    if (
        not skip_amazon
        and source_group == "online"
        and cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID
        and _looks_like_conversational_amazon_deal(
            text_blob,
            source_group=source_group,
            source_channel_id=source_channel_id,
            trace=trace,
        )
    ):
        return cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID, "AMZ_DEALS"

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
        if "http" in blob and (_STORE_DOMAIN_PATTERN.search(blob) or "mavely.app" in blob.lower()):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_reason"] = "store_domain_or_mavely"
                except Exception:
                    pass
            return cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"
        if "http" in blob:
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["affiliate_reason"] = "http_present"
                except Exception:
                    pass
            return cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"

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

    skip_amazon = bool(source_group == "clearance")
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

    # PRICE_ERROR / glitched (add early for order_link_types priority)
    if cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID and PRICE_ERROR_PATTERN.search(text_blob):
        results.append((cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"))
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["price_error"] = True
            except Exception:
                pass

    # Definitive Home Depot clearance monitor embed → MAJOR_CLEARANCE (same rule as select_target_channel_id / live_forwarder).
    mc_id = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
    if (
        mc_id > 0
        and source_group == "clearance"
        and is_definitive_major_clearance_embed(text_blob)
        and not any(tag == "PRICE_ERROR" for _, tag in results)
    ):
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["definitive_major_clearance"] = True
            except Exception:
                pass
        return [(mc_id, "MAJOR_CLEARANCE")]

    # WOOT: Woot deals can include affiliate Amazon tracking (amzn.to links),
    # causing them to be detected as "Amazon". Route them to INSTORE_LEADS
    # so they go through the in-store classification buttons.
    if (
        _store_label_present_in_blob(text_blob, "woot")
        or WOOT_DEALS_PATTERN.search(text_blob or "")
    ) and cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID:
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["primary_store"] = "woot"
            except Exception:
                pass
        return [(cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "INSTORE_LEADS")]

    amazon_detected = False
    amazon_match = AMAZON_LINK_PATTERN.search(text_blob) if not skip_amazon else None
    if amazon_match and (cfg.SMARTFILTER_AMAZON_CHANNEL_ID or cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID):
        matched = amazon_match.group(0).lower()
        if ("amazon." in matched or "amzn.to" in matched or "a.co" in matched or matched.startswith("b0")) and _is_amazon_primary(text_blob):
            amazon_detected = True
            is_profitable = bool(
                PROFITABLE_FLIP_PATTERN.search(text_blob) or AMAZON_PROFITABLE_INDICATOR_PATTERN.search(text_blob)
            )
            if is_profitable and cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID:
                # Canonical tag string (matches global_triggers.py and manual picker)
                results.append((cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "AMAZON_PROFITABLE_LEAD"))
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

    # Conversational Amazon deal bucket (deal templates without explicit amazon.com/amzn.to).
    if (
        (not skip_amazon)
        and source_group == "online"
        and cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID
        and not amazon_detected
        and _looks_like_conversational_amazon_deal(
            text_blob,
            source_group=source_group,
            source_channel_id=source_channel_id,
            trace=trace,
        )
    ):
        results.append((cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID, "AMZ_DEALS"))

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
        or tag in {"MAJOR_STORES", "DISCOUNTED_STORES", "MAJOR_CLEARANCE"}
        for _, tag in results
    ):
        if cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID and (source_group == "online"):
            att_text = " ".join([str(a.get("url", "")) for a in (attachments or []) if isinstance(a, dict)])
            blob = (text_to_check or "") + " " + att_text
            if "http" in blob:
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
                results.append((cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "AFFILIATED_LINKS"))

    # If Amazon detected, suppress other store destinations (keep PRICE_ERROR as it can co-exist)
    if any(tag in ("AMAZON", "AMAZON_PROFITABLE_LEAD", "AMAZON_FALLBACK", "AMZ_DEALS") for _, tag in results):
        results = [(cid, tag) for cid, tag in results if tag in ("AMAZON", "AMAZON_PROFITABLE_LEAD", "AMAZON_FALLBACK", "AMZ_DEALS", "PRICE_ERROR")]

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

