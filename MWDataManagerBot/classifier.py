from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import settings_store as cfg
from keywords import check_keyword_match
from logging_utils import log_debug, log_smartfilter
from patterns import (
    ALL_STORE_PATTERN,
    AMAZON_ASIN_PATTERN,
    AMAZON_LINK_PATTERN,
    CARDS_PATTERN,
    DISCOUNTED_STORE_PATTERN,
    INSTORE_KEYWORDS,
    LABEL_PATTERN,
    MAJOR_STORE_PATTERN,
    SEASONAL_PATTERN,
    SNEAKERS_PATTERN,
    STORE_DOMAINS,
    TIMESTAMP_PATTERN,
)
from utils import matches_instore_theatre


_FIELD_DELIMITER_PATTERN = r"[:\-]"
_INSTORE_PRIMARY_FIELD_PATTERNS = [
    re.compile(rf"retail\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"resell\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
    re.compile(rf"where\s*{_FIELD_DELIMITER_PATTERN}", re.IGNORECASE),
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
    """Require Retail + Resell + Where headers for instore routing."""
    if not text:
        return False
    # Strict: must include all three primary headers to be treated as an in-store formatted lead.
    return all(pattern.search(text) for pattern in _INSTORE_PRIMARY_FIELD_PATTERNS)


def store_category_from_location(where_location: str) -> Optional[str]:
    if not where_location:
        return None
    normalized = where_location.lower()
    if MAJOR_STORE_PATTERN.search(normalized):
        return "major"
    if DISCOUNTED_STORE_PATTERN.search(normalized):
        return "discounted"
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


def select_target_channel_id(
    text_to_check: str,
    attachments: List[Dict[str, Any]],
    keywords_list: List[str] | None = None,
    source_channel_id: Optional[int] = None,
    trace: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[int, str]]:
    """Single-target classifier (used as fallback if multi-type detection returns nothing)."""
    source_group = determine_source_group(source_channel_id)
    is_instore_source = source_group in {"instore", "clearance"}
    instore_required = has_instore_required_fields(text_to_check)
    # Accept in-store formatted leads even if they arrive from an "online" channel.
    instore_formatted_override = bool(instore_required and not is_instore_source)
    is_instore_source = bool(is_instore_source or instore_formatted_override)
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

    where_match = re.search(r"where\s*:\s*([^\n]+)", text_to_check or "", re.IGNORECASE)
    where_location = where_match.group(1).strip() if where_match else ""
    store_category = store_category_from_location(where_location)
    text_blob = text_to_check or ""

    if trace is not None:
        try:
            c = trace.setdefault("classifier", {})
            c.update(
                {
                    "source_group": source_group,
                    "is_instore_source": bool(is_instore_source),
                    "instore_required_fields": bool(instore_required),
                    "instore_formatted_override": bool(instore_formatted_override),
                    "instore_context": bool(instore_context),
                    "where_location": where_location,
                    "store_category": store_category,
                }
            )
        except Exception:
            pass

    # 1) AMAZON (strict)
    amazon_match = AMAZON_LINK_PATTERN.search(text_blob)
    if amazon_match and cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
        matched = amazon_match.group(0).lower()
        if "amazon." in matched or "amzn.to" in matched or "a.co" in matched or matched.startswith("b0"):
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["amazon"] = matched[:200]
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
        return cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID, "MONITORED_KEYWORD"

    # 3-6) INSTORE categories (for instore/clearance channels OR in-store formatted leads)
    if is_instore_source:
        seasonal_hit = bool(SEASONAL_PATTERN.search(text_blob))
        sneakers_hit = bool(SNEAKERS_PATTERN.search(text_blob))
        cards_hit = bool(CARDS_PATTERN.search(text_blob))
        theatre_hit = bool(matches_instore_theatre(text_blob, where_location)) if instore_context else False
        major_hit = bool(MAJOR_STORE_PATTERN.search(text_blob) or store_category == "major")
        discounted_hit = bool(DISCOUNTED_STORE_PATTERN.search(text_blob) or store_category == "discounted")
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

        if instore_required and seasonal_hit and cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID:
            return cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, "INSTORE_SEASONAL"
        if instore_required and sneakers_hit and cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID:
            return cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, "INSTORE_SNEAKERS"
        if instore_required and cards_hit and cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID:
            return cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, "INSTORE_CARDS"
        if instore_required and instore_context and cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID and theatre_hit:
            return cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, "INSTORE_THEATRE"

        # 7) MAJOR_STORES
        if instore_required and major_hit and cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID:
            return cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID, "MAJOR_STORES"

        # 8) DISCOUNTED_STORES
        if instore_required and discounted_hit and cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID:
            return cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, "DISCOUNTED_STORES"

        # instore fallback
        if instore_required and (instore_context or where_location):
            if cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID:
                return cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "INSTORE_LEADS"

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

    # 10) AFFILIATED_LINKS / other stores (online only)
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
    if AMAZON_ASIN_PATTERN.search(text_to_check or "") and cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
        return cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"
    if cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID and ("amazon" in normalized_lower):
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
    is_instore_source = source_group in {"instore", "clearance"}
    instore_required = has_instore_required_fields(text_to_check)
    instore_formatted_override = bool(instore_required and not is_instore_source)
    is_instore_source = bool(is_instore_source or instore_formatted_override)
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

    where_match = re.search(r"where\s*:\s*([^\n]+)", text_to_check or "", re.IGNORECASE)
    where_location = where_match.group(1).strip() if where_match else ""
    store_category = store_category_from_location(where_location)
    text_blob = text_to_check or ""

    if trace is not None:
        try:
            c = trace.setdefault("classifier", {})
            c.update(
                {
                    "source_group": source_group,
                    "is_instore_source": bool(is_instore_source),
                    "instore_required_fields": bool(instore_required),
                    "instore_formatted_override": bool(instore_formatted_override),
                    "instore_context": bool(instore_context),
                    "where_location": where_location,
                    "store_category": store_category,
                }
            )
        except Exception:
            pass

    amazon_detected = False
    amazon_match = AMAZON_LINK_PATTERN.search(text_blob)
    if amazon_match and cfg.SMARTFILTER_AMAZON_CHANNEL_ID:
        amazon_detected = True
        matched = amazon_match.group(0).lower()
        if "amazon." in matched or "amzn.to" in matched or "a.co" in matched or matched.startswith("b0"):
            results.append((cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "AMAZON"))
            if trace is not None:
                try:
                    trace.setdefault("classifier", {}).setdefault("matches", {})["amazon"] = matched[:200]
                except Exception:
                    pass

    keyword_hit = bool(keywords_list and check_keyword_match(text_blob, keywords_list, trace=trace))
    if trace is not None:
        try:
            trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keyword"] = bool(keyword_hit)
        except Exception:
            pass
    if keyword_hit and cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID:
        results.append((cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID, "MONITORED_KEYWORD"))

    instore_selection: Optional[Tuple[int, str]] = None
    if is_instore_source and instore_required and instore_context:
        if SEASONAL_PATTERN.search(text_to_check or "") and cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, "INSTORE_SEASONAL")
        elif SNEAKERS_PATTERN.search(text_to_check or "") and cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, "INSTORE_SNEAKERS")
        elif CARDS_PATTERN.search(text_to_check or "") and cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, "INSTORE_CARDS")
        elif cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID and matches_instore_theatre(text_to_check or "", where_location):
            instore_selection = (cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, "INSTORE_THEATRE")
        elif (MAJOR_STORE_PATTERN.search(text_to_check or "") or store_category == "major") and cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID, "MAJOR_STORES")
        elif (DISCOUNTED_STORE_PATTERN.search(text_to_check or "") or store_category == "discounted") and cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, "DISCOUNTED_STORES")
        elif cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID:
            instore_selection = (cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "INSTORE_LEADS")

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
    if not any(tag.startswith("INSTORE") or tag in {"MAJOR_STORES", "DISCOUNTED_STORES"} for _, tag in results):
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

    # If Amazon detected, suppress all other local destinations
    if any(tag == "AMAZON" for _, tag in results):
        results = [(cid, tag) for cid, tag in results if tag == "AMAZON"]

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

