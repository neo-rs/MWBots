from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set, Tuple

import settings_store as cfg
from logging_utils import log_debug, log_smartfilter
from patterns import AMAZON_LINK_PATTERN, PRICE_ERROR_PATTERN, PROFITABLE_FLIP_PATTERN
from utils import collect_embed_strings, has_product_and_marketplace_links, normalize_message


_GF_DYOR_PATTERN = re.compile(r"\bdyor\b", re.IGNORECASE)

# Hard gating: strict intent patterns (ported intent; conservative)
STRICT_INSTORE_ONLY_PATTERN = re.compile(r"\b(in\s*store\s*only|instore\s*only)\b", re.IGNORECASE)
STRICT_ONLINE_ONLY_PATTERN = re.compile(r"\b(online\s*only)\b", re.IGNORECASE)

# Marketplace / resell indicators (regex *strings* so we can log which matched)
_MARKETPLACE_KEYWORDS: List[str] = [
    r"\bstockx\b",
    r"\bgoat\b",
    r"\bebay\b",
    r"\bgrailed\b",
    r"\bposhmark\b",
    r"\bmercari\b",
    r"\bofferup\b",
    r"\bfacebook\s+marketplace\b",
    r"\bfb\s+marketplace\b",
]
_RESELL_INDICATOR_KEYWORDS: List[str] = [
    r"\b(comps?|sold\s+for|sell\s+for)\b",
    r"\b(resell|flip|flips?\s+for)\b",
    r"\b(after\s+fees?|fees?\s+included)\b",
    r"\b(net\s+profit|profit)\b",
    r"\broi\b",
]

_EXPLICIT_ROI_PATTERN = re.compile(r"\b(\d{3,}%|\d+x)\b", re.IGNORECASE)

# Marketplace domains used for link classification (best-effort).
_RESELL_DOMAINS: Set[str] = {
    "stockx.com",
    "www.stockx.com",
    "goat.com",
    "www.goat.com",
    "ebay.com",
    "www.ebay.com",
    "grailed.com",
    "www.grailed.com",
    "poshmark.com",
    "www.poshmark.com",
    "mercari.com",
    "www.mercari.com",
    "offerup.com",
    "www.offerup.com",
    "facebook.com",
    "www.facebook.com",
}


def _match_patterns(patterns: List[str], *texts: str, max_hits: int = 3) -> List[str]:
    """Return up to max_hits pattern strings that matched any of the given texts."""
    hits: List[str] = []
    try:
        for pat in patterns or []:
            if len(hits) >= max_hits:
                break
            for t in texts:
                if not t:
                    continue
                try:
                    if re.search(pat, t, re.IGNORECASE):
                        hits.append(pat)
                        break
                except re.error:
                    continue
    except Exception:
        return []
    return hits


def _match_compiled(compiled_pattern: Optional[re.Pattern[str]], *, text: str, embed: str) -> Optional[str]:
    """Return which text source matched a compiled regex: 'text' | 'embed' | None."""
    if not compiled_pattern:
        return None
    try:
        if text and compiled_pattern.search(text):
            return "text"
    except Exception:
        pass
    try:
        if embed and compiled_pattern.search(embed):
            return "embed"
    except Exception:
        pass
    return None


def _extract_price_data(text: str, *, embed_text: str = "") -> Dict[str, Any]:
    """
    Best-effort retail/resell extraction for flip ROI evaluation.
    Returns: {"has_price": bool, "retail_price": float|None, "resell_price": float|None}
    """
    blob = (text or "") + "\n" + (embed_text or "")
    blob = blob.replace(",", "")
    retail = None
    resell = None
    # Common labeled lines (most reliable)
    try:
        m = re.search(r"(?im)^\s*(retail|msrp)\s*[:\-]\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", blob)
        if m:
            retail = float(m.group(2))
    except Exception:
        retail = None
    try:
        m = re.search(r"(?im)^\s*(resell|sell)\s*[:\-]\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", blob)
        if m:
            resell = float(m.group(2))
    except Exception:
        resell = None

    # Fallback: inline tags (weaker)
    if retail is None:
        try:
            m = re.search(r"\bretail\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", blob, re.IGNORECASE)
            if m:
                retail = float(m.group(1))
        except Exception:
            retail = None
    if resell is None:
        try:
            m = re.search(r"\bresell\s*\$?\s*([0-9]+(?:\.[0-9]{1,2})?)\b", blob, re.IGNORECASE)
            if m:
                resell = float(m.group(1))
        except Exception:
            resell = None

    return {"has_price": bool(retail is not None and resell is not None), "retail_price": retail, "resell_price": resell}


def calculate_roi_and_profit(retail_price: float, resell_price: float, *, fee_rate: float = 0.15) -> Dict[str, Any]:
    """
    ROI math used by the flip smartfilters (ported shape):
      fees = resell_price * fee_rate
      net_profit = resell_price - fees - retail_price
      roi_percent = (net_profit / retail_price) * 100
    """
    try:
        retail = float(retail_price)
        resell = float(resell_price)
        if retail <= 0 or resell <= 0:
            return {"roi_percent": None, "net_profit": None, "fees": None}
        fees = resell * float(fee_rate)
        net_profit = resell - fees - retail
        roi_percent = (net_profit / retail) * 100.0

        meets_low_threshold = (1.0 <= retail < 20.0) and (roi_percent >= 200.0)
        meets_mid_threshold = (20.0 <= retail < 100.0) and (roi_percent >= 100.0)
        meets_high_threshold = (retail >= 100.0) and (net_profit >= 40.0)

        return {
            "roi_percent": round(roi_percent, 2),
            "net_profit": round(net_profit, 2),
            "fees": round(fees, 2),
            "fee_rate": float(fee_rate),
            "meets_low_threshold": bool(meets_low_threshold),
            "meets_mid_threshold": bool(meets_mid_threshold),
            "meets_high_threshold": bool(meets_high_threshold),
        }
    except Exception:
        return {"roi_percent": None, "net_profit": None, "fees": None}


def _roi_meets_thresholds(roi_result: Dict[str, Any]) -> bool:
    try:
        return bool(
            roi_result.get("meets_low_threshold")
            or roi_result.get("meets_mid_threshold")
            or roi_result.get("meets_high_threshold")
        )
    except Exception:
        return False


def detect_global_triggers(
    text_to_check: str,
    *,
    source_channel_id: int = 0,
    link_tracking_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> List[Tuple[int, str]]:
    """
    Standalone global triggers (ported behavior; simplified but compatible shape).

    Returns a list of (destination_channel_id, tag) pairs.
    """
    results: List[Tuple[int, str]] = []
    if not text_to_check:
        return results

    normalized_text = normalize_message(text_to_check)
    embed_text = " ".join(collect_embed_strings(embeds or []))
    embed_normalized = normalize_message(embed_text)

    source_is_online = bool(source_channel_id and (int(source_channel_id) in cfg.SMART_SOURCE_CHANNELS_ONLINE))
    source_is_instore = bool(source_channel_id and (int(source_channel_id) in cfg.SMART_SOURCE_CHANNELS_INSTORE))

    sf_ctx: Dict[str, Any] = {
        "source_channel_id": int(source_channel_id or 0),
        "source_is_online": bool(source_is_online),
        "source_is_instore": bool(source_is_instore),
    }
    # PRICE_ERROR (online-only intent; here we just require not instore channel list)
    if cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID and (source_channel_id not in cfg.SMART_SOURCE_CHANNELS_INSTORE):
        if PRICE_ERROR_PATTERN.search(normalized_text):
            results.append((cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"))

    # PROFITABLE_FLIP / LUNCHMONEY_FLIP (online only; explainable)
    if (cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID or cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID) and source_is_online:
        # Amazon gating: never send Amazon content to flips profitable/lunchmoney.
        amazon_match_source = _match_compiled(AMAZON_LINK_PATTERN, text=text_to_check, embed=embed_text)
        amazon_hit = bool(amazon_match_source)
        amazon_match = None
        try:
            if amazon_match_source == "text":
                m = AMAZON_LINK_PATTERN.search(text_to_check or "")
                amazon_match = m.group(0) if m else None
            elif amazon_match_source == "embed":
                m = AMAZON_LINK_PATTERN.search(embed_text or "")
                amazon_match = m.group(0) if m else None
        except Exception:
            amazon_match = None

        # Strict intent matches (block mismatched-source flips)
        strict_instore_only_match_source = _match_compiled(STRICT_INSTORE_ONLY_PATTERN, text=text_to_check, embed=embed_text)
        strict_online_only_match_source = _match_compiled(STRICT_ONLINE_ONLY_PATTERN, text=text_to_check, embed=embed_text)
        sf_ctx.update(
            {
                "amazon_hit": bool(amazon_hit),
                "amazon_match_source": amazon_match_source,
                "amazon_match": amazon_match,
                "strict_instore_only": bool(strict_instore_only_match_source),
                "strict_instore_only_match_source": strict_instore_only_match_source,
                "strict_online_only": bool(strict_online_only_match_source),
                "strict_online_only_match_source": strict_online_only_match_source,
            }
        )

        # Basic signals
        dyor_block = bool(_GF_DYOR_PATTERN.search(normalized_text) or _GF_DYOR_PATTERN.search(embed_normalized or ""))
        marketplace_matches = _match_patterns(_MARKETPLACE_KEYWORDS, normalized_text, embed_normalized or "")
        marketplace_hit = bool(marketplace_matches)
        explicit_roi_keywords = _EXPLICIT_ROI_PATTERN.search(normalized_text) or _EXPLICIT_ROI_PATTERN.search(embed_normalized or "")
        explicit_roi_match = explicit_roi_keywords.group(0) if explicit_roi_keywords else None

        price_data = _extract_price_data(text_to_check, embed_text=embed_text)
        has_prices = bool(price_data.get("has_price") and price_data.get("retail_price") and price_data.get("resell_price"))
        retail = price_data.get("retail_price")
        resell = price_data.get("resell_price")

        resell_indicator_matches = _match_patterns(_RESELL_INDICATOR_KEYWORDS, normalized_text, embed_normalized or "")
        resell_indicator = bool(resell_indicator_matches) or bool(resell)

        has_marketplace_link, has_product_link = has_product_and_marketplace_links(
            text_to_check,
            attachments=attachments,
            embeds=embeds,
            resale_domains=_RESELL_DOMAINS,
        )

        sf_ctx.update(
            {
                "dyor_block": bool(dyor_block),
                "marketplace_hit": bool(marketplace_hit),
                "marketplace_matches": marketplace_matches,
                "resell_indicator_matches": resell_indicator_matches,
                "has_prices": bool(has_prices),
                "has_marketplace_link": bool(has_marketplace_link),
                "has_product_link": bool(has_product_link),
            }
        )

        # If Amazon content is present, do not evaluate or route to flip channels.
        # Instead, optionally route to a dedicated Amazon leads channel if configured.
        if amazon_hit:
            if (
                int(getattr(cfg, "SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID", 0) or 0) > 0
                and bool(has_marketplace_link)
                and bool(has_product_link)
                and not bool(dyor_block)
            ):
                results.append((int(cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID), "AMAZON_PROFITABLE_LEAD"))
                log_smartfilter(
                    "AMAZON_PROFITABLE_LEAD",
                    "TRIGGER",
                    {**sf_ctx, "reason": "amazon_content_blocked_from_flips"},
                )
            else:
                # Log as a skip so you can see why flips didn't fire.
                log_smartfilter(
                    "PROFITABLE_FLIP",
                    "SKIP",
                    {**sf_ctx, "reason": "amazon_content_excluded"},
                )
                log_smartfilter(
                    "LUNCHMONEY_FLIP",
                    "SKIP",
                    {**sf_ctx, "reason": "amazon_content_excluded"},
                )
            # Always skip flip evaluation for Amazon content.
            pass

        # Hard gates / blocks (log the first concrete block if message is "flip-shaped" enough)
        flip_shape = bool(marketplace_hit or resell_indicator or has_prices or PROFITABLE_FLIP_PATTERN.search(normalized_text))
        if amazon_hit:
            flip_shape = False

        if flip_shape and dyor_block:
            log_smartfilter("PROFITABLE_FLIP", "BLOCK", {**sf_ctx, "reason": "dyor_block"})
            log_smartfilter("LUNCHMONEY_FLIP", "BLOCK", {**sf_ctx, "reason": "dyor_block"})
        elif flip_shape and sf_ctx.get("strict_instore_only"):
            # If strict "instore only" shows up in an online channel, only allow if a product link exists (order-online/pickup style).
            if not has_product_link:
                log_smartfilter("PROFITABLE_FLIP", "BLOCK", {**sf_ctx, "reason": "strict_instore_only_no_product_link"})
                log_smartfilter("LUNCHMONEY_FLIP", "BLOCK", {**sf_ctx, "reason": "strict_instore_only_no_product_link"})
                flip_shape = False  # stop further evaluation

        # Path A: labeled prices + required link shape
        triggered_profitable = False
        if flip_shape and has_prices and has_marketplace_link and has_product_link and not dyor_block:
            roi_result = calculate_roi_and_profit(float(retail), float(resell))
            sf_ctx.update(
                {
                    k: roi_result.get(k)
                    for k in (
                        "roi_percent",
                        "net_profit",
                        "fees",
                        "fee_rate",
                        "meets_low_threshold",
                        "meets_mid_threshold",
                        "meets_high_threshold",
                    )
                }
            )
            if _roi_meets_thresholds(roi_result) and cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID:
                results.append((cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "PROFITABLE_FLIP"))
                triggered_profitable = True
                log_smartfilter("PROFITABLE_FLIP", "TRIGGER", {**sf_ctx, "stage": "tags_roi_path", "reason": "roi_pass"})
            elif cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID:
                results.append((cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "LUNCHMONEY_FLIP"))
                log_smartfilter(
                    "LUNCHMONEY_FLIP",
                    "FALLBACK",
                    {**sf_ctx, "stage": "tags_roi_path", "reason": "below_profit_threshold"},
                )

        # Path B: marketplace keywords + resell indicators + links
        if flip_shape and (not triggered_profitable) and marketplace_hit and resell_indicator and has_marketplace_link and has_product_link and not dyor_block:
            if explicit_roi_match and cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID:
                results.append((cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "PROFITABLE_FLIP"))
                triggered_profitable = True
                log_smartfilter(
                    "PROFITABLE_FLIP",
                    "TRIGGER",
                    {**sf_ctx, "stage": "marketplace_path", "reason": "explicit_roi_keywords", "explicit_roi_match": explicit_roi_match},
                )
            elif PROFITABLE_FLIP_PATTERN.search(normalized_text):
                if has_prices:
                    roi_result = calculate_roi_and_profit(float(retail), float(resell))
                    sf_ctx.update(
                        {
                            k: roi_result.get(k)
                            for k in (
                                "roi_percent",
                                "net_profit",
                                "fees",
                                "fee_rate",
                                "meets_low_threshold",
                                "meets_mid_threshold",
                                "meets_high_threshold",
                            )
                        }
                    )
                    if _roi_meets_thresholds(roi_result) and cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID:
                        results.append((cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "PROFITABLE_FLIP"))
                        triggered_profitable = True
                        log_smartfilter(
                            "PROFITABLE_FLIP",
                            "TRIGGER",
                            {**sf_ctx, "stage": "marketplace_path", "reason": "keyword_pattern_roi_pass"},
                        )
                    elif cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID:
                        results.append((cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "LUNCHMONEY_FLIP"))
                        log_smartfilter(
                            "LUNCHMONEY_FLIP",
                            "FALLBACK",
                            {**sf_ctx, "stage": "marketplace_path", "reason": "criteria_met_not_profitable"},
                        )
                else:
                    # Backward-compat: if flip-shape + marketplace/product links exist, allow profitable trigger without prices.
                    if cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID:
                        results.append((cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "PROFITABLE_FLIP"))
                        triggered_profitable = True
                        log_smartfilter(
                            "PROFITABLE_FLIP",
                            "TRIGGER",
                            {**sf_ctx, "stage": "marketplace_path", "reason": "keyword_pattern_no_prices_backcompat"},
                        )
            else:
                # Not profitable, but meets "flip shape" with links/signals: Lunchmoney bucket
                if cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID:
                    results.append((cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "LUNCHMONEY_FLIP"))
                    log_smartfilter(
                        "LUNCHMONEY_FLIP",
                        "FALLBACK",
                        {**sf_ctx, "stage": "marketplace_path", "reason": "criteria_met_not_profitable"},
                    )

    # Deduplicate while preserving order
    seen: Set[Tuple[int, str]] = set()
    out: List[Tuple[int, str]] = []
    for cid, tag in results:
        key = (int(cid or 0), str(tag or ""))
        if key in seen:
            continue
        seen.add(key)
        out.append((cid, tag))

    if cfg.VERBOSE and out:
        try:
            tags = ", ".join([t for _, t in out])
            log_debug(f"[GLOBAL] triggers={tags} (source_channel_id={source_channel_id})")
        except Exception:
            pass
    return out

