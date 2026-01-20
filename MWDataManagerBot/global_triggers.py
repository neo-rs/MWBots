from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

import settings_store as cfg
from logging_utils import log_debug
from patterns import PRICE_ERROR_PATTERN, PROFITABLE_FLIP_PATTERN
from utils import extract_urls_from_text, normalize_message


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

    urls = extract_urls_from_text(text_to_check)
    link_channel_union: Set[int] = set()
    if link_tracking_cache and urls:
        for url in urls:
            cache_entry = link_tracking_cache.get(url)
            if not cache_entry:
                continue
            seen_channels = cache_entry.get("channel_ids", set())
            try:
                for ch in seen_channels:
                    if ch:
                        link_channel_union.add(int(ch))
            except Exception:
                pass
    # PRICE_ERROR (online-only intent; here we just require not instore channel list)
    if cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID and (source_channel_id not in cfg.SMART_SOURCE_CHANNELS_INSTORE):
        if PRICE_ERROR_PATTERN.search(normalized_text):
            results.append((cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "PRICE_ERROR"))

    # PROFITABLE_FLIP / LUNCHMONEY_FLIP (simplified heuristic)
    if cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID and (source_channel_id in cfg.SMART_SOURCE_CHANNELS_ONLINE):
        if PROFITABLE_FLIP_PATTERN.search(normalized_text):
            results.append((cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "PROFITABLE_FLIP"))
    if cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID and (source_channel_id in cfg.SMART_SOURCE_CHANNELS_ONLINE):
        if "lunch" in normalized_text or "lunchmoney" in normalized_text:
            results.append((cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "LUNCHMONEY_FLIP"))

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

