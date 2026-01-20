from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_KEYWORDS_PATH = _CONFIG_DIR / "keywords.json"

_CACHE: List[str] = []
_CACHE_TS: float = 0.0
_CACHE_TTL_SECONDS: float = 60.0


def load_keywords() -> List[str]:
    global _CACHE, _CACHE_TS
    now = time.time()
    if _CACHE and (now - _CACHE_TS) < _CACHE_TTL_SECONDS:
        return list(_CACHE)
    kws: List[str] = []
    try:
        if _KEYWORDS_PATH.exists():
            with open(_KEYWORDS_PATH, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            if isinstance(data, list):
                kws = [str(x).strip() for x in data if str(x).strip()]
            elif isinstance(data, dict) and isinstance(data.get("keywords"), list):
                kws = [str(x).strip() for x in data["keywords"] if str(x).strip()]
    except Exception:
        kws = []
    _CACHE = kws
    _CACHE_TS = now
    return list(_CACHE)


def check_keyword_match(
    text_to_check: str, keywords_list: List[str] | None = None, *, trace: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Return True when any monitored keyword appears in text.

    If `trace` is provided, records matched keywords under:
      trace["classifier"]["matches"]["monitored_keywords"]
    """
    keywords_list = keywords_list or load_keywords()
    if not keywords_list:
        if trace is not None:
            try:
                trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keywords"] = []
            except Exception:
                pass
        return False
    matched = scan_keywords(text_to_check or "", keywords_list)
    if trace is not None:
        try:
            # Keep console/trace manageable: record first few matches only.
            trace.setdefault("classifier", {}).setdefault("matches", {})["monitored_keywords"] = matched[:10]
        except Exception:
            pass
    return bool(matched)


def scan_keywords(text_to_check: str, keywords_list: List[str] | None = None) -> List[str]:
    """Return list of matched keywords (case preserved from the keyword list)."""
    keywords_list = keywords_list or load_keywords()
    if not keywords_list:
        return []
    text_lower = (text_to_check or "").lower()
    matched: List[str] = []
    for kw in keywords_list:
        k = str(kw).strip()
        if not k:
            continue
        if k.lower() in text_lower:
            matched.append(k)
    return matched

