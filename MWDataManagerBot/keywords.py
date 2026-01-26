from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_KEYWORDS_PATH = _CONFIG_DIR / "keywords.json"

_CACHE: List[str] = []
_CACHE_TS: float = 0.0
_CACHE_TTL_SECONDS: float = 60.0
_FILE_LOCK = threading.RLock()


def invalidate_keywords_cache() -> None:
    global _CACHE, _CACHE_TS
    _CACHE = []
    _CACHE_TS = 0.0


def load_keywords(*, force: bool = False) -> List[str]:
    global _CACHE, _CACHE_TS
    now = time.time()
    if (not force) and _CACHE and (now - _CACHE_TS) < _CACHE_TTL_SECONDS:
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


def save_keywords(keywords_list: List[str]) -> bool:
    """Persist keywords to config/keywords.json. Returns True if saved."""
    try:
        with _FILE_LOCK:
            _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            cleaned = [str(x).strip() for x in (keywords_list or []) if str(x).strip()]
            # De-dupe case-insensitively while preserving first occurrence
            seen = set()
            out: List[str] = []
            for kw in cleaned:
                k = kw.lower()
                if k in seen:
                    continue
                seen.add(k)
                out.append(kw)
            tmp = Path(str(_KEYWORDS_PATH) + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, ensure_ascii=False)
            try:
                os.replace(str(tmp), str(_KEYWORDS_PATH))
            except Exception:
                with open(_KEYWORDS_PATH, "w", encoding="utf-8") as f:
                    json.dump(out, f, indent=2, ensure_ascii=False)
        invalidate_keywords_cache()
        return True
    except Exception:
        return False


def add_keyword(keyword: str) -> Tuple[bool, str]:
    kw = str(keyword or "").strip()
    if not kw:
        return False, "empty_keyword"
    current = load_keywords(force=True)
    if any(k.lower() == kw.lower() for k in current):
        return False, "already_exists"
    current.append(kw)
    if not save_keywords(current):
        return False, "save_failed"
    return True, "added"


def remove_keyword(keyword: str) -> Tuple[bool, str]:
    kw = str(keyword or "").strip()
    if not kw:
        return False, "empty_keyword"
    current = load_keywords(force=True)
    kept = [k for k in current if k.lower() != kw.lower()]
    if len(kept) == len(current):
        return False, "not_found"
    if not save_keywords(kept):
        return False, "save_failed"
    return True, "removed"


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

