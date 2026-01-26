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
_KEYWORD_CHANNELS_PATH = _CONFIG_DIR / "keyword_channels.json"

_CACHE: List[str] = []
_CACHE_TS: float = 0.0
_CACHE_TTL_SECONDS: float = 60.0
_FILE_LOCK = threading.RLock()

_CHANNEL_MAP_CACHE: Dict[str, int] = {}
_CHANNEL_MAP_CACHE_TS: float = 0.0
_CHANNEL_MAP_CACHE_TTL_SECONDS: float = 60.0


def invalidate_keywords_cache() -> None:
    global _CACHE, _CACHE_TS
    _CACHE = []
    _CACHE_TS = 0.0


def invalidate_keyword_channels_cache() -> None:
    global _CHANNEL_MAP_CACHE, _CHANNEL_MAP_CACHE_TS
    _CHANNEL_MAP_CACHE = {}
    _CHANNEL_MAP_CACHE_TS = 0.0


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


def load_keyword_channel_overrides(*, force: bool = False) -> Dict[str, int]:
    """
    Load keyword->channel_id overrides from config/keyword_channels.json.
    Keys are normalized to lower-case for matching.
    """
    global _CHANNEL_MAP_CACHE, _CHANNEL_MAP_CACHE_TS
    now = time.time()
    if (not force) and _CHANNEL_MAP_CACHE and (now - _CHANNEL_MAP_CACHE_TS) < _CHANNEL_MAP_CACHE_TTL_SECONDS:
        return dict(_CHANNEL_MAP_CACHE)
    mp: Dict[str, int] = {}
    try:
        if _KEYWORD_CHANNELS_PATH.exists():
            with open(_KEYWORD_CHANNELS_PATH, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            if isinstance(data, dict):
                raw = data.get("overrides")
                if isinstance(raw, dict):
                    for k, v in raw.items():
                        kk = str(k or "").strip().lower()
                        if not kk:
                            continue
                        try:
                            cid = int(v)
                        except Exception:
                            continue
                        if cid > 0:
                            mp[kk] = cid
    except Exception:
        mp = {}
    _CHANNEL_MAP_CACHE = dict(mp)
    _CHANNEL_MAP_CACHE_TS = now
    return dict(_CHANNEL_MAP_CACHE)


def save_keyword_channel_overrides(overrides: Dict[str, int]) -> bool:
    """Persist overrides to config/keyword_channels.json. Returns True if saved."""
    try:
        with _FILE_LOCK:
            _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            cleaned: Dict[str, int] = {}
            for k, v in (overrides or {}).items():
                kk = str(k or "").strip().lower()
                if not kk:
                    continue
                try:
                    cid = int(v)
                except Exception:
                    continue
                if cid > 0:
                    cleaned[kk] = cid
            payload = {"overrides": cleaned, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
            tmp = Path(str(_KEYWORD_CHANNELS_PATH) + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            try:
                os.replace(str(tmp), str(_KEYWORD_CHANNELS_PATH))
            except Exception:
                with open(_KEYWORD_CHANNELS_PATH, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False)
        invalidate_keyword_channels_cache()
        return True
    except Exception:
        return False


def set_keyword_channel_override(keyword: str, channel_id: int) -> Tuple[bool, str]:
    kw = str(keyword or "").strip()
    if not kw:
        return False, "empty_keyword"
    try:
        cid = int(channel_id)
    except Exception:
        cid = 0
    if cid <= 0:
        return False, "invalid_channel_id"
    mp = load_keyword_channel_overrides(force=True)
    mp[kw.lower()] = cid
    if not save_keyword_channel_overrides(mp):
        return False, "save_failed"
    return True, "saved"


def remove_keyword_channel_override(keyword: str) -> Tuple[bool, str]:
    kw = str(keyword or "").strip()
    if not kw:
        return False, "empty_keyword"
    mp = load_keyword_channel_overrides(force=True)
    if kw.lower() not in mp:
        return False, "not_found"
    mp.pop(kw.lower(), None)
    if not save_keyword_channel_overrides(mp):
        return False, "save_failed"
    return True, "removed"


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
    # Best-effort: also remove any per-keyword channel override.
    try:
        remove_keyword_channel_override(kw)
    except Exception:
        pass
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

