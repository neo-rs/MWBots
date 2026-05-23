from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import List, Optional, Tuple

_MODULE: Optional[ModuleType] = None


def _load_universal_module() -> ModuleType:
    global _MODULE
    if _MODULE is not None:
        return _MODULE
    here = Path(__file__).resolve().parent
    path = here / "universal_link_resolver_v2_ready" / "universal_link_resolver.py"
    spec = importlib.util.spec_from_file_location("mw_universal_link_resolver", str(path))
    if spec is None or spec.loader is None:
        raise ImportError("Failed to import universal_link_resolver (spec is None).")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[str(spec.name)] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    _MODULE = mod
    return mod


def resolve_universal_sync(
    url: str,
    *,
    timeout: int,
    max_depth: int,
) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Returns: (final_url, method_used, error)
    """
    mod = _load_universal_module()
    fn = getattr(mod, "resolve_universal", None)
    if not callable(fn):
        return None, "missing", "resolve_universal not found"
    try:
        r = fn(str(url or "").strip(), timeout=int(timeout or 0), max_depth=int(max_depth or 0))
        final = getattr(r, "final_url", None)
        method = str(getattr(r, "method_used", "") or "")
        err = getattr(r, "error", None)
        return (str(final) if final else None), method, (str(err) if err else None)
    except Exception as e:
        return None, "error", str(e)


_RESOLVE_CACHE: dict[str, Tuple[float, Optional[str], str, Optional[str]]] = {}
_MC_CLEAN_CACHE: dict[str, Tuple[float, Optional[str], str, Optional[str]]] = {}


def resolve_to_clean_merchant_url(
    url: str,
    *,
    timeout: int,
    max_depth: int,
) -> Tuple[Optional[str], str, Optional[str]]:
    """
    Resolve redirector/affiliate URLs to a clean merchant destination (no tracking query params).

    When the resolver stops on an intermediate host (e.g. mavely.app.link) but the chain already
    reached a merchant URL, pick the best final-destination step from the chain.
    """
    mod = _load_universal_module()
    fn = getattr(mod, "resolve_universal", None)
    if not callable(fn):
        return None, "missing", "resolve_universal not found"
    is_final = getattr(mod, "is_final_destination", None)
    score_fn = getattr(mod, "score_candidate", None)
    if not callable(is_final) or not callable(score_fn):
        return None, "missing", "resolver helpers unavailable"
    try:
        from utils import normalize_url as mw_normalize_url
    except Exception:
        mw_normalize_url = None

    try:
        result = fn(str(url or "").strip(), timeout=int(timeout or 0), max_depth=int(max_depth or 0))
    except Exception as e:
        return None, "error", str(e)

    candidates: List[str] = []
    final = getattr(result, "final_url", None)
    if final:
        candidates.append(str(final))
    chain = getattr(result, "chain", None) or []
    for step in chain:
        try:
            su = str(getattr(step, "url", "") or "").strip()
        except Exception:
            su = ""
        if su:
            candidates.append(su)

    best: Optional[str] = None
    best_score = -10_000
    seen_c: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen_c:
            continue
        seen_c.add(cand)
        if not is_final(cand):
            continue
        try:
            sc = int(score_fn(cand))
        except Exception:
            sc = 0
        if sc > best_score:
            best_score = sc
            best = cand

    method = str(getattr(result, "method_used", "") or "")
    err = getattr(result, "error", None)
    err_s = str(err) if err else None
    if not best:
        return None, method, err_s
    if mw_normalize_url is not None:
        try:
            return str(mw_normalize_url(best)).strip() or None, method, err_s
        except Exception:
            pass
    return str(best).strip() or None, method, err_s


def url_should_resolve_for_major_clearance(url: str) -> bool:
    """True for mavely/short-link redirectors — not plain merchant or Discord URLs."""
    u = str(url or "").strip()
    if not u.startswith(("http://", "https://")):
        return False
    try:
        from utils import is_discord_chat_or_media_url, _is_affiliate_domain
    except Exception:
        return False
    if is_discord_chat_or_media_url(u):
        return False
    if _is_affiliate_domain(u):
        return True
    try:
        mod = _load_universal_module()
        host_of = getattr(mod, "host_of", None)
        is_redirector = getattr(mod, "is_redirector_host", None)
        if callable(host_of) and callable(is_redirector):
            return bool(is_redirector(host_of(u)))
    except Exception:
        pass
    return False


def resolve_to_clean_merchant_url_cached(
    url: str,
    *,
    timeout: int,
    max_depth: int,
    ttl_seconds: int,
) -> Tuple[Optional[str], str, Optional[str]]:
    now = time.time()
    u = str(url or "").strip()
    if not u:
        return None, "empty", None
    try:
        ttl = max(30, int(ttl_seconds or 0))
    except Exception:
        ttl = 15 * 60

    try:
        exp, final, method, err = _MC_CLEAN_CACHE.get(u, (0.0, None, "", None))
    except Exception:
        exp, final, method, err = 0.0, None, "", None
    if final and (now - exp) < ttl:
        return final, str(method or "cache"), err

    final, method, err = resolve_to_clean_merchant_url(u, timeout=timeout, max_depth=max_depth)
    _MC_CLEAN_CACHE[u] = (now, final, method, err)
    if len(_MC_CLEAN_CACHE) > 2000:
        for _ in range(200):
            try:
                _MC_CLEAN_CACHE.pop(next(iter(_MC_CLEAN_CACHE)), None)
            except Exception:
                break
    return final, method, err


def resolve_universal_cached(
    url: str,
    *,
    timeout: int,
    max_depth: int,
    ttl_seconds: int,
) -> Tuple[Optional[str], str, Optional[str]]:
    now = time.time()
    u = str(url or "").strip()
    if not u:
        return None, "empty", None
    try:
        ttl = max(30, int(ttl_seconds or 0))
    except Exception:
        ttl = 15 * 60

    try:
        exp, final, method, err = _RESOLVE_CACHE.get(u, (0.0, None, "", None))
    except Exception:
        exp, final, method, err = 0.0, None, "", None
    if final and (now - exp) < ttl:
        return final, str(method or "cache"), err

    final, method, err = resolve_universal_sync(u, timeout=timeout, max_depth=max_depth)
    _RESOLVE_CACHE[u] = (now, final, method, err)
    if len(_RESOLVE_CACHE) > 2000:
        for _ in range(200):
            try:
                _RESOLVE_CACHE.pop(next(iter(_RESOLVE_CACHE)), None)
            except Exception:
                break
    return final, method, err
