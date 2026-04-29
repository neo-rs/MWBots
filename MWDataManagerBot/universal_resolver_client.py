from __future__ import annotations

import importlib.util
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Optional, Tuple

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
    # cap growth
    if len(_RESOLVE_CACHE) > 2000:
        # drop ~10% oldest by inserted order (dict preserves insertion in py3.7+)
        for _ in range(200):
            try:
                _RESOLVE_CACHE.pop(next(iter(_RESOLVE_CACHE)), None)
            except Exception:
                break
    return final, method, err
