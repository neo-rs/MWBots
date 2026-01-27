from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

from logging_utils import log_info, log_warn

import settings_store as cfg

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"

# Secrets: this file contains webhook URLs and is intentionally ignored by git.
# Format: { "<destination_channel_id>": "<webhook_url>" }
_WEBHOOK_MAP_PATH = _CONFIG_DIR / "channel_map.json"

_LOCK = threading.RLock()
_CACHE: Dict[int, str] = {}


def _load_webhook_map() -> Dict[int, str]:
    try:
        with _LOCK:
            if not _WEBHOOK_MAP_PATH.exists():
                return {}
            raw = json.loads(_WEBHOOK_MAP_PATH.read_text(encoding="utf-8") or "{}")
            if not isinstance(raw, dict):
                return {}
            out: Dict[int, str] = {}
            for k, v in raw.items():
                try:
                    cid = int(str(k).strip())
                except Exception:
                    continue
                url = str(v or "").strip()
                if cid > 0 and url:
                    out[cid] = url
            return out
    except Exception:
        return {}


def _save_webhook_map(m: Dict[int, str]) -> None:
    try:
        with _LOCK:
            _WEBHOOK_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
            raw: Dict[str, str] = {str(int(k)): str(v) for k, v in (m or {}).items() if int(k) > 0 and str(v or "").strip()}
            tmp = Path(str(_WEBHOOK_MAP_PATH) + ".tmp")
            tmp.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(_WEBHOOK_MAP_PATH)
    except Exception:
        pass


def _is_webhook_url(url: str) -> bool:
    u = str(url or "").strip()
    return "/webhooks/" in u and len(u) > 20


async def _get_or_create_webhook_url(*, dest_channel, reason: str) -> str:
    """
    Return a webhook URL for a destination channel.
    Creates one if missing. Requires "Manage Webhooks" permission.
    """
    try:
        import discord  # type: ignore
    except Exception:
        return ""

    try:
        cid = int(getattr(dest_channel, "id", 0) or 0)
    except Exception:
        cid = 0
    if cid <= 0:
        return ""

    # Config gate (default False; must be enabled).
    enabled = bool(getattr(cfg, "USE_WEBHOOKS_FOR_FORWARDING", False))
    if not enabled:
        return ""

    # In-memory cache
    try:
        cached = _CACHE.get(cid) or ""
    except Exception:
        cached = ""
    if _is_webhook_url(cached):
        return cached

    # File cache
    m = _load_webhook_map()
    url = str(m.get(cid) or "").strip()
    if _is_webhook_url(url):
        try:
            _CACHE[cid] = url
        except Exception:
            pass
        return url

    # Create new webhook
    try:
        wh = await dest_channel.create_webhook(name="MWDataManagerBot", reason=str(reason or "MWDataManagerBot webhook sender"))
        url2 = str(getattr(wh, "url", "") or "").strip()
    except Exception as e:
        try:
            log_warn(f"[WEBHOOK] create_webhook failed channel_id={cid} ({type(e).__name__}: {e})")
        except Exception:
            pass
        return ""

    if not _is_webhook_url(url2):
        return ""

    m[cid] = url2
    _save_webhook_map(m)
    try:
        _CACHE[cid] = url2
    except Exception:
        pass
    try:
        log_info(f"[WEBHOOK] created for channel_id={cid}")
    except Exception:
        pass
    return url2


async def send_via_webhook_or_bot(
    *,
    dest_channel,
    content: str,
    embeds,
    username: str = "",
    avatar_url: str = "",
    reason: str = "",
) -> None:
    """
    Send message to a destination channel:
    - Prefer webhook (if enabled and creatable)
    - Fallback to normal bot send
    """
    try:
        import discord  # type: ignore
    except Exception:
        # If discord isn't available, we can't send.
        return

    allowed_mentions = None
    try:
        allowed_mentions = discord.AllowedMentions.none()
    except Exception:
        allowed_mentions = None

    # Normalize username/avatar for webhook constraints
    uname = str(username or "").strip()
    if uname:
        uname = uname[:80]
    av = str(avatar_url or "").strip()

    url = await _get_or_create_webhook_url(dest_channel=dest_channel, reason=reason)
    if url:
        # Webhook execute does not require bot token, but discord.py needs an aiohttp session.
        import aiohttp  # type: ignore

        async with aiohttp.ClientSession() as session:
            wh = discord.Webhook.from_url(url, session=session)  # type: ignore[arg-type]
            kwargs: Dict[str, Any] = {"content": str(content or ""), "embeds": list(embeds or [])[:10], "allowed_mentions": allowed_mentions}
            if uname:
                kwargs["username"] = uname
            if av:
                kwargs["avatar_url"] = av
            # wait=False keeps it fast (no message id needed).
            await wh.send(wait=False, **kwargs)
            return

    # Fallback: normal bot send
    await dest_channel.send(content=str(content or ""), embeds=list(embeds or [])[:10], allowed_mentions=allowed_mentions)

