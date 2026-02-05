"""Minimal Discord API client using a user (self) token for MWDiscumBot browse.

Used by discum_command_bot for /discum browse: list guilds, list channels, fetch message previews.
No dependency on MWDataManagerBot; uses aiohttp for async GET.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple

_DISCORD_API_BASE = "https://discord.com/api/v10"


def _jump_url(guild_id: int, channel_id: int) -> str:
    if guild_id <= 0 or channel_id <= 0:
        return ""
    return f"https://discord.com/channels/{guild_id}/{channel_id}"


async def _api_get(
    url: str,
    user_token: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
) -> Tuple[int, Any]:
    """GET JSON with user token. Returns (status_code, json_or_none)."""
    token = str(user_token or "").strip()
    if not url or not token:
        return 0, None
    try:
        import aiohttp
    except ImportError:
        return 0, None
    headers = {
        "Authorization": token,
        "User-Agent": "MWDiscumBot/1.0",
        "Accept": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=20)
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url, params=params) as resp:
                    status = getattr(resp, "status", 0) or 0
                    if status == 429:
                        try:
                            data = await resp.json()
                            retry_after = float(data.get("retry_after") or 1.0)
                        except Exception:
                            retry_after = 1.0
                        await asyncio.sleep(max(0.5, min(10.0, retry_after)))
                        continue
                    if 200 <= status < 300:
                        try:
                            return status, await resp.json()
                        except Exception:
                            return status, None
                    if 400 <= status < 500:
                        return status, None
                    await asyncio.sleep(min(3.0, 0.5 * (attempt + 1)))
        except Exception:
            await asyncio.sleep(min(3.0, 0.5 * (attempt + 1)))
    return 0, None


async def list_user_guilds(*, user_token: str) -> Dict[str, Any]:
    """List guilds the user token can see. Returns {ok, guilds, reason?, http_status?}."""
    token = str(user_token or "").strip()
    if not token:
        return {"ok": False, "reason": "missing_user_token"}
    url = f"{_DISCORD_API_BASE}/users/@me/guilds"
    status, data = await _api_get(url, user_token=token, params={"with_counts": "true"})
    if status != 200 or not isinstance(data, list):
        return {"ok": False, "reason": "failed_to_list_user_guilds", "http_status": int(status or 0)}
    out: List[Dict[str, Any]] = []
    for g in data:
        if not isinstance(g, dict):
            continue
        try:
            gid = int(g.get("id") or 0)
        except Exception:
            gid = 0
        if gid <= 0:
            continue
        name = str(g.get("name") or "").strip() or f"guild_{gid}"
        out.append({"id": gid, "name": name, "owner": bool(g.get("owner")), "icon": str(g.get("icon") or "")})
    out.sort(key=lambda x: (str(x.get("name") or "").lower(), int(x.get("id") or 0)))
    return {"ok": True, "http_status": status, "guilds": out}


async def list_source_guild_channels(
    *, source_guild_id: int, user_token: str
) -> Dict[str, Any]:
    """List categories and messageable channels in a guild. Returns {ok, categories, channels, ...}."""
    sgid = int(source_guild_id or 0)
    token = str(user_token or "").strip()
    if sgid <= 0:
        return {"ok": False, "reason": "invalid_source_guild_id"}
    if not token:
        return {"ok": False, "reason": "missing_user_token"}
    url = f"{_DISCORD_API_BASE}/guilds/{sgid}/channels"
    status, channels = await _api_get(url, user_token=token)
    if status != 200 or not isinstance(channels, list):
        return {"ok": False, "reason": "failed_to_list_channels", "http_status": int(status or 0)}
    cats: List[Dict[str, Any]] = []
    chan: List[Dict[str, Any]] = []
    for c in channels:
        if not isinstance(c, dict):
            continue
        try:
            raw_type = c.get("type", None)
            t = int(raw_type) if raw_type is not None else -1
        except Exception:
            t = -1
        if t == 4:
            try:
                cid = int(c.get("id") or 0)
            except Exception:
                cid = 0
            if cid > 0:
                cats.append({
                    "id": cid,
                    "name": str(c.get("name") or ""),
                    "position": int(c.get("position") or 0) if str(c.get("position") or "").strip() else 0,
                    "url": _jump_url(sgid, cid),
                })
        elif t in (0, 5):
            try:
                chid = int(c.get("id") or 0)
            except Exception:
                chid = 0
            if chid <= 0:
                continue
            parent_id = 0
            try:
                pid = c.get("parent_id")
                if pid is not None and str(pid).strip():
                    parent_id = int(pid)
            except Exception:
                pass
            chan.append({
                "id": chid,
                "name": str(c.get("name") or f"channel_{chid}"),
                "parent_id": parent_id,
                "type": t,
                "position": int(c.get("position") or 0) if str(c.get("position") or "").strip() else 0,
                "url": _jump_url(sgid, chid),
            })
    cats.sort(key=lambda x: (int(x.get("position") or 0), int(x.get("id") or 0)))
    chan.sort(key=lambda x: (int(x.get("parent_id") or 0), int(x.get("position") or 0), int(x.get("id") or 0)))
    return {
        "ok": True,
        "http_status": int(status or 0),
        "source_guild_id": sgid,
        "categories": cats,
        "channels": chan,
        "total": len(channels),
    }


async def fetch_channel_messages_page(
    *,
    source_channel_id: int,
    user_token: str,
    limit: int = 1,
    after: Optional[str] = None,
) -> Tuple[bool, List[Dict[str, Any]], str]:
    """Fetch a page of messages. Returns (ok, messages, reason)."""
    cid = int(source_channel_id or 0)
    if cid <= 0:
        return False, [], "invalid_channel_id"
    lim = max(1, min(int(limit or 1), 50))
    params: Dict[str, Any] = {"limit": str(lim)}
    if after:
        params["after"] = str(after)
    url = f"{_DISCORD_API_BASE}/channels/{cid}/messages"
    status, data = await _api_get(url, user_token=user_token, params=params)
    if status == 200 and isinstance(data, list):
        return True, [m for m in data if isinstance(m, dict)], ""
    if status in (401, 403):
        return False, [], "forbidden_or_unauthorized"
    if status == 404:
        return False, [], "not_found"
    return False, [], f"http_{status or 0}"
