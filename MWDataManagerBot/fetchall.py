from __future__ import annotations

import asyncio
import json
import os
import time
import threading
import unicodedata
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from logging_utils import log_fetchall, log_warn
import settings_store as cfg
from utils import append_image_attachments_as_embeds, chunk_text, format_embeds_for_forwarding, is_image_attachment

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_FETCHALL_PATH = _CONFIG_DIR / "fetchall_mappings.json"

_FILE_LOCK = threading.RLock()
_DISCORD_API_BASE = "https://discord.com/api/v9"
_CATEGORY_CHANNEL_LIMIT = 50


def _is_substantive_fetched_message(*, content: str, embeds: List[Dict[str, Any]], attachments: List[Dict[str, Any]], min_chars: int) -> bool:
    """
    Fetchsync filter:
    - Drop empty messages and "ping-only / attention-only" messages.
    - Keep messages with embeds/attachments or links even if short.
    """
    try:
        content = str(content or "")
    except Exception:
        content = ""
    try:
        embeds = [e for e in (embeds or []) if isinstance(e, dict)]
    except Exception:
        embeds = []
    try:
        attachments = [a for a in (attachments or []) if isinstance(a, dict)]
    except Exception:
        attachments = []

    # If it has media/embeds, keep it.
    if embeds or attachments:
        return True

    raw = (content or "").strip()
    if not raw:
        return False

    import re

    # Pure mention blast (roles/users/everyone/here).
    if re.fullmatch(r"(?:\s|<@[!&]?\d+>|@everyone|@here)+", raw):
        return False

    # Keep anything with a URL even if short (links often are the whole payload).
    if re.search(r"https?://\S+", raw):
        return True

    # Remove mention tokens, then apply minimum length.
    cleaned = re.sub(r"<@[!&]?\d+>", " ", raw)
    cleaned = cleaned.replace("@everyone", " ").replace("@here", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return False

    try:
        mc = int(min_chars or 0)
    except Exception:
        mc = 0
    mc = max(0, min(500, mc))
    if mc <= 0:
        return True
    return len(cleaned) >= mc


def load_fetchall_mappings() -> Dict[str, Any]:
    try:
        if _FETCHALL_PATH.exists():
            with open(_FETCHALL_PATH, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {"guilds": []}
    except Exception as e:
        log_warn(f"[FETCHALL] Failed to load fetchall mappings: {e}")
    return {"guilds": []}


def save_fetchall_mappings(data: Dict[str, Any]) -> None:
    try:
        with _FILE_LOCK:
            _FETCHALL_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = Path(str(_FETCHALL_PATH) + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            try:
                os.replace(str(tmp), str(_FETCHALL_PATH))
            except Exception:
                with open(_FETCHALL_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log_warn(f"[FETCHALL] Failed to save fetchall mappings: {e}")


def iter_fetchall_entries() -> List[Dict[str, Any]]:
    data = load_fetchall_mappings()
    guilds = data.get("guilds", [])
    if not isinstance(guilds, list):
        return []
    out: List[Dict[str, Any]] = []
    for entry in guilds:
        if isinstance(entry, dict):
            out.append(entry)
    return out


def upsert_mapping(
    *,
    source_guild_id: int,
    name: Optional[str] = None,
    destination_category_id: Optional[int] = None,
    source_category_ids: Optional[List[int]] = None,
    require_date: Optional[bool] = None,
) -> Dict[str, Any]:
    config = load_fetchall_mappings()
    guilds = config.get("guilds")
    if not isinstance(guilds, list):
        guilds = []
        config["guilds"] = guilds
    target: Optional[Dict[str, Any]] = None
    for entry in guilds:
        try:
            if int(entry.get("source_guild_id", 0)) == int(source_guild_id):
                target = entry
                break
        except Exception:
            continue
    if target is None:
        target = {"source_guild_id": int(source_guild_id)}
        guilds.append(target)
    if name is not None:
        target["name"] = str(name)
    if destination_category_id is not None:
        target["destination_category_id"] = int(destination_category_id)
    if source_category_ids is not None:
        target["source_category_ids"] = [int(x) for x in source_category_ids if int(x) > 0]
    if require_date is not None:
        target["require_date"] = bool(require_date)
    target["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    save_fetchall_mappings(config)
    return target


def _find_entry(config: Dict[str, Any], *, source_guild_id: int) -> Optional[Dict[str, Any]]:
    guilds = config.get("guilds", [])
    if not isinstance(guilds, list):
        return None
    for entry in guilds:
        if not isinstance(entry, dict):
            continue
        try:
            if int(entry.get("source_guild_id", 0)) == int(source_guild_id):
                return entry
        except Exception:
            continue
    return None


def set_ignored_channel_ids(*, source_guild_id: int, ignored_channel_ids: List[int]) -> Optional[Dict[str, Any]]:
    """Set ignored_channel_ids for an existing mapping (creates mapping entry if missing)."""
    sgid = int(source_guild_id or 0)
    if sgid <= 0:
        return None
    # Ensure entry exists
    upsert_mapping(source_guild_id=sgid)
    config = load_fetchall_mappings()
    entry = _find_entry(config, source_guild_id=sgid)
    if entry is None:
        return None
    try:
        ids = [int(x) for x in (ignored_channel_ids or []) if int(x) > 0]
    except Exception:
        ids = []
    entry["ignored_channel_ids"] = ids
    entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    save_fetchall_mappings(config)
    return entry


def _get_cursor(entry: Dict[str, Any], *, source_channel_id: int) -> str:
    try:
        state = entry.get("state") if isinstance(entry.get("state"), dict) else {}
        curs = state.get("last_seen_message_id_by_channel") if isinstance(state.get("last_seen_message_id_by_channel"), dict) else {}
        return str(curs.get(str(int(source_channel_id)), "") or "").strip()
    except Exception:
        return ""


def _set_cursor_in_config(
    config: Dict[str, Any], *, source_guild_id: int, source_channel_id: int, last_seen_message_id: str
) -> None:
    entry = _find_entry(config, source_guild_id=int(source_guild_id))
    if entry is None:
        return
    state = entry.get("state")
    if not isinstance(state, dict):
        state = {}
        entry["state"] = state
    curs = state.get("last_seen_message_id_by_channel")
    if not isinstance(curs, dict):
        curs = {}
        state["last_seen_message_id_by_channel"] = curs
    curs[str(int(source_channel_id))] = str(last_seen_message_id)
    entry["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")


def persist_channel_cursor(*, source_guild_id: int, source_channel_id: int, last_seen_message_id: str) -> None:
    """Persist last seen message cursor for a specific channel into fetchall_mappings.json."""
    if int(source_guild_id or 0) <= 0 or int(source_channel_id or 0) <= 0:
        return
    if not str(last_seen_message_id or "").strip():
        return
    config = load_fetchall_mappings()
    _set_cursor_in_config(
        config,
        source_guild_id=int(source_guild_id),
        source_channel_id=int(source_channel_id),
        last_seen_message_id=str(last_seen_message_id).strip(),
    )
    save_fetchall_mappings(config)


def _slugify_channel_name(name: str, fallback_prefix: str = "channel") -> str:
    import re

    # Normalize unicode so names like "pokémon" become "pokemon" (instead of "pok-mon").
    raw = str(name or "").strip()
    try:
        norm = unicodedata.normalize("NFKD", raw)
        norm = norm.encode("ascii", errors="ignore").decode("ascii")
    except Exception:
        norm = raw

    slug = re.sub(r"[^a-z0-9\-_]+", "-", (norm or "").lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = f"{fallback_prefix}-{int(time.time())}"
    return slug[:90]


# Cache for source guild branding (name + icon url) to avoid repeated REST calls during polling.
_GUILD_BRAND_CACHE: Dict[int, Tuple[float, str, str]] = {}
_GUILD_BRAND_TTL_SECONDS: int = 60 * 60


async def _get_source_guild_brand(*, source_guild_id: int, user_token: str, fallback_name: str) -> Tuple[str, str]:
    """
    Return (guild_name, guild_icon_url) best-effort using the user token.
    Uses an in-memory cache to reduce Discord REST calls.
    """
    sgid = int(source_guild_id or 0)
    token = str(user_token or "").strip()
    name = str(fallback_name or "").strip() or f"guild_{sgid}"
    if sgid <= 0 or not token:
        return name, ""

    now = float(time.time())
    try:
        cached = _GUILD_BRAND_CACHE.get(sgid)
    except Exception:
        cached = None
    if cached is not None:
        try:
            ts, nm, icon = cached
            if (now - float(ts)) < float(_GUILD_BRAND_TTL_SECONDS):
                return (str(nm or name).strip() or name, str(icon or "").strip())
        except Exception:
            pass

    icon_url = ""
    try:
        status, ginfo = await _fetch_guild_info_via_user_token(source_guild_id=sgid, user_token=token)
        if status == 200 and isinstance(ginfo, dict):
            nm = str(ginfo.get("name") or "").strip()
            if nm:
                name = nm
            icon_hash = str(ginfo.get("icon") or "").strip()
            if icon_hash:
                icon_url = _guild_icon_url(guild_id=sgid, icon_hash=icon_hash)
    except Exception:
        icon_url = ""

    try:
        _GUILD_BRAND_CACHE[sgid] = (now, name, icon_url)
    except Exception:
        pass
    return name, icon_url


MIRROR_TOPIC_PREFIX = "MIRROR:"


def _build_mirror_topic(source_guild_id: int, source_channel_id: int) -> str:
    return f"{MIRROR_TOPIC_PREFIX}{int(source_guild_id)}:{int(source_channel_id)}"


def _parse_mirror_topic(topic: Optional[str]) -> Optional[tuple[int, int]]:
    """
    Parse a mirror topic.

    Current topics are written as:
      MIRROR:<source_guild_id>:<source_channel_id> | source=<guild>#<channel>

    Older topics may be exactly:
      MIRROR:<source_guild_id>:<source_channel_id>

    We intentionally ignore any trailing metadata after the two numeric ids.
    """
    if not topic:
        return None
    t = str(topic or "").strip()
    if not t.startswith(MIRROR_TOPIC_PREFIX):
        return None
    try:
        import re

        m = re.match(rf"^{re.escape(MIRROR_TOPIC_PREFIX)}(\d+):(\d+)", t)
        if not m:
            return None
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return None


def _separator_channel_name(guild_name: str) -> str:
    display = _slugify_channel_name(guild_name or "guild", fallback_prefix="guild")
    return f"📅---{display}---"[:90]


async def _ensure_separator(dest_category, *, source_guild_id: int, source_guild_name: str) -> Optional[int]:
    """Create or return a separator channel id for a source guild."""
    try:
        import discord
    except Exception:
        return None
    desired = _separator_channel_name(source_guild_name)
    # prefer topic match, then name
    for ch in list(getattr(dest_category, "text_channels", []) or []):
        topic = getattr(ch, "topic", "") or ""
        if topic.startswith("separator for") and str(source_guild_id) in topic:
            return int(ch.id)
        if getattr(ch, "name", "") == desired:
            return int(ch.id)
    try:
        created = await dest_category.create_text_channel(
            desired,
            topic=f"separator for {source_guild_name} ({source_guild_id})",
            reason="MWDataManagerBot fetchall separator",
        )
        return int(created.id)
    except Exception:
        return None


def _overflow_category_name(base_name: str, *, idx: int) -> str:
    bn = str(base_name or "").strip() or "mirror"
    return f"{bn}-overflow-{int(idx)}"[:90]


def _category_channel_count(cat) -> int:
    try:
        # Discord limit applies to total channels in category (text/voice/etc).
        return int(len(getattr(cat, "channels", []) or []))
    except Exception:
        try:
            return int(len(getattr(cat, "text_channels", []) or []))
        except Exception:
            return 0


def _category_has_capacity(cat) -> bool:
    try:
        return _category_channel_count(cat) < int(_CATEGORY_CHANNEL_LIMIT)
    except Exception:
        return True


async def _get_or_create_overflow_category(destination_guild, *, base_category, idx: int):
    """
    Find or create an overflow category for the given base category.
    Returns a discord.CategoryChannel or None.
    """
    try:
        import discord
    except Exception:
        return None
    name = _overflow_category_name(getattr(base_category, "name", "") or "mirror", idx=idx)
    # Find existing by name
    try:
        for c in list(getattr(destination_guild, "categories", []) or []):
            if isinstance(c, discord.CategoryChannel) and str(getattr(c, "name", "")) == str(name):
                return c
    except Exception:
        pass
    # Create new one
    try:
        created = await destination_guild.create_category(
            name,
            reason="MWDataManagerBot fetchall overflow category",
        )
        try:
            # place it right after base for readability
            if hasattr(created, "edit") and hasattr(base_category, "position"):
                await created.edit(position=int(getattr(base_category, "position", 0) or 0) + int(idx))
        except Exception:
            pass
        return created
    except Exception:
        return None


def _list_overflow_categories(destination_guild, *, base_category) -> List[Any]:
    """Return overflow categories in numeric order."""
    base_name = str(getattr(base_category, "name", "") or "").strip()
    out = []
    try:
        for c in list(getattr(destination_guild, "categories", []) or []):
            nm = str(getattr(c, "name", "") or "")
            if base_name and nm.startswith(f"{base_name}-overflow-"):
                out.append(c)
    except Exception:
        return []
    # Sort by suffix int if possible
    def _key(cat):
        nm = str(getattr(cat, "name", "") or "")
        try:
            tail = nm.split("-overflow-", 1)[1]
            return int(tail)
        except Exception:
            return 999999
    try:
        out.sort(key=_key)
    except Exception:
        pass
    return out


async def _pick_category_for_new_channel(destination_guild, *, base_category):
    """
    Choose a destination category with capacity.
    Creates overflow categories as needed.
    """
    if _category_has_capacity(base_category):
        return base_category
    # Try existing overflows first
    for c in _list_overflow_categories(destination_guild, base_category=base_category):
        if _category_has_capacity(c):
            return c
    # Create a new overflow category
    idx = 2
    try:
        existing = _list_overflow_categories(destination_guild, base_category=base_category)
        if existing:
            try:
                last_nm = str(getattr(existing[-1], "name", "") or "")
                idx = int(last_nm.split("-overflow-", 1)[1]) + 1
            except Exception:
                idx = 2 + len(existing)
    except Exception:
        idx = 2
    created = await _get_or_create_overflow_category(destination_guild, base_category=base_category, idx=idx)
    if created is not None:
        return created
    return base_category


async def _ensure_separator_anywhere(destination_guild, *, base_category, source_guild_id: int, source_guild_name: str) -> Optional[int]:
    """Ensure separator exists; create it in a category with capacity if needed."""
    try:
        import discord
    except Exception:
        return None
    # Search across all text channels for existing separator
    try:
        for ch in list(getattr(destination_guild, "text_channels", []) or []):
            topic = getattr(ch, "topic", "") or ""
            if topic.startswith("separator for") and str(source_guild_id) in topic:
                return int(ch.id)
    except Exception:
        pass
    # Create in a category that has room
    cat = await _pick_category_for_new_channel(destination_guild, base_category=base_category)
    return await _ensure_separator(cat, source_guild_id=source_guild_id, source_guild_name=source_guild_name)

async def _fetch_guild_channels_via_user_token(
    *, source_guild_id: int, user_token: str
) -> Tuple[int, Optional[List[Dict[str, Any]]]]:
    """
    Fetch source guild channel list using a Discum/user token.
    Returns (http_status, channels_or_none).
    """
    if source_guild_id <= 0 or not user_token:
        return 0, None
    url = f"{_DISCORD_API_BASE}/guilds/{int(source_guild_id)}/channels"
    status, data = await _discord_api_get_json(url=url, user_token=str(user_token).strip(), params=None)
    if status == 200 and isinstance(data, list):
        return status, [c for c in data if isinstance(c, dict)]
    return status, None


async def _fetch_guild_info_via_user_token(*, source_guild_id: int, user_token: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    """
    Fetch source guild metadata (name/icon) using a user token.
    Returns (http_status, guild_dict_or_none).
    """
    if source_guild_id <= 0 or not user_token:
        return 0, None
    url = f"{_DISCORD_API_BASE}/guilds/{int(source_guild_id)}"
    status, data = await _discord_api_get_json(url=url, user_token=str(user_token).strip(), params=None)
    if status == 200 and isinstance(data, dict):
        return status, data
    return status, None


def _guild_icon_url(*, guild_id: int, icon_hash: str) -> str:
    try:
        h = str(icon_hash or "").strip()
    except Exception:
        h = ""
    if not h:
        return ""
    ext = "png"
    try:
        if h.startswith("a_"):
            ext = "gif"
    except Exception:
        ext = "png"
    return f"https://cdn.discordapp.com/icons/{int(guild_id)}/{h}.{ext}?size=64"


def _source_brand_embed(
    *, source_guild_id: int, source_guild_name: str, source_guild_icon_url: str, source_channel_id: int, source_channel_name: str
) -> Dict[str, Any]:
    """Small header embed to show source server avatar + name for mirrored messages."""
    sgid = int(source_guild_id or 0)
    cid = int(source_channel_id or 0)
    cname = str(source_channel_name or "").strip()
    gname = str(source_guild_name or "").strip() or f"guild_{sgid}"
    icon_url = str(source_guild_icon_url or "").strip()
    chan_url = ""
    if sgid > 0 and cid > 0:
        chan_url = f"https://discord.com/channels/{sgid}/{cid}"
    author: Dict[str, Any] = {"name": gname}
    if chan_url:
        author["url"] = chan_url
    if icon_url:
        author["icon_url"] = icon_url
    desc = f"#{cname}" if cname else ""
    out: Dict[str, Any] = {"author": author}
    if desc:
        out["description"] = desc
    return out


async def _discord_api_get_json(
    *,
    url: str,
    user_token: str,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
) -> Tuple[int, Any]:
    """GET JSON with basic rate-limit handling. Returns (status_code, json_or_text)."""
    if not url or not user_token:
        return 0, None
    try:
        import aiohttp  # type: ignore
    except Exception:
        return 0, None
    headers = {
        "Authorization": str(user_token).strip(),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) MWDataManagerBot/Fetchall",
        "Accept": "application/json",
    }
    timeout = aiohttp.ClientTimeout(total=25)
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url, params=params) as resp:
                    status = int(getattr(resp, "status", 0) or 0)
                    if status == 429:
                        try:
                            data = await resp.json()
                        except Exception:
                            data = {}
                        retry_after = 1.0
                        try:
                            retry_after = float(data.get("retry_after") or 1.0)
                        except Exception:
                            retry_after = 1.0
                        # Best-effort: surface rate limiting in logs (first hit per call).
                        if attempt == 1:
                            try:
                                log_warn(f"[FETCHALL] rate_limited url={str(url)[:80]} retry_after={retry_after}")
                            except Exception:
                                pass
                        await asyncio.sleep(max(0.5, min(10.0, retry_after)))
                        continue
                    if status and 200 <= status < 300:
                        try:
                            return status, await resp.json()
                        except Exception:
                            try:
                                return status, await resp.text()
                            except Exception:
                                return status, None
                    # 4xx: likely terminal (no access / missing)
                    if status and 400 <= status < 500:
                        try:
                            return status, await resp.text()
                        except Exception:
                            return status, None
                    # 5xx / network: retry with backoff
                    await asyncio.sleep(min(6.0, 0.5 * attempt))
        except Exception:
            await asyncio.sleep(min(6.0, 0.5 * attempt))
    return 0, None


async def _fetch_channel_messages_page(
    *,
    source_channel_id: int,
    user_token: str,
    limit: int,
    after: Optional[str] = None,
) -> Tuple[bool, List[Dict[str, Any]], str]:
    """
    Returns (ok, messages, reason).
    Messages are raw Discord API dicts (newest-first as returned by API).
    """
    cid = int(source_channel_id or 0)
    if cid <= 0:
        return False, [], "invalid_channel_id"
    lim = int(limit or 0)
    if lim <= 0:
        lim = 50
    params: Dict[str, Any] = {"limit": str(lim)}
    if after:
        params["after"] = str(after)
    url = f"{_DISCORD_API_BASE}/channels/{cid}/messages"
    status, data = await _discord_api_get_json(url=url, user_token=user_token, params=params)
    if status == 200 and isinstance(data, list):
        msgs = [m for m in data if isinstance(m, dict)]
        return True, msgs, ""
    if status in (401, 403):
        return False, [], "forbidden_or_unauthorized"
    if status == 404:
        return False, [], "not_found"
    return False, [], f"http_{status or 0}"


def _select_source_text_channels_from_api(
    channels: List[Dict[str, Any]],
    *,
    source_category_ids: List[int],
    ignored_channel_ids: Set[int],
) -> List[Tuple[int, str]]:
    """
    Return list of (channel_id, channel_name) for text channels.
    Discord channel types:
      - 0 = GUILD_TEXT
      - 5 = GUILD_ANNOUNCEMENT (messageable)
    """
    out: List[Tuple[int, str]] = []
    allow_categories: Set[int] = {int(x) for x in (source_category_ids or []) if int(x) > 0}
    allowed_types: Set[int] = {0, 5}
    for ch in channels or []:
        if not isinstance(ch, dict):
            continue
        try:
            ch_id = int(ch.get("id") or 0)
        except Exception:
            continue
        if ch_id <= 0 or ch_id in ignored_channel_ids:
            continue
        # Important: channel type 0 is valid but falsy, so do NOT use `or -1`.
        try:
            raw_type = ch.get("type", None)
            ch_type = int(raw_type) if raw_type is not None else -1
        except Exception:
            ch_type = -1
        if ch_type not in allowed_types:
            continue
        parent_id = None
        try:
            pid = ch.get("parent_id")
            if pid is not None and str(pid).strip():
                parent_id = int(pid)
        except Exception:
            parent_id = None
        if allow_categories:
            if parent_id is None or int(parent_id) not in allow_categories:
                continue
        name = str(ch.get("name") or f"channel_{ch_id}")
        out.append((ch_id, name))
    return out


def _summarize_api_channels(channels: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Best-effort summary for debugging (no secrets)."""
    type_counts: Dict[int, int] = {}
    categories: List[Tuple[int, str]] = []
    for ch in channels or []:
        if not isinstance(ch, dict):
            continue
        try:
            raw_type = ch.get("type", None)
            t = int(raw_type) if raw_type is not None else -1
        except Exception:
            t = -1
        type_counts[t] = int(type_counts.get(t, 0) or 0) + 1
        if t == 4:
            try:
                cid = int(ch.get("id") or 0)
            except Exception:
                cid = 0
            if cid > 0:
                categories.append((cid, str(ch.get("name") or "")))
    categories = categories[:12]
    return {"total": len(channels or []), "type_counts": type_counts, "categories_preview": categories}


def _discord_jump_url(guild_id: int, channel_id: int) -> str:
    gid = int(guild_id or 0)
    cid = int(channel_id or 0)
    if gid <= 0 or cid <= 0:
        return ""
    return f"https://discord.com/channels/{gid}/{cid}"


async def list_source_guild_channels(
    *, source_guild_id: int, user_token: str
) -> Dict[str, Any]:
    """
    List source guild categories + messageable channels using user token.
    Returns a dict suitable for UI/debug (no secrets).
    """
    sgid = int(source_guild_id or 0)
    token = str(user_token or "").strip()
    if sgid <= 0:
        return {"ok": False, "reason": "invalid_source_guild_id"}
    if not token:
        return {"ok": False, "reason": "missing_user_token"}
    status, channels = await _fetch_guild_channels_via_user_token(source_guild_id=sgid, user_token=token)
    if not channels:
        return {"ok": False, "reason": "failed_to_list_source_channels_via_token", "http_status": int(status or 0)}

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
                cats.append(
                    {
                        "id": cid,
                        "name": str(c.get("name") or ""),
                        "position": int(c.get("position") or 0) if str(c.get("position") or "").strip() else 0,
                        "url": _discord_jump_url(sgid, cid),
                    }
                )
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
                parent_id = 0
            chan.append(
                {
                    "id": chid,
                    "name": str(c.get("name") or f"channel_{chid}"),
                    "parent_id": int(parent_id or 0),
                    "type": int(t),
                    "position": int(c.get("position") or 0) if str(c.get("position") or "").strip() else 0,
                    "url": _discord_jump_url(sgid, chid),
                }
            )

    cats_sorted = sorted(cats, key=lambda x: (int(x.get("position", 0) or 0), int(x.get("id", 0) or 0)))
    chan_sorted = sorted(chan, key=lambda x: (int(x.get("parent_id", 0) or 0), int(x.get("position", 0) or 0), int(x.get("id", 0) or 0)))
    type_counts = {}
    try:
        type_counts = _summarize_api_channels(channels).get("type_counts", {})  # type: ignore[assignment]
    except Exception:
        type_counts = {}
    return {
        "ok": True,
        "http_status": int(status or 0),
        "source_guild_id": sgid,
        "categories": cats_sorted,
        "channels": chan_sorted,
        "total": int(len(channels)),
        "type_counts": type_counts,
    }

def _author_avatar_url(author: Dict[str, Any]) -> str:
    try:
        aid = str(author.get("id") or "").strip()
    except Exception:
        aid = ""
    if not aid:
        return ""
    try:
        avatar = str(author.get("avatar") or "").strip()
    except Exception:
        avatar = ""
    if not avatar:
        return ""
    ext = "png"
    try:
        if avatar.startswith("a_"):
            ext = "gif"
    except Exception:
        ext = "png"
    return f"https://cdn.discordapp.com/avatars/{aid}/{avatar}.{ext}?size=64"


def _author_display_name(author: Dict[str, Any]) -> str:
    # Prefer global_name when present; fallback to username.
    try:
        gn = str(author.get("global_name") or "").strip()
    except Exception:
        gn = ""
    if gn:
        return gn
    try:
        un = str(author.get("username") or "").strip()
    except Exception:
        un = ""
    return un


async def _send_message_to_channel(
    *,
    dest_channel,
    content: str,
    embeds: List[Dict[str, Any]],
    webhook_username: str = "",
    webhook_avatar_url: str = "",
    reason: str = "",
) -> None:
    """
    Send to destination channel.
    Prefer webhooks (clean identity) when enabled; otherwise normal bot send.
    """
    try:
        import discord  # type: ignore
    except Exception:
        discord = None  # type: ignore

    embed_objs = []
    if discord is not None:
        for ed in embeds or []:
            try:
                embed_objs.append(discord.Embed.from_dict(ed))
            except Exception:
                continue

    from webhook_sender import send_via_webhook_or_bot

    chunks = chunk_text(content, 2000)
    for i, chunk in enumerate(chunks):
        if i == 0 and embed_objs:
            await send_via_webhook_or_bot(
                dest_channel=dest_channel,
                content=chunk,
                embeds=embed_objs[:10],
                username=webhook_username,
                avatar_url=webhook_avatar_url,
                reason=reason or "MWDataManagerBot fetchsync mirror",
            )
        else:
            await send_via_webhook_or_bot(
                dest_channel=dest_channel,
                content=chunk,
                embeds=[],
                username=webhook_username,
                avatar_url=webhook_avatar_url,
                reason=reason or "MWDataManagerBot fetchsync mirror (chunk)",
            )


async def run_fetchall(
    *,
    bot,
    entry: Dict[str, Any],
    destination_guild,
    source_user_token: Optional[str] = None,
    progress_cb: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """
    Basic fetch-all (standalone):
      - Ensures a destination category exists (by ID)
      - Ensures a per-guild separator channel
      - Mirrors source guild channels into destination category

    Mirror channels are linked by topic: `MIRROR:<source_guild_id>:<source_channel_id>`.
    """
    try:
        import discord
    except Exception as e:
        return {"ok": False, "reason": f"discord_import_failed: {e}"}

    source_guild_id = int(entry.get("source_guild_id", 0) or 0)
    if source_guild_id <= 0:
        if progress_cb is not None:
            try:
                await progress_cb({"stage": "fail", "reason": "missing_source_guild_id"})
            except Exception:
                pass
        log_warn("[FETCHALL] missing_source_guild_id")
        return {"ok": False, "reason": "missing_source_guild_id"}

    # User token (optional). Used for source guild discovery when bot is not in guild,
    # and for best-effort guild metadata (name/icon) when available.
    token = str(source_user_token or "").strip()

    dest_category_id = int(entry.get("destination_category_id", 0) or 0) or int(cfg.FETCHALL_DEFAULT_DEST_CATEGORY_ID or 0)
    if dest_category_id <= 0:
        if progress_cb is not None:
            try:
                await progress_cb(
                    {"stage": "fail", "reason": "missing_destination_category_id", "source_guild_id": int(source_guild_id)}
                )
            except Exception:
                pass
        log_warn(f"[FETCHALL] missing_destination_category_id (source_guild_id={source_guild_id})")
        return {"ok": False, "reason": "missing_destination_category_id"}

    if destination_guild is None:
        # pick first destination guild the bot is in
        for gid in cfg.DESTINATION_GUILD_IDS:
            g = bot.get_guild(int(gid))
            if g:
                destination_guild = g
                break
    if destination_guild is None:
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "fail",
                        "reason": "destination_guild_not_found",
                        "source_guild_id": int(source_guild_id),
                        "destination_category_id": int(dest_category_id),
                    }
                )
            except Exception:
                pass
        log_warn(f"[FETCHALL] destination_guild_not_found (source_guild_id={source_guild_id})")
        return {"ok": False, "reason": "destination_guild_not_found"}

    dest_category = destination_guild.get_channel(int(dest_category_id))
    if not isinstance(dest_category, discord.CategoryChannel):
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "fail",
                        "reason": f"destination_category_not_found:{dest_category_id}",
                        "source_guild_id": int(source_guild_id),
                        "destination_category_id": int(dest_category_id),
                    }
                )
            except Exception:
                pass
        log_warn(f"[FETCHALL] destination_category_not_found:{dest_category_id} (source_guild_id={source_guild_id})")
        return {"ok": False, "reason": f"destination_category_not_found:{dest_category_id}"}

    source_guild = bot.get_guild(int(source_guild_id))
    source_guild_name = str(entry.get("name") or "").strip() or f"guild_{source_guild_id}"
    # Optional branding: icon URL for the source guild (best-effort). If unavailable, leave blank.
    # This MUST be defined so downstream embed builders don't crash.
    source_guild_icon_url = ""

    # Always emit an "init" progress/log so the caller can see fetchall is running,
    # even if we later exit early (e.g. missing categories).
    if progress_cb is not None:
        try:
            await progress_cb(
                {
                    "stage": "init",
                    "mode": "bot" if source_guild is not None else "user_token",
                    "source_guild_id": int(source_guild_id),
                    "source_guild_name": str(source_guild_name),
                    "destination_category_id": int(dest_category_id),
                    "total_sources": 0,
                    "attempted": 0,
                    "created": 0,
                    "existing": 0,
                    "errors": 0,
                }
            )
        except Exception:
            pass
    try:
        log_fetchall(
            f"start source={source_guild_id} dest_category={dest_category_id} mode={'bot' if source_guild is not None else 'user_token'}"
        )
    except Exception:
        pass
    # Note: We intentionally do NOT fetch guild metadata (name/icon) via REST here.
    # It is non-essential and can stall the command if Discord REST is slow/rate-limited.
    # Mappings already carry a friendly name, which is sufficient for fetchall channel setup.
    try:
        await _ensure_separator_anywhere(
            destination_guild, base_category=dest_category, source_guild_id=source_guild_id, source_guild_name=source_guild_name
        )
    except Exception as e:
        log_warn(f"[FETCHALL] failed to ensure separator (source_guild_id={source_guild_id}): {type(e).__name__}: {e}")

    source_category_ids = entry.get("source_category_ids") if isinstance(entry.get("source_category_ids"), list) else []
    # Safety: require explicit source_category_ids so we don't accidentally mirror entire servers.
    try:
        _cats = [int(x) for x in (source_category_ids or []) if int(x) > 0]
    except Exception:
        _cats = []
    if not _cats:
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "fail",
                        "reason": "missing_source_category_ids",
                        "source_guild_id": int(source_guild_id),
                        "source_guild_name": str(source_guild_name),
                        "destination_category_id": int(dest_category_id),
                    }
                )
            except Exception:
                pass
        log_warn(f"[FETCHALL] missing_source_category_ids (source_guild_id={source_guild_id})")
        return {"ok": False, "reason": "missing_source_category_ids", "source_guild_id": int(source_guild_id)}
    ignored_ids: Set[int] = set()
    try:
        raw_ignored = entry.get("ignored_channel_ids") if isinstance(entry.get("ignored_channel_ids"), list) else []
        ignored_ids = {int(x) for x in raw_ignored if int(x) > 0}
    except Exception:
        ignored_ids = set()

    src_channels_to_mirror: List[Tuple[int, str]] = []
    mode = "bot"
    if source_guild is not None:
        # Bot is in source guild: enumerate via cached guild/category objects.
        source_categories: list[discord.CategoryChannel] = []
        if _cats:
            for cid in _cats:
                try:
                    cat = source_guild.get_channel(int(cid))
                except Exception:
                    cat = None
                if isinstance(cat, discord.CategoryChannel):
                    source_categories.append(cat)
        else:
            source_categories = []

        for cat in source_categories:
            for src in list(getattr(cat, "text_channels", []) or []):
                try:
                    src_id = int(getattr(src, "id", 0) or 0)
                except Exception:
                    continue
                if src_id <= 0 or src_id in ignored_ids:
                    continue
                src_name = getattr(src, "name", "") or f"channel_{src_id}"
                src_channels_to_mirror.append((src_id, str(src_name)))
    else:
        # Fallback: use Discum/user token to list source channels via REST.
        mode = "user_token"
        if not token:
            if progress_cb is not None:
                try:
                    await progress_cb(
                        {
                            "stage": "fail",
                            "mode": "user_token",
                            "reason": f"bot_not_in_source_guild:{source_guild_id} and no source_user_token provided",
                            "source_guild_id": int(source_guild_id),
                            "source_guild_name": str(source_guild_name),
                            "destination_category_id": int(dest_category_id),
                        }
                    )
                except Exception:
                    pass
            log_warn(f"[FETCHALL] bot_not_in_source_guild:{source_guild_id} and no source_user_token provided")
            return {"ok": False, "reason": f"bot_not_in_source_guild:{source_guild_id} and no source_user_token provided"}
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "list_channels",
                        "mode": "user_token",
                        "source_guild_id": int(source_guild_id),
                        "source_guild_name": str(source_guild_name),
                        "destination_category_id": int(dest_category_id),
                        "attempted": 0,
                        "created": 0,
                        "existing": 0,
                        "errors": 0,
                    }
                )
            except Exception:
                pass
        status, api_channels = await _fetch_guild_channels_via_user_token(source_guild_id=source_guild_id, user_token=token)
        if not api_channels:
            if progress_cb is not None:
                try:
                    await progress_cb(
                        {
                            "stage": "fail",
                            "mode": "user_token",
                            "reason": f"failed_to_list_source_channels_via_token:{source_guild_id}",
                            "http_status": int(status or 0),
                            "source_guild_id": int(source_guild_id),
                            "source_guild_name": str(source_guild_name),
                            "destination_category_id": int(dest_category_id),
                        }
                    )
                except Exception:
                    pass
            log_warn(f"[FETCHALL] failed_to_list_source_channels_via_token:{source_guild_id} http={int(status or 0)}")
            return {
                "ok": False,
                "reason": f"failed_to_list_source_channels_via_token:{source_guild_id}",
                "http_status": int(status or 0),
            }
        selected = _select_source_text_channels_from_api(
            api_channels,
            source_category_ids=_cats,
            ignored_channel_ids=ignored_ids,
        )
        src_channels_to_mirror.extend(selected)

    # Build existing mirror index by topic (scan across ALL destination channels, including overflow categories)
    existing_by_source: dict[int, discord.TextChannel] = {}
    for ch in list(getattr(destination_guild, "text_channels", []) or []):
        info = _parse_mirror_topic(getattr(ch, "topic", None))
        if info and info[0] == source_guild_id:
            existing_by_source[info[1]] = ch

    created = 0
    kept = 0
    attempted = 0
    errors = 0
    total_sources = int(len(src_channels_to_mirror or []))
    if progress_cb is not None:
        try:
            await progress_cb(
                {
                    "stage": "start",
                    "mode": str(mode),
                    "source_guild_id": int(source_guild_id),
                    "source_guild_name": str(source_guild_name),
                    "destination_category_id": int(dest_category_id),
                    "total_sources": int(total_sources),
                    "attempted": 0,
                    "created": 0,
                    "existing": int(len(existing_by_source)),
                    "errors": 0,
                }
            )
        except Exception:
            pass
    for src_id, src_name in src_channels_to_mirror:
        attempted += 1
        src_id = int(src_id or 0)
        if src_id <= 0:
            continue
        if src_id in existing_by_source:
            kept += 1
            if progress_cb is not None:
                try:
                    await progress_cb(
                        {
                            "stage": "existing",
                            "mode": str(mode),
                            "source_guild_id": int(source_guild_id),
                            "source_guild_name": str(source_guild_name),
                            "destination_category_id": int(dest_category_id),
                            "total_sources": int(total_sources),
                            "attempted": int(attempted),
                            "created": int(created),
                            "existing": int(kept),
                            "errors": int(errors),
                            "current_channel_id": int(src_id or 0),
                            "current_channel_name": str(src_name or ""),
                        }
                    )
                except Exception:
                    pass
            continue
        desired_name = _slugify_channel_name(str(src_name), fallback_prefix="mirror")
        topic = _build_mirror_topic(source_guild_id, src_id)
        full_topic = f"{topic} | source={source_guild_name}#{src_name}"
        # Choose a category with capacity (base or overflow)
        dest_cat = await _pick_category_for_new_channel(destination_guild, base_category=dest_category)
        # ensure unique name within chosen category
        final_name = desired_name
        try:
            taken = {c.name for c in getattr(dest_cat, "text_channels", []) or []}
        except Exception:
            taken = set()
        if final_name in taken:
            final_name = (final_name[:80] + f"-{str(src_id)[-4:]}")[:90]
        # Emit progress BEFORE the create call, so the user sees activity even if Discord rate-limits creation.
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "creating",
                        "mode": str(mode),
                        "source_guild_id": int(source_guild_id),
                        "source_guild_name": str(source_guild_name),
                        "destination_category_id": int(dest_category_id),
                        "total_sources": int(total_sources),
                        "attempted": int(attempted),
                        "created": int(created),
                        "existing": int(kept),
                        "errors": int(errors),
                        "current_channel_id": int(src_id or 0),
                        "current_channel_name": str(src_name or ""),
                    }
                )
            except Exception:
                pass
        try:
            await destination_guild.create_text_channel(
                final_name,
                category=dest_cat,
                topic=full_topic,
                reason="MWDataManagerBot fetchall mirror channel",
            )
            created += 1
        except Exception as e:
            # If the category filled up between check+create, retry once in a new overflow category.
            err_s = str(e)
            if "Maximum number of channels in category reached" in err_s or "Maximum number of channels in category" in err_s:
                try:
                    dest_cat2 = await _pick_category_for_new_channel(destination_guild, base_category=dest_category)
                    if dest_cat2 is not None and dest_cat2 != dest_cat:
                        await destination_guild.create_text_channel(
                            final_name,
                            category=dest_cat2,
                            topic=full_topic,
                            reason="MWDataManagerBot fetchall mirror channel (overflow retry)",
                        )
                        created += 1
                        continue
                except Exception:
                    pass
            errors += 1
            log_warn(f"[FETCHALL] failed to create mirror for {source_guild_id}:{src_id}: {e}")
        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "mirrors",
                        "mode": str(mode),
                        "source_guild_id": int(source_guild_id),
                        "source_guild_name": str(source_guild_name),
                        "destination_category_id": int(dest_category_id),
                        "total_sources": int(total_sources),
                        "attempted": int(attempted),
                        "created": int(created),
                        "existing": int(kept),
                        "errors": int(errors),
                        "current_channel_id": int(src_id or 0),
                        "current_channel_name": str(src_name or ""),
                    }
                )
            except Exception:
                pass

    log_fetchall(
        f"source={source_guild_id} dest_category={dest_category_id} mode={mode} attempted={attempted} created={created} existing={kept}"
    )
    if progress_cb is not None:
        try:
            await progress_cb(
                {
                    "stage": "done",
                    "mode": str(mode),
                    "source_guild_id": int(source_guild_id),
                    "source_guild_name": str(source_guild_name),
                    "destination_category_id": int(dest_category_id),
                    "total_sources": int(total_sources),
                    "attempted": int(attempted),
                    "created": int(created),
                    "existing": int(kept),
                    "errors": int(errors),
                }
            )
        except Exception:
            pass
    if attempted == 0:
        # Treat as failure so commands/users see a concrete reason.
        extra: Dict[str, Any] = {
            "ok": False,
            "reason": "no_source_channels_selected",
            "source_guild_id": source_guild_id,
            "destination_category_id": dest_category_id,
            "attempted": attempted,
            "created": created,
            "existing": kept,
        }
        if mode == "user_token":
            try:
                extra["source_category_ids"] = _cats
                extra["ignored_count"] = int(len(ignored_ids or set()))
            except Exception:
                pass
        try:
            if mode == "user_token":
                # Best-effort debug summary. If api_channels was not in scope (early fail), this won't run.
                # We re-fetch quickly for logging.
                status2, api_channels2 = await _fetch_guild_channels_via_user_token(
                    source_guild_id=source_guild_id, user_token=str(source_user_token or "").strip()
                )
                extra["http_status"] = int(status2 or 0)
                if api_channels2:
                    extra.update(_summarize_api_channels(api_channels2))
        except Exception:
            pass
        try:
            log_warn(f"[FETCHALL] no_source_channels_selected details={extra}")
        except Exception:
            pass
        return extra

    return {
        "ok": True,
        "source_guild_id": source_guild_id,
        "destination_category_id": dest_category_id,
        "attempted": attempted,
        "created": created,
        "existing": kept,
        "errors": errors,
    }


async def run_fetchsync(
    *,
    bot,
    entry: Dict[str, Any],
    destination_guild,
    source_user_token: str,
    dryrun: bool = False,
    progress_cb: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
) -> Dict[str, Any]:
    """
    Fetch messages from source channels using a user token and mirror them into destination channels.

    - Reads ONLY from source servers using user token.
    - Writes ONLY to destination guild (Mirror World) using bot token.
    - No writes to source servers.
    """
    try:
        import discord
    except Exception as e:
        return {"ok": False, "reason": f"discord_import_failed: {e}"}

    source_guild_id = int(entry.get("source_guild_id", 0) or 0)
    if source_guild_id <= 0:
        return {"ok": False, "reason": "missing_source_guild_id"}
    token = str(source_user_token or "").strip()
    if not token:
        return {"ok": False, "reason": "missing_source_user_token"}

    dest_category_id = int(entry.get("destination_category_id", 0) or 0) or int(cfg.FETCHALL_DEFAULT_DEST_CATEGORY_ID or 0)
    if dest_category_id <= 0:
        return {"ok": False, "reason": "missing_destination_category_id"}

    if destination_guild is None:
        for gid in cfg.DESTINATION_GUILD_IDS:
            g = bot.get_guild(int(gid))
            if g:
                destination_guild = g
                break
    if destination_guild is None:
        return {"ok": False, "reason": "destination_guild_not_found"}

    dest_category = destination_guild.get_channel(int(dest_category_id))
    if not isinstance(dest_category, discord.CategoryChannel):
        return {"ok": False, "reason": f"destination_category_not_found:{dest_category_id}"}

    source_guild_name = str(entry.get("name") or "").strip() or f"guild_{source_guild_id}"
    # Optional branding: icon URL for the source guild (best-effort). If unavailable, leave blank.
    # This MUST be defined so embed builders don't crash (auto-poller previously crashed here).
    source_guild_icon_url = ""
    try:
        source_guild_name, source_guild_icon_url = await _get_source_guild_brand(
            source_guild_id=int(source_guild_id),
            user_token=token,
            fallback_name=source_guild_name,
        )
    except Exception:
        pass

    # In dryrun we avoid creating channels; for run mode we ensure separator exists.
    if not dryrun:
        try:
            await _ensure_separator_anywhere(
                destination_guild,
                base_category=dest_category,
                source_guild_id=source_guild_id,
                source_guild_name=source_guild_name,
            )
        except Exception:
            pass

    source_category_ids = entry.get("source_category_ids") if isinstance(entry.get("source_category_ids"), list) else []
    # Safety: require explicit categories (prevents "mirror whole server" accidents).
    try:
        _cats = [int(x) for x in (source_category_ids or []) if int(x) > 0]
    except Exception:
        _cats = []
    if not _cats:
        return {"ok": False, "reason": "missing_source_category_ids", "source_guild_id": int(source_guild_id)}
    ignored_ids: Set[int] = set()
    try:
        raw_ignored = entry.get("ignored_channel_ids") if isinstance(entry.get("ignored_channel_ids"), list) else []
        ignored_ids = {int(x) for x in raw_ignored if int(x) > 0}
    except Exception:
        ignored_ids = set()

    # Always enumerate channels via user token (source access is assumed to be via user token).
    status, api_channels = await _fetch_guild_channels_via_user_token(source_guild_id=source_guild_id, user_token=token)
    if not api_channels:
        return {
            "ok": False,
            "reason": f"failed_to_list_source_channels_via_token:{source_guild_id}",
            "http_status": int(status or 0),
        }
    src_channels_to_mirror = _select_source_text_channels_from_api(
        api_channels,
        source_category_ids=_cats,
        ignored_channel_ids=ignored_ids,
    )
    if not src_channels_to_mirror:
        extra = {
            "ok": False,
            "reason": "no_source_channels_selected",
            "http_status": int(status or 0),
        }
        try:
            extra.update(_summarize_api_channels(api_channels))
            extra["source_category_ids"] = _cats
            extra["ignored_count"] = int(len(ignored_ids or set()))
        except Exception:
            pass
        return extra

    # Build existing mirror index by topic (scan across ALL destination channels, including overflow categories)
    existing_by_source: dict[int, discord.TextChannel] = {}
    for ch in list(getattr(destination_guild, "text_channels", []) or []):
        info = _parse_mirror_topic(getattr(ch, "topic", None))
        if info and info[0] == source_guild_id:
            existing_by_source[info[1]] = ch

    mirror_by_source: Dict[int, discord.TextChannel] = dict(existing_by_source)
    created_channels = 0
    if not dryrun:
        # Track taken names per destination category
        taken_by_cat: Dict[int, Set[str]] = {}
        try:
            taken_by_cat[int(dest_category.id)] = {c.name for c in dest_category.text_channels}
        except Exception:
            taken_by_cat[int(dest_category.id)] = set()
        for src_id, src_name in src_channels_to_mirror:
            sid = int(src_id or 0)
            if sid <= 0 or sid in mirror_by_source:
                continue
            desired_name = _slugify_channel_name(str(src_name), fallback_prefix="mirror")
            topic = _build_mirror_topic(source_guild_id, sid)
            full_topic = f"{topic} | source={source_guild_name}#{src_name}"
            # Choose category with capacity (base or overflow)
            dest_cat = await _pick_category_for_new_channel(destination_guild, base_category=dest_category)
            try:
                taken = taken_by_cat.setdefault(int(dest_cat.id), {c.name for c in getattr(dest_cat, "text_channels", []) or []})
            except Exception:
                taken = taken_by_cat.setdefault(int(getattr(dest_cat, "id", 0) or 0), set())
            final_name = desired_name
            if final_name in taken:
                final_name = (final_name[:80] + f"-{str(sid)[-4:]}")[:90]
            try:
                created = await destination_guild.create_text_channel(
                    final_name,
                    category=dest_cat,
                    topic=full_topic,
                    reason="MWDataManagerBot fetchsync mirror channel",
                )
                mirror_by_source[sid] = created
                try:
                    taken.add(final_name)
                except Exception:
                    pass
                created_channels += 1
            except Exception as e:
                err_s = str(e)
                if "Maximum number of channels in category reached" in err_s or "Maximum number of channels in category" in err_s:
                    try:
                        dest_cat2 = await _pick_category_for_new_channel(destination_guild, base_category=dest_category)
                        if dest_cat2 is not None and dest_cat2 != dest_cat:
                            created = await destination_guild.create_text_channel(
                                final_name,
                                category=dest_cat2,
                                topic=full_topic,
                                reason="MWDataManagerBot fetchsync mirror channel (overflow retry)",
                            )
                            mirror_by_source[sid] = created
                            created_channels += 1
                            continue
                    except Exception:
                        pass
                log_warn(f"[FETCHSYNC] failed to create mirror for {source_guild_id}:{sid}: {e}")

    channels_processed = 0
    would_send = 0
    sent = 0
    errors = 0
    # initial backfill limit for channels with no cursor yet
    per_channel_limit = int(getattr(cfg, "FETCHSYNC_INITIAL_BACKFILL_LIMIT", 20) or 20)
    if per_channel_limit < 1:
        per_channel_limit = 1
    if per_channel_limit > 50:
        per_channel_limit = 50
    max_per_channel = int(getattr(cfg, "FETCHALL_MAX_MESSAGES_PER_CHANNEL", 400) or 400)
    send_min_interval = float(getattr(cfg, "SEND_MIN_INTERVAL_SECONDS", 0.0) or 0.0)
    min_chars = int(getattr(cfg, "FETCHSYNC_MIN_CONTENT_CHARS", 25) or 25)
    channels_total = int(len(src_channels_to_mirror or []))

    if progress_cb is not None:
        try:
            await progress_cb(
                {
                    "stage": "start",
                    "dryrun": bool(dryrun),
                    "source_guild_id": int(source_guild_id),
                    "source_guild_name": str(source_guild_name),
                    "destination_category_id": int(dest_category_id),
                    "channels_total": int(channels_total),
                    "channels_processed": 0,
                    "created_mirror_channels": int(created_channels),
                    "sent": 0,
                    "would_send": 0,
                    "errors": 0,
                }
            )
        except Exception:
            pass

    import re

    def _strip_mentions_only(text: str) -> str:
        s = str(text or "")
        s = re.sub(r"<@[!&]?\d+>", " ", s)
        s = s.replace("@everyone", " ").replace("@here", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _bundle_candidate(*, content: str, embeds: List[Dict[str, Any]], attachments: List[Dict[str, Any]]) -> bool:
        # Attachments-only consecutive posts by same author should be grouped.
        if not attachments:
            return False
        if embeds:
            return False
        cleaned = _strip_mentions_only(content)
        if not cleaned:
            return True
        # Treat "..." / "." / punctuation-only as empty for bundling.
        if re.fullmatch(r"[\W_]+", cleaned):
            return True
        return False

    async def _process_msgs(
        *,
        msgs_to_send: List[Dict[str, Any]],
        dest_channel,
        src_channel_id: int,
        src_channel_name: str,
        last_cursor_id: str,
    ) -> Tuple[int, int, str, int]:
        """
        Returns (sent_inc, would_send_inc, new_last_cursor_id, errors_inc).
        - In dryrun: counts would_send (outbound message count).
        - In run: counts sent (outbound message count).
        """
        sent_inc = 0
        would_inc = 0
        err_inc = 0
        cursor_id = str(last_cursor_id or "").strip()

        i = 0
        while i < len(msgs_to_send or []):
            m = msgs_to_send[i]
            if not isinstance(m, dict):
                i += 1
                continue
            mid = str(m.get("id") or "").strip()
            content = str(m.get("content") or "")
            attachments = m.get("attachments") if isinstance(m.get("attachments"), list) else []
            embeds = m.get("embeds") if isinstance(m.get("embeds"), list) else []
            embeds_dicts_raw = [e for e in embeds if isinstance(e, dict)]
            author = m.get("author") if isinstance(m.get("author"), dict) else {}
            author_id = str(author.get("id") or "").strip()
            wh_username = _author_display_name(author) or str(source_guild_name or "")
            wh_avatar = _author_avatar_url(author) or str(source_guild_icon_url or "")

            # Group consecutive attachment-only messages by the same author.
            if _bundle_candidate(content=content, embeds=embeds_dicts_raw, attachments=attachments):
                bundle_attachments: List[Dict[str, Any]] = []
                bundle_last_id = mid
                j = i
                while j < len(msgs_to_send):
                    mm = msgs_to_send[j]
                    if not isinstance(mm, dict):
                        break
                    mmid = str(mm.get("id") or "").strip()
                    mmcontent = str(mm.get("content") or "")
                    mmattachments = mm.get("attachments") if isinstance(mm.get("attachments"), list) else []
                    mmembeds = mm.get("embeds") if isinstance(mm.get("embeds"), list) else []
                    mmauthor = mm.get("author") if isinstance(mm.get("author"), dict) else {}
                    mmauthor_id = str(mmauthor.get("id") or "").strip()
                    if mmauthor_id != author_id:
                        break
                    if not _bundle_candidate(
                        content=mmcontent, embeds=[e for e in mmembeds if isinstance(e, dict)], attachments=mmattachments
                    ):
                        break
                    for a in mmattachments:
                        if isinstance(a, dict):
                            bundle_attachments.append(a)
                    if mmid:
                        bundle_last_id = mmid
                    j += 1

                # Build one outbound message
                non_image_urls = []
                for a in bundle_attachments:
                    if not isinstance(a, dict):
                        continue
                    if is_image_attachment(a):
                        continue
                    u = str(a.get("url") or a.get("proxy_url") or "").strip()
                    if u:
                        non_image_urls.append(u)
                out_content = ""
                if non_image_urls:
                    out_content = "\n".join(non_image_urls[:10]).strip()
                # Reserve 1 embed slot for branding, rest for images
                embed_body = append_image_attachments_as_embeds([], bundle_attachments, max_embeds=9)
                embeds_out: List[Dict[str, Any]] = [_source_brand_embed(
                    source_guild_id=source_guild_id,
                    source_guild_name=source_guild_name,
                    source_guild_icon_url=source_guild_icon_url,
                    source_channel_id=int(src_channel_id),
                    source_channel_name=str(src_channel_name or ""),
                )] + embed_body

                # If nothing to send, just advance cursor.
                if not out_content and not embed_body:
                    if bundle_last_id:
                        cursor_id = bundle_last_id
                    i = j
                    continue

                if dryrun:
                    would_inc += 1
                    if bundle_last_id:
                        cursor_id = bundle_last_id
                    i = j
                    continue

                try:
                    await _send_message_to_channel(
                        dest_channel=dest_channel,
                        content=out_content[:1950],
                        embeds=embeds_out[:10],
                        webhook_username=wh_username,
                        webhook_avatar_url=wh_avatar,
                        reason="MWDataManagerBot fetchsync bundle",
                    )
                    sent_inc += 1
                    if bundle_last_id:
                        cursor_id = bundle_last_id
                    if send_min_interval > 0:
                        await asyncio.sleep(send_min_interval)
                except Exception as e:
                    log_warn(f"[FETCHSYNC] send_failed source_channel_id={src_channel_id} ({type(e).__name__}: {e})")
                    err_inc += 1
                    break
                i = j
                continue

            # Non-bundled message: apply substantive filter
            if not _is_substantive_fetched_message(
                content=content, embeds=embeds_dicts_raw, attachments=attachments, min_chars=min_chars
            ):
                if mid:
                    cursor_id = mid
                i += 1
                continue

            # Build embeds (reserve slot for branding)
            embed_body = format_embeds_for_forwarding(embeds_dicts_raw)[:9]
            try:
                embed_body = append_image_attachments_as_embeds(embed_body, attachments, max_embeds=9)
            except Exception:
                embed_body = embed_body
            # Non-image attachments as URLs
            non_image_urls = []
            for a in attachments:
                if not isinstance(a, dict):
                    continue
                if is_image_attachment(a):
                    continue
                u = str(a.get("url") or a.get("proxy_url") or "").strip()
                if u:
                    non_image_urls.append(u)
            if non_image_urls:
                content = (content + "\n" + "\n".join(non_image_urls[:10])).strip()

            # If nothing, advance cursor.
            if not content and not embed_body:
                if mid:
                    cursor_id = mid
                i += 1
                continue

            embeds_out: List[Dict[str, Any]] = [_source_brand_embed(
                source_guild_id=source_guild_id,
                source_guild_name=source_guild_name,
                source_guild_icon_url=source_guild_icon_url,
                source_channel_id=int(src_channel_id),
                source_channel_name=str(src_channel_name or ""),
            )] + embed_body

            if dryrun:
                would_inc += 1
                if mid:
                    cursor_id = mid
                i += 1
                continue

            try:
                await _send_message_to_channel(
                    dest_channel=dest_channel,
                    content=str(content or "")[:1950],
                    embeds=embeds_out[:10],
                    webhook_username=wh_username,
                    webhook_avatar_url=wh_avatar,
                    reason="MWDataManagerBot fetchsync",
                )
                sent_inc += 1
                if mid:
                    cursor_id = mid
                if send_min_interval > 0:
                    await asyncio.sleep(send_min_interval)
            except Exception as e:
                log_warn(f"[FETCHSYNC] send_failed source_channel_id={src_channel_id} ({type(e).__name__}: {e})")
                err_inc += 1
                break
            i += 1

        return sent_inc, would_inc, cursor_id, err_inc

    for src_id, src_name in src_channels_to_mirror:
        sid = int(src_id or 0)
        if sid <= 0:
            continue
        channels_processed += 1

        dest_channel = mirror_by_source.get(sid)
        if dest_channel is None and not dryrun:
            continue

        cursor = _get_cursor(entry, source_channel_id=sid)
        total_fetched = 0
        last_cursor_id: str = ""

        if not cursor:
            ok, msgs, reason = await _fetch_channel_messages_page(
                source_channel_id=sid, user_token=token, limit=per_channel_limit, after=None
            )
            if not ok:
                log_warn(f"[FETCHSYNC] source_channel_id={sid} fetch_failed reason={reason}")
                errors += 1
                continue
            msgs_to_send = list(reversed(msgs))
            s_inc, w_inc, new_cursor, e_inc = await _process_msgs(
                msgs_to_send=msgs_to_send,
                dest_channel=dest_channel,
                src_channel_id=sid,
                src_channel_name=str(src_name or ""),
                last_cursor_id="",
            )
            sent += int(s_inc)
            would_send += int(w_inc)
            errors += int(e_inc)
            if new_cursor:
                persist_channel_cursor(source_guild_id=source_guild_id, source_channel_id=sid, last_seen_message_id=str(new_cursor))
            continue

        after = cursor
        while total_fetched < max_per_channel:
            page_limit = min(50, max_per_channel - total_fetched)
            ok, msgs, reason = await _fetch_channel_messages_page(
                source_channel_id=sid, user_token=token, limit=page_limit, after=after
            )
            if not ok:
                log_warn(f"[FETCHSYNC] source_channel_id={sid} fetch_failed reason={reason}")
                errors += 1
                break
            if not msgs:
                break
            msgs_to_send = list(reversed(msgs))
            total_fetched += len(msgs_to_send)
            s_inc, w_inc, new_cursor, e_inc = await _process_msgs(
                msgs_to_send=msgs_to_send,
                dest_channel=dest_channel,
                src_channel_id=sid,
                src_channel_name=str(src_name or ""),
                last_cursor_id=last_cursor_id,
            )
            sent += int(s_inc)
            would_send += int(w_inc)
            errors += int(e_inc)
            if new_cursor:
                last_cursor_id = str(new_cursor)
                after = str(new_cursor)
            else:
                break
            # if we hit a send error, stop paging this channel to preserve ordering
            if e_inc:
                break

        if last_cursor_id:
            persist_channel_cursor(source_guild_id=source_guild_id, source_channel_id=sid, last_seen_message_id=last_cursor_id)

        if progress_cb is not None:
            try:
                await progress_cb(
                    {
                        "stage": "channels",
                        "dryrun": bool(dryrun),
                        "source_guild_id": int(source_guild_id),
                        "source_guild_name": str(source_guild_name),
                        "destination_category_id": int(dest_category_id),
                        "channels_total": int(channels_total),
                        "channels_processed": int(channels_processed),
                        "current_channel_id": int(sid),
                        "created_mirror_channels": int(created_channels),
                        "sent": int(sent),
                        "would_send": int(would_send),
                        "errors": int(errors),
                    }
                )
            except Exception:
                pass

    try:
        log_fetchall(
            f"FETCHSYNC source={source_guild_id} dest_category={dest_category_id} channels={len(src_channels_to_mirror)} "
            f"created_channels={created_channels} dryrun={bool(dryrun)} would_send={would_send} sent={sent}"
        )
    except Exception:
        pass

    return {
        "ok": True,
        "source_guild_id": source_guild_id,
        "destination_category_id": dest_category_id,
        "channels": len(src_channels_to_mirror),
        "created_channels": created_channels,
        "would_send": would_send,
        "sent": sent,
        "errors": errors,
    }

