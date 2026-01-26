from __future__ import annotations

import asyncio
import json
import os
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from logging_utils import log_fetchall, log_warn
import settings_store as cfg

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_FETCHALL_PATH = _CONFIG_DIR / "fetchall_mappings.json"

_FILE_LOCK = threading.RLock()
_DISCORD_API_BASE = "https://discord.com/api/v9"


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

    slug = re.sub(r"[^a-z0-9-]+", "-", (name or "").lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    if not slug:
        slug = f"{fallback_prefix}-{int(time.time())}"
    return slug[:90]


MIRROR_TOPIC_PREFIX = "MIRROR:"


def _build_mirror_topic(source_guild_id: int, source_channel_id: int) -> str:
    return f"{MIRROR_TOPIC_PREFIX}{int(source_guild_id)}:{int(source_channel_id)}"


def _parse_mirror_topic(topic: Optional[str]) -> Optional[tuple[int, int]]:
    if not topic or not topic.startswith(MIRROR_TOPIC_PREFIX):
        return None
    payload = topic[len(MIRROR_TOPIC_PREFIX) :]
    parts = payload.split(":")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
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
        try:
            ch_type = int(ch.get("type") or -1)
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
            t = int(ch.get("type") or -1)
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


def _format_embeds_for_forwarding(embeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Trim/clean embeds to a safe dict shape before sending."""
    out: List[Dict[str, Any]] = []
    for e in embeds or []:
        if not isinstance(e, dict):
            continue
        embed: Dict[str, Any] = {}
        if e.get("title"):
            embed["title"] = e.get("title")
        if e.get("url"):
            embed["url"] = e.get("url")
        desc = e.get("description") or ""
        fields = e.get("fields") if isinstance(e.get("fields"), list) else []
        if desc or fields:
            embed["description"] = desc or "\u200b"
            embed_fields = []
            for field in fields:
                if not isinstance(field, dict):
                    continue
                name = field.get("name") or "\u200b"
                value = field.get("value")
                if not value:
                    continue
                cleaned = {"name": name, "value": value}
                if field.get("inline") is not None:
                    cleaned["inline"] = field.get("inline")
                embed_fields.append(cleaned)
            if embed_fields:
                embed["fields"] = embed_fields
        if "image" in e and isinstance(e.get("image"), dict) and e["image"].get("url"):
            embed["image"] = {"url": e["image"]["url"]}
        if "thumbnail" in e and isinstance(e.get("thumbnail"), dict) and e["thumbnail"].get("url"):
            embed["thumbnail"] = {"url": e["thumbnail"]["url"]}
        if "author" in e and isinstance(e.get("author"), dict) and e["author"].get("name"):
            embed["author"] = {"name": e["author"].get("name"), "url": e["author"].get("url")}
        if "footer" in e and isinstance(e.get("footer"), dict) and e["footer"].get("text"):
            embed["footer"] = {"text": e["footer"].get("text")}
        if embed:
            out.append(embed)
    return out[:10]


def _chunk_text(text: str, limit: int = 2000) -> List[str]:
    if not text:
        return [""]
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        chunks.append(remaining[:limit])
        remaining = remaining[limit:]
    return chunks


async def _send_message_to_channel(*, dest_channel, content: str, embeds: List[Dict[str, Any]]) -> None:
    try:
        import discord
    except Exception:
        discord = None  # type: ignore
    allowed_mentions = None
    try:
        if discord is not None:
            allowed_mentions = discord.AllowedMentions.none()
    except Exception:
        allowed_mentions = None

    embed_objs = []
    if discord is not None:
        for ed in embeds or []:
            try:
                embed_objs.append(discord.Embed.from_dict(ed))
            except Exception:
                continue

    chunks = _chunk_text(content, 2000)
    for i, chunk in enumerate(chunks):
        if i == 0 and embed_objs:
            await dest_channel.send(content=chunk, embeds=embed_objs[:10], allowed_mentions=allowed_mentions)
        else:
            await dest_channel.send(content=chunk, allowed_mentions=allowed_mentions)


async def run_fetchall(
    *,
    bot,
    entry: Dict[str, Any],
    destination_guild,
    source_user_token: Optional[str] = None,
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
        return {"ok": False, "reason": "missing_source_guild_id"}

    dest_category_id = int(entry.get("destination_category_id", 0) or 0) or int(cfg.FETCHALL_DEFAULT_DEST_CATEGORY_ID or 0)
    if dest_category_id <= 0:
        return {"ok": False, "reason": "missing_destination_category_id"}

    if destination_guild is None:
        # pick first destination guild the bot is in
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

    source_guild = bot.get_guild(int(source_guild_id))
    source_guild_name = str(entry.get("name") or "").strip() or f"guild_{source_guild_id}"
    await _ensure_separator(dest_category, source_guild_id=source_guild_id, source_guild_name=source_guild_name)

    source_category_ids = entry.get("source_category_ids") if isinstance(entry.get("source_category_ids"), list) else []
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
        if source_category_ids:
            for cid in source_category_ids:
                try:
                    cat = source_guild.get_channel(int(cid))
                except Exception:
                    cat = None
                if isinstance(cat, discord.CategoryChannel):
                    source_categories.append(cat)
        else:
            source_categories = list(getattr(source_guild, "categories", []) or [])

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
        token = str(source_user_token or "").strip()
        if not token:
            return {"ok": False, "reason": f"bot_not_in_source_guild:{source_guild_id} and no source_user_token provided"}
        status, api_channels = await _fetch_guild_channels_via_user_token(source_guild_id=source_guild_id, user_token=token)
        if not api_channels:
            return {
                "ok": False,
                "reason": f"failed_to_list_source_channels_via_token:{source_guild_id}",
                "http_status": int(status or 0),
            }
        selected = _select_source_text_channels_from_api(
            api_channels,
            source_category_ids=[int(x) for x in (source_category_ids or []) if int(x) > 0],
            ignored_channel_ids=ignored_ids,
        )
        src_channels_to_mirror.extend(selected)

    # Build existing mirror index by topic
    existing_by_source: dict[int, discord.TextChannel] = {}
    for ch in list(getattr(dest_category, "text_channels", []) or []):
        info = _parse_mirror_topic(getattr(ch, "topic", None))
        if info and info[0] == source_guild_id:
            existing_by_source[info[1]] = ch

    created = 0
    kept = 0
    attempted = 0
    for src_id, src_name in src_channels_to_mirror:
        attempted += 1
        src_id = int(src_id or 0)
        if src_id <= 0:
            continue
        if src_id in existing_by_source:
            kept += 1
            continue
        desired_name = _slugify_channel_name(str(src_name), fallback_prefix="mirror")
        topic = _build_mirror_topic(source_guild_id, src_id)
        full_topic = f"{topic} | source={source_guild_name}#{src_name}"
        # ensure unique name within category
        final_name = desired_name
        taken = {c.name for c in dest_category.text_channels}
        if final_name in taken:
            final_name = (final_name[:80] + f"-{str(src_id)[-4:]}")[:90]
        try:
            await destination_guild.create_text_channel(
                final_name,
                category=dest_category,
                topic=full_topic,
                reason="MWDataManagerBot fetchall mirror channel",
            )
            created += 1
        except Exception as e:
            log_warn(f"[FETCHALL] failed to create mirror for {source_guild_id}:{src_id}: {e}")

    log_fetchall(
        f"source={source_guild_id} dest_category={dest_category_id} mode={mode} attempted={attempted} created={created} existing={kept}"
    )
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
                extra["source_category_ids"] = [int(x) for x in (source_category_ids or []) if int(x) > 0]
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
    }


async def run_fetchsync(
    *,
    bot,
    entry: Dict[str, Any],
    destination_guild,
    source_user_token: str,
    dryrun: bool = False,
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

    # In dryrun we avoid creating channels; for run mode we ensure separator exists.
    if not dryrun:
        try:
            await _ensure_separator(dest_category, source_guild_id=source_guild_id, source_guild_name=source_guild_name)
        except Exception:
            pass

    source_category_ids = entry.get("source_category_ids") if isinstance(entry.get("source_category_ids"), list) else []
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
        source_category_ids=[int(x) for x in (source_category_ids or []) if int(x) > 0],
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
            extra["source_category_ids"] = [int(x) for x in (source_category_ids or []) if int(x) > 0]
            extra["ignored_count"] = int(len(ignored_ids or set()))
        except Exception:
            pass
        return extra

    # Build existing mirror index by topic
    existing_by_source: dict[int, discord.TextChannel] = {}
    for ch in list(getattr(dest_category, "text_channels", []) or []):
        info = _parse_mirror_topic(getattr(ch, "topic", None))
        if info and info[0] == source_guild_id:
            existing_by_source[info[1]] = ch

    mirror_by_source: Dict[int, discord.TextChannel] = dict(existing_by_source)
    created_channels = 0
    if not dryrun:
        taken = {c.name for c in dest_category.text_channels}
        for src_id, src_name in src_channels_to_mirror:
            sid = int(src_id or 0)
            if sid <= 0 or sid in mirror_by_source:
                continue
            desired_name = _slugify_channel_name(str(src_name), fallback_prefix="mirror")
            topic = _build_mirror_topic(source_guild_id, sid)
            full_topic = f"{topic} | source={source_guild_name}#{src_name}"
            final_name = desired_name
            if final_name in taken:
                final_name = (final_name[:80] + f"-{str(sid)[-4:]}")[:90]
            try:
                created = await destination_guild.create_text_channel(
                    final_name,
                    category=dest_category,
                    topic=full_topic,
                    reason="MWDataManagerBot fetchsync mirror channel",
                )
                mirror_by_source[sid] = created
                taken.add(final_name)
                created_channels += 1
            except Exception as e:
                log_warn(f"[FETCHSYNC] failed to create mirror for {source_guild_id}:{sid}: {e}")

    channels_processed = 0
    would_send = 0
    sent = 0
    per_channel_limit = 50  # initial backfill limit (your selection)
    max_per_channel = int(getattr(cfg, "FETCHALL_MAX_MESSAGES_PER_CHANNEL", 400) or 400)
    send_min_interval = float(getattr(cfg, "SEND_MIN_INTERVAL_SECONDS", 0.0) or 0.0)

    for src_id, _src_name in src_channels_to_mirror:
        sid = int(src_id or 0)
        if sid <= 0:
            continue
        channels_processed += 1

        dest_channel = mirror_by_source.get(sid)
        if dest_channel is None and not dryrun:
            continue

        cursor = _get_cursor(entry, source_channel_id=sid)
        total_fetched = 0
        last_sent_id: str = ""

        if not cursor:
            ok, msgs, reason = await _fetch_channel_messages_page(
                source_channel_id=sid, user_token=token, limit=per_channel_limit, after=None
            )
            if not ok:
                log_warn(f"[FETCHSYNC] source_channel_id={sid} fetch_failed reason={reason}")
                continue
            msgs_to_send = list(reversed(msgs))
            if dryrun:
                would_send += len(msgs_to_send)
                if msgs_to_send:
                    last_sent_id = str(msgs_to_send[-1].get("id") or "").strip()
            else:
                for m in msgs_to_send:
                    mid = str(m.get("id") or "").strip()
                    content = str(m.get("content") or "")
                    attachments = m.get("attachments") if isinstance(m.get("attachments"), list) else []
                    att_urls = [str(a.get("url") or "") for a in attachments if isinstance(a, dict) and a.get("url")]
                    embeds = m.get("embeds") if isinstance(m.get("embeds"), list) else []
                    embed_dicts = _format_embeds_for_forwarding([e for e in embeds if isinstance(e, dict)])
                    if att_urls:
                        content = (content + "\n" + "\n".join(att_urls[:10])).strip()
                    if not content and not embed_dicts:
                        continue
                    try:
                        await _send_message_to_channel(dest_channel=dest_channel, content=content, embeds=embed_dicts)
                        sent += 1
                        if mid:
                            last_sent_id = mid
                        if send_min_interval > 0:
                            await asyncio.sleep(send_min_interval)
                    except Exception as e:
                        log_warn(f"[FETCHSYNC] send_failed source_channel_id={sid} ({type(e).__name__}: {e})")
                        break
            if (not dryrun) and last_sent_id:
                persist_channel_cursor(source_guild_id=source_guild_id, source_channel_id=sid, last_seen_message_id=last_sent_id)
            continue

        after = cursor
        while total_fetched < max_per_channel:
            page_limit = min(50, max_per_channel - total_fetched)
            ok, msgs, reason = await _fetch_channel_messages_page(
                source_channel_id=sid, user_token=token, limit=page_limit, after=after
            )
            if not ok:
                log_warn(f"[FETCHSYNC] source_channel_id={sid} fetch_failed reason={reason}")
                break
            if not msgs:
                break
            msgs_to_send = list(reversed(msgs))
            total_fetched += len(msgs_to_send)
            if dryrun:
                would_send += len(msgs_to_send)
                last_id = str(msgs_to_send[-1].get("id") or "").strip()
                if last_id:
                    after = last_id
                    last_sent_id = last_id
                else:
                    break
                continue

            for m in msgs_to_send:
                mid = str(m.get("id") or "").strip()
                content = str(m.get("content") or "")
                attachments = m.get("attachments") if isinstance(m.get("attachments"), list) else []
                att_urls = [str(a.get("url") or "") for a in attachments if isinstance(a, dict) and a.get("url")]
                embeds = m.get("embeds") if isinstance(m.get("embeds"), list) else []
                embed_dicts = _format_embeds_for_forwarding([e for e in embeds if isinstance(e, dict)])
                if att_urls:
                    content = (content + "\n" + "\n".join(att_urls[:10])).strip()
                if not content and not embed_dicts:
                    continue
                try:
                    await _send_message_to_channel(dest_channel=dest_channel, content=content, embeds=embed_dicts)
                    sent += 1
                    if mid:
                        last_sent_id = mid
                    if send_min_interval > 0:
                        await asyncio.sleep(send_min_interval)
                except Exception as e:
                    log_warn(f"[FETCHSYNC] send_failed source_channel_id={sid} ({type(e).__name__}: {e})")
                    break

            if last_sent_id:
                after = last_sent_id
            else:
                break

        if (not dryrun) and last_sent_id:
            persist_channel_cursor(source_guild_id=source_guild_id, source_channel_id=sid, last_seen_message_id=last_sent_id)

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
    }

