from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from logging_utils import log_fetchall, log_warn
import settings_store as cfg

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_FETCHALL_PATH = _CONFIG_DIR / "fetchall_mappings.json"


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
        _FETCHALL_PATH.parent.mkdir(parents=True, exist_ok=True)
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


async def _fetch_guild_channels_via_user_token(*, source_guild_id: int, user_token: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch source guild channel list using a Discum/user token (fallback for fetchall)."""
    if source_guild_id <= 0 or not user_token:
        return None
    try:
        import aiohttp  # type: ignore
    except Exception:
        return None
    try:
        headers = {"Authorization": str(user_token).strip()}
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            url = f"https://discord.com/api/v9/guilds/{int(source_guild_id)}/channels"
            async with session.get(url) as resp:
                if int(getattr(resp, "status", 0) or 0) != 200:
                    return None
                data = await resp.json()
                return data if isinstance(data, list) else None
    except Exception:
        return None


def _select_source_text_channels_from_api(
    channels: List[Dict[str, Any]],
    *,
    source_category_ids: List[int],
    ignored_channel_ids: Set[int],
) -> List[Tuple[int, str]]:
    """
    Return list of (channel_id, channel_name) for text channels.
    Discord channel type 0 = GUILD_TEXT.
    """
    out: List[Tuple[int, str]] = []
    allow_categories: Set[int] = {int(x) for x in (source_category_ids or []) if int(x) > 0}
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
        if ch_type != 0:
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
        api_channels = await _fetch_guild_channels_via_user_token(source_guild_id=source_guild_id, user_token=token)
        if not api_channels:
            return {"ok": False, "reason": f"failed_to_list_source_channels_via_token:{source_guild_id}"}
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
    return {
        "ok": True,
        "source_guild_id": source_guild_id,
        "destination_category_id": dest_category_id,
        "attempted": attempted,
        "created": created,
        "existing": kept,
    }

