"""
Standalone script: post an organized, guild-by-guild summary of channel mappings
to a fixed Discord channel (#mirror-channel-map). Does not modify discum_command_bot
or any other code.

IMPORTANT — API usage:
  When resolving missing source names, this script uses the user token and makes
  one API call per missing channel (and per guild), with 5s delay between calls.
  Run this script sparingly (e.g. once per day or after updating mappings), not in tight loops.

Run from repo root or MWDiscumBot:
  python -m MWBots.MWDiscumBot.post_mirror_channel_map
  # or from MWDiscumBot:
  python post_mirror_channel_map.py

Uses BOT_TOKEN from MWDiscumBot/config/tokens.env. The bot must be in the server
that contains the output channel and all destination channels (for resolving names).

Guild/source channel names come from (in order):
  1. source_channels.json (guilds + channels from discumbot user)
  2. channel_map_info.json (from scripts/verify_discum_channel_ids.py)
  3. Discord API with DISCUM_USER_DISCUMBOT (fetches missing channel/guild names, 5s delay)

Set DISCUM_USER_DISCUMBOT in tokens.env to avoid "Unknown guild" / "#channel-<id>"
for source channels not yet in the JSON files.
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure we can load MWDiscumBot config
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR.parent.parent))

import discord
import requests

# Output channel: #mirror-channel-map
OUTPUT_CHANNEL_ID = 1482374889594818681

# Rate limiting: delay between API calls (seconds). Be conservative to avoid flags.
DELAY_BETWEEN_CHANNEL_FETCH = 5.0
DELAY_AFTER_GUILD_FETCH = 5.0
DELAY_BETWEEN_WEBHOOK_LOOKUPS = 5.0

CONFIG_DIR = _SCRIPT_DIR / "config"
CHANNEL_MAP_PATH = CONFIG_DIR / "channel_map.json"
SOURCE_CHANNELS_PATH = CONFIG_DIR / "source_channels.json"
CHANNEL_MAP_INFO_PATH = CONFIG_DIR / "channel_map_info.json"
TOKENS_ENV_PATH = CONFIG_DIR / "tokens.env"


def load_env(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip().strip("'\"").strip()
                if key:
                    out[key] = value
    except Exception:
        pass
    return out


def load_channel_map() -> Dict[int, str]:
    out: Dict[int, str] = {}
    try:
        with open(CHANNEL_MAP_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return out
        for k, v in data.items():
            try:
                cid = int(str(k).strip())
                url = str(v or "").strip()
                if url:
                    out[cid] = url
            except (ValueError, TypeError):
                continue
    except Exception:
        pass
    return out


def load_source_channel_info() -> Tuple[Dict[int, int], Dict[int, str], Dict[int, str]]:
    """Returns (channel_id -> guild_id, channel_id -> channel_name, guild_id -> guild_name).
    Loads source_channels.json first, then merges in channel_map_info.json if present.
    """
    ch_to_guild: Dict[int, int] = {}
    ch_to_name: Dict[int, str] = {}
    guild_to_name: Dict[int, str] = {}
    # 1) source_channels.json (guilds + channels)
    try:
        with open(SOURCE_CHANNELS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for guild in data.get("guilds", []) or []:
            try:
                gid = int(guild.get("guild_id", 0) or 0)
            except (TypeError, ValueError):
                continue
            gname = (guild.get("guild_name") or f"Guild-{gid}").strip() or "Unknown"
            guild_to_name[gid] = gname
            for ch in guild.get("channels", []) or []:
                try:
                    cid = int(ch.get("id", 0) or 0)
                    cname = (ch.get("name") or f"channel-{cid}").strip() or "?"
                    ch_to_guild[cid] = gid
                    ch_to_name[cid] = cname
                except (TypeError, ValueError):
                    continue
    except Exception:
        pass
    # 2) channel_map_info.json (from scripts/verify_discum_channel_ids.py) — fills gaps
    try:
        if CHANNEL_MAP_INFO_PATH.exists():
            with open(CHANNEL_MAP_INFO_PATH, "r", encoding="utf-8") as f:
                info = json.load(f)
            for cid_str, ent in (info.get("channels") or {}).items():
                try:
                    cid = int(cid_str)
                    gid = int(ent.get("guild_id", 0) or 0)
                    gname = (ent.get("guild_name") or f"Guild-{gid}").strip() or "Unknown"
                    cname = (ent.get("name") or f"channel-{cid}").strip() or "?"
                    ch_to_guild[cid] = gid
                    ch_to_name[cid] = cname
                    if gid:
                        guild_to_name[gid] = gname
                except (TypeError, ValueError, AttributeError):
                    continue
    except Exception:
        pass
    return ch_to_guild, ch_to_name, guild_to_name


def fetch_missing_source_names(
    user_token: str,
    source_cids: List[int],
    ch_to_guild: Dict[int, int],
    ch_to_name: Dict[int, str],
    guild_to_name: Dict[int, str],
) -> None:
    """Fetch channel + guild name from Discord API (user token) for IDs not in the dicts. Mutates the three dicts. Run in executor. Rate-limited and capped."""
    if not user_token or not source_cids:
        return
    headers = {"Content-Type": "application/json", "Authorization": user_token}
    to_fetch = [c for c in source_cids if c not in ch_to_name or ch_to_guild.get(c, 0) not in guild_to_name]
    for i, cid in enumerate(to_fetch):
        if i > 0:
            time.sleep(DELAY_BETWEEN_CHANNEL_FETCH)
        try:
            r = requests.get(f"https://discord.com/api/v10/channels/{cid}", headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            data = r.json()
            gid = int(data.get("guild_id", 0) or 0)
            cname = (data.get("name") or "").strip() or f"channel-{cid}"
            ch_to_guild[cid] = gid
            ch_to_name[cid] = cname
            if gid and gid not in guild_to_name:
                time.sleep(DELAY_AFTER_GUILD_FETCH)
                r2 = requests.get(f"https://discord.com/api/v10/guilds/{gid}", headers=headers, timeout=10)
                if r2.status_code == 200:
                    gname = (r2.json().get("name") or "").strip() or f"Guild-{gid}"
                    guild_to_name[gid] = gname
        except Exception:
            continue


def resolve_webhook_dest_http(webhook_url: str) -> Tuple[Optional[int], str]:
    """Call Discord API for webhook; return (channel_id, fallback_name). No bot. Run in executor."""
    try:
        m = re.search(r"/webhooks/(\d+)/([^/?]+)", webhook_url)
        if not m:
            return None, "?"
        wh_id, wh_token = m.group(1), m.group(2)
        url = f"https://discord.com/api/v10/webhooks/{wh_id}/{wh_token}"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None, "?"
        data = resp.json()
        cid = data.get("channel_id")
        if cid is None:
            return None, (data.get("name") or "?")
        return int(cid), (data.get("name") or "?")
    except Exception:
        return None, "?"


def build_by_guild(
    channel_map: Dict[int, str],
    ch_to_guild: Dict[int, int],
    ch_to_name: Dict[int, str],
    guild_to_name: Dict[int, str],
    webhook_dest_cache: Dict[str, Tuple[Optional[int], str]],
) -> List[Tuple[int, str, List[Tuple[Optional[int], str, List[Tuple[str, int]]]]]]:
    """
    Returns list of (guild_id, guild_name, destination_buckets) sorted by guild name.
    destination_buckets item: (dest_cid, dest_display, [(source_name, source_cid), ...])
    """
    by_guild: Dict[int, List[Tuple[int, str]]] = {}
    for src_cid, wh_url in channel_map.items():
        if not wh_url:
            continue
        gid = ch_to_guild.get(src_cid, 0)
        if gid <= 0:
            gid = 0
        by_guild.setdefault(gid, []).append((src_cid, wh_url))

    result: List[Tuple[int, str, List[Tuple[Optional[int], str, List[Tuple[str, int]]]]]] = []
    for gid in sorted(by_guild.keys(), key=lambda x: guild_to_name.get(x, "").lower()):
        gname = guild_to_name.get(gid, f"Guild-{gid}" if gid else "Unknown guild")
        grouped_dest: Dict[str, Tuple[Optional[int], str, List[Tuple[str, int]]]] = {}
        for src_cid, wh_url in by_guild[gid]:
            src_name = ch_to_name.get(src_cid, f"channel-{src_cid}")
            dest_cid, dest_name = webhook_dest_cache.get(wh_url, (None, "?"))
            dest_display = f"<#{dest_cid}>" if dest_cid else (dest_name or "?")
            key = str(dest_cid) if dest_cid else f"name:{dest_name}"
            if key not in grouped_dest:
                grouped_dest[key] = (dest_cid, dest_display, [])
            grouped_dest[key][2].append((src_name, src_cid))

        buckets = list(grouped_dest.values())
        # Sort destinations by rendered display; sources by source channel name.
        buckets.sort(key=lambda b: (str(b[1]).lower(), int(b[0] or 0)))
        for idx, (dcid, ddisp, srcs) in enumerate(buckets):
            srcs.sort(key=lambda s: (s[0].lower(), s[1]))
            buckets[idx] = (dcid, ddisp, srcs)
        result.append((gid, gname, buckets))
    return result


def _build_guild_embed_parts(
    guild_name: str,
    guild_id: int,
    destination_buckets: List[Tuple[Optional[int], str, List[Tuple[str, int]]]],
) -> List[discord.Embed]:
    """
    Build one or more embeds for a single guild using compact destination-grouped layout.
    The layout places two destination groups side-by-side using padded text rows.
    """
    # Build textual rows first, then chunk into 4096-safe embed descriptions.
    rows: List[str] = []
    # Padding width for the left destination column when rendering two columns.
    # Discord wraps aggressively in embeds; keeping this smaller reduces total line length.
    col_width = 46
    for i in range(0, len(destination_buckets), 2):
        left = destination_buckets[i]
        right = destination_buckets[i + 1] if i + 1 < len(destination_buckets) else None

        left_header = left[1]
        right_header = right[1] if right else ""
        rows.append(f"{left_header.ljust(col_width)}{right_header}")

        left_lines = [f"-# {src_name} <#{src_id}>" for src_name, src_id in left[2]]
        right_lines = [f"-# {src_name} <#{src_id}>" for src_name, src_id in (right[2] if right else [])]
        max_len = max(len(left_lines), len(right_lines), 1)
        for j in range(max_len):
            ltxt = left_lines[j] if j < len(left_lines) else ""
            rtxt = right_lines[j] if j < len(right_lines) else ""
            rows.append(f"{ltxt.ljust(col_width)}{rtxt}".rstrip())
        rows.append("")

    if not rows:
        rows = ["_No mappings_"]

    # Split into multiple embeds for large guilds.
    parts: List[str] = []
    current = ""
    for line in rows:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > 3900:
            parts.append(current)
            current = line
        else:
            current = candidate
    if current:
        parts.append(current)
    if not parts:
        parts = ["_No mappings_"]

    total_sources = sum(len(bucket[2]) for bucket in destination_buckets)
    embeds: List[discord.Embed] = []
    for idx, part in enumerate(parts):
        title = f"📁 {guild_name} - `{guild_id}`"
        if len(parts) > 1:
            title = f"{title} (part {idx + 1}/{len(parts)})"
        emb = discord.Embed(
            title=title,
            description=part,
            color=0x5865F2,
        )
        emb.set_footer(text=f"Guild ID: {guild_id}  •  {total_sources} channel(s)")
        embeds.append(emb)
    return embeds


def build_embeds(
    by_guild: List[Tuple[int, str, List[Tuple[Optional[int], str, List[Tuple[str, int]]]]]]
) -> List[discord.Embed]:
    """Build one or more embeds per guild in compact destination-grouped layout."""
    embeds: List[discord.Embed] = []
    for gid, gname, destination_buckets in by_guild:
        embeds.extend(_build_guild_embed_parts(gname, gid, destination_buckets))
    return embeds


def _progress(msg: str) -> None:
    print(f"[post_mirror_channel_map] {msg}", flush=True)


def _parse_guild_id_from_embed(embed: discord.Embed) -> Optional[int]:
    """Extract Guild ID from embed footer text (matches build_embeds footer format)."""
    try:
        footer_text = getattr(getattr(embed, "footer", None), "text", None) or ""
        m = re.search(r"Guild ID:\s*(\d+)", str(footer_text))
        return int(m.group(1)) if m else None
    except Exception:
        return None


async def _find_existing_guild_message(
    channel: Any,
    guild_id: int,
    *,
    history_limit: int,
) -> Optional[discord.Message]:
    """Find an existing mirror-channel-map message for a given guild by scanning recent history."""
    try:
        async for msg in channel.history(limit=history_limit):
            for emb in getattr(msg, "embeds", []) or []:
                parsed_gid = _parse_guild_id_from_embed(emb)
                if parsed_gid == int(guild_id):
                    return msg
    except Exception:
        return None
    return None


async def _cleanup_existing_mapping_messages(
    channel: Any,
    *,
    bot_user_id: int,
    history_limit: int,
) -> int:
    """
    Delete previous mapping report messages authored by this bot.
    We only delete messages that have an embed with footer containing 'Guild ID:'.
    """
    deleted = 0
    try:
        async for msg in channel.history(limit=history_limit):
            if int(getattr(getattr(msg, "author", None), "id", 0) or 0) != int(bot_user_id):
                continue
            is_mapping_card = False
            for emb in getattr(msg, "embeds", []) or []:
                footer_text = getattr(getattr(emb, "footer", None), "text", None) or ""
                if "Guild ID:" in str(footer_text):
                    is_mapping_card = True
                    break
            if not is_mapping_card:
                continue
            try:
                await msg.delete()
                deleted += 1
            except Exception:
                continue
    except Exception:
        return deleted
    return deleted


async def run_report(
    bot: discord.Client,
    user_token: str,
    *,
    guild_id: Optional[int] = None,
    upsert: bool = False,
    cleanup_before_send: bool = True,
    history_limit: int = 200,
) -> None:
    _progress("Loading channel_map.json and source info...")
    channel_map = load_channel_map()
    ch_to_guild, ch_to_name, guild_to_name = load_source_channel_info()
    _progress(f"Loaded {len(channel_map)} mapping(s), building report...")

    # 3) Fetch missing source channel/guild names via Discord API (user token) — run in executor to avoid blocking
    missing_cids = [cid for cid in channel_map if cid not in ch_to_name or ch_to_guild.get(cid, 0) not in guild_to_name]
    if missing_cids and user_token:
        _progress(f"Fetching missing channel/guild names ({len(missing_cids)} channel(s), {DELAY_BETWEEN_CHANNEL_FETCH}s delay)...")
        await asyncio.to_thread(
            fetch_missing_source_names,
            user_token,
            missing_cids,
            ch_to_guild,
            ch_to_name,
            guild_to_name,
        )
        _progress("Done fetching missing names.")

    target_guild_id: Optional[int] = int(guild_id or 0) if guild_id is not None else None
    if target_guild_id and target_guild_id > 0:
        # Filter to only the target guild for efficiency + avoids updating unrelated guilds.
        channel_map = {cid: url for cid, url in channel_map.items() if ch_to_guild.get(cid, 0) == target_guild_id}

    # Resolve webhook URLs in executor (avoid blocking event loop / heartbeat). Rate-limit.
    unique_urls = list(dict.fromkeys(channel_map.values()))
    _progress(f"Resolving {len(unique_urls)} webhook URL(s)...")
    webhook_cache: Dict[str, Tuple[Optional[int], str]] = {}
    for j, wh_url in enumerate(unique_urls):
        if j > 0:
            await asyncio.sleep(DELAY_BETWEEN_WEBHOOK_LOOKUPS)
        if j > 0 and j % 5 == 0:
            _progress(f"  webhooks: {j}/{len(unique_urls)}...")
        cid, fallback_name = await asyncio.to_thread(resolve_webhook_dest_http, wh_url)
        name = fallback_name
        if cid and hasattr(bot, "get_channel"):
            ch = bot.get_channel(cid)
            if ch and getattr(ch, "name", None):
                name = str(ch.name)
        if name == "?" and cid:
            name = f"channel-{cid}"
        webhook_cache[wh_url] = (cid, name)
    _progress("Done resolving webhooks.")

    _progress("Building embeds by guild...")
    by_guild = build_by_guild(channel_map, ch_to_guild, ch_to_name, guild_to_name, webhook_cache)
    if target_guild_id and target_guild_id > 0 and not by_guild:
        # If we were asked for one guild but it currently has no mappings, still upsert an explicit "no mappings" card.
        gname = guild_to_name.get(target_guild_id, f"Guild-{target_guild_id}") or f"Guild-{target_guild_id}"
        embed = discord.Embed(title=f"📁 {gname} - `{target_guild_id}`", description="_No mappings_", color=0x5865F2)
        embed.set_footer(text=f"Guild ID: {target_guild_id}  •  0 channel(s)")
        all_embeds = [embed]
        by_guild = [(target_guild_id, gname, [])]  # for progress logging only
    else:
        if not by_guild:
            _progress("No mappings in channel_map.json.")
            return
        all_embeds = build_embeds(by_guild)
    _progress(f"Built {len(all_embeds)} embed(s) for {len(by_guild)} guild(s).")

    _progress("Getting output channel...")
    channel = bot.get_channel(OUTPUT_CHANNEL_ID)
    if channel is None:
        try:
            channel = await bot.fetch_channel(OUTPUT_CHANNEL_ID)
        except Exception as e:
            _progress(f"Could not get output channel: {e}")
            return
    if cleanup_before_send and not upsert:
        _progress(f"Cleaning existing mapping messages (history limit {history_limit})...")
        bot_uid = int(getattr(getattr(bot, "user", None), "id", 0) or 0)
        if bot_uid:
            deleted = await _cleanup_existing_mapping_messages(
                channel,
                bot_user_id=bot_uid,
                history_limit=history_limit,
            )
            _progress(f"Deleted {deleted} old mapping message(s).")
        else:
            _progress("Skipped cleanup (bot user id unavailable).")
    if upsert:
        _progress(f"Upserting {len(all_embeds)} embed(s) into channel {OUTPUT_CHANNEL_ID}...")
        for i, embed in enumerate(all_embeds):
            gid = _parse_guild_id_from_embed(embed) or target_guild_id
            if not gid:
                # Fallback: if we can't determine which guild this belongs to, send a new message.
                await channel.send(embed=embed)
                continue
            existing = await _find_existing_guild_message(channel, int(gid), history_limit=history_limit)
            if existing is None:
                await channel.send(embed=embed)
                _progress(f"  sent (guild={gid}) {i + 1}/{len(all_embeds)}.")
            else:
                await existing.edit(embed=embed)
                _progress(f"  edited (guild={gid}) {i + 1}/{len(all_embeds)}.")
        _progress(f"Done. Upserted {len(all_embeds)} embed(s) into channel {OUTPUT_CHANNEL_ID}.")
    else:
        # One embed per message so we stay under Discord's 6000 total-embed-size limit per message
        _progress(f"Sending {len(all_embeds)} embed(s) (1 per message, 1 guild per message)...")
        for i, embed in enumerate(all_embeds):
            try:
                await channel.send(embed=embed)
                _progress(f"  sent message {i + 1}/{len(all_embeds)}.")
            except Exception as e:
                _progress(f"Send error: {e}")
        _progress(f"Done. Sent {len(all_embeds)} embed(s) to channel {OUTPUT_CHANNEL_ID}.")


def main() -> None:
    print("[post_mirror_channel_map] Starting... (connecting to Discord)", flush=True)
    env = load_env(TOKENS_ENV_PATH)
    token = (
        env.get("BOT_TOKEN")
        or env.get("DISCORD_BOT_TOKEN")
        or env.get("DISCORD_BOT_DISCUMBOT")
        or ""
    ).strip()
    if not token:
        print("Set BOT_TOKEN in MWDiscumBot/config/tokens.env")
        sys.exit(1)
    user_token = (
        env.get("DISCUM_USER_DISCUMBOT")
        or env.get("DISCUM_BOT")
        or env.get("DISCORD_TOKEN")
        or ""
    ).strip()
    # Flags:
    # --no-cleanup  => keep previous report messages and append new ones
    # --upsert      => edit/insert per-guild report cards instead of cleanup+send
    no_cleanup = "--no-cleanup" in sys.argv
    upsert = "--upsert" in sys.argv

    intents = discord.Intents.default()
    intents.guilds = True
    intents.message_content = True

    class OneShotClient(discord.Client):
        def __init__(self, user_tok: str, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._user_token = user_tok

        async def on_ready(self) -> None:
            await run_report(
                self,
                getattr(self, "_user_token", "") or "",
                upsert=upsert,
                cleanup_before_send=(not no_cleanup),
            )
            await self.close()

    client = OneShotClient(user_token, intents=intents)
    try:
        client.run(token)
    except Exception as e:
        print(f"[post_mirror_channel_map] Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
