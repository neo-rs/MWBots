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
) -> List[Tuple[int, str, List[Tuple[str, str, int, Optional[int]]]]]:
    """
    Returns list of (guild_id, guild_name, [(source_name, dest_name, src_cid, dest_cid), ...]) sorted by guild name.
    webhook_dest_cache must be pre-filled (url -> (channel_id, channel_name)).
    """
    by_guild: Dict[int, List[Tuple[int, str]]] = {}
    for src_cid, wh_url in channel_map.items():
        if not wh_url:
            continue
        gid = ch_to_guild.get(src_cid, 0)
        if gid <= 0:
            gid = 0
        by_guild.setdefault(gid, []).append((src_cid, wh_url))

    result: List[Tuple[int, str, List[Tuple[str, str, int, Optional[int]]]]] = []
    for gid in sorted(by_guild.keys(), key=lambda x: guild_to_name.get(x, "").lower()):
        gname = guild_to_name.get(gid, f"Guild-{gid}" if gid else "Unknown guild")
        rows: List[Tuple[str, str, int, Optional[int]]] = []
        for src_cid, wh_url in by_guild[gid]:
            src_name = ch_to_name.get(src_cid, f"channel-{src_cid}")
            dest_cid, dest_name = webhook_dest_cache.get(wh_url, (None, "?"))
            rows.append((src_name, dest_name, src_cid, dest_cid))
        rows.sort(key=lambda r: (r[0].lower(), r[1].lower()))
        result.append((gid, gname, rows))
    return result


def build_embeds(by_guild: List[Tuple[int, str, List[Tuple[str, str, int, Optional[int]]]]]) -> List[discord.Embed]:
    """
    Build one embed per guild. Each mapping: bold names (no # prefix), then a line with clickable <#id> links.
    Format: **source-name** → **dest-name**
            -# <#src_id> → <#dest_id>
    """
    embeds: List[discord.Embed] = []
    for gid, gname, rows in by_guild:
        lines: List[str] = []
        for src_name, dest_name, src_cid, dest_cid in rows:
            lines.append(f"**{src_name}** → **{dest_name}**")
            dest_part = f"<#{dest_cid}>" if dest_cid else "?"
            lines.append(f"-# <#{src_cid}> → {dest_part}")
        body = "\n".join(lines) if lines else "_No mappings_"
        if len(body) > 4096:
            body = body[:4090] + "\n…"
        embed = discord.Embed(
            title=f"📁 {gname}",
            description=body,
            color=0x5865F2,
        )
        embed.set_footer(text=f"Guild ID: {gid}  •  {len(rows)} channel(s)")
        embeds.append(embed)
    return embeds


def _progress(msg: str) -> None:
    print(f"[post_mirror_channel_map] {msg}", flush=True)


async def run_report(bot: discord.Client, user_token: str) -> None:
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

    intents = discord.Intents.default()
    intents.guilds = True
    intents.message_content = True

    class OneShotClient(discord.Client):
        def __init__(self, user_tok: str, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._user_token = user_tok

        async def on_ready(self) -> None:
            await run_report(self, getattr(self, "_user_token", "") or "")
            await self.close()

    client = OneShotClient(user_token, intents=intents)
    try:
        client.run(token)
    except Exception as e:
        print(f"[post_mirror_channel_map] Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
