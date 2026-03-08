"""Discum Command Bot - Slash Command Handler for MWDiscumBot

This bot handles slash commands for the MWDiscumBot, specifically the /discum browse command.
It runs separately from the main discum client (which uses a user account token) and uses
a regular bot token (discord.py) to handle slash commands.

Commands:
- /discum browse: View current mappings, or browse source guilds/channels and map to webhooks.
"""

import asyncio
import re
import sys
import os
import json
import time
from typing import Optional, Dict, List, Set, Tuple, Any
from pathlib import Path

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

import discord
from discord import app_commands
from discord.ext import commands
import requests

# Canonical config (single source of truth)
from discum_config import (
    CHANNEL_MAP_PATH as _CHANNEL_MAP_PATH,
    TOKENS_ENV_PATH,
    SETTINGS_JSON_PATH,
    SETTINGS_RUNTIME_PATH,
    load_env_file,
    load_channel_map,
    save_channel_map,
)
_CONFIG_RAW: Dict[str, str] = load_env_file(TOKENS_ENV_PATH)

# Cache from source_channels.json (written by discumbot) for guild name only. Channel display is always <#channel_id>.
_SOURCE_CHANNEL_FULL: Dict[int, Tuple[int, str]] = {}  # channel_id -> (guild_id, guild_name)
_SOURCE_GUILD_NAMES: Dict[int, str] = {}  # guild_id -> guild_name
_SOURCE_NAMES_LOADED = 0.0


def _load_source_channel_names() -> Dict[int, Tuple[int, str]]:
    """Load channel_id -> (guild_id, guild_name) from config/source_channels.json. Used only for ' — Server Name' next to <#id>."""
    global _SOURCE_CHANNEL_FULL, _SOURCE_GUILD_NAMES, _SOURCE_NAMES_LOADED
    path = str(Path(_CHANNEL_MAP_PATH).resolve().parent / "source_channels.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return _SOURCE_CHANNEL_FULL
    full: Dict[int, Tuple[int, str]] = {}
    guild_names: Dict[int, str] = {}
    for guild in data.get("guilds", []) or []:
        try:
            gid = int(guild.get("guild_id", 0))
        except (TypeError, ValueError):
            continue
        guild_name = (guild.get("guild_name") or f"Guild-{gid}").strip() or "Unknown"
        guild_names[gid] = guild_name
        for ch in guild.get("channels", []) or []:
            try:
                cid = int(ch.get("id", 0))
                full[cid] = (gid, guild_name)
            except (TypeError, ValueError):
                continue
    _SOURCE_CHANNEL_FULL = full
    _SOURCE_GUILD_NAMES = guild_names
    _SOURCE_NAMES_LOADED = time.time()
    return _SOURCE_CHANNEL_FULL


def _source_guild_name_only(bot: commands.Bot, channel_id: int) -> str:
    """Return the server/guild name for a channel when known, else empty string. Used for 'channel — Server Name' display."""
    try:
        ch = bot.get_channel(channel_id)
        if ch and hasattr(ch, "guild") and ch.guild:
            return str(getattr(ch.guild, "name", "") or "").strip()
    except Exception:
        pass
    _load_source_channel_names()
    info = _SOURCE_CHANNEL_FULL.get(channel_id)
    if info:
        return str(info[1] or "").strip()  # guild_name (info = (guild_id, guild_name))
    return ""


def _format_mapping_line(channel_name: str, dest_display: str, guild_id: int, channel_id: int) -> str:
    """
    One mapping line: source → destination. Uses <#channel_id> for source (clickable).
    dest_display should be <#dest_id> when destination is a channel.
    """
    src = f"<#{channel_id}>" if channel_id else (channel_name or "?")
    return f"💥・{src} → {dest_display}"


def _format_channel_mention(channel_id: int) -> str:
    """Single channel display: <#channel_id> (Discord resolves to #channel-name and server)."""
    return f"<#{channel_id}>"

def cfg_get(key: str, default: str = "") -> str:
    """Get config value from env file, then os.environ, then settings.json."""
    env_val = _CONFIG_RAW.get(key, "").strip()
    if env_val:
        return env_val
    env_val = os.environ.get(key, "").strip() or os.environ.get(key.upper(), "").strip()
    if env_val:
        return env_val
    try:
        with open(SETTINGS_JSON_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
            if isinstance(data, dict):
                val = data.get(key, default)
                return str(val).strip() if val else default
    except Exception:
        pass
    return default

# Bot token for slash command registration (add to MWDiscumBot/config/tokens.env — same file as user token)
BOT_TOKEN = str(
    cfg_get("DISCORD_BOT_TOKEN")
    or cfg_get("DISCORD_BOT_DISCUMBOT")
    or cfg_get("BOT_TOKEN")
    or ""
).strip()

# Guild ID for command sync (same sources as main discumbot)
MIRRORWORLD_SERVER_ID = int(
    cfg_get("mirrorworld_server_id")
    or cfg_get("MIRRORWORLD_SERVER")
    or cfg_get("MIRRORWORLD_GUILD_ID")
    or cfg_get("mirrorworld_server_id")
    or "0"
) or 0

# User token for browsing source guilds/channels (same as main discumbot; set in config/tokens.env).
USER_TOKEN = str(
    cfg_get("DISCUM_USER_DISCUMBOT")
    or cfg_get("DISCUM_BOT")
    or cfg_get("DISCORD_TOKEN")
    or ""
).strip()


def _get_browse_user_token() -> str:
    """Return the user token for browsing source guilds. Uses same source as main discumbot when in same process."""
    token = (USER_TOKEN or "").strip()
    if token:
        return token
    # When slash bot runs inside discumbot process (e.g. run_bot.sh discumbot → python discumbot.py), use the main discumbot's token
    try:
        mods = __import__("sys").modules
        for name in ("discumbot", "__main__"):
            m = mods.get(name)
            if m and getattr(m, "__file__", "").endswith("discumbot.py"):
                t = getattr(m, "DISCUM_BOT", None) or ""
                if isinstance(t, str) and t.strip():
                    return t.strip()
    except Exception:
        pass
    return ""


# Only exit when run as main; when imported (e.g. by discumbot.py) allow missing token
if not MIRRORWORLD_SERVER_ID:
    print("[WARN] mirrorworld_server_id not set. Commands will be global (may take up to 1 hour to sync)")

def _load_channel_map() -> Dict[int, str]:
    """Load channel map from canonical path."""
    return load_channel_map(_CHANNEL_MAP_PATH)


def _save_channel_map(channel_map: Dict[int, str]) -> bool:
    """Save channel map to canonical path."""
    ok = save_channel_map(_CHANNEL_MAP_PATH, channel_map)
    if not ok:
        print("[ERROR] Failed to save channel map.")
    return ok


def ensure_discum_source_guild_id(guild_id: int) -> None:
    """Append guild_id to settings.runtime.json source_guild_ids so discumbot can listen."""
    gid = int(guild_id or 0)
    if gid <= 0:
        return
    try:
        data: Dict[str, Any] = {}
        if os.path.exists(SETTINGS_RUNTIME_PATH):
            with open(SETTINGS_RUNTIME_PATH, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
        if not isinstance(data, dict):
            data = {}
        raw = data.get("source_guild_ids") if isinstance(data.get("source_guild_ids"), list) else []
        gids = [str(x).strip() for x in raw if str(x).strip()]
        if str(gid) not in gids:
            gids.append(str(gid))
        data["source_guild_ids"] = gids
        data["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(SETTINGS_RUNTIME_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def _parse_channel_id(text: str) -> int:
    s = str(text or "").strip()
    if not s:
        return 0
    m = re.search(r"(\d{15,22})", s)
    return int(m.group(1)) if m else 0


def _preview_line_from_message(msg: Dict[str, Any]) -> str:
    content = str(msg.get("content") or "").strip()
    content = re.sub(r"\s+", " ", content).strip()
    embeds = msg.get("embeds") if isinstance(msg.get("embeds"), list) else []
    atts = msg.get("attachments") if isinstance(msg.get("attachments"), list) else []
    tags = []
    if embeds:
        tags.append("embeds")
    if atts:
        tags.append(f"files:{len(atts)}")
    if re.search(r"https?://", content):
        tags.append("link")
    tag_txt = f" [{' '.join(tags)}]" if tags else ""
    if not content and embeds:
        try:
            e0 = embeds[0] if isinstance(embeds[0], dict) else {}
            content = str(e0.get("title") or e0.get("description") or "").strip()
        except Exception:
            pass
    if not content:
        content = "(no text)"
    return (content[:70] + ("…" if len(content) > 70 else "")) + tag_txt

def resolve_webhook_destination(webhook_url: str, bot: commands.Bot) -> Tuple[Optional[int], Optional[str]]:
    """Resolve webhook destination channel ID and name."""
    try:
        import re
        match = re.search(r'/webhooks/(\d+)/([^/?]+)', webhook_url)
        if not match:
            return None, None
        
        wh_id, wh_token = match.group(1), match.group(2)
        info_url = f"https://discord.com/api/v10/webhooks/{wh_id}/{wh_token}"
        
        try:
            resp = requests.get(info_url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                channel_id = int(data.get("channel_id", 0)) if data.get("channel_id") else None
                webhook_name = data.get("name", "")
                
                # Try to get actual channel name from bot
                channel_name = None
                if channel_id and bot:
                    try:
                        channel = bot.get_channel(channel_id)
                        if channel:
                            channel_name = getattr(channel, 'name', None)
                            if channel_name:
                                return channel_id, channel_name
                    except Exception:
                        pass
                
                # Fallback to webhook name or channel ID
                if webhook_name:
                    return channel_id, webhook_name
                if channel_id:
                    return channel_id, f"channel-{channel_id}"
        except Exception:
            pass
    except Exception:
        pass
    return None, None


def _build_destination_pages(
    channel_map: Dict[int, str], bot: commands.Bot
) -> List[Tuple[str, Optional[int], str, List[int]]]:
    """
    Group channel_map by Mirror World destination (webhook). Returns list of
    (dest_key, dest_channel_id, dest_display_name, [source_channel_ids]) sorted by dest_display_name.
    """
    cache: Dict[str, Tuple[Optional[int], str]] = {}
    groups: Dict[str, List[int]] = {}
    destinations: Dict[str, Tuple[Optional[int], str]] = {}

    for src_channel_id, webhook_url in (channel_map or {}).items():
        wh = str(webhook_url or "").strip()
        if not wh:
            continue
        if wh in cache:
            dest_id, dest_name = cache[wh]
        else:
            dest_id, dest_name = resolve_webhook_destination(wh, bot)
            dest_name = (dest_name or "").strip()
            cache[wh] = (dest_id, dest_name)

        if dest_id:
            key = f"dest:{int(dest_id)}"
            # Default format for channel refs: <#channel_id> (clickable, Discord shows name)
            disp = f"<#{int(dest_id)}>"
        else:
            name = dest_name or "Unknown destination"
            key = f"name:{name}"
            disp = name

        groups.setdefault(key, []).append(int(src_channel_id))
        if key not in destinations:
            destinations[key] = (int(dest_id) if dest_id else None, disp)

    items = []
    for key, (did, disp) in destinations.items():
        items.append((key, did, disp, groups.get(key, [])))
    items.sort(key=lambda x: str(x[2]).lower())
    for t in items:
        t[3].sort(key=lambda cid: (_source_guild_name_only(bot, cid).lower(), cid))
    return items


# ---- Interaction helpers (used across ALL Views) ----

def _resp_done(i: discord.Interaction) -> bool:
    try:
        return bool(i.response.is_done())
    except Exception:
        return False


async def _safe_defer_ephemeral(i: discord.Interaction) -> None:
    """Acknowledge interaction if not yet acknowledged (prevents 'This interaction failed')."""
    if _resp_done(i):
        return
    try:
        await i.response.defer(ephemeral=True)
    except Exception:
        return


async def _safe_edit(i: discord.Interaction, **kwargs) -> None:
    """
    Edit the original interaction message safely whether or not we've deferred already.
    Works for both slash-command responses and component interactions.
    """
    try:
        if _resp_done(i):
            await i.edit_original_response(**kwargs)
        else:
            await i.response.edit_message(**kwargs)
    except Exception:
        try:
            await i.edit_original_response(**kwargs)
        except Exception:
            try:
                if getattr(i, "message", None) is not None and hasattr(i, "followup"):
                    await i.followup.edit_message(i.message.id, **kwargs)
            except Exception:
                return


async def _safe_send_ephemeral(i: discord.Interaction, content: str) -> None:
    """Send an ephemeral message safely whether or not we've responded already."""
    try:
        if _resp_done(i):
            await i.followup.send(content=content, ephemeral=True)
        else:
            await i.response.send_message(content=content, ephemeral=True)
    except Exception:
        return

class MappingViewView(discord.ui.View):
    """View for channel mappings organized by Mirror World destination. Each page = one destination; manage (remove/update) from here."""
    
    def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
        super().__init__(timeout=600)
        self.bot = bot_obj
        self.channel_map = channel_map
        self.owner_id = owner_id
        self.current_page = 0
        self.source_page = 0
        self.selected_source_id: Optional[int] = None
        # (dest_key, dest_id, dest_display, [source_ids])
        self.dest_pages: List[Tuple[str, Optional[int], str, List[int]]] = []
        self._build_pages()
        self._rebuild_buttons()
    
    def _build_pages(self) -> None:
        _load_source_channel_names()
        self.dest_pages = _build_destination_pages(self.channel_map, self.bot)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        try:
            if int(interaction.user.id) != self.owner_id:
                await _safe_send_ephemeral(interaction, "❌ This view is not for you.")
                return False
        except Exception:
            return False
        return True
    
    def _get_page_content(self, page: int) -> Tuple[str, int]:
        """One page = one Mirror World destination. Body: dest then numbered lines 1. <#id>, 2. <#id>, ..."""
        if not self.dest_pages:
            return "**No channel mappings configured.**", 0
        max_page = max(0, len(self.dest_pages) - 1)
        page = max(0, min(page, max_page))
        dest_key, _did, dest_display, src_ids = self.dest_pages[page]
        lines = [f"**{dest_display}**", ""]
        for i, cid in enumerate(src_ids, 1):
            lines.append(f"{i}. {_format_channel_mention(cid)}")
        return "\n".join(lines), max_page
    
    def _current_sources(self) -> List[int]:
        if not self.dest_pages or self.current_page < 0 or self.current_page >= len(self.dest_pages):
            return []
        return self.dest_pages[self.current_page][3]
    
    def _rebuild_buttons(self) -> None:
        """Rebuild components once (clear then add). One view per message, no duplicate buttons."""
        self.clear_items()
        _content, max_page = self._get_page_content(self.current_page)
        src_ids = self._current_sources()
        per = 25
        source_max = max(0, (len(src_ids) - 1) // per) if src_ids else 0
        self.source_page = max(0, min(int(self.source_page), source_max))
        start = int(self.source_page) * per
        page_srcs = src_ids[start : start + per]

        if max_page > 0:
            prev_btn = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.secondary, disabled=(self.current_page <= 0), row=0)
            next_btn = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.secondary, disabled=(self.current_page >= max_page), row=0)
            prev_btn.callback = self._prev_page
            next_btn.callback = self._next_page
            self.add_item(prev_btn)
            self.add_item(next_btn)
        refresh_btn = discord.ui.Button(label="🔄 Refresh", style=discord.ButtonStyle.primary, row=0)
        refresh_btn.callback = self._refresh
        self.add_item(refresh_btn)

        if page_srcs:
            opts = []
            for idx, cid in enumerate(page_srcs, 1):
                label = f"{idx}. {_format_channel_mention(cid)}"
                if len(label) > 100:
                    label = label[:97] + "..."
                opts.append(discord.SelectOption(label=label, value=str(cid)))
            sel = discord.ui.Select(
                placeholder="Select source to remove/update…",
                min_values=1,
                max_values=1,
                options=opts,
                row=1,
            )
            sel.callback = self._select_source
            self.add_item(sel)
        if source_max > 0:
            prev_s = discord.ui.Button(label="◀ Prev src", style=discord.ButtonStyle.secondary, disabled=(self.source_page <= 0), row=1)
            next_s = discord.ui.Button(label="Next src ▶", style=discord.ButtonStyle.secondary, disabled=(self.source_page >= source_max), row=1)
            prev_s.callback = self._prev_source_page
            next_s.callback = self._next_source_page
            self.add_item(prev_s)
            self.add_item(next_s)

        remove_btn = discord.ui.Button(label="🗑️ Remove", style=discord.ButtonStyle.danger, row=2, disabled=(self.selected_source_id is None))
        remove_btn.callback = self._remove_mapping
        self.add_item(remove_btn)
        update_btn = discord.ui.Button(label="✏️ Update Webhook", style=discord.ButtonStyle.primary, row=2, disabled=(self.selected_source_id is None))
        update_btn.callback = self._update_webhook
        self.add_item(update_btn)

    async def on_timeout(self) -> None:
        """When the view expires, replace the message so the user doesn't get 'This interaction failed'."""
        try:
            if self.message is not None:
                await self.message.edit(
                    content="⏱️ Session expired. Use **/discum** again to view mappings.",
                    embed=None,
                    view=None,
                )
        except Exception:
            pass

    def _build_embed(self) -> discord.Embed:
        content, max_page = self._get_page_content(self.current_page)
        # Log exact description sample so journal shows what we sent (proves which code path ran)
        sample = (content or "").strip().split("\n")[:4]
        sample_str = " | ".join(s for s in sample if s)
        _log_channel_mapping(f"SENT_DESCRIPTION sample (format=numbered <#id>): {sample_str[:350]}", level="INFO")
        embed = discord.Embed(title="Channel Mappings", description=content, color=discord.Color.blurple())
        footer = f"Page {self.current_page + 1} of {max_page + 1} ({len(self.channel_map)} total mappings)"
        footer += " • Source channels in other servers may show as Channel-XXX (Discord unresolved mention)"
        embed.set_footer(text=footer)
        if self.selected_source_id is not None:
            cid = self.selected_source_id
            wh = self.channel_map.get(cid, "")
            embed.add_field(name="Selected", value=_format_channel_mention(cid), inline=False)
            embed.add_field(name="Webhook", value=f"`{wh[:80]}…`" if len(wh) > 80 else f"`{wh}`", inline=False)
        return embed
    
    async def _update_message(self, interaction: discord.Interaction) -> None:
        await _safe_defer_ephemeral(interaction)
        embed = self._build_embed()
        self._rebuild_buttons()
        await _safe_edit(interaction, embed=embed, view=self)
    
    async def _prev_page(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        _log_channel_mapping("MappingViewView Prev page clicked")
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        if self.current_page > 0:
            self.current_page -= 1
            self.source_page = 0
            self.selected_source_id = None
        await self._update_message(interaction)
    
    async def _next_page(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        _log_channel_mapping("MappingViewView Next page clicked")
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        max_page = max(0, len(self.dest_pages) - 1)
        if self.current_page < max_page:
            self.current_page += 1
            self.source_page = 0
            self.selected_source_id = None
        await self._update_message(interaction)
    
    async def _prev_source_page(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        if self.source_page > 0:
            self.source_page -= 1
        await self._update_message(interaction)
    
    async def _next_source_page(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        src_ids = self._current_sources()
        per = 25
        source_max = max(0, (len(src_ids) - 1) // per) if src_ids else 0
        if self.source_page < source_max:
            self.source_page += 1
        await self._update_message(interaction)
    
    async def _refresh(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        self.channel_map = _load_channel_map()
        self._build_pages()
        self.current_page = 0
        self.source_page = 0
        self.selected_source_id = None
        await self._update_message(interaction)
    
    async def _select_source(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        await _safe_defer_ephemeral(interaction)
        try:
            self.selected_source_id = int(interaction.data["values"][0])
            self._rebuild_buttons()
            embed = self._build_embed()
            await _safe_edit(interaction, embed=embed, view=self)
        except Exception as e:
            await _safe_send_ephemeral(interaction, f"❌ Error: {e}")
    
    async def _remove_mapping(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        if self.selected_source_id is None:
            await _safe_send_ephemeral(interaction, "❌ Select a source first.")
            return
        cid = self.selected_source_id
        if cid in self.channel_map:
            del self.channel_map[cid]
            if _save_channel_map(self.channel_map):
                await _safe_send_ephemeral(interaction, f"✅ Removed mapping for {_format_channel_mention(cid)}")
                self.selected_source_id = None
                self._build_pages()
                embed = self._build_embed()
                self._rebuild_buttons()
                await _safe_edit(interaction, embed=embed, view=self)
            else:
                await _safe_send_ephemeral(interaction, "❌ Failed to save.")
        else:
            await _safe_send_ephemeral(interaction, "❌ Mapping not found.")
    
    async def _update_webhook(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        if not await self._guard(interaction):
            return
        if self.selected_source_id is None:
            await _safe_send_ephemeral(interaction, "❌ Select a source first.")
            return
        modal = WebhookUpdateModal(self.bot, self.channel_map, self.selected_source_id, self.owner_id)
        await interaction.response.send_modal(modal)

class WebhookUpdateModal(discord.ui.Modal, title="Update Webhook URL"):
    """Modal for updating webhook URL."""
    
    webhook_url_input = discord.ui.TextInput(
        label="Webhook URL",
        placeholder="https://discord.com/api/webhooks/...",
        required=True,
        max_length=200
    )
    
    def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], channel_id: int, owner_id: int):
        super().__init__()
        self.bot = bot_obj
        self.channel_map = channel_map
        self.channel_id = channel_id
        self.owner_id = owner_id
        # Pre-fill current webhook URL
        current_url = channel_map.get(channel_id, "")
        self.webhook_url_input.default = current_url
    
    async def on_submit(self, interaction: discord.Interaction) -> None:
        if _resp_done(interaction):
            return
        try:
            if int(interaction.user.id) != self.owner_id:
                await _safe_send_ephemeral(interaction, "❌ This modal is not for you.")
                return
            
            new_url = str(self.webhook_url_input.value).strip()
            if not new_url.startswith("https://discord.com/api/webhooks/"):
                await _safe_send_ephemeral(interaction, "❌ Invalid webhook URL format.")
                return
            
            # Update mapping
            self.channel_map[self.channel_id] = new_url
            if _save_channel_map(self.channel_map):
                await _safe_send_ephemeral(interaction, f"✅ Updated webhook URL for channel `{self.channel_id}`")
            else:
                await _safe_send_ephemeral(interaction, "❌ Failed to save changes.")
        except Exception as e:
            await _safe_send_ephemeral(interaction, f"❌ Error: {e}")

def _ui_embed(title: str, description: str = "", *, color: int = 0x5865F2) -> discord.Embed:
    return discord.Embed(title=str(title or "Discum"), description=(str(description) or None), color=int(color))


class _GuildPickView(discord.ui.View):
    """Guild picker for /discum browse."""
    def __init__(self, guilds: List[Dict[str, Any]], bot_obj: commands.Bot, owner_id: int):
        super().__init__(timeout=600)
        self.guilds = guilds
        self.bot = bot_obj
        self.owner_id = owner_id
        self.page = 0
        self._render_select()

    def _render_select(self) -> None:
        self.clear_items()
        page_size = 25
        start = max(0, self.page * page_size)
        page = self.guilds[start : start + page_size]
        opts = []
        for g in page:
            gid = int(g.get("id") or 0)
            if gid <= 0:
                continue
            nm = str(g.get("name") or "").strip() or f"guild_{gid}"
            opts.append(discord.SelectOption(label=nm[:100], value=str(gid), description=str(gid)))
        if opts:
            sel = discord.ui.Select(placeholder="Select source guild...", min_values=1, max_values=1, options=opts[:25])
            sel.callback = self._on_select
            self.add_item(sel)
        if len(self.guilds) > page_size:
            prev_btn = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary)
            next_btn = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary)
            prev_btn.callback = self._prev
            next_btn.callback = self._next
            self.add_item(prev_btn)
            self.add_item(next_btn)

    async def _guard(self, i: discord.Interaction) -> bool:
        if int(i.user.id) != self.owner_id:
            await i.response.send_message("This menu is not for you.", ephemeral=True)
            return False
        return True

    async def _on_select(self, i: discord.Interaction) -> None:
        if not await self._guard(i):
            return
        try:
            chosen = int((i.data.get("values") or ["0"])[0])
        except Exception:
            chosen = 0
        if chosen <= 0:
            await i.response.send_message("Invalid selection.", ephemeral=True)
            return
        await _discum_browse_for_guild(i, source_guild_id=chosen)

    async def _prev(self, i: discord.Interaction) -> None:
        if not await self._guard(i):
            return
        self.page = max(0, self.page - 1)
        self._render_select()
        await i.response.edit_message(embed=_ui_embed("Discum browse", f"Pick a source guild (page {self.page+1})"), view=self)

    async def _next(self, i: discord.Interaction) -> None:
        if not await self._guard(i):
            return
        self.page += 1
        self._render_select()
        await i.response.edit_message(embed=_ui_embed("Discum browse", f"Pick a source guild (page {self.page+1})"), view=self)


async def _discum_browse_for_guild(interaction: discord.Interaction, *, source_guild_id: int) -> None:
    """Browse categories/channels of a source guild and map/unmap to webhooks."""
    try:
        await interaction.response.defer(ephemeral=True, thinking=True)
    except Exception:
        pass
    sgid = int(source_guild_id or 0)
    user_token = _get_browse_user_token()
    if sgid <= 0 or not user_token:
        await interaction.followup.send(
            embed=_ui_embed("Discum browse", "Invalid source guild or missing user token.", color=0xED4245),
            ephemeral=True,
        )
        return
    from discord_user_api import list_source_guild_channels, fetch_channel_messages_page
    info = await list_source_guild_channels(source_guild_id=sgid, user_token=user_token)
    if not info.get("ok"):
        await interaction.followup.send(
            embed=_ui_embed("Discum browse", f"Browse failed: {info.get('reason')} http={info.get('http_status')}", color=0xED4245),
            ephemeral=True,
        )
        return
    categories = info.get("categories") or []
    channels = info.get("channels") or []
    if not categories:
        await interaction.followup.send(
            embed=_ui_embed("Discum browse", "No categories found in source guild.", color=0xED4245),
            ephemeral=True,
        )
        return
    by_parent: Dict[int, List[Dict[str, Any]]] = {}
    for ch in channels:
        if not isinstance(ch, dict):
            continue
        pid = int(ch.get("parent_id") or 0)
        by_parent.setdefault(pid, []).append(ch)
    mapped = _load_channel_map()
    mapped_ids: Set[int] = set(mapped.keys())

    async def _fetch_preview(cid: int) -> str:
        ok, msgs, reason = await fetch_channel_messages_page(source_channel_id=cid, user_token=user_token, limit=1)
        if not ok:
            if reason == "forbidden_or_unauthorized":
                return "(no access)"
            if reason == "not_found":
                return "(not found)"
            return f"({reason})"
        if not msgs:
            return "(no messages)"
        return _preview_line_from_message(msgs[0] if isinstance(msgs[0], dict) else {})

    def _build_embed(cat_idx: int, chan_page: int, previews: Dict[int, str]) -> discord.Embed:
        cat = categories[cat_idx]
        cid = int(cat.get("id") or 0)
        cname = str(cat.get("name") or "").strip() or f"category_{cid}"
        url = str(cat.get("url") or "").strip() or f"https://discord.com/channels/{sgid}/{cid}"
        emb = discord.Embed(title=f"Discum browse: {sgid}", color=0x5865F2)
        emb.add_field(name="Source category", value=f"**{cname}**\n`{cid}`\n[open]({url})", inline=False)
        emb.add_field(name="Mapped channels", value=str(len(mapped_ids)), inline=True)
        emb.add_field(name="Map file", value="`MWDiscumBot/config/channel_map.json`", inline=True)
        chs = list(by_parent.get(cid, []) or [])
        chs.sort(key=lambda x: (int(x.get("position", 0) or 0), int(x.get("id", 0) or 0)))
        page_size = 10
        start = max(0, chan_page * page_size)
        page = chs[start : start + page_size]
        lines = []
        for ch in page:
            chid = int(ch.get("id") or 0)
            nm = str(ch.get("name") or f"channel_{chid}")
            mark = "✅" if chid in mapped_ids else "⬜"
            prev = previews.get(chid) or ""
            link = str(ch.get("url") or "").strip() or f"https://discord.com/channels/{sgid}/{chid}"
            line = f"{mark} <#{chid}>"
            if prev:
                line += f"\n- {prev}"
            lines.append(line)
        if not lines:
            lines = ["(No messageable channels in this category.)"]
        emb.add_field(name=f"Channels (page {chan_page+1})", value="\n".join(lines)[:1024], inline=False)
        emb.set_footer(text="Select channels, then Map → destination (creates/uses webhook).")
        return emb

    class _DestModal(discord.ui.Modal, title="Map to destination"):
        dest_input = discord.ui.TextInput(
            label="Destination channel (mention or id)",
            placeholder="#channel or 1435066421133443174",
            required=True,
            max_length=80,
        )
        def __init__(self, on_submit_cb):
            super().__init__()
            self._cb = on_submit_cb
        async def on_submit(self, i: discord.Interaction) -> None:
            await self._cb(i, str(self.dest_input.value or ""))

    class _BrowseView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60 * 20)
            self.cat_idx = 0
            self.chan_page = 0
            self.selected_ids: List[int] = []
            self._select: Optional[discord.ui.Select] = None
            self._refresh_select(previews={})

        def _current_cat_id(self) -> int:
            return int(categories[self.cat_idx].get("id") or 0)

        def _current_page_channels(self) -> List[Dict[str, Any]]:
            cid = self._current_cat_id()
            chs = list(by_parent.get(cid, []) or [])
            chs.sort(key=lambda x: (int(x.get("position", 0) or 0), int(x.get("id") or 0)))
            start = max(0, self.chan_page * 10)
            return chs[start : start + 10]

        def _refresh_select(self, *, previews: Dict[int, str]) -> None:
            if self._select is not None:
                try:
                    self.remove_item(self._select)
                except Exception:
                    pass
            page = self._current_page_channels()
            opts = []
            for ch in page:
                chid = int(ch.get("id") or 0)
                if chid <= 0:
                    continue
                nm = str(ch.get("name") or f"channel_{chid}")
                desc = "mapped" if chid in mapped_ids else "unmapped"
                prev = str(previews.get(chid) or "").strip()
                if prev:
                    desc = (desc + f" • {prev}")[:100]
                opts.append(discord.SelectOption(label=nm[:100], value=str(chid), description=desc[:100] if desc else None))
            if opts:
                self._select = discord.ui.Select(
                    placeholder="Select source channels (this page)",
                    min_values=1,
                    max_values=min(10, len(opts)),
                    options=opts[:25],
                )
                self._select.callback = self._on_select
                self.add_item(self._select)

        async def _on_select(self, i: discord.Interaction) -> None:
            vals = i.data.get("values") or []
            self.selected_ids = [int(x) for x in vals if str(x).strip().isdigit()]
            await i.response.edit_message(content=None, embed=_build_embed(self.cat_idx, self.chan_page, {}), view=self)

        async def _refresh_and_render(self, i: discord.Interaction) -> None:
            page = self._current_page_channels()
            previews: Dict[int, str] = {}
            sem = asyncio.Semaphore(3)
            async def one(chid: int):
                async with sem:
                    previews[chid] = await _fetch_preview(chid)
            await asyncio.gather(*[one(int(ch.get("id") or 0)) for ch in page if int(ch.get("id") or 0) > 0])
            self._refresh_select(previews=previews)
            await i.response.edit_message(content=None, embed=_build_embed(self.cat_idx, self.chan_page, previews), view=self)

        async def _compute_previews(self) -> Dict[int, str]:
            page = self._current_page_channels()
            previews: Dict[int, str] = {}
            sem = asyncio.Semaphore(3)
            async def one(chid: int):
                async with sem:
                    previews[chid] = await _fetch_preview(chid)
            await asyncio.gather(*[one(int(ch.get("id") or 0)) for ch in page if int(ch.get("id") or 0) > 0])
            return previews

        @discord.ui.button(label="Prev category", style=discord.ButtonStyle.secondary)
        async def prev_cat(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            self.cat_idx = (self.cat_idx - 1) % len(categories)
            self.chan_page = 0
            self.selected_ids = []
            await self._refresh_and_render(i)

        @discord.ui.button(label="Next category", style=discord.ButtonStyle.secondary)
        async def next_cat(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            self.cat_idx = (self.cat_idx + 1) % len(categories)
            self.chan_page = 0
            self.selected_ids = []
            await self._refresh_and_render(i)

        @discord.ui.button(label="Prev channels", style=discord.ButtonStyle.secondary)
        async def prev_ch(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            self.chan_page = max(0, self.chan_page - 1)
            self.selected_ids = []
            await self._refresh_and_render(i)

        @discord.ui.button(label="Next channels", style=discord.ButtonStyle.secondary)
        async def next_ch(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            self.chan_page += 1
            self.selected_ids = []
            await self._refresh_and_render(i)

        @discord.ui.button(label="Refresh previews", style=discord.ButtonStyle.primary)
        async def refresh_btn(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            await self._refresh_and_render(i)

        @discord.ui.button(label="Map → destination", style=discord.ButtonStyle.success)
        async def map_btn(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            if _resp_done(i):
                return
            if not self.selected_ids:
                await i.response.send_message("No source channels selected.", ephemeral=True)
                return
            async def on_modal_submit(ii: discord.Interaction, dest_text: str) -> None:
                dest_id = _parse_channel_id(dest_text)
                if dest_id <= 0:
                    await ii.response.send_message("Invalid destination channel (mention or id).", ephemeral=True)
                    return
                dest = ii.guild.get_channel(dest_id) if ii.guild else None
                if dest is None:
                    try:
                        dest = await bot.fetch_channel(dest_id)
                    except Exception:
                        dest = None
                if dest is None or not hasattr(dest, "create_webhook"):
                    await ii.response.send_message("Destination channel not found or not a text channel.", ephemeral=True)
                    return
                try:
                    wh_list = await dest.webhooks()
                    wh_url = None
                    for w in wh_list:
                        if getattr(w, "name", None) == "MWDiscumBot":
                            wh_url = getattr(w, "url", None)
                            break
                    if not wh_url:
                        wh = await dest.create_webhook(name="MWDiscumBot", reason="MWDiscumBot mapping")
                        wh_url = wh.url if wh else None
                    if not wh_url:
                        await ii.response.send_message("Failed to create/use webhook (need Manage Webhooks).", ephemeral=True)
                        return
                    m = _load_channel_map()
                    for src_cid in self.selected_ids:
                        m[int(src_cid)] = str(wh_url)
                        mapped_ids.add(int(src_cid))
                    _save_channel_map(m)
                    ensure_discum_source_guild_id(sgid)
                    await ii.response.send_message(
                        embed=_ui_embed(
                            "Discum mapping saved",
                            f"- source_guild_id: `{sgid}`\n- mapped: `{len(self.selected_ids)}` channel(s)\n- destination: <#{dest_id}>\n- DiscumBot reloads channel_map within ~10s.",
                            color=0x57F287,
                        ),
                        ephemeral=True,
                    )
                except Exception as e:
                    await ii.response.send_message(f"Error: {e}", ephemeral=True)
            await i.response.send_modal(_DestModal(on_modal_submit))

        @discord.ui.button(label="Unmap selected", style=discord.ButtonStyle.danger)
        async def unmap_btn(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
            if _resp_done(i):
                return
            if not self.selected_ids:
                await i.response.send_message("No source channels selected.", ephemeral=True)
                return
            m = _load_channel_map()
            removed = 0
            for src_cid in self.selected_ids:
                if int(src_cid) in m:
                    m.pop(int(src_cid), None)
                    removed += 1
                mapped_ids.discard(int(src_cid))
            _save_channel_map(m)
            await i.response.send_message(
                embed=_ui_embed("Discum mapping updated", f"Unmapped `{removed}` channel(s).", color=0xFEE75C),
                ephemeral=True,
            )

    view = _BrowseView()
    previews0 = await view._compute_previews()
    view._refresh_select(previews=previews0)
    await interaction.followup.send(embed=_build_embed(0, 0, previews0), view=view, ephemeral=True)


class DiscumCommandBot(commands.Bot):
    """Command bot for handling /discum browse command."""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        # Do not create a new CommandTree - Bot already has self.tree; a second one raises
        # "This client already has an associated command tree"
    
    async def setup_hook(self) -> None:
        """Sync slash commands to the Mirror World server so /discum is visible when typing slash."""
        try:
            if MIRRORWORLD_SERVER_ID:
                guild_obj = discord.Object(id=MIRRORWORLD_SERVER_ID)
                # Commands are registered globally on the tree; copy to guild then sync so they appear in the server
                self.tree.copy_global_to(guild=guild_obj)
                synced = await self.tree.sync(guild=guild_obj)
                print(f"[INFO] Slash commands synced to guild {MIRRORWORLD_SERVER_ID}: {len(synced)} command(s) (/discum should appear in server)")
            else:
                synced = await self.tree.sync()
                print(f"[INFO] Slash commands synced globally: {len(synced)} command(s) (may take up to 1 hour to appear)")
        except Exception as e:
            print(f"[ERROR] Slash command sync failed: {e}")
            import traceback
            traceback.print_exc()

    async def on_ready(self) -> None:
        print(f"[INFO] Logged in as {self.user}")
        app_id = self.user.id if self.user else None
        if app_id and MIRRORWORLD_SERVER_ID:
            # If /discum doesn't show in Discord, re-invite the bot with applications.commands scope
            invite_url = (
                f"https://discord.com/api/oauth2/authorize?client_id={app_id}"
                f"&permissions=0"
                f"&scope=bot%20applications.commands"
                f"&guild_id={MIRRORWORLD_SERVER_ID}"
            )
            print(f"[INFO] If /discum is not visible: ensure this bot is in the server with slash command scope. Re-invite: {invite_url}")
        print(f"[INFO] Ready to handle /discum browse")
        print(f"[INFO] Channel Mappings display: numbered list (1. <#id>, 2. <#id>...) — if you see 'Channel-XXXXX' the wrong code path is running")

# Log once at import so deploy can confirm this file is the one running (Channel Mappings use "1. <#id>")
print("[discum_command_bot] loaded — Channel Mappings format: 1. <#id>, 2. <#id> ...")

# Create bot instance
bot = DiscumCommandBot()


def _log_channel_mapping(msg: str, level: str = "INFO") -> None:
    """Diagnostic log for /discum flows. Appears in Data Manager bot stdout (journalctl). Prefix so you can grep [Channel Mapping]."""
    try:
        prefix = "[Channel Mapping]"
        line = f"{prefix} [{level}] {msg}"
        print(line, flush=True)
    except Exception:
        pass


async def _discum_browse_impl(
    interaction: discord.Interaction,
    action: app_commands.Choice[str],
    bot_obj: commands.Bot,
) -> None:
    """Shared /discum browse handler (used by standalone bot or when registered on DataManagerBot)."""
    _log_channel_mapping(f"/discum triggered (action={action.value}) — handler=discum_command_bot.py")
    if action.value != "browse":
        await _safe_send_ephemeral(interaction, "❌ Unknown action.")
        return
    try:
        await _safe_defer_ephemeral(interaction)
        channel_map = _load_channel_map()
        owner_id = int(interaction.user.id)
        _log_channel_mapping(f"/discum browse: channel_map size={len(channel_map or {})}")
    except Exception as e:
        _log_channel_mapping(f"/discum browse error: {e}", level="ERROR")
        import traceback
        traceback.print_exc()
        await _safe_send_ephemeral(interaction, f"❌ Error loading /discum: {e}. Check Data Manager bot logs for [Channel Mapping].")
        return

    class BrowseView(discord.ui.View):
        def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
            super().__init__(timeout=600)
            self.bot = bot_obj
            self.channel_map = channel_map
            self.owner_id = owner_id

        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return int(interaction.user.id) == self.owner_id

        async def on_timeout(self) -> None:
            try:
                if self.message is not None:
                    await self.message.edit(
                        content="⏱️ Session expired. Use **/discum** again to continue.",
                        embed=None,
                        view=None,
                    )
            except Exception:
                pass

        @discord.ui.button(label="View Current Mappings", style=discord.ButtonStyle.primary, emoji="📋", row=0)
        async def view_mappings(self, interaction: discord.Interaction, button: discord.ui.Button):
            if _resp_done(interaction):
                return
            _log_channel_mapping("View Current Mappings button clicked")
            await _safe_defer_ephemeral(interaction)
            try:
                self.disable_all_items()
            except Exception:
                pass
            await _safe_edit(
                interaction,
                content="Loading current mappings…",
                embed=None,
                view=self,
            )
            try:
                channel_map = _load_channel_map()
                if not channel_map:
                    _log_channel_mapping("View Current Mappings: no mappings")
                    await _safe_edit(
                        interaction,
                        content="**No channel mappings configured.** Use « Browse source & map » or the main discum bot to add mappings.",
                        embed=None,
                        view=None,
                    )
                    return
                _log_channel_mapping(f"View Current Mappings: building view for {len(channel_map)} mappings (discum_command_bot.MappingViewView)")
                view = MappingViewView(self.bot, channel_map, self.owner_id)
                embed = view._build_embed()
                await _safe_edit(interaction, content=None, embed=embed, view=view)
                _log_channel_mapping(f"View Current Mappings done (count={len(channel_map)})")
            except Exception as e:
                _log_channel_mapping(f"View Current Mappings error: {e}", level="ERROR")
                import traceback
                traceback.print_exc()
                err_msg = f"**Error loading mappings.**\n`{type(e).__name__}: {str(e)[:200]}`\n\nCheck Data Manager bot logs for `[Channel Mapping]`."
                await _safe_edit(interaction, content=err_msg, embed=None, view=None)

        @discord.ui.button(label="Browse source & map", style=discord.ButtonStyle.secondary, emoji="🗺️", row=0)
        async def browse_source(self, interaction: discord.Interaction, button: discord.ui.Button):
            if _resp_done(interaction):
                return
            _log_channel_mapping("Browse source & map button clicked")
            await _safe_defer_ephemeral(interaction)
            try:
                self.disable_all_items()
            except Exception:
                pass
            await _safe_edit(
                interaction,
                content="Loading servers/channels…",
                embed=None,
                view=self,
            )
            try:
                browse_token = _get_browse_user_token()
                if not browse_token:
                    _log_channel_mapping("Browse source: missing user token", level="WARN")
                    await _safe_edit(
                        interaction,
                        content="**Missing user token for browsing.** Set DISCUM_BOT or DISCUM_USER_DISCUMBOT in config/tokens.env (same as the main discumbot).",
                        embed=None,
                        view=None,
                    )
                    return
                _log_channel_mapping("Browse source: fetching guilds")
                from discord_user_api import list_user_guilds
                info = await list_user_guilds(user_token=browse_token)
                if not info.get("ok"):
                    _log_channel_mapping(f"Browse source: list guilds failed reason={info.get('reason', 'unknown')}", level="WARN")
                    await _safe_edit(
                        interaction,
                        content=f"**Could not list guilds.** {info.get('reason', 'unknown')}",
                        embed=None,
                        view=None,
                    )
                    return
                guilds = info.get("guilds") or []
                if not guilds:
                    _log_channel_mapping("Browse source: no guilds")
                    await _safe_edit(
                        interaction,
                        content="**No guilds found** for the configured user token.",
                        embed=None,
                        view=None,
                    )
                    return
                view_guild_pick = _GuildPickView(guilds, self.bot, self.owner_id)
                embed = discord.Embed(
                    title="Discum browse",
                    description="Pick a source guild to browse:",
                    color=discord.Color.blurple(),
                )
                await _safe_edit(interaction, content=None, embed=embed, view=view_guild_pick)
                _log_channel_mapping(f"Browse source done (guilds={len(guilds)})")
            except Exception as e:
                _log_channel_mapping(f"Browse source error: {e}", level="ERROR")
                import traceback
                traceback.print_exc()
                err_msg = f"**Error loading servers.**\n`{type(e).__name__}: {str(e)[:200]}`\n\nCheck Data Manager bot logs for `[Channel Mapping]`."
                await _safe_edit(interaction, content=err_msg, embed=None, view=None)

    try:
        view = BrowseView(bot_obj, channel_map, owner_id)
        if not channel_map:
            embed = discord.Embed(
                title="Discum Bot Mappings",
                description="No channel mappings configured yet.\n\n👉 **Click the button below** to open the mappings viewer (you can add mappings via the main discum bot, then use this to view/remove/update).",
                color=discord.Color.blurple(),
            )
        else:
            embed = discord.Embed(
                title="Discum Bot Mappings",
                description=f"**{len(channel_map)}** channel mapping(s) configured.\n\n👉 **Click the button below** to view mappings by server and to remove/update them.",
                color=discord.Color.blurple(),
            )
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        _log_channel_mapping("/discum browse initial message sent")
    except Exception as e:
        _log_channel_mapping(f"/discum browse send error: {e}", level="ERROR")
        import traceback
        traceback.print_exc()
        await _safe_send_ephemeral(interaction, f"❌ Error sending /discum menu: {e}. Check Data Manager bot logs for [Channel Mapping].")


def register_discum_commands_to_bot(bot_instance: commands.Bot) -> None:
    """Register /discum slash command on an existing bot (e.g. DataManagerBot). Call before bot.run(). Single registration path — no duplicate handlers."""
    @bot_instance.tree.command(name="discum", description="Browse and manage Discum bot channel mappings")
    @app_commands.describe(action="Action to perform")
    @app_commands.choices(action=[
        app_commands.Choice(name="browse", value="browse"),
    ])
    async def _discum_cmd(interaction: discord.Interaction, action: app_commands.Choice[str]):
        await _discum_browse_impl(interaction, action, bot_instance)


# So the module's bot has /discum when run via main() or when another process imports and runs it
register_discum_commands_to_bot(bot)


def _list_guild_commands_via_api(token: str, guild_id: int) -> None:
    """List slash commands registered for the guild via Discord API (no bot run)."""
    if not token or not guild_id:
        print("[ERROR] Need BOT_TOKEN and guild ID to list commands.")
        return
    headers = {"Authorization": f"Bot {token}"}
    try:
        r = requests.get("https://discord.com/api/v10/users/@me", headers=headers, timeout=10)
        if r.status_code != 200:
            print(f"[ERROR] Token invalid or expired: HTTP {r.status_code}")
            return
        app_id = r.json().get("id")
        if not app_id:
            print("[ERROR] Could not get application ID")
            return
        url = f"https://discord.com/api/v10/applications/{app_id}/guilds/{guild_id}/commands"
        r2 = requests.get(url, headers=headers, timeout=10)
        if r2.status_code != 200:
            print(f"[ERROR] Failed to fetch guild commands: HTTP {r2.status_code} - {r2.text[:200]}")
            return
        commands = r2.json()
        print(f"Guild {guild_id} (Mirror World): {len(commands)} command(s) registered for this application")
        for c in commands:
            name = c.get("name", "?")
            desc = (c.get("description") or "")[:60]
            print(f"  /{name}  — {desc}")
        if not commands:
            print("  (none — start the discumbot with a bot token so it can sync /discum)")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


async def main():
    """Main entry point. Bot already has /discum from module load; start it."""
    print("=" * 50)
    print("DISCUM COMMAND BOT")
    print("=" * 50)
    print(f"[INFO] Channel map path: {_CHANNEL_MAP_PATH}")
    print(f"[INFO] Target guild: {MIRRORWORLD_SERVER_ID or 'Global'}")
    await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Discum slash command bot (/discum browse)")
    ap.add_argument("--list-commands", action="store_true", help="List slash commands registered in Mirror World guild (then exit)")
    ap.add_argument("--guild", type=int, default=None, help="Guild ID for --list-commands (default: mirrorworld_server_id from config)")
    args = ap.parse_args()

    if args.list_commands:
        gid = args.guild or MIRRORWORLD_SERVER_ID
        if not BOT_TOKEN:
            print("[ERROR] No bot token. Set BOT_TOKEN (or DISCORD_BOT_TOKEN) in MWDiscumBot/config/tokens.env to register /discum.")
            sys.exit(1)
        if not gid:
            print("[ERROR] No guild ID. Set mirrorworld_server_id in config/settings.json or use --guild 1431314516364230689")
            sys.exit(1)
        print(f"[INFO] guild_id={gid}")
        _list_guild_commands_via_api(BOT_TOKEN, gid)
        sys.exit(0)

    import asyncio
    if not BOT_TOKEN:
        print("[ERROR] Bot token not found. Set BOT_TOKEN or DISCORD_BOT_TOKEN in MWDiscumBot/config/tokens.env to register /discum.")
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
