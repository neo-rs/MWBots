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

# Cache of source channel names (from discumbot-written source_channels.json) for "Server / #channel" display
_SOURCE_CHANNEL_NAMES: Dict[int, Tuple[str, str]] = {}  # channel_id -> (guild_name, channel_name)
_SOURCE_CHANNEL_FULL: Dict[int, Tuple[int, str, str]] = {}  # channel_id -> (guild_id, guild_name, channel_name)
_SOURCE_GUILD_NAMES: Dict[int, str] = {}  # guild_id -> guild_name (for grouping when bot not in guild)
_SOURCE_NAMES_LOADED = 0.0


def _load_source_channel_names() -> Dict[int, Tuple[str, str]]:
    """Load channel_id -> (guild_name, channel_name) from config/source_channels.json (written by discumbot)."""
    global _SOURCE_CHANNEL_NAMES, _SOURCE_CHANNEL_FULL, _SOURCE_GUILD_NAMES, _SOURCE_NAMES_LOADED
    path = str(Path(_CHANNEL_MAP_PATH).resolve().parent / "source_channels.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return _SOURCE_CHANNEL_NAMES
    out = {}
    full: Dict[int, Tuple[int, str, str]] = {}
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
                cname = (ch.get("name") or f"Channel-{cid}").strip() or str(cid)
                out[cid] = (guild_name, cname)
                full[cid] = (gid, guild_name, cname)
            except (TypeError, ValueError):
                continue
    _SOURCE_CHANNEL_NAMES = out
    _SOURCE_CHANNEL_FULL = full
    _SOURCE_GUILD_NAMES = guild_names
    _SOURCE_NAMES_LOADED = time.time()
    return out


def _get_channel_display_name(bot: commands.Bot, channel_id: int) -> str:
    """Return 'ServerName / #channel-name' or fallback to 'Channel-XXXXXX' using bot cache or source_channels.json."""
    try:
        ch = bot.get_channel(channel_id)
        if ch:
            gname = getattr(ch.guild, "name", "Unknown") if hasattr(ch, "guild") and ch.guild else "Unknown"
            cname = getattr(ch, "name", f"Channel-{channel_id}")
            return f"{gname} / #{cname}"
    except Exception:
        pass
    names = _load_source_channel_names()
    if channel_id in names:
        gname, cname = names[channel_id]
        return f"{gname} / #{cname}"
    return f"Channel-{str(channel_id)[-6:]}"

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

class MappingViewView(discord.ui.View):
    """View for displaying channel mappings organized by guild."""
    
    def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
        super().__init__(timeout=600)
        self.bot = bot_obj
        self.channel_map = channel_map
        self.owner_id = owner_id
        self.current_page = 0
        # (guild or None, mappings, guild_id) so we can show/sort by name when bot not in guild
        self.guild_mappings: List[Tuple[Optional[discord.Guild], List[Tuple[int, str, Optional[str]]], int]] = []
        self._build_guild_mappings()
        self._rebuild_buttons()
    
    def _build_guild_mappings(self) -> None:
        """Build list of guilds and their channel mappings."""
        guild_dict: Dict[int, List[Tuple[int, str, Optional[str]]]] = {}
        
        _load_source_channel_names()  # ensure cache for fallback
        for channel_id, webhook_url in self.channel_map.items():
            dest_channel_id, dest_channel_name = resolve_webhook_destination(webhook_url, self.bot)
            dest_display = dest_channel_name if dest_channel_name else "webhook"
            try:
                channel = self.bot.get_channel(channel_id)
                if channel and hasattr(channel, 'guild') and channel.guild:
                    guild_id = channel.guild.id
                    channel_name = getattr(channel, 'name', f'Channel-{str(channel_id)[-6:]}')
                    if guild_id not in guild_dict:
                        guild_dict[guild_id] = []
                    guild_dict[guild_id].append((channel_id, channel_name, dest_display))
                else:
                    # Bot not in source guild: use source_channels.json names
                    info = _SOURCE_CHANNEL_FULL.get(channel_id)
                    if info:
                        gid, guild_name, channel_name = info
                        if gid not in guild_dict:
                            guild_dict[gid] = []
                        guild_dict[gid].append((channel_id, channel_name, dest_display))
                    else:
                        if 0 not in guild_dict:
                            guild_dict[0] = []
                        guild_dict[0].append((channel_id, f"Channel-{str(channel_id)[-6:]}", dest_display))
            except Exception:
                if 0 not in guild_dict:
                    guild_dict[0] = []
                guild_dict[0].append((channel_id, f"Channel-{str(channel_id)[-6:]}", dest_display))
        
        # Build list (guild or None, mappings, guild_id); use cached guild name when bot not in guild
        self.guild_mappings = []
        for guild_id, mappings in guild_dict.items():
            try:
                guild = self.bot.get_guild(guild_id) if guild_id > 0 else None
                self.guild_mappings.append((guild, sorted(mappings, key=lambda x: x[1]), guild_id))
            except Exception:
                self.guild_mappings.append((None, sorted(mappings, key=lambda x: x[1]), guild_id))
        
        # Sort by guild name (cached name when guild object is None)
        def _sort_key(item):
            g, _, gid = item
            return g.name if g else _SOURCE_GUILD_NAMES.get(gid, "ZZZ")
        self.guild_mappings.sort(key=_sort_key)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        """Check if user is authorized."""
        try:
            if int(interaction.user.id) != self.owner_id:
                await interaction.response.send_message("❌ This view is not for you.", ephemeral=True)
                return False
        except Exception:
            return False
        return True
    
    def _get_page_content(self, page: int) -> Tuple[str, int]:
        """Get content for a specific page."""
        if not self.guild_mappings:
            return "**No channel mappings configured.**", 0
        
        max_page = max(0, len(self.guild_mappings) - 1)
        page = max(0, min(page, max_page))
        
        guild, mappings, guild_id = self.guild_mappings[page]
        guild_name = guild.name if guild else _SOURCE_GUILD_NAMES.get(guild_id, "Unknown Guild")
        
        lines = [f"**{guild_name}**"]
        lines.append(f"*Guild ID: {guild.id if guild else guild_id}*")
        lines.append("")
        
        for channel_id, channel_name, dest_name in mappings:
            dest_display = dest_name if dest_name else "webhook"
            lines.append(f"💥・{channel_name} `{channel_id}` → {dest_display}")
        
        content = "\n".join(lines)
        return content, max_page
    
    def _rebuild_buttons(self) -> None:
        """Rebuild buttons for current page."""
        self.clear_items()
        content, max_page = self._get_page_content(self.current_page)
        
        # Navigation buttons
        if max_page > 0:
            prev_btn = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.secondary, disabled=(self.current_page <= 0))
            next_btn = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.secondary, disabled=(self.current_page >= max_page))
            prev_btn.callback = self._prev_page
            next_btn.callback = self._next_page
            self.add_item(prev_btn)
            self.add_item(next_btn)
        
        # Refresh button
        refresh_btn = discord.ui.Button(label="🔄 Refresh", style=discord.ButtonStyle.primary)
        refresh_btn.callback = self._refresh
        self.add_item(refresh_btn)
        
        # Manage button
        manage_btn = discord.ui.Button(label="⚙️ Manage Mappings", style=discord.ButtonStyle.success)
        manage_btn.callback = self._manage_mappings
        self.add_item(manage_btn)
    
    async def _update_message(self, interaction: discord.Interaction) -> None:
        """Update the message with current page."""
        content, max_page = self._get_page_content(self.current_page)
        
        embed = discord.Embed(
            title="Channel Mappings",
            description=content,
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Page {self.current_page + 1} of {max_page + 1} ({len(self.channel_map)} total mappings)")
        
        self._rebuild_buttons()
        
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception:
            try:
                await interaction.followup.edit_message(interaction.message.id, embed=embed, view=self)
            except Exception:
                pass
    
    async def _prev_page(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        if self.current_page > 0:
            self.current_page -= 1
        await self._update_message(interaction)
    
    async def _next_page(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        max_page = max(0, len(self.guild_mappings) - 1)
        if self.current_page < max_page:
            self.current_page += 1
        await self._update_message(interaction)
    
    async def _refresh(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        # Reload channel map
        self.channel_map = _load_channel_map()
        self._build_guild_mappings()
        self.current_page = 0
        await self._update_message(interaction)
    
    async def _manage_mappings(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        # Open manage view
        view = ManageMappingsView(self.bot, self.channel_map, self.owner_id)
        embed = await view._build_embed()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class ManageMappingsView(discord.ui.View):
    """View for managing (removing/updating) channel mappings."""
    
    def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
        super().__init__(timeout=600)
        self.bot = bot_obj
        self.channel_map = channel_map.copy()
        self.owner_id = owner_id
        self.selected_channel_id: Optional[int] = None
        self._rebuild()
    
    async def _guard(self, interaction: discord.Interaction) -> bool:
        """Check if user is authorized."""
        try:
            if int(interaction.user.id) != self.owner_id:
                await interaction.response.send_message("❌ This view is not for you.", ephemeral=True)
                return False
        except Exception:
            return False
        return True
    
    def _rebuild(self) -> None:
        """Rebuild the view UI."""
        self.clear_items()
        
        if not self.channel_map:
            # No mappings - show message
            return
        
        # Build select menu with channels (Server / #channel-name for clarity)
        options: List[discord.SelectOption] = []
        for channel_id, webhook_url in list(self.channel_map.items())[:25]:  # Discord limit
            label = _get_channel_display_name(self.bot, channel_id)
            # Truncate label (Discord select limit 100)
            if len(label) > 100:
                label = label[:97] + "..."
            
            webhook_preview = webhook_url[:30] + "..." if len(webhook_url) > 30 else webhook_url
            description = f"→ {webhook_preview}"[:100]
            
            options.append(discord.SelectOption(
                label=label,
                value=str(channel_id),
                description=description
            ))
        
        if options:
            select = discord.ui.Select(
                placeholder="Select a mapping to manage...",
                min_values=1,
                max_values=1,
                options=options,
                row=0
            )
            select.callback = self._select_channel
            self.add_item(select)
        
        # Action buttons (disabled until channel selected)
        remove_btn = discord.ui.Button(label="🗑️ Remove", style=discord.ButtonStyle.danger, row=1, disabled=(self.selected_channel_id is None))
        remove_btn.callback = self._remove_mapping
        self.add_item(remove_btn)
        
        update_btn = discord.ui.Button(label="✏️ Update Webhook", style=discord.ButtonStyle.primary, row=1, disabled=(self.selected_channel_id is None))
        update_btn.callback = self._update_webhook
        self.add_item(update_btn)
        
        back_btn = discord.ui.Button(label="← Back", style=discord.ButtonStyle.secondary, row=2)
        back_btn.callback = self._back
        self.add_item(back_btn)
    
    async def _build_embed(self) -> discord.Embed:
        """Build the embed for this view."""
        embed = discord.Embed(
            title="Manage Channel Mappings",
            description="Select a channel mapping to remove or update.",
            color=discord.Color.orange()
        )
        
        if self.selected_channel_id:
            webhook_url = self.channel_map.get(self.selected_channel_id, "")
            display_name = _get_channel_display_name(self.bot, self.selected_channel_id)
            embed.add_field(name="Selected Channel", value=f"**{display_name}**\n`{self.selected_channel_id}`", inline=False)
            embed.add_field(name="Current Webhook", value=f"`{webhook_url[:50]}...`" if len(webhook_url) > 50 else f"`{webhook_url}`", inline=False)
        
        embed.set_footer(text=f"{len(self.channel_map)} total mappings")
        return embed
    
    async def _select_channel(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        try:
            self.selected_channel_id = int(interaction.data['values'][0])
            self._rebuild()
            embed = await self._build_embed()
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)
    
    async def _remove_mapping(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        if self.selected_channel_id is None:
            await interaction.response.send_message("❌ Please select a channel first.", ephemeral=True)
            return
        
        # Remove from local copy
        if self.selected_channel_id in self.channel_map:
            del self.channel_map[self.selected_channel_id]
            # Save to file
            if _save_channel_map(self.channel_map):
                await interaction.response.send_message(f"✅ Removed mapping for channel `{self.selected_channel_id}`", ephemeral=True)
                # Reload main view
                self.selected_channel_id = None
                self._rebuild()
                embed = await self._build_embed()
                await interaction.followup.edit_message(interaction.message.id, embed=embed, view=self)
            else:
                await interaction.response.send_message("❌ Failed to save changes.", ephemeral=True)
        else:
            await interaction.response.send_message("❌ Mapping not found.", ephemeral=True)
    
    async def _update_webhook(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        if self.selected_channel_id is None:
            await interaction.response.send_message("❌ Please select a channel first.", ephemeral=True)
            return
        
        # Prompt for new webhook URL
        modal = WebhookUpdateModal(self.bot, self.channel_map, self.selected_channel_id, self.owner_id)
        await interaction.response.send_modal(modal)
    
    async def _back(self, interaction: discord.Interaction) -> None:
        if not await self._guard(interaction):
            return
        # Go back to view mappings
        channel_map = _load_channel_map()
        view = MappingViewView(self.bot, channel_map, self.owner_id)
        content, _ = view._get_page_content(0)
        embed = discord.Embed(
            title="Channel Mappings",
            description=content,
            color=discord.Color.blurple()
        )
        embed.set_footer(text=f"Page 1 of {max(1, len(view.guild_mappings))} ({len(channel_map)} total mappings)")
        await interaction.response.edit_message(embed=embed, view=view)

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
        try:
            if int(interaction.user.id) != self.owner_id:
                await interaction.response.send_message("❌ This modal is not for you.", ephemeral=True)
                return
            
            new_url = str(self.webhook_url_input.value).strip()
            if not new_url.startswith("https://discord.com/api/webhooks/"):
                await interaction.response.send_message("❌ Invalid webhook URL format.", ephemeral=True)
                return
            
            # Update mapping
            self.channel_map[self.channel_id] = new_url
            if _save_channel_map(self.channel_map):
                await interaction.response.send_message(f"✅ Updated webhook URL for channel `{self.channel_id}`", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to save changes.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

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
            line = f"{mark} [{nm}]({link}) `{chid}`"
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

# Create bot instance
bot = DiscumCommandBot()

@bot.tree.command(name="discum", description="Browse and manage Discum bot channel mappings")
@app_commands.describe(action="Action to perform")
@app_commands.choices(action=[
    app_commands.Choice(name="browse", value="browse"),
])
async def discum_command(interaction: discord.Interaction, action: app_commands.Choice[str]):
    """Main /discum command handler."""
    if action.value != "browse":
        await interaction.response.send_message("❌ Unknown action.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    # Load channel map (always show the first screen with the button so the button is visible)
    channel_map = _load_channel_map()
    owner_id = int(interaction.user.id)
    
    class BrowseView(discord.ui.View):
        def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
            super().__init__(timeout=600)
            self.bot = bot_obj
            self.channel_map = channel_map
            self.owner_id = owner_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return int(interaction.user.id) == self.owner_id
        
        @discord.ui.button(label="View Current Mappings", style=discord.ButtonStyle.primary, emoji="📋", row=0)
        async def view_mappings(self, interaction: discord.Interaction, button: discord.ui.Button):
            channel_map = _load_channel_map()
            if not channel_map:
                await interaction.response.edit_message(
                    content="**No channel mappings configured.** Use « Browse source & map » or the main discum bot to add mappings.",
                    embed=None,
                    view=None
                )
                return
            view = MappingViewView(self.bot, channel_map, self.owner_id)
            content, max_page = view._get_page_content(0)
            embed = discord.Embed(
                title="Channel Mappings",
                description=content,
                color=discord.Color.blurple()
            )
            embed.set_footer(text=f"Page 1 of {max(1, len(view.guild_mappings))} ({len(channel_map)} total mappings)")
            await interaction.response.edit_message(embed=embed, view=view)

        @discord.ui.button(label="Browse source & map", style=discord.ButtonStyle.secondary, emoji="🗺️", row=0)
        async def browse_source(self, interaction: discord.Interaction, button: discord.ui.Button):
            browse_token = _get_browse_user_token()
            if not browse_token:
                await interaction.response.edit_message(
                    content="**Missing user token for browsing.** Set DISCUM_BOT or DISCUM_USER_DISCUMBOT in config/tokens.env (same as the main discumbot).",
                    embed=None,
                    view=None
                )
                return
            from discord_user_api import list_user_guilds
            info = await list_user_guilds(user_token=browse_token)
            if not info.get("ok"):
                await interaction.response.edit_message(
                    content=f"**Could not list guilds.** {info.get('reason', 'unknown')}",
                    embed=None,
                    view=None
                )
                return
            guilds = info.get("guilds") or []
            if not guilds:
                await interaction.response.edit_message(
                    content="**No guilds found** for the configured user token.",
                    embed=None,
                    view=None
                )
                return
            view_guild_pick = _GuildPickView(guilds, self.bot, self.owner_id)
            embed = discord.Embed(
                title="Discum browse",
                description="Pick a source guild to browse:",
                color=discord.Color.blurple()
            )
            await interaction.response.edit_message(embed=embed, view=view_guild_pick)
    
    view = BrowseView(bot, channel_map, owner_id)
    
    if not channel_map:
        embed = discord.Embed(
            title="Discum Bot Mappings",
            description="No channel mappings configured yet.\n\n👉 **Click the button below** to open the mappings viewer (you can add mappings via the main discum bot, then use this to view/remove/update).",
            color=discord.Color.blurple()
        )
    else:
        embed = discord.Embed(
            title="Discum Bot Mappings",
            description=f"**{len(channel_map)}** channel mapping(s) configured.\n\n👉 **Click the button below** to view mappings by server and to remove/update them.",
            color=discord.Color.blurple()
        )
    
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

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
    """Main entry point."""
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
