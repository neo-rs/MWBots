"""Discum Command Bot - Slash Command Handler for MWDiscumBot

This bot handles slash commands for the MWDiscumBot, specifically the /discum browse command.
It runs separately from the main discum client (which uses a user account token) and uses
a regular bot token (discord.py) to handle slash commands.

Commands:
- /discum browse: Browse and manage channel mappings
"""

import sys
import os
import json
from typing import Optional, Dict, List, Set, Tuple
from pathlib import Path

# Fix Windows console encoding
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# Standalone root = MWDiscumBot folder
try:
    _project_root = str(Path(__file__).resolve().parent)
except Exception:
    _project_root = os.path.dirname(os.path.abspath(__file__))

import discord
from discord import app_commands
from discord.ext import commands
import requests

# Config paths
_CONFIG_DIR = os.path.join(_project_root, "config")
_CHANNEL_MAP_PATH = os.path.join(_CONFIG_DIR, "channel_map.json")
_TOKENS_ENV_PATH = os.path.join(_CONFIG_DIR, "tokens.env")
_SETTINGS_JSON_PATH = os.path.join(_CONFIG_DIR, "settings.json")

# Load config
_CONFIG_RAW: Dict[str, str] = {}

def _load_env_file(path: str) -> None:
    """Minimal .env reader."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("-"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if not key:
                    continue
                _CONFIG_RAW[key] = value
    except FileNotFoundError:
        return
    except Exception:
        pass

def cfg_get(key: str, default: str = "") -> str:
    """Get config value from env or settings.json."""
    env_val = _CONFIG_RAW.get(key, "").strip()
    if env_val:
        return env_val
    try:
        with open(_SETTINGS_JSON_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
            if isinstance(data, dict):
                val = data.get(key, default)
                return str(val).strip() if val else default
    except Exception:
        pass
    return default

# Load config
_load_env_file(_TOKENS_ENV_PATH)

# Get bot token (for slash commands, we need a bot token, not user account token)
BOT_TOKEN = str(
    cfg_get("DISCORD_BOT_TOKEN")
    or cfg_get("DISCORD_BOT_DISCUMBOT")
    or cfg_get("BOT_TOKEN")
    or ""
).strip()

MIRRORWORLD_SERVER_ID = int(cfg_get("mirrorworld_server_id", "0") or 0)

if not BOT_TOKEN:
    print("[ERROR] Bot token not found. Please set DISCORD_BOT_TOKEN in config/tokens.env")
    sys.exit(1)

if not MIRRORWORLD_SERVER_ID:
    print("[WARN] mirrorworld_server_id not set. Commands will be global (may take up to 1 hour to sync)")

def load_channel_map() -> Dict[int, str]:
    """Load channel map JSON ({source_channel_id: webhook_url})."""
    try:
        with open(_CHANNEL_MAP_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[int, str] = {}
    for k, v in data.items():
        try:
            cid = int(str(k).strip())
        except Exception:
            continue
        url = str(v or "").strip()
        if url:
            out[cid] = url
    return out

def save_channel_map(channel_map: Dict[int, str]) -> bool:
    """Save channel map JSON."""
    try:
        # Convert int keys to strings for JSON
        data = {str(k): v for k, v in channel_map.items()}
        with open(_CHANNEL_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save channel map: {e}")
        return False

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
        self.guild_mappings: List[Tuple[discord.Guild, List[Tuple[int, str, Optional[str]]]]] = []
        self._build_guild_mappings()
        self._rebuild_buttons()
    
    def _build_guild_mappings(self) -> None:
        """Build list of guilds and their channel mappings."""
        guild_dict: Dict[int, List[Tuple[int, str, Optional[str]]]] = {}
        
        for channel_id, webhook_url in self.channel_map.items():
            try:
                channel = self.bot.get_channel(channel_id)
                if channel and hasattr(channel, 'guild') and channel.guild:
                    guild_id = channel.guild.id
                    channel_name = getattr(channel, 'name', f'Channel-{str(channel_id)[-6:]}')
                    # Resolve webhook destination
                    dest_channel_id, dest_channel_name = resolve_webhook_destination(webhook_url, self.bot)
                    dest_display = dest_channel_name if dest_channel_name else "webhook"
                    
                    if guild_id not in guild_dict:
                        guild_dict[guild_id] = []
                    guild_dict[guild_id].append((channel_id, channel_name, dest_display))
            except Exception:
                # Channel not found or error - still show it
                if 0 not in guild_dict:
                    guild_dict[0] = []
                dest_channel_id, dest_channel_name = resolve_webhook_destination(webhook_url, self.bot)
                dest_display = dest_channel_name if dest_channel_name else "webhook"
                guild_dict[0].append((channel_id, f"Channel-{str(channel_id)[-6:]}", dest_display))
        
        # Sort by guild name
        self.guild_mappings = []
        for guild_id, mappings in guild_dict.items():
            try:
                guild = self.bot.get_guild(guild_id) if guild_id > 0 else None
                guild_name = guild.name if guild else "Unknown Guild"
                self.guild_mappings.append((guild, sorted(mappings, key=lambda x: x[1])))
            except Exception:
                self.guild_mappings.append((None, sorted(mappings, key=lambda x: x[1])))
        
        # Sort by guild name
        self.guild_mappings.sort(key=lambda x: x[0].name if x[0] else "ZZZ")
    
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
        
        guild, mappings = self.guild_mappings[page]
        guild_name = guild.name if guild else "Unknown Guild"
        
        lines = [f"**{guild_name}**"]
        if guild:
            lines.append(f"*Guild ID: {guild.id}*")
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
        self.channel_map = load_channel_map()
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
        
        # Build select menu with channels
        options: List[discord.SelectOption] = []
        for channel_id, webhook_url in list(self.channel_map.items())[:25]:  # Discord limit
            try:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    channel_name = getattr(channel, 'name', f'Channel-{channel_id}')
                    guild_name = getattr(channel.guild, 'name', 'Unknown') if hasattr(channel, 'guild') and channel.guild else 'Unknown'
                    label = f"{guild_name} / {channel_name}"
                else:
                    label = f"Channel-{str(channel_id)[-6:]}"
            except Exception:
                label = f"Channel-{str(channel_id)[-6:]}"
            
            # Truncate label
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
            try:
                channel = self.bot.get_channel(self.selected_channel_id)
                if channel:
                    channel_name = getattr(channel, 'name', f'Channel-{self.selected_channel_id}')
                    guild_name = getattr(channel.guild, 'name', 'Unknown') if hasattr(channel, 'guild') and channel.guild else 'Unknown'
                    embed.add_field(name="Selected Channel", value=f"**{guild_name}** / #{channel_name}\n`{self.selected_channel_id}`", inline=False)
                else:
                    embed.add_field(name="Selected Channel", value=f"`{self.selected_channel_id}`", inline=False)
            except Exception:
                embed.add_field(name="Selected Channel", value=f"`{self.selected_channel_id}`", inline=False)
            
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
            if save_channel_map(self.channel_map):
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
        channel_map = load_channel_map()
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
            if save_channel_map(self.channel_map):
                await interaction.response.send_message(f"✅ Updated webhook URL for channel `{self.channel_id}`", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Failed to save changes.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {e}", ephemeral=True)

class DiscumCommandBot(commands.Bot):
    """Command bot for handling /discum browse command."""
    
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)
        self.tree = app_commands.CommandTree(self)
    
    async def setup_hook(self) -> None:
        """Sync commands on startup."""
        if MIRRORWORLD_SERVER_ID:
            guild_obj = discord.Object(id=MIRRORWORLD_SERVER_ID)
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
            print(f"[INFO] Synced commands to guild {MIRRORWORLD_SERVER_ID}")
        else:
            await self.tree.sync()
            print("[INFO] Synced commands globally (may take up to 1 hour)")
    
    async def on_ready(self) -> None:
        print(f"[INFO] Logged in as {self.user}")
        print(f"[INFO] Ready to handle commands")

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
    
    # Load channel map
    channel_map = load_channel_map()
    
    if not channel_map:
        await interaction.followup.send("**No channel mappings configured.**\n\nUse the main discum bot to add mappings.", ephemeral=True)
        return
    
    # Create view with button to view mappings
    owner_id = int(interaction.user.id)
    
    class BrowseView(discord.ui.View):
        def __init__(self, bot_obj: commands.Bot, channel_map: Dict[int, str], owner_id: int):
            super().__init__(timeout=600)
            self.bot = bot_obj
            self.channel_map = channel_map
            self.owner_id = owner_id
        
        async def interaction_check(self, interaction: discord.Interaction) -> bool:
            return int(interaction.user.id) == self.owner_id
        
        @discord.ui.button(label="View Current Mappings", style=discord.ButtonStyle.primary, emoji="📋")
        async def view_mappings(self, interaction: discord.Interaction, button: discord.ui.Button):
            # Reload channel map
            channel_map = load_channel_map()
            view = MappingViewView(self.bot, channel_map, self.owner_id)
            content, max_page = view._get_page_content(0)
            embed = discord.Embed(
                title="Channel Mappings",
                description=content,
                color=discord.Color.blurple()
            )
            embed.set_footer(text=f"Page 1 of {max(1, len(view.guild_mappings))} ({len(channel_map)} total mappings)")
            await interaction.response.edit_message(embed=embed, view=view)
    
    view = BrowseView(bot, channel_map, owner_id)
    
    embed = discord.Embed(
        title="Discum Bot Mappings",
        description=f"**{len(channel_map)}** channel mapping(s) configured.\n\nClick the button below to view mappings organized by guild server.",
        color=discord.Color.blurple()
    )
    
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)

async def main():
    """Main entry point."""
    print("=" * 50)
    print("DISCUM COMMAND BOT")
    print("=" * 50)
    print(f"[INFO] Channel map path: {_CHANNEL_MAP_PATH}")
    print(f"[INFO] Target guild: {MIRRORWORLD_SERVER_ID or 'Global'}")
    await bot.start(BOT_TOKEN)

if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
