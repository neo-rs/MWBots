"""PingBot slash command handler – manage ping channels and delay from Discord.

Similar to /discum browse but for PingBot settings:
- /ping settings: view/edit which channels get pings and the ping delay.
"""

import sys
import os
from typing import Dict, List, Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import discord
from discord import app_commands
from discord.ext import commands

from ping_config import (
    SETTINGS_PATH,
    TOKENS_ENV_PATH,
    load_env_file,
    load_settings,
    save_settings,
    DEFAULT_SETTINGS,
)

_CONFIG_RAW = load_env_file(TOKENS_ENV_PATH)


def _cfg_get(key: str, default: str = "") -> str:
    v = _CONFIG_RAW.get(key, "").strip() or os.environ.get(key, "").strip() or os.environ.get(key.upper(), "").strip()
    return v if v else default


BOT_TOKEN = str(
    _cfg_get("DISCORD_BOT_TOKEN") or _cfg_get("BOT_TOKEN") or ""
).strip()
_env_guild = int(_cfg_get("mirrorworld_server_id") or _cfg_get("MIRRORWORLD_SERVER") or _cfg_get("MIRRORWORLD_GUILD_ID") or "0") or 0
if _env_guild:
    MIRRORWORLD_SERVER_ID = _env_guild
else:
    _s = load_settings(SETTINGS_PATH)
    MIRRORWORLD_SERVER_ID = int(_s.get("mirrorworld_guild_id") or 0)


def _load_settings() -> Dict[str, Any]:
    return load_settings(SETTINGS_PATH)


def _save_settings(settings: Dict[str, Any]) -> bool:
    return save_settings(settings, SETTINGS_PATH)


def _ui_embed(title: str, description: str, color: int = 0x5865F2) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


class PingCommandBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        try:
            if MIRRORWORLD_SERVER_ID:
                guild_obj = discord.Object(id=MIRRORWORLD_SERVER_ID)
                self.tree.copy_global_to(guild=guild_obj)
                await self.tree.sync(guild=guild_obj)
                print(f"[INFO] PingBot slash commands synced to guild {MIRRORWORLD_SERVER_ID}")
            else:
                await self.tree.sync()
        except Exception as e:
            print(f"[ERROR] PingBot slash sync failed: {e}")

    async def on_ready(self) -> None:
        print(f"[INFO] PingBot command bot logged in as {self.user}")


bot = PingCommandBot()


@bot.tree.command(name="ping", description="Manage PingBot: ping channels and delay")
@app_commands.describe(action="Action to perform")
@app_commands.choices(action=[
    app_commands.Choice(name="settings", value="settings"),
])
async def ping_command(interaction: discord.Interaction, action: app_commands.Choice[str]):
    if action.value != "settings":
        await interaction.response.send_message("❌ Unknown action.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    settings = _load_settings()
    channel_ids = settings.get("ping_channel_ids") or []
    delay = settings.get("ping_delay_seconds", 60)
    owner_id = interaction.user.id

    class SettingsView(discord.ui.View):
        def __init__(self, bot_obj: commands.Bot, settings: Dict[str, Any], owner_id: int):
            super().__init__(timeout=600)
            self.bot = bot_obj
            self.settings = settings.copy()
            self.owner_id = owner_id

        async def interaction_check(self, i: discord.Interaction) -> bool:
            return i.user.id == self.owner_id

        def _channels_text(self) -> str:
            cids = self.settings.get("ping_channel_ids") or []
            if not cids:
                return "_No channels configured._"
            lines = []
            for cid in cids[:25]:
                ch = self.bot.get_channel(int(cid))
                name = f"#{ch.name}" if ch else f"`{cid}`"
                lines.append(f"• {name}")
            if len(cids) > 25:
                lines.append(f"_... and {len(cids) - 25} more_")
            return "\n".join(lines)

        @discord.ui.button(label="View channels & delay", style=discord.ButtonStyle.primary, emoji="📋", row=0)
        async def view_btn(self, i: discord.Interaction, _b: discord.ui.Button):
            s = _load_settings()
            self.settings = s
            cids = s.get("ping_channel_ids") or []
            delay_s = s.get("ping_delay_seconds", 60)
            embed = discord.Embed(
                title="PingBot settings",
                description="Current ping channels and delay.",
                color=discord.Color.blurple(),
            )
            embed.add_field(name="Ping delay (seconds)", value=str(delay_s), inline=True)
            embed.add_field(name="Ping channels", value=f"{len(cids)} channel(s)", inline=True)
            embed.add_field(name="Channels", value=self._channels_text(), inline=False)
            embed.set_footer(text="Use buttons below to add/remove channels or set delay.")
            await i.response.edit_message(embed=embed, view=self)

        @discord.ui.button(label="Add channels", style=discord.ButtonStyle.secondary, emoji="➕", row=0)
        async def add_btn(self, i: discord.Interaction, _b: discord.ui.Button):
            guild = i.guild
            if not guild:
                await i.response.send_message("Use this in a server.", ephemeral=True)
                return
            channels = [c for c in guild.text_channels if isinstance(c, discord.TextChannel)][:25]
            if not channels:
                await i.response.send_message("No text channels in this server.", ephemeral=True)
                return
            options = [
                discord.SelectOption(label=f"#{ch.name}"[:100], value=str(ch.id), description=f"ID: {ch.id}")
                for ch in channels[:25]
            ]
            select = discord.ui.Select(
                placeholder="Select channels to add for pinging",
                min_values=1,
                max_values=min(len(options), 25),
                options=options,
            )
            async def select_cb(ii: discord.Interaction):
                if ii.user.id != self.owner_id:
                    await ii.response.send_message("Not your menu.", ephemeral=True)
                    return
                s = _load_settings()
                cids = list(s.get("ping_channel_ids") or [])
                added = 0
                for v in ii.data.get("values", []):
                    try:
                        cid = int(v)
                        if cid not in cids:
                            cids.append(cid)
                            added += 1
                    except ValueError:
                        pass
                s["ping_channel_ids"] = cids
                if _save_settings(s):
                    self.settings = s
                    await ii.response.send_message(f"✅ Added {added} channel(s). Total: {len(cids)}.", ephemeral=True)
                    embed = discord.Embed(title="PingBot settings", description="Channels updated.", color=discord.Color.blurple())
                    embed.add_field(name="Ping channels", value=self._channels_text(), inline=False)
                    await ii.message.edit(embed=embed, view=self)
                else:
                    await ii.response.send_message("❌ Failed to save.", ephemeral=True)
            select.callback = select_cb
            view = discord.ui.View()
            view.add_item(select)
            await i.response.send_message("Select channel(s) to add:", view=view, ephemeral=True)

        @discord.ui.button(label="Remove channel", style=discord.ButtonStyle.danger, emoji="➖", row=1)
        async def remove_btn(self, i: discord.Interaction, _b: discord.ui.Button):
            cids = self.settings.get("ping_channel_ids") or []
            if not cids:
                await i.response.send_message("No channels to remove.", ephemeral=True)
                return
            options = []
            for cid in cids[:25]:
                ch = self.bot.get_channel(int(cid))
                label = f"#{ch.name}" if ch else f"Channel {cid}"
                options.append(discord.SelectOption(label=label[:100], value=str(cid)))
            select = discord.ui.Select(placeholder="Select channel to remove", min_values=1, max_values=1, options=options)
            async def select_cb(ii: discord.Interaction):
                if ii.user.id != self.owner_id:
                    await ii.response.send_message("Not your menu.", ephemeral=True)
                    return
                s = _load_settings()
                cids = list(s.get("ping_channel_ids") or [])
                for v in ii.data.get("values", []):
                    try:
                        cid = int(v)
                        cids = [x for x in cids if x != cid]
                    except ValueError:
                        pass
                s["ping_channel_ids"] = cids
                if _save_settings(s):
                    self.settings = s
                    await ii.response.send_message("✅ Channel removed.", ephemeral=True)
                    embed = discord.Embed(title="PingBot settings", description="Channel removed.", color=discord.Color.blurple())
                    embed.add_field(name="Ping channels", value=self._channels_text(), inline=False)
                    await ii.message.edit(embed=embed, view=self)
                else:
                    await ii.response.send_message("❌ Failed to save.", ephemeral=True)
            select.callback = select_cb
            view = discord.ui.View()
            view.add_item(select)
            await i.response.send_message("Select channel to remove:", view=view, ephemeral=True)

        @discord.ui.button(label="Set delay (seconds)", style=discord.ButtonStyle.secondary, emoji="⏱️", row=1)
        async def delay_btn(self, i: discord.Interaction, _b: discord.ui.Button):
            modal = discord.ui.Modal(title="Ping delay")
            inp = discord.ui.TextInput(
                label="Delay (seconds)",
                placeholder="e.g. 60",
                default=str(self.settings.get("ping_delay_seconds", 60)),
                required=True,
                max_length=10,
            )
            modal.add_item(inp)
            async def on_submit(ii: discord.Interaction):
                if ii.user.id != self.owner_id:
                    await ii.response.send_message("Not your modal.", ephemeral=True)
                    return
                try:
                    val = int(inp.value.strip())
                    if val < 0:
                        val = 0
                    if val > 86400:
                        val = 86400
                except ValueError:
                    await ii.response.send_message("Enter a number (0–86400).", ephemeral=True)
                    return
                s = _load_settings()
                s["ping_delay_seconds"] = val
                if _save_settings(s):
                    self.settings = s
                    await ii.response.send_message(f"✅ Ping delay set to **{val}** seconds.", ephemeral=True)
                else:
                    await ii.response.send_message("❌ Failed to save.", ephemeral=True)
            modal.on_submit = on_submit
            await i.response.send_modal(modal)

    view = SettingsView(bot, settings, owner_id)
    embed = discord.Embed(
        title="PingBot settings",
        description="Manage which channels get pings and the ping delay.",
        color=discord.Color.blurple(),
    )
    embed.add_field(name="Ping delay (seconds)", value=str(delay), inline=True)
    embed.add_field(name="Ping channels", value=f"{len(channel_ids)} channel(s)", inline=True)
    embed.add_field(name="Channels", value=view._channels_text(), inline=False)
    embed.set_footer(text="Use buttons to add/remove channels or set delay.")
    await interaction.followup.send(embed=embed, view=view, ephemeral=True)


async def main():
    await bot.start(BOT_TOKEN)


if __name__ == "__main__":
    if not BOT_TOKEN:
        print("[ERROR] Set BOT_TOKEN or DISCORD_BOT_TOKEN in MWPingBot/config/tokens.env")
        sys.exit(1)
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")
