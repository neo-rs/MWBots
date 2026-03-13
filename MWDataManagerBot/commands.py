from __future__ import annotations

import asyncio
import json
import re
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from keywords import (
    add_keyword,
    invalidate_keywords_cache,
    load_keyword_channel_overrides,
    load_keywords,
    remove_keyword,
    remove_keyword_channel_override,
    scan_keywords,
    set_keyword_channel_override,
)
from logging_utils import log_info, log_warn
import settings_store as cfg

# NOTE: Slash commands require discord.py app_commands to resolve type hints.
# app_commands uses typing.get_type_hints() which evaluates annotation strings
# using module globals. Therefore `discord` MUST exist in module globals.
try:
    import discord  # type: ignore
    from discord import app_commands  # type: ignore
except Exception:
    discord = None  # type: ignore
    app_commands = None  # type: ignore


def register_commands(*, bot, forwarder) -> None:
    """
    Register slash commands on the provided discord.py commands.Bot instance.

    This is intentionally kept as a separate module so command debugging does not
    require scrolling through the live forwarder pipeline.
    """

    def _reload_keywords_into_forwarder() -> int:
        """Reload keyword list and update the forwarder in-memory copy."""
        try:
            invalidate_keywords_cache()
        except Exception:
            pass
        try:
            kws = load_keywords(force=True)
        except Exception:
            kws = []
        try:
            setattr(forwarder, "keywords_list", list(kws))
        except Exception:
            pass
        return int(len(kws))

    def _render_progress_bar(done: int, total: int, *, width: int = 28) -> str:
        try:
            d = int(done or 0)
            t = int(total or 0)
        except Exception:
            d, t = 0, 0
        if t <= 0:
            return "[----------------------------] 0% (0/0)"
        d = max(0, min(t, d))
        filled = int(round((d / t) * width)) if t else 0
        filled = max(0, min(width, filled))
        bar = "[" + ("=" * filled) + ("-" * (width - filled)) + "]"
        pct = int(round((d / t) * 100)) if t else 0
        return f"{bar} {pct}% ({d}/{t})"

    def _parse_csv_ints(text: str) -> List[int]:
        out: List[int] = []
        for part in (text or "").replace("\n", ",").split(","):
            p = part.strip()
            if not p:
                continue
            try:
                v = int(p)
            except Exception:
                continue
            if v > 0:
                out.append(v)
        # De-dupe preserving order
        seen = set()
        dedup: List[int] = []
        for v in out:
            if v in seen:
                continue
            seen.add(v)
            dedup.append(v)
        return dedup

    async def _cmd_whereami(ctx) -> None:
        """Quick runtime proof for discord-side debugging (local-only)."""
        try:
            gid = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
            cid = int(getattr(getattr(ctx, "channel", None), "id", 0) or 0)
            try:
                await ctx.send(embed=_ui_embed("Where am I", f"guild_id={gid}\nchannel_id={cid}"))
            except Exception:
                await ctx.send(f"MWDataManagerBot is running. guild={gid} channel={cid}")
        except Exception:
            await ctx.send("MWDataManagerBot is running.")

    async def _cmd_status(ctx) -> None:
        """Show current monitor/destination configuration (helps diagnose 'no actions')."""
        try:
            monitored = len(cfg.SMART_SOURCE_CHANNELS)
            cats = len(cfg.MONITOR_CATEGORY_IDS)
            monitor_all = bool(cfg.MONITOR_ALL_DESTINATION_CHANNELS)
            webhook_only = bool(getattr(cfg, "MONITOR_WEBHOOK_MESSAGES_ONLY", False))
            raw_unwrap = bool(getattr(cfg, "ENABLE_RAW_LINK_UNWRAP", False))
            use_wh = bool(getattr(cfg, "USE_WEBHOOKS_FOR_FORWARDING", False))
            dests = [
                int(cfg.FALLBACK_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_AMAZON_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_UPCOMING_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_DEFAULT_CHANNEL_ID or 0),
            ]
            globals_ = [
                int(cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID or 0),
                int(cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID or 0),
            ]
            text = (
                "MWDataManagerBot status\n"
                f"- destination_guild_ids={sorted(list(cfg.DESTINATION_GUILD_IDS))}\n"
                f"- monitored_channels={monitored} monitor_category_ids={sorted(list(cfg.MONITOR_CATEGORY_IDS))} "
                f"monitor_all={monitor_all} webhook_only={webhook_only}\n"
                f"- raw_unwrap={raw_unwrap}\n"
                f"- use_webhooks_for_forwarding={use_wh}\n"
                f"- smartfilter_destinations_set={sum(1 for x in dests if x>0)}/{len(dests)}\n"
                f"- global_trigger_destinations_set={sum(1 for x in globals_ if x>0)}/{len(globals_)}\n"
                f"- fallback_channel_id={int(cfg.FALLBACK_CHANNEL_ID or 0)}"
            )
            try:
                await ctx.send(embed=_ui_embed("Status", text[:3500]))
            except Exception:
                await ctx.send(text[:1950])
        except Exception as e:
            log_warn(f"status failed: {e}")
            await ctx.send(f"status failed: {type(e).__name__}: {e}")

    # ---------------------------------------------------------------------
    # Slash commands (registered only to destination guild(s))
    # ---------------------------------------------------------------------
    if discord is None or app_commands is None:
        log_warn("Slash commands disabled: discord.py app_commands not available")
        return

    dest_guild_ids = sorted(int(x) for x in (cfg.DESTINATION_GUILD_IDS or set()) if int(x) > 0)
    if not dest_guild_ids:
        return
    guild_objs = [discord.Object(id=int(gid)) for gid in dest_guild_ids]

    kw = app_commands.Group(name="keywords", description="Manage monitored keywords")
    kwchan = app_commands.Group(name="keywordchannel", description="Route monitored keyword matches to extra channels")

    async def _kw_autocomplete(interaction: discord.Interaction, current: str):
        try:
            cur = str(current or "").strip().lower()
        except Exception:
            cur = ""
        kws = load_keywords(force=True)
        out = []
        for k in kws:
            try:
                if cur and cur not in str(k).lower():
                    continue
                out.append(app_commands.Choice(name=str(k)[:100], value=str(k)))
                if len(out) >= 25:
                    break
            except Exception:
                continue
        return out

    def _ui_embed(title: str, description: str = "", *, color: int = 0x5865F2) -> "discord.Embed":
        emb = discord.Embed(title=str(title or "MWDataManagerBot"), description=(str(description) or None), color=int(color))
        try:
            emb.set_footer(text="MWDataManagerBot")
        except Exception:
            pass
        return emb

    class _SlashCtx:
        """
        Minimal ctx adapter so existing command logic can be reused for slash commands
        without re-implementing everything twice.
        """

        def __init__(self, interaction: discord.Interaction, *, ephemeral: bool = True):
            self._i = interaction
            self.guild = getattr(interaction, "guild", None)
            self.channel = getattr(interaction, "channel", None)
            self.author = getattr(interaction, "user", None)
            self._ephemeral = bool(ephemeral)

        async def send(
            self,
            content: str = None,
            *,
            embed=None,
            embeds=None,
            view=None,
            allowed_mentions=None,
            reference=None,  # ignored (followups don't support message reference)
            wait: bool = True,
        ):
            # All slash responses use followups; wrappers always defer first.
            # IMPORTANT: only pass "view" when it's a real discord.ui.View; discord.py raises on view=None.
            kwargs: Dict[str, Any] = {"ephemeral": self._ephemeral, "wait": bool(wait)}
            if content is not None:
                kwargs["content"] = content
            if embed is not None:
                kwargs["embed"] = embed
            if embeds is not None:
                kwargs["embeds"] = embeds
            if view is not None:
                kwargs["view"] = view
            if allowed_mentions is not None:
                kwargs["allowed_mentions"] = allowed_mentions
            return await self._i.followup.send(**kwargs)

    @app_commands.command(name="status", description="Show current MWDataManagerBot configuration summary")
    async def status_slash(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ctx = _SlashCtx(interaction, ephemeral=True)
        await _cmd_status(ctx)

    @app_commands.command(name="whereami", description="Show guild/channel ids (runtime proof)")
    async def whereami_slash(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ctx = _SlashCtx(interaction, ephemeral=True)
        await _cmd_whereami(ctx)

    @kw.command(name="list", description="List monitored keywords")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        kws = load_keywords(force=True)
        if not kws:
            await interaction.followup.send(embed=_ui_embed("Keywords list", "Monitored keywords: (none)"), ephemeral=True)
            return
        preview = "\n".join(f"- {k}" for k in kws[:60])
        extra = "" if len(kws) <= 60 else f"\n... (+{len(kws)-60} more)"
        await interaction.followup.send(
            embed=_ui_embed("Keywords list", f"count={len(kws)}\n\n{preview}{extra}"[:3500]),
            ephemeral=True,
        )

    @kw.command(name="add", description="Add a monitored keyword")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_add(interaction: discord.Interaction, keyword: str) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ok, reason = add_keyword(keyword)
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(
            embed=_ui_embed("Keywords add", f"ok={ok} reason={reason}\ncount={n}\nkeyword={str(keyword or '').strip()}"),
            ephemeral=True,
        )

    @kw.command(name="remove", description="Remove a monitored keyword")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_remove(interaction: discord.Interaction, keyword: str) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ok, reason = remove_keyword(keyword)
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(
            embed=_ui_embed(
                "Keywords remove",
                f"ok={ok} reason={reason}\ncount={n}\nkeyword={str(keyword or '').strip()}",
            ),
            ephemeral=True,
        )

    @kw.command(name="reload", description="Reload monitored keywords from disk")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_reload(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(embed=_ui_embed("Keywords reload", f"count={n}"), ephemeral=True)

    @kw.command(name="test", description="Test text against monitored keywords (optional: post output)")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(text="Sample text to test", send_output="If true, also post the test output to the MONITORED_KEYWORD channel")
    async def kw_test(interaction: discord.Interaction, text: str, send_output: bool = False) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        sample = str(text or "").strip()
        if not sample:
            await interaction.followup.send(embed=_ui_embed("Keywords test", "Empty text.", color=0xED4245), ephemeral=True)
            return
        kws = load_keywords(force=True)
        matched = scan_keywords(sample, kws)
        preview = ", ".join(matched[:15])
        extra = "" if len(matched) <= 15 else f" ... (+{len(matched)-15} more)"
        await interaction.followup.send(
            embed=_ui_embed("Keywords test", f"matched={len(matched)}\n{preview}{extra}".strip()[:3500]),
            ephemeral=True,
        )

        if not send_output:
            return
        dest = int(getattr(cfg, "SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID", 0) or 0)
        if dest <= 0:
            await interaction.followup.send(
                embed=_ui_embed("Keywords test", "MONITORED_KEYWORD channel is not configured.", color=0xED4245),
                ephemeral=True,
            )
            return
        try:
            who = getattr(getattr(interaction, "user", None), "display_name", None) or getattr(
                getattr(interaction, "user", None), "name", None
            )
        except Exception:
            who = None
        lines = []
        lines.append("Monitored keyword test")
        if who:
            lines.append(f"by: {who}")
        lines.append(f"matched: {len(matched)}")
        if matched:
            lines.append("keywords: " + ", ".join(matched[:25]) + ("" if len(matched) <= 25 else " ..."))
        lines.append("")
        lines.append(sample)
        out = "\n".join(lines).strip()
        try:
            import discord as _discord  # local import to keep module globals clean

            allowed = _discord.AllowedMentions.none()
        except Exception:
            allowed = None
        try:
            await forwarder._send_to_destination(dest_channel_id=dest, content=out, embeds=[], allowed_mentions=allowed)
        except Exception as e:
            await interaction.followup.send(
                embed=_ui_embed("Keywords test", f"Send failed: {type(e).__name__}: {e}", color=0xED4245),
                ephemeral=True,
            )

    @kwchan.command(name="set", description="Send a monitored keyword's matches to an extra channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.autocomplete(keyword=_kw_autocomplete)
    async def keywordchannel_set(interaction: discord.Interaction, keyword: str, channel: discord.TextChannel) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        kw_s = str(keyword or "").strip()
        if not kw_s:
            await interaction.followup.send(embed=_ui_embed("Keywordchannel set", "Empty keyword.", color=0xED4245), ephemeral=True)
            return
        ok, reason = set_keyword_channel_override(kw_s, int(channel.id))
        await interaction.followup.send(
            embed=_ui_embed(
                "Keywordchannel set",
                f"ok={ok} reason={reason}\nkeyword={kw_s}\nchannel=<#{int(channel.id)}>",
            ),
            ephemeral=True,
        )

    @kwchan.command(name="clear", description="Remove an extra channel override for a keyword")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.autocomplete(keyword=_kw_autocomplete)
    async def keywordchannel_clear(interaction: discord.Interaction, keyword: str) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        kw_s = str(keyword or "").strip()
        ok, reason = remove_keyword_channel_override(kw_s)
        await interaction.followup.send(
            embed=_ui_embed("Keywordchannel clear", f"ok={ok} reason={reason}\nkeyword={kw_s}"),
            ephemeral=True,
        )

    @kwchan.command(name="list", description="List keyword->extra channel overrides")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def keywordchannel_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        mp = load_keyword_channel_overrides(force=True)
        if not mp:
            await interaction.followup.send(embed=_ui_embed("Keywordchannel list", "overrides: (none)"), ephemeral=True)
            return
        lines = []
        for k in sorted(mp.keys())[:100]:
            try:
                cid = int(mp.get(k, 0) or 0)
            except Exception:
                cid = 0
            if cid > 0:
                lines.append(f"- **{k}** -> <#{cid}> (`{cid}`)")
        msg = "overrides:\n" + "\n".join(lines[:60])
        if len(lines) > 60:
            msg += f"\n... (+{len(lines)-60} more)"
        await interaction.followup.send(embed=_ui_embed("Keywordchannel list", msg[:3500]), ephemeral=True)

    # Add groups to the tree for destination guild(s) only.
    for g in guild_objs:
        try:
            bot.tree.add_command(kw, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /keywords to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(kwchan, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /keywordchannel to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(status_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /status to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(whereami_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /whereami to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
