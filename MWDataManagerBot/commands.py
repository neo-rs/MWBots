from __future__ import annotations

from typing import Any, Dict, Optional

from fetchall import iter_fetchall_entries, run_fetchall, upsert_mapping
from logging_utils import log_info, log_warn
import settings_store as cfg


def register_commands(*, bot, forwarder) -> None:
    """
    Register prefix commands on the provided discord.py commands.Bot instance.

    This is intentionally kept as a separate module so command debugging does not
    require scrolling through the live forwarder pipeline.
    """

    def _pick_fetchall_source_token() -> str:
        try:
            tokens = getattr(forwarder, "tokens", None)
            if not isinstance(tokens, dict):
                return ""
            return str(tokens.get("FETCHALL_USER_TOKEN") or "").strip()
        except Exception:
            return ""

    @bot.command(name="fetchall")
    async def fetchall_cmd(ctx) -> None:
        """Run fetch-all for all configured guild entries."""
        try:
            entries = iter_fetchall_entries()
            if not entries:
                await ctx.send("No fetchall mappings found (MWDataManagerBot/config/fetchall_mappings.json).")
                return
            await ctx.send(f"Starting fetchall for {len(entries)} mapping(s)...")
            ok = 0
            source_token = _pick_fetchall_source_token() or None
            for entry in entries:
                result = await run_fetchall(
                    bot=bot,
                    entry=entry,
                    destination_guild=getattr(ctx, "guild", None),
                    source_user_token=source_token,
                )
                if result.get("ok"):
                    ok += 1
            await ctx.send(f"Fetchall complete: {ok}/{len(entries)} succeeded.")
        except Exception as e:
            log_warn(f"fetchall command failed: {e}")
            await ctx.send(f"Fetchall failed: {type(e).__name__}: {e}")

    @bot.command(name="fetch")
    async def fetch_cmd(ctx, source_guild_id: str = "") -> None:
        """Run fetch-all for a single source guild id (must exist in mappings)."""
        try:
            sgid = int(source_guild_id)
        except Exception:
            await ctx.send("Usage: !fetch <source_guild_id>")
            return
        entries = iter_fetchall_entries()
        entry = None
        for e in entries:
            try:
                if int(e.get("source_guild_id", 0)) == sgid:
                    entry = e
                    break
            except Exception:
                continue
        if not entry:
            await ctx.send(f"No mapping found for source_guild_id={sgid}. Use !setfetchguild first.")
            return
        source_token = _pick_fetchall_source_token() or None
        result = await run_fetchall(
            bot=bot,
            entry=entry,
            destination_guild=getattr(ctx, "guild", None),
            source_user_token=source_token,
        )
        await ctx.send(f"Fetch result: ok={result.get('ok')} created={result.get('created')} existing={result.get('existing')}")

    @bot.command(name="setfetchguild")
    async def setfetchguild_cmd(ctx, source_guild_id: str = "", destination_category_id: str = "") -> None:
        """Set/update a mapping entry in fetchall_mappings.json."""
        try:
            sgid = int(source_guild_id)
            dcid = int(destination_category_id) if destination_category_id else 0
            name = None
            try:
                if ctx.guild:
                    name = getattr(ctx.guild, "name", None)
            except Exception:
                name = None
            entry = upsert_mapping(source_guild_id=sgid, name=name, destination_category_id=dcid or None)
            await ctx.send(f"Saved mapping: source_guild_id={entry.get('source_guild_id')} destination_category_id={entry.get('destination_category_id')}")
        except Exception as e:
            log_warn(f"setfetchguild failed: {e}")
            await ctx.send(f"setfetchguild failed: {type(e).__name__}: {e}")

    @bot.command(name="whereami")
    async def whereami_cmd(ctx) -> None:
        """Quick runtime proof for discord-side debugging (local-only)."""
        try:
            await ctx.send(f"MWDataManagerBot is running. guild={getattr(ctx.guild,'id',None)} channel={getattr(ctx.channel,'id',None)}")
        except Exception:
            await ctx.send("MWDataManagerBot is running.")

    @bot.command(name="status")
    async def status_cmd(ctx) -> None:
        """Show current monitor/destination configuration (helps diagnose 'no actions')."""
        try:
            monitored = len(cfg.SMART_SOURCE_CHANNELS)
            cats = len(cfg.MONITOR_CATEGORY_IDS)
            monitor_all = bool(cfg.MONITOR_ALL_DESTINATION_CHANNELS)
            webhook_only = bool(getattr(cfg, "MONITOR_WEBHOOK_MESSAGES_ONLY", False))
            raw_unwrap = bool(getattr(cfg, "ENABLE_RAW_LINK_UNWRAP", False))
            raw_followup = bool(getattr(cfg, "SEND_RAW_LINKS_FOLLOWUP", False))
            fetchall_has_token = bool(_pick_fetchall_source_token())
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
            await ctx.send(
                "MWDataManagerBot status:\n"
                f"- destination_guild_ids={sorted(list(cfg.DESTINATION_GUILD_IDS))}\n"
                f"- monitored_channels={monitored} monitor_category_ids={sorted(list(cfg.MONITOR_CATEGORY_IDS))} "
                f"monitor_all={monitor_all} webhook_only={webhook_only}\n"
                f"- raw_unwrap={raw_unwrap} raw_followup={raw_followup}\n"
                f"- fetchall_user_token_loaded={fetchall_has_token}\n"
                f"- smartfilter_destinations_set={sum(1 for x in dests if x>0)}/{len(dests)}\n"
                f"- global_trigger_destinations_set={sum(1 for x in globals_ if x>0)}/{len(globals_)}\n"
                f"- fallback_channel_id={int(cfg.FALLBACK_CHANNEL_ID or 0)}"
            )
        except Exception as e:
            log_warn(f"status failed: {e}")
            await ctx.send(f"status failed: {type(e).__name__}: {e}")

