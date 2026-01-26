from __future__ import annotations

from typing import Any, Dict, List, Optional

from fetchall import (
    iter_fetchall_entries,
    list_source_guild_channels,
    load_fetchall_mappings,
    run_fetchall,
    run_fetchsync,
    set_ignored_channel_ids,
    upsert_mapping,
)
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

    @bot.command(name="fetchall")
    async def fetchall_cmd(ctx) -> None:
        """Run fetch-all for all configured guild entries (mirror channel setup)."""
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
                else:
                    try:
                        sgid = int(entry.get("source_guild_id", 0) or 0)
                    except Exception:
                        sgid = 0
                    reason = str(result.get("reason") or "").strip()
                    hs = result.get("http_status")
                    await ctx.send(f"fetchall failed: sgid={sgid} reason={reason} http={hs}")
            await ctx.send(f"Fetchall complete: {ok}/{len(entries)} succeeded.")
        except Exception as e:
            log_warn(f"fetchall command failed: {e}")
            await ctx.send(f"Fetchall failed: {type(e).__name__}: {e}")

    @bot.command(name="fetchsync")
    async def fetchsync_cmd(ctx, source_guild_id: str = "") -> None:
        """Pull messages via user token and mirror them into Mirror World channels."""
        try:
            entries = iter_fetchall_entries()
            if not entries:
                await ctx.send("No fetchall mappings found (MWDataManagerBot/config/fetchall_mappings.json).")
                return

            sgid_filter: int = 0
            if (source_guild_id or "").strip():
                try:
                    sgid_filter = int(source_guild_id)
                except Exception:
                    await ctx.send("Usage: !fetchsync [source_guild_id]")
                    return

            source_token = _pick_fetchall_source_token()
            if not source_token:
                await ctx.send("Missing FETCHALL_USER_TOKEN (needed to read source servers).")
                return

            selected = []
            for e in entries:
                try:
                    if sgid_filter and int(e.get("source_guild_id", 0)) != int(sgid_filter):
                        continue
                except Exception:
                    continue
                selected.append(e)
            if not selected:
                await ctx.send(f"No mapping found for source_guild_id={sgid_filter}.")
                return

            await ctx.send(f"Starting fetchsync for {len(selected)} mapping(s)...")
            ok = 0
            total_sent = 0
            for entry in selected:
                result = await run_fetchsync(
                    bot=bot,
                    entry=entry,
                    destination_guild=getattr(ctx, "guild", None),
                    source_user_token=source_token,
                    dryrun=False,
                )
                if result.get("ok"):
                    ok += 1
                try:
                    total_sent += int(result.get("sent", 0) or 0)
                except Exception:
                    pass
                if not result.get("ok"):
                    try:
                        sgid = int(entry.get("source_guild_id", 0) or 0)
                    except Exception:
                        sgid = 0
                    reason = str(result.get("reason") or "").strip()
                    hs = result.get("http_status")
                    await ctx.send(f"fetchsync failed: sgid={sgid} reason={reason} http={hs}")
            await ctx.send(f"Fetchsync complete: {ok}/{len(selected)} succeeded. sent={total_sent}")
        except Exception as e:
            log_warn(f"fetchsync command failed: {e}")
            await ctx.send(f"Fetchsync failed: {type(e).__name__}: {e}")

    @bot.command(name="fetchauth")
    async def fetchauth_cmd(ctx, source_guild_id: str = "") -> None:
        """
        Debug fetchall token + mapping selection without leaking tokens.
        Usage: !fetchauth <source_guild_id>
        """
        sgid = 0
        try:
            sgid = int(str(source_guild_id or "").strip())
        except Exception:
            sgid = 0
        if sgid <= 0:
            await ctx.send("Usage: !fetchauth <source_guild_id>")
            return
        token = _pick_fetchall_source_token()
        if not token:
            await ctx.send("Missing FETCHALL_USER_TOKEN (needed to read source servers).")
            return

        # Load mapping entry if present (so category filters are applied)
        cfg_data = load_fetchall_mappings()
        entry = None
        for e in (cfg_data.get("guilds", []) or []):
            if isinstance(e, dict) and int(e.get("source_guild_id", 0) or 0) == sgid:
                entry = e
                break
        if entry is None:
            entry = {"source_guild_id": sgid, "destination_category_id": int(getattr(cfg, "FETCHALL_DEFAULT_DEST_CATEGORY_ID", 0) or 0)}

        result = await run_fetchsync(
            bot=bot,
            entry=entry,
            destination_guild=getattr(ctx, "guild", None),
            source_user_token=token,
            dryrun=True,
        )
        ok = bool(result.get("ok"))
        reason = str(result.get("reason") or "").strip()
        hs = result.get("http_status")
        total = result.get("total")
        types = result.get("type_counts")
        cats = result.get("categories_preview")
        await ctx.send(f"fetchauth: sgid={sgid} ok={ok} reason={reason} http={hs} total={total} types={types} cats={cats}")

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
                f"- raw_unwrap={raw_unwrap}\n"
                f"- fetchall_user_token_loaded={fetchall_has_token}\n"
                f"- smartfilter_destinations_set={sum(1 for x in dests if x>0)}/{len(dests)}\n"
                f"- global_trigger_destinations_set={sum(1 for x in globals_ if x>0)}/{len(globals_)}\n"
                f"- fallback_channel_id={int(cfg.FALLBACK_CHANNEL_ID or 0)}"
            )
        except Exception as e:
            log_warn(f"status failed: {e}")
            await ctx.send(f"status failed: {type(e).__name__}: {e}")

    @bot.command(name="keywords")
    async def keywords_cmd(ctx, action: str = "list", *, value: str = "") -> None:
        """Manage monitored keywords. Usage: !keywords list | add <kw> | remove <kw> | reload"""
        try:
            act = str(action or "").strip().lower()
        except Exception:
            act = "list"
        if act in {"reload", "refresh"}:
            n = _reload_keywords_into_forwarder()
            await ctx.send(f"Keywords reloaded. count={n}")
            return
        if act in {"add", "create", "new"}:
            ok, reason = add_keyword(value)
            n = _reload_keywords_into_forwarder()
            await ctx.send(f"Keyword add: ok={ok} reason={reason} count={n}")
            return
        if act in {"remove", "rm", "del", "delete"}:
            ok, reason = remove_keyword(value)
            n = _reload_keywords_into_forwarder()
            await ctx.send(f"Keyword remove: ok={ok} reason={reason} count={n}")
            return
        # list (default)
        kws = load_keywords(force=True)
        if not kws:
            await ctx.send("Monitored keywords: (none)")
            return
        preview = ", ".join(kws[:40])
        extra = "" if len(kws) <= 40 else f" ... (+{len(kws)-40} more)"
        await ctx.send(f"Monitored keywords ({len(kws)}): {preview}{extra}")

    @bot.command(name="slashstatus")
    async def slashstatus_cmd(ctx) -> None:
        """Debug: show slash commands known to this bot for the current guild."""
        try:
            import discord
        except Exception as e:
            await ctx.send(f"discord import failed: {type(e).__name__}: {e}")
            return
        try:
            gid = int(getattr(getattr(ctx, "guild", None), "id", 0) or 0)
        except Exception:
            gid = 0
        if gid <= 0:
            await ctx.send("slashstatus: not in a guild context")
            return
        try:
            cmds = bot.tree.get_commands(guild=discord.Object(id=gid))
        except Exception as e:
            await ctx.send(f"slashstatus failed: {type(e).__name__}: {e}")
            return
        names = []
        for c in cmds or []:
            try:
                names.append(getattr(c, "name", str(c)))
            except Exception:
                continue
        names = sorted(set([str(n) for n in names if n]))
        await ctx.send(f"slashstatus: guild={gid} tree_commands={len(names)} names={', '.join(names)[:1700]}")

    @bot.command(name="slashsync")
    async def slashsync_cmd(ctx) -> None:
        """Debug: force sync of slash commands to destination guild(s)."""
        try:
            import discord
        except Exception as e:
            await ctx.send(f"discord import failed: {type(e).__name__}: {e}")
            return
        dest_guild_ids = sorted(int(x) for x in (cfg.DESTINATION_GUILD_IDS or set()) if int(x) > 0)
        if not dest_guild_ids:
            await ctx.send("slashsync: no destination_guild_ids configured")
            return
        ok = 0
        lines = []
        for gid in dest_guild_ids:
            try:
                synced = await bot.tree.sync(guild=discord.Object(id=int(gid)))
                ok += 1
                lines.append(f"- guild={int(gid)} ok count={len(synced)}")
            except Exception as e:
                lines.append(f"- guild={int(gid)} fail {type(e).__name__}: {e}")
        await ctx.send("slashsync:\n" + "\n".join(lines[:20]) + f"\nok={ok}/{len(dest_guild_ids)}")

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

    fetchmap = app_commands.Group(name="fetchmap", description="Manage fetchall mappings")
    fetchsync = app_commands.Group(name="fetchsync", description="Pull+mirror messages from source servers")
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

    @fetchmap.command(name="list", description="List current fetchall mappings")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchmap_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send("No fetchall mappings found.", ephemeral=True)
            return

        # One-guild-per-page view (keeps it readable).
        class _Pager(discord.ui.View):
            def __init__(self, entries_list: List[Dict[str, Any]]):
                super().__init__(timeout=60 * 10)
                self.entries = entries_list
                self.idx = 0

            def _embed(self) -> discord.Embed:
                e = self.entries[self.idx] if self.entries else {}
                try:
                    sgid = int(e.get("source_guild_id", 0) or 0)
                except Exception:
                    sgid = 0
                name = str(e.get("name") or "").strip() or f"guild_{sgid}"
                dcid = int(e.get("destination_category_id", 0) or 0)
                cats = e.get("source_category_ids") if isinstance(e.get("source_category_ids"), list) else []
                ignored = e.get("ignored_channel_ids") if isinstance(e.get("ignored_channel_ids"), list) else []
                state = e.get("state") if isinstance(e.get("state"), dict) else {}
                curs = (
                    state.get("last_seen_message_id_by_channel")
                    if isinstance(state.get("last_seen_message_id_by_channel"), dict)
                    else {}
                )

                emb = discord.Embed(title=f"Fetchall mapping: {name}", color=0x5865F2)
                emb.add_field(name="Source guild", value=str(sgid), inline=True)
                emb.add_field(name="Dest category (Mirror World)", value=str(dcid), inline=True)
                emb.add_field(name="Index", value=f"{self.idx+1}/{len(self.entries)}", inline=True)

                # Category links (clickable URLs)
                cat_lines: List[str] = []
                for c in cats[:20]:
                    try:
                        cid = int(c)
                    except Exception:
                        continue
                    if cid > 0 and sgid > 0:
                        cat_lines.append(f"- `{cid}`  [open](https://discord.com/channels/{sgid}/{cid})")
                emb.add_field(
                    name=f"Source categories ({len(cats)})",
                    value="\n".join(cat_lines) if cat_lines else "(none - means ALL categories)",
                    inline=False,
                )
                emb.add_field(name="Ignored channels", value=str(len(ignored)), inline=True)
                emb.add_field(name="Cursors", value=str(len(curs) if isinstance(curs, dict) else 0), inline=True)
                emb.set_footer(text="Use /fetchmap browse to see categories/channels and toggle include/ignore.")
                return emb

            @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
            async def prev_btn(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
                if not self.entries:
                    return
                self.idx = (self.idx - 1) % len(self.entries)
                await interaction.response.edit_message(embed=self._embed(), view=self)

            @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
            async def next_btn(self, interaction: discord.Interaction, _btn: discord.ui.Button) -> None:
                if not self.entries:
                    return
                self.idx = (self.idx + 1) % len(self.entries)
                await interaction.response.edit_message(embed=self._embed(), view=self)

        view = _Pager(entries)
        await interaction.followup.send(embed=view._embed(), view=view, ephemeral=True)

    @fetchmap.command(name="browse", description="Browse a source guild's categories/channels and toggle fetch/ignore")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchmap_browse(interaction: discord.Interaction, source_guild_id: int) -> None:
        """
        Interactive UI:
        - Prev/Next category
        - Toggle current category in mapping
        - Multi-select channels in the category to toggle ignore
        """
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        sgid = int(source_guild_id or 0)
        if sgid <= 0:
            await interaction.followup.send("source_guild_id must be a positive integer.", ephemeral=True)
            return
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send("Missing FETCHALL_USER_TOKEN (needed to read source servers).", ephemeral=True)
            return

        # Load mapping entry (if present)
        cfg_data = load_fetchall_mappings()
        entry: Dict[str, Any] = {}
        for e in (cfg_data.get("guilds", []) or []):
            if isinstance(e, dict) and int(e.get("source_guild_id", 0) or 0) == sgid:
                entry = e
                break
        name = str(entry.get("name") or "").strip() or f"guild_{sgid}"
        dest_cat = int(entry.get("destination_category_id", 0) or 0)
        selected_cats: List[int] = []
        try:
            raw = entry.get("source_category_ids") if isinstance(entry.get("source_category_ids"), list) else []
            selected_cats = [int(x) for x in raw if int(x) > 0]
        except Exception:
            selected_cats = []
        ignored: List[int] = []
        try:
            raw_ig = entry.get("ignored_channel_ids") if isinstance(entry.get("ignored_channel_ids"), list) else []
            ignored = [int(x) for x in raw_ig if int(x) > 0]
        except Exception:
            ignored = []

        info = await list_source_guild_channels(source_guild_id=sgid, user_token=source_token)
        if not info.get("ok"):
            await interaction.followup.send(
                f"Browse failed: reason={info.get('reason')} http={info.get('http_status')}",
                ephemeral=True,
            )
            return
        categories = info.get("categories") if isinstance(info.get("categories"), list) else []
        channels = info.get("channels") if isinstance(info.get("channels"), list) else []

        # Map parent_id -> channels
        by_parent: Dict[int, List[Dict[str, Any]]] = {}
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            try:
                pid = int(ch.get("parent_id") or 0)
            except Exception:
                pid = 0
            by_parent.setdefault(pid, []).append(ch)

        # Keep only categories that actually have messageable channels (optional UX)
        cat_list: List[Dict[str, Any]] = []
        for c in categories:
            if not isinstance(c, dict):
                continue
            try:
                cid = int(c.get("id") or 0)
            except Exception:
                continue
            if cid <= 0:
                continue
            cat_list.append(c)
        if not cat_list:
            await interaction.followup.send("No categories found in source guild.", ephemeral=True)
            return

        ignored_set = set(ignored)
        selected_set = set(selected_cats)

        def _build_embed(cat_idx: int, chan_page: int) -> discord.Embed:
            cat = cat_list[cat_idx]
            cid = int(cat.get("id") or 0)
            cname = str(cat.get("name") or "").strip() or f"category_{cid}"
            url = str(cat.get("url") or "").strip()
            in_map = cid in selected_set

            emb = discord.Embed(title=f"{name} (sgid={sgid})", color=0x57F287 if in_map else 0xFEE75C)
            emb.add_field(name="Dest category (Mirror World)", value=str(dest_cat), inline=True)
            emb.add_field(name="Selected source categories", value=str(len(selected_set)), inline=True)
            emb.add_field(name="Ignored channels", value=str(len(ignored_set)), inline=True)

            cat_line = f"**{cname}**\n`{cid}`"
            if url:
                cat_line += f"\n[open]({url})"
            cat_line += f"\nselected_in_mapping={bool(in_map)}"
            emb.add_field(name=f"Category {cat_idx+1}/{len(cat_list)}", value=cat_line, inline=False)

            chs = list(by_parent.get(cid, []) or [])
            page_size = 20
            start = max(0, int(chan_page) * page_size)
            page = chs[start : start + page_size]
            lines: List[str] = []
            for ch in page:
                try:
                    chid = int(ch.get("id") or 0)
                except Exception:
                    continue
                nm = str(ch.get("name") or f"channel_{chid}")
                u = str(ch.get("url") or "").strip() or f"https://discord.com/channels/{sgid}/{chid}"
                mark = " (ignored)" if chid in ignored_set else ""
                lines.append(f"- [{nm}]({u}) `{chid}`{mark}")
            if not lines:
                lines = ["(No messageable channels found in this category.)"]
            emb.add_field(name=f"Channels (page {chan_page+1})", value="\n".join(lines)[:1024], inline=False)
            emb.set_footer(text="Tip: if your mapping has the wrong category IDs, toggle the correct category on this screen.")
            return emb

        class _BrowseView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60 * 20)
                self.cat_idx = 0
                self.chan_page = 0
                self._select = None
                self._refresh_select()

            def _current_cat_id(self) -> int:
                try:
                    return int(cat_list[self.cat_idx].get("id") or 0)
                except Exception:
                    return 0

            def _refresh_select(self) -> None:
                # rebuild select menu for current category + page
                if self._select is not None:
                    try:
                        self.remove_item(self._select)
                    except Exception:
                        pass
                cid = self._current_cat_id()
                chs = list(by_parent.get(cid, []) or [])
                page_size = 25
                start = max(0, int(self.chan_page) * page_size)
                page = chs[start : start + page_size]
                opts: List[discord.SelectOption] = []
                for ch in page:
                    try:
                        chid = int(ch.get("id") or 0)
                    except Exception:
                        continue
                    nm = str(ch.get("name") or f"channel_{chid}")
                    desc = "ignored" if chid in ignored_set else "not ignored"
                    opts.append(discord.SelectOption(label=nm[:100], value=str(chid), description=desc))
                if not opts:
                    return
                sel = discord.ui.Select(
                    placeholder="Toggle ignore for selected channels (this page)",
                    min_values=1,
                    max_values=min(25, len(opts)),
                    options=opts[:25],
                )

                async def _on_select(i: discord.Interaction) -> None:
                    # toggle ignore status
                    try:
                        vals = list(sel.values or [])
                    except Exception:
                        vals = []
                    changed = 0
                    for v in vals:
                        try:
                            chid = int(v)
                        except Exception:
                            continue
                        if chid in ignored_set:
                            ignored_set.remove(chid)
                        else:
                            ignored_set.add(chid)
                        changed += 1
                    if changed:
                        set_ignored_channel_ids(source_guild_id=sgid, ignored_channel_ids=sorted(list(ignored_set)))
                    self._refresh_select()
                    await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

                sel.callback = _on_select  # type: ignore
                self._select = sel
                self.add_item(sel)

            @discord.ui.button(label="Prev category", style=discord.ButtonStyle.secondary)
            async def prev_cat(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
                self.cat_idx = (self.cat_idx - 1) % len(cat_list)
                self.chan_page = 0
                self._refresh_select()
                await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

            @discord.ui.button(label="Next category", style=discord.ButtonStyle.secondary)
            async def next_cat(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
                self.cat_idx = (self.cat_idx + 1) % len(cat_list)
                self.chan_page = 0
                self._refresh_select()
                await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

            @discord.ui.button(label="Toggle category in mapping", style=discord.ButtonStyle.primary)
            async def toggle_cat(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
                cid = self._current_cat_id()
                if cid <= 0:
                    await i.response.send_message("Invalid category.", ephemeral=True)
                    return
                if cid in selected_set:
                    selected_set.remove(cid)
                else:
                    selected_set.add(cid)
                upsert_mapping(source_guild_id=sgid, source_category_ids=sorted(list(selected_set)))
                await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

            @discord.ui.button(label="Prev channels", style=discord.ButtonStyle.secondary)
            async def prev_ch(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
                self.chan_page = max(0, int(self.chan_page) - 1)
                self._refresh_select()
                await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

            @discord.ui.button(label="Next channels", style=discord.ButtonStyle.secondary)
            async def next_ch(self, i: discord.Interaction, _b: discord.ui.Button) -> None:
                self.chan_page = int(self.chan_page) + 1
                self._refresh_select()
                await i.response.edit_message(embed=_build_embed(self.cat_idx, self.chan_page), view=self)

        view = _BrowseView()
        await interaction.followup.send(embed=_build_embed(0, 0), view=view, ephemeral=True)

    @fetchmap.command(name="upsert", description="Add/update a fetchall mapping entry")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(
        source_guild_id="Source guild/server ID to mirror from",
        destination_category="Destination category in Mirror World",
        name="Friendly label (optional)",
        source_category_ids_csv="Comma-separated category IDs in the SOURCE guild (optional)",
        ignored_channel_ids_csv="Comma-separated channel IDs in the SOURCE guild to ignore (optional)",
        require_date="Stored flag (legacy; optional)",
    )
    async def fetchmap_upsert(
        interaction: discord.Interaction,
        source_guild_id: int,
        destination_category: discord.CategoryChannel,
        name: str = "",
        source_category_ids_csv: str = "",
        ignored_channel_ids_csv: str = "",
        require_date: bool = True,
    ) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass

        sgid = int(source_guild_id or 0)
        if sgid <= 0:
            await interaction.followup.send("source_guild_id must be a positive integer.", ephemeral=True)
            return

        entry = upsert_mapping(
            source_guild_id=sgid,
            name=str(name).strip() or None,
            destination_category_id=int(destination_category.id),
            source_category_ids=_parse_csv_ints(source_category_ids_csv),
            require_date=bool(require_date),
        )
        ignored_ids = _parse_csv_ints(ignored_channel_ids_csv)
        if ignored_ids:
            set_ignored_channel_ids(source_guild_id=sgid, ignored_channel_ids=ignored_ids)

        await interaction.followup.send(
            f"Saved mapping: sgid={entry.get('source_guild_id')} dest_category_id={entry.get('destination_category_id')} "
            f"source_category_ids={len(entry.get('source_category_ids') or [])} ignored={len(ignored_ids) if ignored_ids else len(entry.get('ignored_channel_ids') or [])}",
            ephemeral=True,
        )

    @fetchmap.command(name="ignore_add", description="Add an ignored source channel id to a mapping")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchmap_ignore_add(interaction: discord.Interaction, source_guild_id: int, channel_id: int) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entry = upsert_mapping(source_guild_id=int(source_guild_id))
        existing = entry.get("ignored_channel_ids") if isinstance(entry.get("ignored_channel_ids"), list) else []
        try:
            ignored = [int(x) for x in existing if int(x) > 0]
        except Exception:
            ignored = []
        if int(channel_id) > 0 and int(channel_id) not in ignored:
            ignored.append(int(channel_id))
        set_ignored_channel_ids(source_guild_id=int(source_guild_id), ignored_channel_ids=ignored)
        await interaction.followup.send(f"Updated ignored list size={len(ignored)} for sgid={int(source_guild_id)}", ephemeral=True)

    @fetchmap.command(name="ignore_remove", description="Remove an ignored source channel id from a mapping")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchmap_ignore_remove(interaction: discord.Interaction, source_guild_id: int, channel_id: int) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entry = upsert_mapping(source_guild_id=int(source_guild_id))
        existing = entry.get("ignored_channel_ids") if isinstance(entry.get("ignored_channel_ids"), list) else []
        try:
            ignored = [int(x) for x in existing if int(x) > 0]
        except Exception:
            ignored = []
        ignored = [x for x in ignored if int(x) != int(channel_id)]
        set_ignored_channel_ids(source_guild_id=int(source_guild_id), ignored_channel_ids=ignored)
        await interaction.followup.send(f"Updated ignored list size={len(ignored)} for sgid={int(source_guild_id)}", ephemeral=True)

    @fetchsync.command(name="dryrun", description="Show what would be fetched/sent without sending")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchsync_dryrun(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send("No fetchall mappings found.", ephemeral=True)
            return
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send("Missing FETCHALL_USER_TOKEN (needed to read source servers).", ephemeral=True)
            return
        selected = []
        for e in entries:
            try:
                if int(source_guild_id or 0) > 0 and int(e.get("source_guild_id", 0)) != int(source_guild_id):
                    continue
            except Exception:
                continue
            selected.append(e)
        if not selected:
            await interaction.followup.send(f"No mapping found for source_guild_id={int(source_guild_id)}.", ephemeral=True)
            return
        summaries: List[str] = []
        for entry in selected:
            result = await run_fetchsync(
                bot=bot,
                entry=entry,
                destination_guild=getattr(interaction, "guild", None),
                source_user_token=source_token,
                dryrun=True,
            )
            summaries.append(
                f"- sgid={int(entry.get('source_guild_id',0) or 0)} ok={bool(result.get('ok'))} "
                f"channels={int(result.get('channels',0) or 0)} would_send={int(result.get('would_send',0) or 0)} reason={result.get('reason') or ''}"
            )
        msg = "Fetchsync dryrun:\n" + "\n".join(summaries[:50])
        if len(summaries) > 50:
            msg += f"\n... and {len(summaries)-50} more"
        await interaction.followup.send(msg, ephemeral=True)

    @fetchsync.command(name="run", description="Pull and mirror messages for one mapping (or all)")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchsync_run(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send("No fetchall mappings found.", ephemeral=True)
            return
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send("Missing FETCHALL_USER_TOKEN (needed to read source servers).", ephemeral=True)
            return
        selected = []
        for e in entries:
            try:
                if int(source_guild_id or 0) > 0 and int(e.get("source_guild_id", 0)) != int(source_guild_id):
                    continue
            except Exception:
                continue
            selected.append(e)
        if not selected:
            await interaction.followup.send(f"No mapping found for source_guild_id={int(source_guild_id)}.", ephemeral=True)
            return
        ok = 0
        total_sent = 0
        for entry in selected:
            result = await run_fetchsync(
                bot=bot,
                entry=entry,
                destination_guild=getattr(interaction, "guild", None),
                source_user_token=source_token,
                dryrun=False,
            )
            if result.get("ok"):
                ok += 1
            try:
                total_sent += int(result.get("sent", 0) or 0)
            except Exception:
                pass
        await interaction.followup.send(f"Fetchsync complete: {ok}/{len(selected)} ok; sent={total_sent}", ephemeral=True)

    @kw.command(name="list", description="List monitored keywords")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        kws = load_keywords(force=True)
        if not kws:
            await interaction.followup.send("Monitored keywords: (none)", ephemeral=True)
            return
        preview = "\n".join(f"- {k}" for k in kws[:60])
        extra = "" if len(kws) <= 60 else f"\n... (+{len(kws)-60} more)"
        await interaction.followup.send(f"Monitored keywords ({len(kws)}):\n{preview}{extra}", ephemeral=True)

    @kw.command(name="add", description="Add a monitored keyword")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_add(interaction: discord.Interaction, keyword: str) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ok, reason = add_keyword(keyword)
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(f"Keyword add: ok={ok} reason={reason} count={n}", ephemeral=True)

    @kw.command(name="remove", description="Remove a monitored keyword")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_remove(interaction: discord.Interaction, keyword: str) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ok, reason = remove_keyword(keyword)
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(f"Keyword remove: ok={ok} reason={reason} count={n}", ephemeral=True)

    @kw.command(name="reload", description="Reload monitored keywords from disk")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def kw_reload(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        n = _reload_keywords_into_forwarder()
        await interaction.followup.send(f"Keywords reloaded. count={n}", ephemeral=True)

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
            await interaction.followup.send("Empty text.", ephemeral=True)
            return
        kws = load_keywords(force=True)
        matched = scan_keywords(sample, kws)
        preview = ", ".join(matched[:15])
        extra = "" if len(matched) <= 15 else f" ... (+{len(matched)-15} more)"
        await interaction.followup.send(
            f"Keyword test: matched={len(matched)} {preview}{extra}".strip(),
            ephemeral=True,
        )

        if not send_output:
            return
        dest = int(getattr(cfg, "SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID", 0) or 0)
        if dest <= 0:
            await interaction.followup.send("MONITORED_KEYWORD channel is not configured.", ephemeral=True)
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
            await interaction.followup.send(f"Send failed: {type(e).__name__}: {e}", ephemeral=True)

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
            await interaction.followup.send("Empty keyword.", ephemeral=True)
            return
        ok, reason = set_keyword_channel_override(kw_s, int(channel.id))
        await interaction.followup.send(
            f"keywordchannel set: ok={ok} reason={reason} keyword={kw_s} channel=<#{int(channel.id)}>",
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
        await interaction.followup.send(f"keywordchannel clear: ok={ok} reason={reason} keyword={kw_s}", ephemeral=True)

    @kwchan.command(name="list", description="List keyword->extra channel overrides")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def keywordchannel_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        mp = load_keyword_channel_overrides(force=True)
        if not mp:
            await interaction.followup.send("keywordchannel overrides: (none)", ephemeral=True)
            return
        lines = []
        for k in sorted(mp.keys())[:100]:
            try:
                cid = int(mp.get(k, 0) or 0)
            except Exception:
                cid = 0
            if cid > 0:
                lines.append(f"- **{k}** -> <#{cid}> (`{cid}`)")
        msg = "keywordchannel overrides:\n" + "\n".join(lines[:60])
        if len(lines) > 60:
            msg += f"\n... (+{len(lines)-60} more)"
        await interaction.followup.send(msg, ephemeral=True)

    # Add groups to the tree for destination guild(s) only.
    for g in guild_objs:
        try:
            bot.tree.add_command(fetchmap, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /fetchmap to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(fetchsync, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /fetchsync to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(kw, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /keywords to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(kwchan, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /keywordchannel to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
