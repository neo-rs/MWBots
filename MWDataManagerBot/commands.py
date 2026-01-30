from __future__ import annotations

import re
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
    Register slash commands on the provided discord.py commands.Bot instance.

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

    async def _cmd_fetchall(ctx, source_guild_id: int = 0) -> None:
        """Run fetch-all for configured guild entries (mirror channel setup)."""
        try:
            import asyncio
            import time as _time

            entries = iter_fetchall_entries()
            try:
                sgid_filter = int(source_guild_id or 0)
            except Exception:
                sgid_filter = 0
            if sgid_filter > 0:
                filtered = []
                for e in entries:
                    if not isinstance(e, dict):
                        continue
                    try:
                        if int(e.get("source_guild_id", 0) or 0) == sgid_filter:
                            filtered.append(e)
                    except Exception:
                        continue
                entries = filtered
            if not entries:
                if sgid_filter > 0:
                    try:
                        await ctx.send(embed=_ui_embed("Fetchall", f"No mapping found.\nsource_guild_id={sgid_filter}", color=0xED4245))
                    except Exception:
                        await ctx.send(f"No fetchall mapping found for source_guild_id={sgid_filter}.")
                else:
                    try:
                        await ctx.send(embed=_ui_embed("Fetchall", "No fetchall mappings found.", color=0xED4245))
                    except Exception:
                        await ctx.send("No fetchall mappings found (MWDataManagerBot/config/fetchall_mappings.json).")
                return
            total_maps = int(len(entries))
            try:
                progress_msg = await ctx.send(embed=_ui_embed("Fetchall", f"Starting...\n- mappings: `{total_maps}`"))
            except Exception:
                progress_msg = await ctx.send(f"Fetchall starting... mappings={total_maps}")
            try:
                log_info(f"[fetchall] invoked mappings={total_maps}", event="fetchall_invoked", mappings=int(total_maps))
            except Exception:
                pass
            ok = 0
            source_token = _pick_fetchall_source_token() or None
            if not source_token:
                try:
                    await ctx.send(
                        embed=_ui_embed(
                            "Fetchall note",
                            "FETCHALL_USER_TOKEN is missing.\n"
                            "Fetchall will only work for source guilds the bot is in; other mappings will fail.",
                            color=0xFEE75C,
                        )
                    )
                except Exception:
                    await ctx.send(
                        "Fetchall note: FETCHALL_USER_TOKEN is missing. "
                        "Fetchall will only work for source guilds the bot is in; other mappings will fail."
                    )
            map_idx = 0
            last_edit_ts = 0.0
            for entry in entries:
                map_idx += 1
                try:
                    sgid = int(entry.get("source_guild_id", 0) or 0)
                except Exception:
                    sgid = 0
                name = str(entry.get("name") or "").strip() or f"guild_{sgid}"

                async def _progress_cb(payload: Dict[str, Any]) -> None:
                    nonlocal last_edit_ts
                    try:
                        now = float(_time.time())
                    except Exception:
                        now = 0.0
                    # throttle edits to avoid rate limits
                    if last_edit_ts and now and (now - last_edit_ts) < 1.0:
                        return
                    last_edit_ts = now
                    stage = str(payload.get("stage") or "")
                    fail_reason = str(payload.get("reason") or "").strip()
                    total_sources = int(payload.get("total_sources", 0) or 0)
                    attempted = int(payload.get("attempted", 0) or 0)
                    created = int(payload.get("created", 0) or 0)
                    existing = int(payload.get("existing", 0) or 0)
                    errs = int(payload.get("errors", 0) or 0)
                    bar = _render_progress_bar(attempted, total_sources)
                    current = str(payload.get("current_channel_name") or "").strip()
                    header = f"Fetchall: {name} ({map_idx}/{total_maps})"
                    lines = [header, bar, f"created={created} existing={existing} errors={errs} stage={stage}"]
                    if stage == "fail" and fail_reason:
                        lines.append(f"reason: {fail_reason}")
                    if current:
                        lines.append(f"channel: {current}")
                    text = "\n".join(lines).strip()
                    try:
                        await progress_msg.edit(embed=_ui_embed(header, text[:3500]), content=None)
                    except Exception:
                        return

                try:
                    # Prevent a single mapping from hanging the whole command forever.
                    # Creating large numbers of channels can be slow due to Discord rate limits,
                    # so this timeout must be generous.
                    result = await asyncio.wait_for(
                        run_fetchall(
                            bot=bot,
                            entry=entry,
                            destination_guild=getattr(ctx, "guild", None),
                            source_user_token=source_token,
                            progress_cb=_progress_cb,
                        ),
                        timeout=1800,
                    )
                except asyncio.TimeoutError:
                    result = {"ok": False, "reason": "timeout"}
                except Exception as e:
                    result = {"ok": False, "reason": f"exception:{type(e).__name__}:{e}"}

                if result.get("ok"):
                    ok += 1
                else:
                    reason = str(result.get("reason") or "").strip()
                    hs = result.get("http_status")
                    extra = ""
                    if reason == "missing_source_category_ids":
                        extra = " (set categories via /fetchmap browse)"
                    try:
                        await ctx.send(
                            embed=_ui_embed(
                                "Fetchall failed",
                                f"name={name}\nsgid={sgid}\nreason={reason}{extra}\nhttp={hs}",
                                color=0xED4245,
                            )
                        )
                    except Exception:
                        await ctx.send(f"fetchall failed: {name} sgid={sgid} reason={reason}{extra} http={hs}")
            try:
                await progress_msg.edit(embed=_ui_embed("Fetchall complete", f"ok={ok}/{total_maps}"), content=None)
            except Exception:
                await ctx.send(f"Fetchall complete: {ok}/{total_maps} succeeded.")
        except Exception as e:
            log_warn(f"fetchall command failed: {e}")
            try:
                await ctx.send(embed=_ui_embed("Fetchall failed", f"{type(e).__name__}: {e}", color=0xED4245))
            except Exception:
                await ctx.send(f"Fetchall failed: {type(e).__name__}: {e}")

    async def _cmd_fetchclear(ctx, *args: str) -> None:
        """
        Delete Mirror World mirror/separator channels in a destination category.

        Safety:
        - Dryrun by default (lists what would be deleted).
        - Run with `confirm` to actually delete.
        - Add `all` to delete non-mirror channels too.
        - If no category is provided, shows a dropdown so you can select one or more categories.

        Examples:
          /fetchclear
          /fetchclear confirm:true
          /fetchclear category_ids_csv:1437856372300451851
          /fetchclear category_ids_csv:1437856372300451851 confirm:true
          /fetchclear category_ids_csv:1437856372300451851 delete_all:true confirm:true
          /fetchclear category_ids_csv:111111111111111111,222222222222222222 confirm:true
        """
        if discord is None:
            await ctx.send("fetchclear failed: discord.py import error (discord is unavailable in this runtime).")
            return

        try:
            import asyncio
            import time as _time
        except Exception as e:
            await ctx.send(f"fetchclear failed: runtime import error: {type(e).__name__}: {e}")
            return

        tokens = [str(a or "").strip() for a in (args or [])]
        tokens = [t for t in tokens if t]
        flags = set()
        category_ids: List[int] = []

        def _extract_ids(token: str) -> List[int]:
            # Accept raw ids, CSV, and mentions like <#123>.
            t = str(token or "").strip()
            if not t:
                return []
            cleaned = re.sub(r"[^0-9,]", "", t)
            return _parse_csv_ints(cleaned) if cleaned else []

        for t in tokens:
            tl = t.lower().strip()
            if tl in {"confirm", "all"}:
                flags.add(tl)
                continue
            ids = _extract_ids(t)
            if ids:
                category_ids.extend(ids)
                continue
            flags.add(tl)
        # de-dupe preserving order
        seen_c = set()
        dedup_cats: List[int] = []
        for cid in category_ids:
            if cid in seen_c:
                continue
            seen_c.add(cid)
            dedup_cats.append(cid)
        category_ids = dedup_cats

        do_confirm = "confirm" in flags
        delete_all = "all" in flags

        if ctx.guild is None:
            await ctx.send("fetchclear: must be run inside a guild/server.")
            return

        try:
            log_info(
                f"[fetchclear] invoked confirm={do_confirm} delete_all={delete_all} args={tokens}",
                event="fetchclear_invoked",
                confirm=bool(do_confirm),
                delete_all=bool(delete_all),
                args=list(tokens),
                guild_id=int(getattr(ctx.guild, "id", 0) or 0),
                channel_id=int(getattr(getattr(ctx, "channel", None), "id", 0) or 0),
                author_id=int(getattr(getattr(ctx, "author", None), "id", 0) or 0),
            )
        except Exception:
            pass

        async def _prompt_pick_categories() -> List[int]:
            """
            Prompt the command invoker to pick one/many destination categories.
            Prefer configured mapping destination categories if available.
            """
            # Prefer: default dest category, then mapping dest categories, then current channel category.
            preferred_ids: List[int] = []
            try:
                default_cat = int(getattr(cfg, "FETCHALL_DEFAULT_DEST_CATEGORY_ID", 0) or 0)
            except Exception:
                default_cat = 0
            if default_cat > 0:
                preferred_ids.append(default_cat)

            try:
                cfg_data = load_fetchall_mappings()
            except Exception:
                cfg_data = {}
            for e in (cfg_data.get("guilds", []) or []):
                if not isinstance(e, dict):
                    continue
                try:
                    dcid = int(e.get("destination_category_id", 0) or 0)
                except Exception:
                    dcid = 0
                if dcid > 0:
                    preferred_ids.append(dcid)

            try:
                chan_cat_id = int(getattr(getattr(ctx, "channel", None), "category_id", 0) or 0)
            except Exception:
                chan_cat_id = 0
            if chan_cat_id > 0:
                preferred_ids.append(chan_cat_id)

            # De-dupe while preserving order
            seen_p = set()
            dedup_pref: List[int] = []
            for x in preferred_ids:
                if x <= 0:
                    continue
                if x in seen_p:
                    continue
                seen_p.add(x)
                dedup_pref.append(x)
            preferred_ids = dedup_pref

            guild_cats_all = [c for c in list(getattr(ctx.guild, "categories", []) or []) if isinstance(c, discord.CategoryChannel)]
            if not guild_cats_all:
                return []

            # Prefer listing overflow categories for the selected base categories (if any),
            # otherwise fall back to all guild categories.
            preferred_set = set(preferred_ids)

            def _is_overflow_of(base_name: str, cat_name: str) -> bool:
                try:
                    return bool(base_name) and str(cat_name or "").startswith(f"{base_name}-overflow-")
                except Exception:
                    return False

            # Build "focused" list: base dest categories + their overflows (in guild order).
            focused: List["discord.CategoryChannel"] = []
            base_cats: List["discord.CategoryChannel"] = []
            for c in guild_cats_all:
                try:
                    if int(getattr(c, "id", 0) or 0) in preferred_set:
                        base_cats.append(c)
                except Exception:
                    continue
            base_names = [str(getattr(c, "name", "") or "").strip() for c in base_cats]
            for c in guild_cats_all:
                try:
                    cid = int(getattr(c, "id", 0) or 0)
                except Exception:
                    cid = 0
                if cid <= 0:
                    continue
                nm = str(getattr(c, "name", "") or "").strip()
                if cid in preferred_set:
                    focused.append(c)
                    continue
                if any(_is_overflow_of(bn, nm) for bn in base_names if bn):
                    focused.append(c)
                    continue

            candidates_all = focused if focused else guild_cats_all

            # De-dupe preserving order
            seen_ids = set()
            dedup_candidates: List["discord.CategoryChannel"] = []
            for c in candidates_all:
                try:
                    cid = int(getattr(c, "id", 0) or 0)
                except Exception:
                    cid = 0
                if cid <= 0:
                    continue
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
                dedup_candidates.append(c)
            candidates_all = dedup_candidates

            author_id = int(getattr(getattr(ctx, "author", None), "id", 0) or 0)

            class _PickCatsView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=60)
                    self.page = 0
                    self.selected: Dict[int, str] = {}  # id -> name
                    self._sel: Optional["discord.ui.Select"] = None
                    self._refresh_select()

                def _page_count(self) -> int:
                    try:
                        return max(1, int((len(candidates_all) + 24) // 25))
                    except Exception:
                        return 1

                def _page_slice(self) -> List["discord.CategoryChannel"]:
                    p = max(0, int(self.page))
                    start = p * 25
                    return candidates_all[start : start + 25]

                def _summary(self) -> str:
                    ids = list(self.selected.keys())
                    shown = []
                    for cid in ids[:6]:
                        shown.append(f"{self.selected.get(cid, str(cid))} ({cid})")
                    more = ""
                    if len(ids) > 6:
                        more = f" +{len(ids)-6} more"
                    return f"selected={len(ids)} " + (", ".join(shown) + more if shown else "")

                def _refresh_select(self) -> None:
                    if self._sel is not None:
                        try:
                            self.remove_item(self._sel)
                        except Exception:
                            pass
                    page_items = self._page_slice()
                    opts: List["discord.SelectOption"] = []
                    for c in page_items:
                        try:
                            cid = int(getattr(c, "id", 0) or 0)
                        except Exception:
                            continue
                        if cid <= 0:
                            continue
                        name = str(getattr(c, "name", "") or "").strip() or f"category_{cid}"
                        try:
                            n_channels = int(len(getattr(c, "channels", []) or []))
                        except Exception:
                            n_channels = 0
                        desc = f"{n_channels} channel(s)"
                        opts.append(discord.SelectOption(label=name[:100], value=str(cid), description=desc[:100]))
                    if not opts:
                        self._sel = None
                        return
                    self._sel = discord.ui.Select(
                        placeholder=f"Select categories to clear (multi-select) • page {self.page+1}/{self._page_count()}",
                        min_values=1,
                        max_values=min(25, len(opts)),
                        options=opts[:25],
                    )
                    self._sel.callback = self._on_select  # type: ignore
                    self.add_item(self._sel)

                async def interaction_check(self, interaction: "discord.Interaction") -> bool:  # type: ignore[override]
                    try:
                        uid = int(getattr(getattr(interaction, "user", None), "id", 0) or 0)
                    except Exception:
                        uid = 0
                    if author_id > 0 and uid != author_id:
                        try:
                            await interaction.response.send_message("This dropdown is only for the command invoker.", ephemeral=True)
                        except Exception:
                            pass
                        return False
                    return True

                async def _on_select(self, interaction: "discord.Interaction") -> None:
                    sel = self._sel
                    vals = list(getattr(sel, "values", []) or []) if sel is not None else []
                    for v in vals:
                        try:
                            cid = int(str(v))
                        except Exception:
                            continue
                        if cid <= 0:
                            continue
                        # resolve name from candidates list
                        nm = ""
                        for c in candidates_all:
                            try:
                                if int(getattr(c, "id", 0) or 0) == cid:
                                    nm = str(getattr(c, "name", "") or "").strip()
                                    break
                            except Exception:
                                continue
                        self.selected[cid] = nm or f"category_{cid}"
                    try:
                        mode = "DELETE" if do_confirm else "DRYRUN"
                        await interaction.response.edit_message(
                            content=(
                                "Fetchclear: pick destination categories\n"
                                f"{mode} • delete_all={delete_all} • {self._summary()}\n"
                                "Tip: add selections across pages, then press Done."
                            )[:1950],
                            view=self,
                        )
                    except Exception:
                        try:
                            await interaction.response.defer()
                        except Exception:
                            pass

                @discord.ui.button(label="Prev page", style=discord.ButtonStyle.secondary)
                async def prev_page(self, interaction: "discord.Interaction", _b: "discord.ui.Button") -> None:
                    self.page = (int(self.page) - 1) % self._page_count()
                    self._refresh_select()
                    await interaction.response.edit_message(view=self)

                @discord.ui.button(label="Next page", style=discord.ButtonStyle.secondary)
                async def next_page(self, interaction: "discord.Interaction", _b: "discord.ui.Button") -> None:
                    self.page = (int(self.page) + 1) % self._page_count()
                    self._refresh_select()
                    await interaction.response.edit_message(view=self)

                @discord.ui.button(label="Clear selection", style=discord.ButtonStyle.secondary)
                async def clear_sel(self, interaction: "discord.Interaction", _b: "discord.ui.Button") -> None:
                    self.selected = {}
                    await interaction.response.edit_message(
                        content=(
                            "Fetchclear: pick destination categories\n"
                            f"{'DELETE' if do_confirm else 'DRYRUN'} • delete_all={delete_all} • selected=0\n"
                            "Tip: choose multiple categories from the dropdown, then press Done."
                        )[:1950],
                        view=self,
                    )

                @discord.ui.button(label="Done", style=discord.ButtonStyle.success)
                async def done_btn(self, interaction: "discord.Interaction", _b: "discord.ui.Button") -> None:
                    if not self.selected:
                        await interaction.response.send_message("Select at least 1 category first.", ephemeral=True)
                        return
                    try:
                        await interaction.response.defer()
                    except Exception:
                        pass
                    self.stop()

            view = _PickCatsView()
            prompt = (
                "Fetchclear: pick destination categories\n"
                f"{'DELETE' if do_confirm else 'DRYRUN'} • delete_all={delete_all} • selected=0\n"
                "Tip: choose multiple categories from the dropdown, then press Done."
            )
            try:
                msg = await ctx.send(prompt, view=view)
            except Exception:
                return []
            await view.wait()
            try:
                for item in getattr(view, "children", []) or []:
                    try:
                        item.disabled = True
                    except Exception:
                        pass
                await msg.edit(view=view)
            except Exception:
                pass
            ids = []
            try:
                ids = [int(x) for x in list(getattr(view, "selected", {}).keys()) if int(x) > 0]
            except Exception:
                ids = []
            # preserve insertion order from dict keys (python 3.7+)
            return ids

        # If no category ids were provided, use dropdown selection.
        if not category_ids:
            picked = await _prompt_pick_categories()
            if not picked:
                await ctx.send("fetchclear: no categories selected (or timed out).")
                return
            category_ids = picked

        # Resolve categories
        cats: List["discord.CategoryChannel"] = []
        bad: List[int] = []
        for cid in category_ids:
            ch = ctx.guild.get_channel(int(cid))
            if ch is None:
                # Channel/category might not be cached; fetch by id.
                try:
                    ch = await bot.fetch_channel(int(cid))
                except Exception:
                    ch = None
            if isinstance(ch, discord.CategoryChannel):
                cats.append(ch)
            else:
                bad.append(int(cid))
        # de-dupe preserving order
        seen_cat = set()
        dedup_cats_obj: List["discord.CategoryChannel"] = []
        for c in cats:
            try:
                cid = int(getattr(c, "id", 0) or 0)
            except Exception:
                cid = 0
            if cid <= 0:
                continue
            if cid in seen_cat:
                continue
            seen_cat.add(cid)
            dedup_cats_obj.append(c)
        cats = dedup_cats_obj
        if not cats:
            await ctx.send(f"fetchclear: category not found or not a category: {', '.join(str(x) for x in bad[:10])}")
            return

        if bad:
            await ctx.send(f"fetchclear: ignoring invalid category id(s): {', '.join(str(x) for x in bad[:10])}")

        ids_csv = ",".join(str(int(getattr(c, "id", 0) or 0)) for c in cats if int(getattr(c, "id", 0) or 0) > 0)

        def _pick_targets(cat: "discord.CategoryChannel") -> List[Any]:
            targets: List[Any] = []
            for ch in list(getattr(cat, "channels", []) or []):
                try:
                    if not hasattr(ch, "delete"):
                        continue
                except Exception:
                    continue
                topic = str(getattr(ch, "topic", "") or "")
                name = str(getattr(ch, "name", "") or "")
                is_mirror = bool(topic.startswith("MIRROR:"))
                is_separator = bool(topic.startswith("separator for") or name.startswith("📅---"))
                if delete_all or is_mirror or is_separator:
                    targets.append(ch)
            return targets

        targets_by_cat: List[tuple["discord.CategoryChannel", List[Any]]] = []
        for c in cats:
            targets_by_cat.append((c, _pick_targets(c)))

        total = int(sum(len(t) for _c, t in targets_by_cat))
        if total <= 0:
            await ctx.send(f"fetchclear: nothing to delete in categories [{ids_csv}] (delete_all={delete_all}).")
            return

        # Dryrun listing
        if not do_confirm:
            try:
                log_info(
                    f"[fetchclear] dryrun categories={len(cats)} would_delete_total={total} delete_all={delete_all} ids={ids_csv}",
                    event="fetchclear_dryrun",
                    categories=int(len(cats)),
                    would_delete_total=int(total),
                    delete_all=bool(delete_all),
                    category_ids_csv=str(ids_csv),
                )
            except Exception:
                pass

            # Summary embed
            try:
                summary = discord.Embed(title="Fetchclear (DRYRUN)", color=0xFEE75C)
                summary.description = "\n".join(
                    [
                        _render_progress_bar(0, max(1, int(total))),
                        f"would_delete_total={total} categories={len(cats)} delete_all={delete_all}",
                        "NOTE: DRYRUN ONLY — nothing was deleted.",
                    ]
                )[:4096]
                summary.add_field(
                    name="To delete for real",
                    value=(
                        f"/fetchclear category_ids_csv:{ids_csv} "
                        + (f"delete_all:true " if delete_all else "")
                        + "confirm:true"
                    )[:1024],
                    inline=False,
                )
                await ctx.send(embed=summary)
            except Exception:
                await ctx.send(
                    (
                        "Fetchclear (DRYRUN)\n"
                        f"would_delete_total={total} categories={len(cats)} delete_all={delete_all}\n"
                        "NOTE: DRYRUN ONLY — nothing was deleted.\n\n"
                        "To delete for real, run:\n"
                        f"/fetchclear category_ids_csv:{ids_csv} "
                        + ("delete_all:true " if delete_all else "")
                        + "confirm:true"
                    )[:1950]
                )
                return

            # One embed per category (keeps output readable)
            max_cat_embeds = 12
            for c, t in targets_by_cat[:max_cat_embeds]:
                try:
                    cid = int(getattr(c, "id", 0) or 0)
                except Exception:
                    cid = 0
                nm = str(getattr(c, "name", "") or "").strip() or f"category_{cid}"
                sample_lines: List[str] = []
                for ch in t[:10]:
                    try:
                        sample_lines.append(f"#{getattr(ch,'name','')} ({getattr(ch,'id',0)})")
                    except Exception:
                        continue
                if len(t) > 10:
                    sample_lines.append("... (truncated)")
                try:
                    e2 = discord.Embed(title=nm[:256], color=0x5865F2)
                    e2.description = f"category_id={cid}\nwould_delete={len(t)}"
                    if sample_lines:
                        e2.add_field(name="Sample channels", value="\n".join(sample_lines)[:1024], inline=False)
                    await ctx.send(embed=e2)
                except Exception:
                    break
            return

        # Execute deletes with progress
        # Permission sanity check (avoids silent “nothing happened” confusion).
        try:
            me = getattr(ctx.guild, "me", None) or getattr(ctx.guild, "guild_me", None)
            perms = getattr(me, "guild_permissions", None)
            if perms is not None and not bool(getattr(perms, "manage_channels", False)):
                await ctx.send("Fetchclear: missing permission `Manage Channels` (cannot delete channels).")
                try:
                    log_warn(
                        "[fetchclear] abort: missing manage_channels permission",
                        event="fetchclear_missing_permission",
                        guild_id=int(getattr(ctx.guild, "id", 0) or 0),
                    )
                except Exception:
                    pass
                return
        except Exception:
            pass

        try:
            log_info(
                f"[fetchclear] delete start categories={len(cats)} total={total} delete_all={delete_all} ids={ids_csv}",
                event="fetchclear_delete_start",
                categories=int(len(cats)),
                total=int(total),
                delete_all=bool(delete_all),
                category_ids_csv=str(ids_csv),
            )
        except Exception:
            pass
        progress = await ctx.send(
            "\n".join(
                [
                    "Fetchclear (DELETE)",
                    _render_progress_bar(0, int(total)),
                    f"deleted=0 errors=0 total={total} categories={len(cats)} delete_all={delete_all}",
                ]
            )[:1950]
        )
        deleted = 0
        errors = 0
        last_edit = 0.0
        done_total = 0
        for cat_idx, (cat, targets) in enumerate(targets_by_cat, start=1):
            cat_id = int(getattr(cat, "id", 0) or 0)
            cat_name = str(getattr(cat, "name", "") or "").strip() or f"category_{cat_id}"
            for ch in targets:
                done_total += 1
                try:
                    await ch.delete(reason="MWDataManagerBot fetchclear")
                    deleted += 1
                except Exception as e:
                    errors += 1
                    log_warn(f"fetchclear delete failed (channel_id={getattr(ch,'id',None)}): {type(e).__name__}: {e}")
                # throttle edits
                try:
                    now = float(_time.time())
                except Exception:
                    now = 0.0
                if now and (now - last_edit) >= 1.0:
                    last_edit = now
                    bar = _render_progress_bar(done_total, total)
                    try:
                        await progress.edit(
                            content=(
                                "Fetchclear (DELETE)\n"
                                f"{bar}\n"
                                f"deleted={deleted} errors={errors} total={total} categories={len(cats)} delete_all={delete_all}\n"
                                f"category {cat_idx}/{len(cats)}: {cat_name} ({cat_id})"
                            )[:1950]
                        )
                    except Exception:
                        pass
                # Small delay to be gentle
                await asyncio.sleep(0.35)

        await progress.edit(
            content=(
                "Fetchclear (DONE)\n"
                f"{_render_progress_bar(int(total), int(total))}\n"
                f"deleted={deleted}/{total} errors={errors} categories={len(cats)} delete_all={delete_all}"
            )[:1950]
        )

        try:
            log_info(
                f"[fetchclear] delete done deleted={deleted}/{total} errors={errors} categories={len(cats)} delete_all={delete_all}",
                event="fetchclear_delete_done",
                deleted=int(deleted),
                total=int(total),
                errors=int(errors),
                categories=int(len(cats)),
                delete_all=bool(delete_all),
                category_ids_csv=str(ids_csv),
            )
        except Exception:
            pass

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
            fetchall_has_token = bool(_pick_fetchall_source_token())
            try:
                poll_s = int(getattr(cfg, "FETCHSYNC_AUTO_POLL_SECONDS", 0) or 0)
            except Exception:
                poll_s = 0
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
                f"- fetchall_user_token_loaded={fetchall_has_token}\n"
                f"- fetchsync_auto_poll_seconds={poll_s}\n"
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

    async def _fetchmap_guild_autocomplete(interaction: discord.Interaction, current: str):
        """Autocomplete source_guild_id from fetchall_mappings.json (no manual typing)."""
        try:
            cur = str(current or "").strip().lower()
        except Exception:
            cur = ""
        entries = iter_fetchall_entries()
        out = []
        for e in entries:
            if not isinstance(e, dict):
                continue
            try:
                sgid = int(e.get("source_guild_id", 0) or 0)
            except Exception:
                sgid = 0
            if sgid <= 0:
                continue
            name = str(e.get("name") or "").strip() or f"guild_{sgid}"
            label = f"{name} ({sgid})"
            try:
                if cur and cur not in label.lower():
                    continue
            except Exception:
                pass
            try:
                out.append(app_commands.Choice(name=label[:100], value=int(sgid)))
            except Exception:
                continue
            if len(out) >= 25:
                break
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

        async def send(self, content: str = None, *, embed=None, embeds=None, view=None, allowed_mentions=None, reference=None, wait: bool = True):
            # All slash responses use followups; wrappers always defer first.
            return await self._i.followup.send(
                content=content,
                embed=embed,
                embeds=embeds,
                view=view,
                allowed_mentions=allowed_mentions,
                ephemeral=self._ephemeral,
                wait=bool(wait),
            )

    @app_commands.command(name="fetchall", description="Create/update mirror channels from fetchall mappings")
    @app_commands.checks.has_permissions(manage_channels=True)
    @app_commands.autocomplete(source_guild_id=_fetchmap_guild_autocomplete)
    async def fetchall_slash(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        ctx = _SlashCtx(interaction, ephemeral=True)
        await _cmd_fetchall(ctx, source_guild_id=int(source_guild_id or 0))

    @app_commands.command(name="fetchclear", description="Delete mirror/separator channels in destination categories")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def fetchclear_slash(
        interaction: discord.Interaction,
        category_ids_csv: str = "",
        confirm: bool = False,
        delete_all: bool = False,
    ) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        args: List[str] = []
        if str(category_ids_csv or "").strip():
            args.append(str(category_ids_csv or "").strip())
        if bool(delete_all):
            args.append("all")
        if bool(confirm):
            args.append("confirm")
        ctx = _SlashCtx(interaction, ephemeral=True)
        await _cmd_fetchclear(ctx, *args)

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

    @fetchmap.command(name="list", description="List current fetchall mappings")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchmap_list(interaction: discord.Interaction) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send(embed=_ui_embed("Fetchmap list", "No fetchall mappings found.", color=0xED4245), ephemeral=True)
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
                    value="\n".join(cat_lines) if cat_lines else "(none - REQUIRED; set via /fetchmap browse)",
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

    async def _fetchmap_browse_for_guild(interaction: discord.Interaction, source_guild_id: int) -> None:
        """
        Internal handler for browsing a specific source guild.
        (This must be a normal coroutine function so UI callbacks can call it.)
        """
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        sgid = int(source_guild_id or 0)
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send(
                embed=_ui_embed("Fetchmap browse", "Missing FETCHALL_USER_TOKEN (needed to read source servers).", color=0xED4245),
                ephemeral=True,
            )
            return
        if sgid <= 0:
            await interaction.followup.send(embed=_ui_embed("Fetchmap browse", "Invalid source guild id.", color=0xED4245), ephemeral=True)
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
            await interaction.followup.send(embed=_ui_embed("Fetchmap browse", "No categories found in source guild.", color=0xED4245), ephemeral=True)
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
                    await i.response.send_message(embed=_ui_embed("Fetchmap browse", "Invalid category.", color=0xED4245), ephemeral=True)
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

    @fetchmap.command(name="browse", description="Browse a source guild's categories/channels and toggle fetch/ignore")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.autocomplete(source_guild_id=_fetchmap_guild_autocomplete)
    async def fetchmap_browse(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        """
        Interactive UI:
        - Pick mapping via dropdown (if no source_guild_id)
        - Prev/Next category
        - Toggle current category in mapping
        - Multi-select channels in the category to toggle ignore
        """
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        sgid = int(source_guild_id or 0)

        # If no sgid provided, show a dropdown selection of mappings (RSAdminBot-style).
        if sgid <= 0:
            entries = iter_fetchall_entries()
            opts: List[discord.SelectOption] = []
            for e in entries:
                if not isinstance(e, dict):
                    continue
                try:
                    g = int(e.get("source_guild_id", 0) or 0)
                except Exception:
                    g = 0
                if g <= 0:
                    continue
                nm = str(e.get("name") or "").strip() or f"guild_{g}"
                opts.append(discord.SelectOption(label=nm[:100], value=str(g), description=str(g)))
                if len(opts) >= 25:
                    break
            if not opts:
                await interaction.followup.send(embed=_ui_embed("Fetchmap browse", "No fetchall mappings found.", color=0xED4245), ephemeral=True)
                return

            class _PickView(discord.ui.View):
                def __init__(self):
                    super().__init__(timeout=60 * 10)
                    sel = discord.ui.Select(placeholder="Select source server...", min_values=1, max_values=1, options=opts)
                    sel.callback = self._on_select  # type: ignore
                    self.add_item(sel)
                    self._sel = sel

                async def _on_select(self, i: discord.Interaction) -> None:
                    try:
                        v = str((self._sel.values or [""])[0])
                        chosen = int(v)
                    except Exception:
                        chosen = 0
                    if chosen <= 0:
                        await i.response.send_message(embed=_ui_embed("Fetchmap browse", "Invalid selection.", color=0xED4245), ephemeral=True)
                        return
                    await _fetchmap_browse_for_guild(i, int(chosen))

            await interaction.followup.send(embed=_ui_embed("Fetchmap browse", "Pick a source guild to browse:"), view=_PickView(), ephemeral=True)
            return

        await _fetchmap_browse_for_guild(interaction, sgid)

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
            await interaction.followup.send(
                embed=_ui_embed("Fetchmap upsert", "source_guild_id must be a positive integer.", color=0xED4245),
                ephemeral=True,
            )
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

        msg = (
            f"Saved mapping.\n"
            f"- source_guild_id: `{entry.get('source_guild_id')}`\n"
            f"- destination_category_id: `{entry.get('destination_category_id')}`\n"
            f"- source_category_ids: `{len(entry.get('source_category_ids') or [])}`\n"
            f"- ignored_channel_ids: `{len(ignored_ids) if ignored_ids else len(entry.get('ignored_channel_ids') or [])}`"
        )
        await interaction.followup.send(embed=_ui_embed("Fetchmap upsert", msg), ephemeral=True)

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
        await interaction.followup.send(
            embed=_ui_embed(
                "Fetchmap ignore add",
                f"Updated ignored list.\n- sgid: `{int(source_guild_id)}`\n- size: `{len(ignored)}`",
            ),
            ephemeral=True,
        )

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
        await interaction.followup.send(
            embed=_ui_embed(
                "Fetchmap ignore remove",
                f"Updated ignored list.\n- sgid: `{int(source_guild_id)}`\n- size: `{len(ignored)}`",
            ),
            ephemeral=True,
        )

    @fetchsync.command(name="dryrun", description="Show what would be fetched/sent without sending")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchsync_dryrun(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        import time as _time

        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send(
                embed=_ui_embed("Fetchsync dryrun", "No fetchall mappings found.", color=0xED4245),
                ephemeral=True,
            )
            return
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send(
                embed=_ui_embed(
                    "Fetchsync dryrun",
                    "Missing FETCHALL_USER_TOKEN (needed to read source servers).",
                    color=0xED4245,
                ),
                ephemeral=True,
            )
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
            await interaction.followup.send(
                embed=_ui_embed(
                    "Fetchsync dryrun",
                    f"No mapping found for source_guild_id `{int(source_guild_id)}`.",
                    color=0xED4245,
                ),
                ephemeral=True,
            )
            return
        total_maps = int(len(selected))
        msg_obj = None
        try:
            msg_obj = await interaction.followup.send(
                embed=_ui_embed("Fetchsync dryrun", f"Starting...\n- mappings: `{total_maps}`"),
                ephemeral=True,
                wait=True,
            )
        except Exception:
            msg_obj = None
        summaries: List[str] = []
        map_idx = 0
        last_edit_ts = 0.0
        for entry in selected:
            map_idx += 1
            try:
                sgid = int(entry.get("source_guild_id", 0) or 0)
            except Exception:
                sgid = 0
            name = str(entry.get("name") or "").strip() or f"guild_{sgid}"

            async def _progress_cb(payload: Dict[str, Any]) -> None:
                nonlocal last_edit_ts
                if msg_obj is None:
                    return
                try:
                    now = float(_time.time())
                except Exception:
                    now = 0.0
                if last_edit_ts and now and (now - last_edit_ts) < 1.0:
                    return
                last_edit_ts = now
                stage = str(payload.get("stage") or "")
                total_ch = int(payload.get("channels_total", 0) or 0)
                done_ch = int(payload.get("channels_processed", 0) or 0)
                would_send = int(payload.get("would_send", 0) or 0)
                errs = int(payload.get("errors", 0) or 0)
                bar = _render_progress_bar(done_ch, total_ch)
                header = f"Fetchsync dryrun: {name} ({map_idx}/{total_maps})"
                text = "\n".join([bar, f"would_send={would_send} errors={errs} stage={stage}"]).strip()
                try:
                    emb = _ui_embed(header, text)
                    await msg_obj.edit(embed=emb, content=None)
                except Exception:
                    return

            result = await run_fetchsync(
                bot=bot,
                entry=entry,
                destination_guild=getattr(interaction, "guild", None),
                source_user_token=source_token,
                dryrun=True,
                progress_cb=_progress_cb,
            )
            summaries.append(
                f"- sgid={int(entry.get('source_guild_id',0) or 0)} ok={bool(result.get('ok'))} "
                f"channels={int(result.get('channels',0) or 0)} would_send={int(result.get('would_send',0) or 0)} reason={result.get('reason') or ''}"
            )
        final = "Results:\n" + "\n".join(summaries[:50])
        if len(summaries) > 50:
            final += f"\n... and {len(summaries)-50} more"
        if msg_obj is not None:
            try:
                await msg_obj.edit(embed=_ui_embed("Fetchsync dryrun", final[:3500]), content=None)
                return
            except Exception:
                pass
        await interaction.followup.send(embed=_ui_embed("Fetchsync dryrun", final[:3500]), ephemeral=True)

    @fetchsync.command(name="run", description="Pull and mirror messages for one mapping (or all)")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def fetchsync_run(interaction: discord.Interaction, source_guild_id: int = 0) -> None:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass
        import time as _time

        entries = iter_fetchall_entries()
        if not entries:
            await interaction.followup.send(
                embed=_ui_embed("Fetchsync run", "No fetchall mappings found.", color=0xED4245),
                ephemeral=True,
            )
            return
        source_token = _pick_fetchall_source_token()
        if not source_token:
            await interaction.followup.send(
                embed=_ui_embed(
                    "Fetchsync run",
                    "Missing FETCHALL_USER_TOKEN (needed to read source servers).",
                    color=0xED4245,
                ),
                ephemeral=True,
            )
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
            await interaction.followup.send(
                embed=_ui_embed(
                    "Fetchsync run",
                    f"No mapping found for source_guild_id `{int(source_guild_id)}`.",
                    color=0xED4245,
                ),
                ephemeral=True,
            )
            return
        total_maps = int(len(selected))
        msg_obj = None
        try:
            msg_obj = await interaction.followup.send(
                embed=_ui_embed("Fetchsync run", f"Starting...\n- mappings: `{total_maps}`"),
                ephemeral=True,
                wait=True,
            )
        except Exception:
            msg_obj = None
        ok = 0
        total_sent = 0
        map_idx = 0
        last_edit_ts = 0.0
        for entry in selected:
            map_idx += 1
            try:
                sgid = int(entry.get("source_guild_id", 0) or 0)
            except Exception:
                sgid = 0
            name = str(entry.get("name") or "").strip() or f"guild_{sgid}"

            async def _progress_cb(payload: Dict[str, Any]) -> None:
                nonlocal last_edit_ts
                if msg_obj is None:
                    return
                try:
                    now = float(_time.time())
                except Exception:
                    now = 0.0
                if last_edit_ts and now and (now - last_edit_ts) < 1.0:
                    return
                last_edit_ts = now
                stage = str(payload.get("stage") or "")
                total_ch = int(payload.get("channels_total", 0) or 0)
                done_ch = int(payload.get("channels_processed", 0) or 0)
                sent = int(payload.get("sent", 0) or 0)
                errs = int(payload.get("errors", 0) or 0)
                bar = _render_progress_bar(done_ch, total_ch)
                header = f"Fetchsync: {name} ({map_idx}/{total_maps})"
                text = "\n".join([bar, f"sent={sent} errors={errs} stage={stage}"]).strip()
                try:
                    await msg_obj.edit(embed=_ui_embed(header, text), content=None)
                except Exception:
                    return

            result = await run_fetchsync(
                bot=bot,
                entry=entry,
                destination_guild=getattr(interaction, "guild", None),
                source_user_token=source_token,
                dryrun=False,
                progress_cb=_progress_cb,
            )
            if result.get("ok"):
                ok += 1
            try:
                total_sent += int(result.get("sent", 0) or 0)
            except Exception:
                pass
        final = f"Fetchsync complete: {ok}/{total_maps} ok; sent={total_sent}"
        if msg_obj is not None:
            try:
                await msg_obj.edit(embed=_ui_embed("Fetchsync run", final), content=None)
                return
            except Exception:
                pass
        await interaction.followup.send(embed=_ui_embed("Fetchsync run", final), ephemeral=True)

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
        try:
            bot.tree.add_command(fetchall_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /fetchall to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(fetchclear_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /fetchclear to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(status_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /status to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
        try:
            bot.tree.add_command(whereami_slash, guild=g)
        except Exception as e:
            log_warn(f"Failed to add /whereami to tree for guild={getattr(g,'id',None)}: {type(e).__name__}: {e}")
