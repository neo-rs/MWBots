from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from classifier import detect_all_link_types, order_link_types, select_target_channel_id
from global_triggers import detect_global_triggers
from keywords import load_keywords
from logging_utils import log_debug, log_error, log_global, log_info, log_filter, log_forward, log_warn, write_trace_log
import settings_store as cfg
from utils import (
    augment_text_with_affiliate_redirects,
    augment_text_with_dmflip,
    augment_text_with_ringinthedeals,
    append_image_attachments_as_embeds,
    chunk_text,
    collect_embed_strings,
    extract_all_raw_links_from_text,
    extract_urls_from_text,
    format_embeds_for_forwarding,
    generate_content_signature,
    is_image_attachment,
    rewrite_affiliate_links_in_message,
)

from fetchall import iter_fetchall_entries, run_fetchsync

def _should_filter_message(payload: Dict[str, Any]) -> bool:
    try:
        content = (payload.get("content", "") or "").strip()
        embeds: List[Dict[str, Any]] = payload.get("embeds", []) or []
        attachments: List[Dict[str, Any]] = payload.get("attachments", []) or []
        if not content and not embeds and not attachments:
            return True
        # skip pure mention blasts
        import re

        if re.fullmatch(r"(<@[!&]?\d+>|@everyone|@here)+", content):
            return True
        return False
    except Exception:
        return True

class MessageForwarder:
    def __init__(self, *, bot, keywords_list: List[str], tokens: Optional[Dict[str, str]] = None):
        self.bot = bot
        self.keywords_list = keywords_list
        self.tokens: Dict[str, str] = dict(tokens or {})

        self.processed_ids: Set[int] = set()
        self.recent_hashes: Dict[str, float] = {}
        self.recent_ttl_seconds: int = int(cfg.RECENT_TTL_SECONDS or 10)

        self.send_min_interval_seconds: float = float(getattr(cfg, "SEND_MIN_INTERVAL_SECONDS", 0.0) or 0.0)
        self._send_locks: Dict[int, asyncio.Lock] = {}
        self._last_send_ts: Dict[int, float] = {}

        self.sent_to_destinations: Dict[Tuple[int, str], float] = {}
        self.link_tracking_cache: Dict[str, Dict[str, Any]] = {}
        self.link_tracking_ttl_seconds: int = int(cfg.LINK_TRACKING_TTL_SECONDS or 86400)

        self.global_content_cache: Dict[str, float] = {}
        self.global_content_ttl_seconds: int = int(cfg.GLOBAL_DUPLICATE_TTL_SECONDS or 300)

    def _is_global_duplicate(self, signature: str) -> bool:
        now = asyncio.get_event_loop().time()
        last = self.global_content_cache.get(signature, 0.0)
        if last and (now - last) < self.global_content_ttl_seconds:
            return True
        self.global_content_cache[signature] = now
        # bound size
        if len(self.global_content_cache) > 2000:
            cutoff = now - self.global_content_ttl_seconds
            self.global_content_cache = {k: v for k, v in self.global_content_cache.items() if v > cutoff}
        return False

    def _cleanup_link_tracking_cache(self) -> None:
        if not self.link_tracking_cache:
            return
        now = asyncio.get_event_loop().time()
        expired = [
            url
            for url, entry in self.link_tracking_cache.items()
            if (now - float(entry.get("timestamp", 0.0))) > self.link_tracking_ttl_seconds
        ]
        for url in expired:
            self.link_tracking_cache.pop(url, None)

    def _track_link_occurrences(self, text: str, channel_id: int) -> None:
        if not text:
            return
        urls = extract_urls_from_text(text)
        if not urls:
            return
        now = asyncio.get_event_loop().time()
        self._cleanup_link_tracking_cache()
        for url in urls:
            entry = self.link_tracking_cache.setdefault(url, {"timestamp": now, "channel_ids": set()})
            entry["timestamp"] = now
            try:
                entry.setdefault("channel_ids", set()).add(int(channel_id))
            except Exception:
                pass

    def _to_filter_payload(self, message) -> Dict[str, Any]:
        embeds: List[Dict[str, Any]] = []
        for e in getattr(message, "embeds", []) or []:
            try:
                if hasattr(e, "to_dict"):
                    embeds.append(e.to_dict())
            except Exception:
                continue
        attachments: List[Dict[str, Any]] = []
        for a in getattr(message, "attachments", []) or []:
            try:
                attachments.append(
                    {
                        "url": getattr(a, "url", None),
                        "proxy_url": getattr(a, "proxy_url", None),
                        "filename": getattr(a, "filename", None),
                        "content_type": getattr(a, "content_type", None),
                    }
                )
            except Exception:
                continue
        author = getattr(message, "author", None)
        author_dict: Dict[str, Any] = {}
        try:
            if author is not None:
                author_dict = {
                    "id": str(getattr(author, "id", "")),
                    "username": getattr(author, "name", None) or getattr(author, "display_name", None) or "Unknown",
                }
        except Exception:
            author_dict = {}
        return {
            "id": str(getattr(message, "id", "")),
            "channel_id": int(getattr(getattr(message, "channel", None), "id", 0) or 0),
            "guild_id": int(getattr(getattr(message, "guild", None), "id", 0) or 0),
            "content": getattr(message, "content", "") or "",
            "embeds": embeds,
            "attachments": attachments,
            "author": author_dict,
        }

    async def _send_to_destination(
        self,
        *,
        dest_channel_id: int,
        content: str,
        embeds: List[Dict[str, Any]],
        webhook_username: str = "",
        webhook_avatar_url: str = "",
        allowed_mentions=None,
        reference=None,
        view=None,
        return_first_message: bool = False,
    ):
        import discord
        from webhook_sender import send_via_webhook_or_bot

        channel = self.bot.get_channel(int(dest_channel_id))
        if channel is None:
            # Channel may not be in cache; fetch it explicitly.
            try:
                channel = await self.bot.fetch_channel(int(dest_channel_id))
            except discord.Forbidden as e:
                raise RuntimeError(
                    f"Destination channel forbidden (bot lacks access / missing perms): {dest_channel_id}"
                ) from e
            except discord.NotFound as e:
                raise RuntimeError(
                    f"Destination channel not found (404 / bot not in that guild / wrong id): {dest_channel_id}"
                ) from e
            except discord.HTTPException as e:
                status = getattr(e, "status", None)
                raise RuntimeError(
                    f"Destination channel fetch failed (HTTP {status or 'unknown'}): {dest_channel_id}"
                ) from e
            except Exception as e:
                raise RuntimeError(f"Destination channel fetch failed: {dest_channel_id} ({type(e).__name__})") from e
        if channel is None:
            raise RuntimeError(f"Destination channel not found: {dest_channel_id}")
        # Defensive: only send to messageable channels
        try:
            if not hasattr(channel, "send"):
                raise RuntimeError(f"Destination channel is not messageable: {dest_channel_id}")
        except Exception:
            raise RuntimeError(f"Destination channel is not messageable: {dest_channel_id}")
        embed_objs = []
        for ed in embeds or []:
            try:
                embed_objs.append(discord.Embed.from_dict(ed))
            except Exception:
                continue

        first_msg = None
        chunks = chunk_text(content, 2000)
        for i, chunk in enumerate(chunks):
            # Simple per-destination throttle to reduce 429 spam (discord.py will still handle true rate limits).
            try:
                lock = self._send_locks.setdefault(int(dest_channel_id), asyncio.Lock())
            except Exception:
                lock = asyncio.Lock()
            async with lock:
                try:
                    min_interval = float(self.send_min_interval_seconds or 0.0)
                except Exception:
                    min_interval = 0.0
                if min_interval > 0:
                    now = asyncio.get_event_loop().time()
                    last = float(self._last_send_ts.get(int(dest_channel_id), 0.0) or 0.0)
                    wait = min_interval - (now - last)
                    if wait > 0:
                        await asyncio.sleep(wait)
            if i == 0 and embed_objs:
                # Only use webhook for plain forwards (no reference/view/components).
                if reference is None and view is None:
                    await send_via_webhook_or_bot(
                        dest_channel=channel,
                        content=chunk,
                        embeds=embed_objs[:10],
                        username=webhook_username,
                        avatar_url=webhook_avatar_url,
                        reason="MWDataManagerBot live forward",
                    )
                    first_msg = None
                else:
                    first_msg = await channel.send(
                        content=chunk,
                        embeds=embed_objs[:10],
                        allowed_mentions=allowed_mentions,
                        reference=reference,
                        view=view,
                    )
            else:
                # Only attach reference/view on the first chunk.
                if reference is None and view is None:
                    await send_via_webhook_or_bot(
                        dest_channel=channel,
                        content=chunk,
                        embeds=[],
                        username=webhook_username,
                        avatar_url=webhook_avatar_url,
                        reason="MWDataManagerBot live forward (chunk)",
                    )
                    msg = None
                else:
                    msg = await channel.send(content=chunk, allowed_mentions=allowed_mentions)
                if first_msg is None:
                    first_msg = msg
            try:
                self._last_send_ts[int(dest_channel_id)] = asyncio.get_event_loop().time()
            except Exception:
                pass
        if return_first_message:
            return first_msg

    async def validate_route_maps(self) -> None:
        """
        Validate MIRRORWORLD route-map destinations at startup.
        Warn early if a route target channel id doesn't exist or isn't messageable.
        """
        try:
            import discord
        except Exception:
            discord = None  # type: ignore

        maps: Dict[str, Dict[int, int]] = {}
        try:
            maps["online"] = dict(getattr(cfg, "MIRRORWORLD_ROUTE_ONLINE", {}) or {})
        except Exception:
            maps["online"] = {}
        try:
            maps["instore"] = dict(getattr(cfg, "MIRRORWORLD_ROUTE_INSTORE", {}) or {})
        except Exception:
            maps["instore"] = {}

        # Build reverse index so we can report which sources map into the same target.
        targets: Dict[int, Dict[str, List[int]]] = {}
        for map_name, mp in maps.items():
            for src, dst in (mp or {}).items():
                try:
                    dst_id = int(dst or 0)
                except Exception:
                    continue
                if dst_id <= 0:
                    continue
                try:
                    src_id = int(src or 0)
                except Exception:
                    src_id = 0
                targets.setdefault(dst_id, {}).setdefault(map_name, []).append(src_id)

        if not targets:
            return

        bad = 0
        for dst_id, by_map in sorted(targets.items(), key=lambda kv: kv[0]):
            try:
                channel = self.bot.get_channel(int(dst_id))
                if channel is None:
                    channel = await self.bot.fetch_channel(int(dst_id))
                if not hasattr(channel, "send"):
                    raise RuntimeError("destination is not messageable")
            except Exception as e:
                bad += 1
                # Keep warning actionable and include mapping sources.
                try:
                    maps_str = ", ".join(
                        f"{m}<-{sorted([x for x in srcs if x > 0])[:6]}{'...' if len([x for x in srcs if x > 0]) > 6 else ''}"
                        for m, srcs in by_map.items()
                    )
                except Exception:
                    maps_str = ", ".join(sorted(by_map.keys()))
                log_warn(
                    f"[ROUTE_MAP] destination {dst_id} invalid/unreachable ({type(e).__name__}: {e}). "
                    f"Used by: {maps_str}"
                )

        if bad == 0:
            log_info(f"[ROUTE_MAP] validated {len(targets)} destination(s)")

    def _apply_route_map(self, *, source_group: str, dest_channel_id: int) -> int:
        """Apply MirrorWorld route-maps to a destination channel id."""
        dest_after = int(dest_channel_id or 0)
        if dest_after <= 0:
            return 0
        try:
            if source_group == "online":
                dest_after = int(getattr(cfg, "MIRRORWORLD_ROUTE_ONLINE", {}).get(dest_after, dest_after))
            elif source_group == "instore":
                dest_after = int(getattr(cfg, "MIRRORWORLD_ROUTE_INSTORE", {}).get(dest_after, dest_after))
        except Exception:
            pass
        return int(dest_after or 0)

    def _manual_classification_options(self) -> List[Tuple[str, int, str]]:
        """
        Build a list of assignable buckets for the unclassified picker.
        Returns (tag, channel_id, label) for destinations that are configured (>0).
        """
        out: List[Tuple[str, int, str]] = []

        def _add(tag: str, channel_id: int, label: str) -> None:
            try:
                cid = int(channel_id or 0)
            except Exception:
                cid = 0
            if cid <= 0:
                return
            out.append((str(tag or ""), cid, str(label or tag or "")))

        # In-store buckets
        _add("INSTORE_SEASONAL", cfg.SMARTFILTER_INSTORE_SEASONAL_CHANNEL_ID, "In-store • Seasonal")
        _add("INSTORE_SNEAKERS", cfg.SMARTFILTER_INSTORE_SNEAKERS_CHANNEL_ID, "In-store • Sneakers")
        _add("INSTORE_CARDS", cfg.SMARTFILTER_INSTORE_CARDS_CHANNEL_ID, "In-store • Cards")
        _add("INSTORE_THEATRE", cfg.SMARTFILTER_INSTORE_THEATRE_CHANNEL_ID, "In-store • Theatre")
        _add("MAJOR_STORES", cfg.SMARTFILTER_MAJOR_STORES_CHANNEL_ID, "In-store • Major stores")
        _add("DISCOUNTED_STORES", cfg.SMARTFILTER_DISCOUNTED_STORES_CHANNEL_ID, "In-store • Discounted stores")
        _add("INSTORE_LEADS", cfg.SMARTFILTER_INSTORE_LEADS_CHANNEL_ID, "In-store • General")

        # Online buckets
        _add("UPCOMING", cfg.SMARTFILTER_UPCOMING_CHANNEL_ID, "Online • Upcoming")
        _add("AFFILIATED_LINKS", cfg.SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID, "Online • Affiliate links")
        _add("MONITORED_KEYWORD", cfg.SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID, "Monitored keyword")
        _add("AMAZON", cfg.SMARTFILTER_AMAZON_CHANNEL_ID, "Amazon")
        _add("AMAZON_FALLBACK", cfg.SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID, "Amazon (fallback)")

        # Global-trigger buckets
        _add("PRICE_ERROR", cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "Global • Price error/glitched")
        _add("PROFITABLE_FLIP", cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "Global • Profitable flip")
        _add("LUNCHMONEY_FLIP", cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "Global • Lunchmoney flip")
        _add("AMAZON_PROFITABLE_LEAD", cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "Global • Amazon profitable lead")

        # Discord select supports max 25 options.
        return out[:25]

    async def _send_unclassified_with_picker(
        self,
        *,
        source_group: str,
        formatted_content: str,
        embeds_out: List[Dict[str, Any]],
        source_message_id: int,
        source_channel_id: int,
        source_jump_url: str,
        trace: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Forward the message into UNCLASSIFIED and post an interactive picker underneath it.
        """
        try:
            import discord
        except Exception:
            return False

        dest_unclassified = int(getattr(cfg, "SMARTFILTER_UNCLASSIFIED_CHANNEL_ID", 0) or 0)
        if dest_unclassified <= 0:
            return False

        allowed_mentions = discord.AllowedMentions.none()
        sent_msg = await self._send_to_destination(
            dest_channel_id=dest_unclassified,
            content=formatted_content,
            embeds=embeds_out,
            allowed_mentions=allowed_mentions,
            return_first_message=True,
        )
        if sent_msg is None:
            return False

        options = self._manual_classification_options()
        if not options:
            return True

        tag_to_channel: Dict[str, int] = {t: int(cid) for (t, cid, _lbl) in options}

        class _ReasonModal(discord.ui.Modal):
            def __init__(self, *, parent_view, selected_tag: str):
                super().__init__(title="Why this classification?")
                self._parent_view = parent_view
                self._selected_tag = str(selected_tag or "")
                self.reason = discord.ui.TextInput(
                    label="Deciding factor / note",
                    placeholder="E.g. \"Valentine's Day\" seasonal keyword in title; \"Disneyland\" in Where:",
                    required=True,
                    max_length=300,
                    style=discord.TextStyle.paragraph,
                )
                self.add_item(self.reason)

            async def on_submit(self, interaction: discord.Interaction) -> None:
                await self._parent_view._complete(interaction, self._selected_tag, str(self.reason.value or "").strip())

        class _AssignView(discord.ui.View):
            def __init__(self):
                super().__init__(timeout=60 * 60 * 24)
                self.assigned: bool = False
                self.assigned_by: str = ""
                self.assigned_tag: str = ""
                self.assigned_reason: str = ""

                opts = []
                for t, cid, lbl in options:
                    opts.append(
                        discord.SelectOption(
                            label=str(lbl)[:100],
                            value=str(t),
                            description=f"dest={int(cid)}",
                        )
                    )
                self.select = discord.ui.Select(
                    placeholder="Assign classification...",
                    min_values=1,
                    max_values=1,
                    options=opts[:25],
                )
                self.select.callback = self._on_select  # type: ignore
                self.add_item(self.select)

            async def _on_select(self, interaction: discord.Interaction) -> None:
                try:
                    perms = getattr(getattr(interaction, "user", None), "guild_permissions", None)
                    can = bool(getattr(perms, "manage_guild", False) or getattr(perms, "manage_messages", False))
                except Exception:
                    can = False
                if not can:
                    await interaction.response.send_message("Missing permission (manage guild/messages).", ephemeral=True)
                    return
                if self.assigned:
                    await interaction.response.send_message("Already assigned.", ephemeral=True)
                    return
                try:
                    selected = (self.select.values or [""])[0]
                except Exception:
                    selected = ""
                if not selected:
                    await interaction.response.send_message("No selection.", ephemeral=True)
                    return
                await interaction.response.send_modal(_ReasonModal(parent_view=self, selected_tag=selected))

            async def _complete(self, interaction: discord.Interaction, selected_tag: str, reason: str) -> None:
                if self.assigned:
                    try:
                        await interaction.response.send_message("Already assigned.", ephemeral=True)
                    except Exception:
                        pass
                    return
                tag = str(selected_tag or "").strip()
                dest_before = int(tag_to_channel.get(tag, 0) or 0)
                dest_after = self_forwarder._apply_route_map(source_group=source_group, dest_channel_id=dest_before)
                if dest_after <= 0:
                    await interaction.response.send_message("Destination not configured.", ephemeral=True)
                    return
                try:
                    await interaction.response.defer(ephemeral=True, thinking=True)
                except Exception:
                    pass
                try:
                    await self_forwarder._send_to_destination(
                        dest_channel_id=dest_after,
                        content=formatted_content,
                        embeds=embeds_out,
                        allowed_mentions=allowed_mentions,
                    )
                except Exception as e:
                    await interaction.followup.send(f"Send failed: {type(e).__name__}: {e}", ephemeral=True)
                    return

                self.assigned = True
                try:
                    u = getattr(interaction, "user", None)
                    self.assigned_by = getattr(u, "display_name", None) or getattr(u, "name", None) or "unknown"
                except Exception:
                    self.assigned_by = "unknown"
                self.assigned_tag = tag
                self.assigned_reason = reason
                try:
                    self.select.disabled = True
                except Exception:
                    pass

                try:
                    emb = discord.Embed(title="Manual classification", color=0x2ECC71)
                    emb.add_field(name="Assigned", value=f"**{tag}** → `{dest_after}`", inline=False)
                    emb.add_field(name="By", value=str(self.assigned_by or "unknown"), inline=True)
                    emb.add_field(name="Reason", value=str(reason or "—")[:1000], inline=False)
                    if source_jump_url:
                        emb.add_field(name="Source link", value=str(source_jump_url), inline=False)
                    await interaction.message.edit(embed=emb, view=self)
                except Exception:
                    pass

                # Trace log entry
                try:
                    if trace is not None:
                        trace.setdefault("manual_classification", []).append(
                            {
                                "source_message_id": int(source_message_id or 0),
                                "source_channel_id": int(source_channel_id or 0),
                                "assigned_tag": tag,
                                "dest_before": dest_before,
                                "dest_after": dest_after,
                                "reason": reason,
                                "by": self.assigned_by,
                            }
                        )
                        write_trace_log(trace)
                except Exception:
                    pass

                await interaction.followup.send(f"Assigned to **{tag}**.", ephemeral=True)

        self_forwarder = self
        view = _AssignView()
        hints = {}
        try:
            hints = (trace or {}).get("classifier", {}) if isinstance((trace or {}).get("classifier", {}), dict) else {}
        except Exception:
            hints = {}
        try:
            emb = discord.Embed(title="No classification matched", color=0xE67E22)
            emb.description = "Pick the correct bucket and add a short note. The bot will repost into that channel."
            emb.add_field(name="Source", value=f"channel=`{source_channel_id}` msg=`{source_message_id}`", inline=False)
            if source_jump_url:
                emb.add_field(name="Source link", value=str(source_jump_url), inline=False)
            # Small hint dump
            where_loc = str(hints.get("where_location") or "").strip()
            instore_req = bool(hints.get("instore_required_fields"))
            if where_loc or instore_req:
                emb.add_field(
                    name="Hints",
                    value=f"instore_required={instore_req}\nwhere={where_loc or '—'}",
                    inline=False,
                )
            await self._send_to_destination(
                dest_channel_id=dest_unclassified,
                content="\u200b",
                embeds=[emb.to_dict()],
                allowed_mentions=allowed_mentions,
                reference=sent_msg,
                view=view,
            )
        except Exception:
            # If the picker fails, still keep the unclassified forwarded message.
            return True

        return True

    async def handle_message(self, message) -> None:
        # Gate
        try:
            guild_id = int(getattr(getattr(message, "guild", None), "id", 0) or 0)
            channel_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
            category_id = int(getattr(getattr(message, "channel", None), "category_id", 0) or 0)
        except Exception:
            return

        if not cfg.is_destination_guild(guild_id):
            return
        if not cfg.is_monitored_source_channel(channel_id, category_id=category_id or None):
            return
        # In production this bot should only process DiscumBot webhook-forwarded messages.
        if cfg.MONITOR_WEBHOOK_MESSAGES_ONLY:
            try:
                if getattr(message, "webhook_id", None) is None:
                    return
            except Exception:
                return

        if getattr(message, "author", None) == self.bot.user:
            return

        if message.id in self.processed_ids:
            return
        self.processed_ids.add(int(message.id))

        payload = self._to_filter_payload(message)

        # Deterministic per-channel dedupe + trace (keyed by message.id so logs don't interleave)
        content = (payload.get("content") or "").strip()
        embeds: List[Dict[str, Any]] = payload.get("embeds", []) or []
        attachments: List[Dict[str, Any]] = payload.get("attachments", []) or []
        trace: Dict[str, Any] = {
            "message_id": str(getattr(message, "id", "")),
            "channel_id": int(channel_id),
            "guild_id": int(guild_id),
            "webhook_id": str(getattr(message, "webhook_id", "") or ""),
            "author": payload.get("author") or {},
            "content_len": len(content),
            "content_preview": (content[:160] + ("..." if len(content) > 160 else "")) if isinstance(content, str) else "",
            "embed_count": len(embeds or []),
            "attachment_count": len(attachments or []),
        }

        if _should_filter_message(payload):
            trace["decision"] = {"action": "skip", "reason": "filter"}
            try:
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_filter(f"skipped message {message.id} in channel {channel_id}")
            return

        content_sig = generate_content_signature(content, embeds, attachments)
        key = f"{channel_id}:{content_sig}"
        now = asyncio.get_event_loop().time()
        last = self.recent_hashes.get(key, 0.0)
        if last and (now - last) < self.recent_ttl_seconds:
            trace["decision"] = {
                "action": "skip",
                "reason": "per_channel_duplicate",
                "dedupe_key": key,
                "age_seconds": round(now - last, 3),
            }
            try:
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_info(f"Duplicate message detected (posted {round(now-last,1)}s ago) in channel {channel_id}")
            return
        self.recent_hashes[key] = now
        if len(self.recent_hashes) > 2000:
            cutoff = now - self.recent_ttl_seconds
            self.recent_hashes = {k: v for k, v in self.recent_hashes.items() if v > cutoff}

        # Build classification text
        embed_texts = collect_embed_strings(embeds)
        # Do not include attachment CDN URLs in classification text; it adds noise and false matches.
        text_to_check = (content + " " + " ".join(embed_texts)).strip()
        original_text_for_raw = (content + " " + " ".join(embed_texts)).strip()

        raw_links: List[str] = []
        if cfg.ENABLE_RAW_LINK_UNWRAP:
            try:
                # Extract ONLY hidden/unwrapped destinations from known affiliate wrappers
                raw_links = extract_all_raw_links_from_text(text_to_check)
                text_to_check, dmflip_links = await augment_text_with_dmflip(text_to_check)
                text_to_check, ring_links = await augment_text_with_ringinthedeals(text_to_check)
                text_to_check, affiliate_links = await augment_text_with_affiliate_redirects(text_to_check)
                # Merge and de-dupe while preserving order
                seen = set()
                merged: List[str] = []
                for u in (raw_links or []) + (dmflip_links or []) + (ring_links or []) + (affiliate_links or []):
                    if not u or not isinstance(u, str):
                        continue
                    if u in seen:
                        continue
                    seen.add(u)
                    merged.append(u)
                # Only keep links that were NOT already present in the original (visible) content/embeds.
                # This prevents "Raw links" spam listing normal links like ebay/mattel or Discord CDN images.
                raw_links = []
                for u in merged:
                    try:
                        if u and isinstance(original_text_for_raw, str) and u in original_text_for_raw:
                            continue
                    except Exception:
                        pass
                    raw_links.append(u)
                # Improve classification: include raw/unwrapped links in the text blob (legacy behavior).
                if raw_links:
                    text_to_check = (text_to_check + " " + " ".join(raw_links)).strip()
            except Exception as e:
                if cfg.VERBOSE:
                    log_warn(f"[FILTER] raw-link unwrap failed: {e}")

        # Track links for global triggers
        self._track_link_occurrences(text_to_check, channel_id)

        # Global duplicate (cross-channel signature) - do NOT include channel_id.
        global_sig = generate_content_signature(content, embeds, attachments)
        if self._is_global_duplicate(global_sig):
            trace["decision"] = {"action": "skip", "reason": "global_duplicate", "global_sig": global_sig}
            try:
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_global(f"skip duplicate content signature in channel {channel_id}", event="global_duplicate")
            return

        all_link_types = detect_all_link_types(
            text_to_check,
            attachments,
            self.keywords_list,
            embeds,
            source_channel_id=channel_id,
            trace=trace,
        )
        global_types = detect_global_triggers(
            text_to_check,
            source_channel_id=channel_id,
            link_tracking_cache=self.link_tracking_cache,
            embeds=embeds,
            attachments=attachments,
        )
        if global_types:
            all_link_types.extend(global_types)

        dispatch_link_types: List[Tuple[int, str]] = []
        stop_after_first = False
        if all_link_types:
            # dedupe pairs
            seen_pairs: Set[Tuple[int, str]] = set()
            deduped: List[Tuple[int, str]] = []
            for cid, tag in all_link_types:
                k = (int(cid or 0), str(tag or ""))
                if k in seen_pairs:
                    continue
                seen_pairs.add(k)
                deduped.append((cid, tag))
            dispatch_link_types, stop_after_first = order_link_types(deduped)

        # Format output (also used for UNCLASSIFIED fallback)
        formatted_content = content
        replaced = False
        if cfg.ENABLE_RAW_LINK_UNWRAP:
            try:
                formatted_content, inline_raw, did_inline = await rewrite_affiliate_links_in_message(
                    formatted_content, raw_links
                )
                if inline_raw:
                    seen = set(raw_links or [])
                    for u in inline_raw:
                        if u and u not in seen:
                            seen.add(u)
                            raw_links.append(u)
                replaced = bool(did_inline)
            except Exception:
                replaced = False

        embeds_out = format_embeds_for_forwarding(embeds)
        # Render image attachments as embeds for better Discord UX (no "image.png" link spam).
        try:
            embeds_out = append_image_attachments_as_embeds(embeds_out, attachments, max_embeds=10)
        except Exception:
            embeds_out = embeds_out
        # For non-image attachments, append URLs (keeps access to files without reupload).
        try:
            non_image_urls = []
            for a in attachments:
                if not isinstance(a, dict):
                    continue
                if is_image_attachment(a):
                    continue
                u = str(a.get("url") or "").strip()
                if u:
                    non_image_urls.append(u)
            if non_image_urls:
                formatted_content = (formatted_content + "\n\n" + "\n".join(non_image_urls[:10])).strip()
        except Exception:
            pass

        # Route maps apply to *destinations* (legacy MirrorWorld routing maps).
        source_group = "unknown"
        try:
            if int(channel_id) in cfg.SMART_SOURCE_CHANNELS_INSTORE:
                source_group = "instore"
            elif int(channel_id) in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()):
                source_group = "instore"
            elif int(channel_id) in cfg.SMART_SOURCE_CHANNELS_ONLINE:
                source_group = "online"
            elif int(channel_id) in cfg.SMART_SOURCE_CHANNELS_MISC:
                source_group = "misc"
        except Exception:
            source_group = "unknown"

        if not dispatch_link_types:
            fallback = select_target_channel_id(
                text_to_check, attachments, self.keywords_list, source_channel_id=channel_id, trace=trace
            )
            if fallback:
                dispatch_link_types = [fallback]

        try:
            trace.setdefault("classifier", {})["dispatch_link_types"] = dispatch_link_types
            trace["stop_after_first"] = bool(stop_after_first)
            trace["raw_links_count"] = len(raw_links or [])
            trace["raw_link_replaced_in_content"] = bool(replaced)
        except Exception:
            pass

        # Webhook identity (so forwarded posts look like the original sender).
        wh_username = ""
        wh_avatar_url = ""
        try:
            author_obj = getattr(message, "author", None)
            if author_obj is not None:
                wh_username = str(getattr(author_obj, "display_name", None) or getattr(author_obj, "name", None) or "").strip()
                try:
                    av = getattr(author_obj, "display_avatar", None)
                    wh_avatar_url = str(getattr(av, "url", "") or "").strip()
                except Exception:
                    wh_avatar_url = ""
        except Exception:
            wh_username = ""
            wh_avatar_url = ""

        if not dispatch_link_types:
            trace["decision"] = {"action": "unclassified", "reason": "no_destination"}
            try:
                write_trace_log(trace)
            except Exception:
                pass
            # UNCLASSIFIED fallback channel (interactive assignment)
            try:
                sent = await self._send_unclassified_with_picker(
                    source_group=source_group,
                    formatted_content=formatted_content,
                    embeds_out=embeds_out,
                    source_message_id=int(getattr(message, "id", 0) or 0),
                    source_channel_id=int(channel_id or 0),
                    source_jump_url=str(getattr(message, "jump_url", "") or ""),
                    trace=trace,
                )
            except Exception as e:
                sent = False
                if cfg.VERBOSE:
                    log_warn(f"UNCLASSIFIED fallback failed (msg={message.id}): {type(e).__name__}: {e}")
            if sent:
                return
            if cfg.VERBOSE:
                log_warn(f"No destination after classification (msg={message.id}) for source channel {channel_id}")
            return

        forwarded = 0
        dest_traces: List[Dict[str, Any]] = []

        for dest_channel_id, tag in dispatch_link_types:
            dest_before = int(dest_channel_id or 0)
            dest_after = dest_before
            dest_trace: Dict[str, Any] = {"tag": str(tag or ""), "dest_before": dest_before}
            if dest_before <= 0:
                dest_trace["decision"] = {"action": "skip", "reason": "invalid_destination"}
                dest_traces.append(dest_trace)
                continue

            # Apply MIRRORWORLD routing maps (from legacy settings.env) so we don't forward into intermediate channels.
            dest_after = self._apply_route_map(source_group=source_group, dest_channel_id=dest_after)
            dest_trace["dest_after"] = dest_after
            dest_channel_id = dest_after

            # Destination+content dedupe (prevents rerouter/webhook reposts from re-forwarding).
            try:
                sig_key = (dest_channel_id, f"sig-{content_sig}")
                last_sig = self.sent_to_destinations.get(sig_key, 0.0)
                if last_sig and (now - last_sig) < float(self.global_content_ttl_seconds or 300):
                    dest_trace["decision"] = {
                        "action": "skip",
                        "reason": "dest_signature_duplicate",
                        "age_seconds": round(now - last_sig, 3),
                    }
                    dest_traces.append(dest_trace)
                    continue
            except Exception:
                pass

            dest_key = (dest_channel_id, f"live-{message.id}-{tag}")
            last_sent = self.sent_to_destinations.get(dest_key, 0.0)
            if last_sent and (now - last_sent) < self.recent_ttl_seconds:
                dest_trace["decision"] = {
                    "action": "skip",
                    "reason": "dest_message_tag_throttle",
                    "age_seconds": round(now - last_sent, 3),
                }
                dest_traces.append(dest_trace)
                continue
            try:
                await self._send_to_destination(
                    dest_channel_id=dest_channel_id,
                    content=formatted_content,
                    embeds=embeds_out,
                    webhook_username=wh_username,
                    webhook_avatar_url=wh_avatar_url,
                )
                self.sent_to_destinations[dest_key] = now
                try:
                    self.sent_to_destinations[(dest_channel_id, f"sig-{content_sig}")] = now
                except Exception:
                    pass
                forwarded += 1
                dest_trace["decision"] = {"action": "sent"}
                dest_traces.append(dest_trace)
                why = ""
                try:
                    matches = (trace.get("classifier") or {}).get("matches") or {}
                    tag_s = str(tag or "")
                    if tag_s == "AMAZON":
                        amazon = str(matches.get("amazon") or "").strip()
                        if amazon:
                            why = f"amazon={amazon[:80]}"
                    elif tag_s == "MONITORED_KEYWORD":
                        kws = matches.get("monitored_keywords") or []
                        if isinstance(kws, list) and kws:
                            why = "kw=" + ",".join(str(k) for k in kws[:3])
                    elif tag_s == "AFFILIATED_LINKS":
                        dom = str(matches.get("affiliate_domain") or "").strip()
                        reason = str(matches.get("affiliate_reason") or "").strip()
                        if dom:
                            why = f"domain={dom}"
                        elif reason:
                            why = reason
                except Exception:
                    why = ""
                if why:
                    log_forward(f"msg={message.id} {channel_id} -> {dest_channel_id} (tag={tag} why={why})")
                else:
                    log_forward(f"msg={message.id} {channel_id} -> {dest_channel_id} (tag={tag})")
                if stop_after_first:
                    break
            except Exception as e:
                dest_trace["decision"] = {"action": "error", "error": str(e), "error_type": type(e).__name__}
                dest_traces.append(dest_trace)
                log_error(f"Failed forwarding (msg={message.id}) to {dest_channel_id} (tag={tag})", error=e)

        if forwarded == 0 and cfg.VERBOSE:
            log_warn(f"All destinations blocked or failed (msg={message.id})")
        try:
            trace["forwarded_count"] = int(forwarded)
            trace["destinations"] = dest_traces
            trace.setdefault("decision", {"action": "processed"})
            write_trace_log(trace)
        except Exception:
            pass

    async def handle_edit(self, payload) -> None:
        # payload: discord.RawMessageUpdateEvent
        try:
            channel_id = int(getattr(payload, "channel_id", 0) or 0)
            message_id = int(getattr(payload, "message_id", 0) or 0)
        except Exception:
            return
        if channel_id <= 0 or message_id <= 0:
            return
        channel = self.bot.get_channel(channel_id)
        category_id = 0
        try:
            category_id = int(getattr(channel, "category_id", 0) or 0) if channel else 0
        except Exception:
            category_id = 0
        if not cfg.is_monitored_source_channel(channel_id, category_id=category_id or None):
            return
        # Edit cooldown per message id (config-driven)
        now = asyncio.get_event_loop().time()
        edit_key = f"edit-{message_id}"
        last_edit = self.recent_hashes.get(edit_key, 0.0)
        if last_edit and (now - last_edit) < int(cfg.EDIT_COOLDOWN_SECONDS or 30):
            return
        self.recent_hashes[edit_key] = now

        if not channel:
            return
        try:
            message = await channel.fetch_message(message_id)
        except Exception:
            return
        await self.handle_message(message)


def run_bot(*, settings: Dict[str, Any], token: str) -> Optional[int]:
    try:
        import discord
        from discord.ext import commands
    except Exception as e:
        log_error("discord.py is not installed or failed to import", error=e)
        return 2

    # Initialize standalone config store for migrated modules
    cfg.init(settings)

    intents = discord.Intents.default()
    if hasattr(intents, "message_content"):
        intents.message_content = True
    else:
        intents.messages = True

    prefix = str(settings.get("command_prefix") or "!").strip() or "!"
    bot = commands.Bot(command_prefix=prefix, intents=intents, help_command=None)

    # Pass tokens through so fetchall can use Discum/user token when needed.
    tokens = settings.get("_tokens") if isinstance(settings.get("_tokens"), dict) else None
    forwarder = MessageForwarder(bot=bot, keywords_list=load_keywords(), tokens=tokens)
    try:
        from commands import register_commands

        register_commands(bot=bot, forwarder=forwarder)
    except Exception as e:
        log_warn(f"Failed to register commands: {e}")

    @bot.event
    async def on_ready() -> None:
        try:
            user = bot.user
            log_info(
                f"Logged in as {getattr(user, 'name', 'Unknown')}#{getattr(user, 'discriminator', '0000')} (id={getattr(user, 'id', '0')})"
            )
        except Exception:
            log_info("Logged in (user unknown)")

        log_info(f"destination_guild_ids={sorted(list(cfg.DESTINATION_GUILD_IDS))}")
        log_info(
            f"monitored_channels={len(cfg.SMART_SOURCE_CHANNELS)} categories={len(cfg.MONITOR_CATEGORY_IDS)} "
            f"monitor_all={bool(cfg.MONITOR_ALL_DESTINATION_CHANNELS)} webhook_only={bool(cfg.MONITOR_WEBHOOK_MESSAGES_ONLY)} "
            f"raw_unwrap={bool(cfg.ENABLE_RAW_LINK_UNWRAP)} ttl={cfg.RECENT_TTL_SECONDS}s"
        )
        if not cfg.SMART_SOURCE_CHANNELS and not cfg.MONITOR_CATEGORY_IDS and not cfg.MONITOR_ALL_DESTINATION_CHANNELS:
            log_warn(
                "No monitored channels configured. Set one of: "
                "`source_channel_ids_*` OR `monitor_category_ids` OR `monitor_all_destination_channels=true`."
            )

        # Destination sanity: if everything is 0, the bot will classify but never forward.
        dest_ids = [
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
            int(getattr(cfg, "SMARTFILTER_UNCLASSIFIED_CHANNEL_ID", 0) or 0),
            int(cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID or 0),
            int(cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID or 0),
            int(cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID or 0),
            int(getattr(cfg, "SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID", 0) or 0),
        ]
        if not any(x > 0 for x in dest_ids):
            log_warn(
                "No destination channels configured (all IDs are 0). "
                "Set `fallback_channel_id` and/or `smartfilter_destinations` / `global_trigger_destinations` in settings.json."
            )

        # Route-map safety: validate mapped destination IDs are messageable.
        try:
            await forwarder.validate_route_maps()
        except Exception:
            pass

        # Ensure we keep slash commands guild-scoped only:
        # clear any previously-registered GLOBAL app commands for this application.
        try:
            bot.tree.clear_commands(guild=None)
            cleared = await bot.tree.sync()
            try:
                log_info(f"Global slash commands cleared (count={len(cleared)})")
            except Exception:
                pass
        except Exception as e:
            try:
                log_warn(f"Global slash clear failed ({type(e).__name__}: {e})")
            except Exception:
                pass

        # Slash commands: sync ONLY to destination guild(s) (fast propagation).
        try:
            dest_guild_ids = sorted(int(x) for x in (cfg.DESTINATION_GUILD_IDS or set()) if int(x) > 0)
        except Exception:
            dest_guild_ids = []
        if dest_guild_ids:
            try:
                synced = 0
                for gid in dest_guild_ids:
                    try:
                        cmds = await bot.tree.sync(guild=discord.Object(id=int(gid)))
                        try:
                            log_info(f"Slash commands sync ok: guild={int(gid)} count={len(cmds)}")
                        except Exception:
                            pass
                        synced += 1
                    except Exception as e:
                        try:
                            log_warn(f"Slash commands sync failed: guild={int(gid)} ({type(e).__name__}: {e})")
                        except Exception:
                            pass
                        continue
                if synced:
                    log_info(f"Slash commands synced to {synced} destination guild(s).")
            except Exception:
                pass

        # --- Fetchsync auto-poller (live updates via user token) ---
        try:
            poll_s = int(getattr(cfg, "FETCHSYNC_AUTO_POLL_SECONDS", 0) or 0)
        except Exception:
            poll_s = 0
        if poll_s > 0:
            try:
                user_token = str((forwarder.tokens or {}).get("FETCHALL_USER_TOKEN") or "").strip()
            except Exception:
                user_token = ""
            if not user_token:
                log_warn("Fetchsync auto-poller disabled: missing FETCHALL_USER_TOKEN.")
            else:
                try:
                    existing = getattr(bot, "_fetchsync_auto_task", None)
                except Exception:
                    existing = None
                if existing is None:
                    async def _auto_fetchsync_loop() -> None:
                        await bot.wait_until_ready()
                        while not bot.is_closed():
                            started = time.time()
                            try:
                                entries = iter_fetchall_entries()
                            except Exception:
                                entries = []
                            # Use the first destination guild (Mirror World) for all mappings.
                            dest_guild = None
                            try:
                                for gid in sorted(int(x) for x in (cfg.DESTINATION_GUILD_IDS or set()) if int(x) > 0):
                                    dest_guild = bot.get_guild(int(gid))
                                    if dest_guild is not None:
                                        break
                            except Exception:
                                dest_guild = None
                            for entry in entries or []:
                                try:
                                    await run_fetchsync(
                                        bot=bot,
                                        entry=entry,
                                        destination_guild=dest_guild,
                                        source_user_token=user_token,
                                        dryrun=False,
                                        progress_cb=None,
                                    )
                                except Exception as e:
                                    log_warn(f"[FETCHSYNC] auto poll failed ({type(e).__name__}: {e})")
                                # small gap between mappings
                                await asyncio.sleep(1.0)
                            elapsed = max(0.0, time.time() - started)
                            sleep_for = float(poll_s) - float(elapsed)
                            if sleep_for < 5.0:
                                sleep_for = 5.0
                            await asyncio.sleep(sleep_for)

                    try:
                        bot._fetchsync_auto_task = asyncio.create_task(_auto_fetchsync_loop())
                        log_info(f"Fetchsync auto-poller enabled: every {int(poll_s)}s")
                    except Exception as e:
                        log_warn(f"Fetchsync auto-poller failed to start ({type(e).__name__}: {e})")
        else:
            try:
                log_info("Fetchsync auto-poller disabled: fetchsync_auto_poll_seconds=0")
            except Exception:
                pass

    @bot.event
    async def on_message(message) -> None:
        # Ensure prefix commands still work even when forwarder gating is strict.
        try:
            await bot.process_commands(message)
        except Exception:
            pass
        await forwarder.handle_message(message)

    @bot.event
    async def on_raw_message_edit(payload) -> None:
        await forwarder.handle_edit(payload)

    try:
        bot.run(token)
        return 0
    except Exception as e:
        log_error("Bot crashed", error=e)
        return 2

