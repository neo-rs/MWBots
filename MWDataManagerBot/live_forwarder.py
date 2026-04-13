from __future__ import annotations

import asyncio
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from classifier import (
    detect_all_link_types,
    is_definitive_major_clearance_embed,
    is_major_clearance_followup_blob,
    is_major_clearance_monitor_embed_blob,
    order_link_types,
    select_target_channel_id,
)
from global_triggers import detect_global_triggers
from keywords import load_keywords
from logging_utils import (
    log_debug,
    log_error,
    log_explainable_forward_summary,
    log_explainable_major_clearance_send,
    log_global,
    log_info,
    log_filter,
    log_warn,
    write_trace_log,
)
import settings_store as cfg
from utils import (
    augment_text_with_affiliate_redirects,
    augment_text_with_dealshacks_hiddendealsociety,
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

def _dispatch_tag_priority(tag: str) -> int:
    """
    When multiple classifier tags route to the same destination channel, prefer the more specific bucket.
    (e.g. INSTORE_* over MONITORED_KEYWORD when both map to the same id via config or route maps.)
    """
    t = str(tag or "")
    if t == "PRICE_ERROR":
        return 200
    if t == "HD_TOTAL_INVENTORY":
        return 196
    if t == "MAJOR_CLEARANCE":
        return 195
    if t.startswith("INSTORE"):
        return 170
    # classify_instore_destination store buckets (often paired with MONITORED_KEYWORD on same lead)
    if t in ("MAJOR_STORES", "DISCOUNTED_STORES"):
        return 130
    if t in ("AMAZON_PROFITABLE_LEAD", "AMAZON", "AMAZON_FALLBACK", "AMZ_DEALS"):
        return 160
    if t == "UPCOMING":
        return 140
    if t == "AFFILIATED_LINKS":
        return 60
    if t == "MONITORED_KEYWORD":
        return 50
    if t == "DEFAULT":
        return 10
    return 40


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
        # Major-clearance pairing cache:
        # - key is (source_channel_id, embed_message_id) so interleaving messages from the same sender do not overwrite.
        # - follow-ups without a message-link will pair to the most recent pending embed for the same sender in that channel.
        self.pending_major_clearance_followups: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self.pending_major_clearance: Dict[Tuple[int, int], Dict[str, Any]] = {}
        self._pending_major_clearance_latest_by_sender: Dict[Tuple[int, str], int] = {}

    async def _debug_react(self, message, *, allowed: Optional[bool], reason: str) -> None:
        """
        Optional debug reactions so operators can see whether a message was processed/forwarded.
        Disabled by default; scoped by allowlist in settings (`debug_reactions.allow_channel_ids`).
        """
        if not bool(getattr(cfg, "DEBUG_REACTIONS_ENABLED", False)):
            return
        try:
            ch_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        except Exception:
            ch_id = 0
        if ch_id <= 0:
            return
        try:
            allow = set(getattr(cfg, "DEBUG_REACTIONS_ALLOW_CHANNEL_IDS", set()) or set())
        except Exception:
            allow = set()
        if allow and ch_id not in allow:
            return
        # If allowlist is empty, treat as "off" (safety: no accidental global spam).
        if not allow:
            return

        emoji = ""
        if allowed is True:
            emoji = str(getattr(cfg, "DEBUG_REACTIONS_EMOJI_ALLOWED", "✅") or "✅")
        elif allowed is False:
            emoji = str(getattr(cfg, "DEBUG_REACTIONS_EMOJI_BLOCKED", "❌") or "❌")
        else:
            return
        try:
            await message.add_reaction(emoji)
        except Exception:
            # Never let debug UX affect forwarding.
            if cfg.VERBOSE:
                try:
                    log_debug(f"[debug_reactions] failed add_reaction ({reason}) msg={getattr(message,'id',0)} ch={ch_id}")
                except Exception:
                    pass

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

    def _is_short_bare_embed_payload(self, payload: Dict[str, Any]) -> bool:
        """Discum-style guard: wait if embed payload looks like a thin placeholder."""
        try:
            embeds: List[Dict[str, Any]] = payload.get("embeds", []) or []
            if not embeds:
                return False
            content = str(payload.get("content", "") or "").strip()
            threshold = int(getattr(cfg, "SHORT_EMBED_CHAR_THRESHOLD", 50) or 50)
            if len(content) >= max(0, threshold):
                return False
            for raw in embeds:
                if not isinstance(raw, dict):
                    continue
                if str(raw.get("description", "") or "").strip():
                    return False
                fields = raw.get("fields") or []
                if isinstance(fields, list):
                    for f in fields:
                        if not isinstance(f, dict):
                            continue
                        if str(f.get("name", "") or "").strip() or str(f.get("value", "") or "").strip():
                            return False
                if raw.get("image") or raw.get("thumbnail") or raw.get("video"):
                    return False
                # Monitors often attach footer/author on the first gateway payload while the title
                # is still truncated and fields/description/image arrive on MESSAGE_UPDATE.
                # Do not treat footer/author/provider as proof the embed is complete.
            return True
        except Exception:
            return False

    async def _hydrate_short_embed_message(self, message, payload: Dict[str, Any]):
        """
        Re-fetch short/bare embeds for a bounded window to avoid forwarding placeholders
        like: title + "..." before MESSAGE_UPDATE hydration lands.
        """
        if not self._is_short_bare_embed_payload(payload):
            return message, payload
        try:
            delay = float(getattr(cfg, "SHORT_EMBED_RETRY_DELAY_SECONDS", 5.0) or 5.0)
        except Exception:
            delay = 5.0
        try:
            max_wait = float(getattr(cfg, "SHORT_EMBED_MAX_WAIT_SECONDS", 35.0) or 35.0)
        except Exception:
            max_wait = 35.0
        if delay <= 0 or max_wait <= 0:
            return message, payload
        waited = 0.0
        channel = getattr(message, "channel", None)
        message_id = int(getattr(message, "id", 0) or 0)
        while waited < max_wait:
            await asyncio.sleep(delay)
            waited += delay
            if channel is None or message_id <= 0:
                break
            try:
                refreshed = await channel.fetch_message(message_id)
            except Exception:
                break
            refreshed_payload = self._to_filter_payload(refreshed)
            if not self._is_short_bare_embed_payload(refreshed_payload):
                return refreshed, refreshed_payload
        return message, payload

    def _sender_key(self, message, payload: Dict[str, Any]) -> str:
        try:
            wh = str(getattr(message, "webhook_id", "") or "").strip()
            if wh:
                return f"wh:{wh}"
        except Exception:
            pass
        try:
            aid = str(((payload.get("author") or {}).get("id") or "")).strip()
            if aid:
                return f"aid:{aid}"
        except Exception:
            pass
        try:
            an = str(((payload.get("author") or {}).get("username") or "")).strip().lower()
            if an:
                return f"an:{an}"
        except Exception:
            pass
        return "unknown"

    def _extract_discord_message_id_from_text(self, text: str) -> int:
        """
        If a follow-up message includes a Discord message link, use its message_id to pair.
        Accepts discord.com, discordapp.com, ptb/canary.
        """
        raw = str(text or "")
        m = re.search(
            r"https?://(?:www\.|(?:(?:ptb|canary)\.)?)discord(?:app)?\.com/channels/\d+/\d+/(\d+)\b",
            raw,
            re.IGNORECASE,
        )
        if not m:
            return 0
        try:
            return int(m.group(1))
        except Exception:
            return 0

    def _embed_dict_has_image(self, ed: Dict[str, Any]) -> bool:
        try:
            if not isinstance(ed, dict):
                return False
            for key in ("image", "thumbnail"):
                block = ed.get(key)
                if isinstance(block, dict) and str(block.get("url") or "").strip():
                    return True
        except Exception:
            pass
        return False

    def _major_clearance_attachments_without_barcode_noise(
        self,
        attachments: Optional[List[Dict[str, Any]]],
        raw_embeds: Optional[List[Dict[str, Any]]],
    ) -> Optional[List[Dict[str, Any]]]:
        """
        When the Tempo-style embed already includes a product image, image *file* attachments
        are usually duplicate barcodes / UPC scans. Drop those so timeout/pair sends are not
        cluttered with barcode-only images.
        """
        if not attachments:
            return attachments
        has_embed_image = any(self._embed_dict_has_image(ed) for ed in (raw_embeds or []) if isinstance(ed, dict))
        if not has_embed_image:
            return attachments
        out: List[Dict[str, Any]] = []
        for a in attachments:
            if not isinstance(a, dict):
                continue
            if not is_image_attachment(a):
                out.append(a)
                continue
            fn = str(a.get("filename") or "").lower()
            if any(x in fn for x in ("barcode", "upc", "code128", "ean", "sku-scan")):
                continue
            # Embed already shows the product; drop generic images (typical barcode noise).
            continue
        return out or None

    def _collapse_dispatch_same_destination(
        self,
        pairs: List[Tuple[int, str]],
        *,
        source_group: str,
    ) -> List[Tuple[int, str]]:
        """
        One outbound post per routed destination channel per source message.
        detect_all_link_types can emit several (cid, tag) pairs that differ only by tag but map to the
        same channel after MIRRORWORLD route maps — without this, the forwarder loops and sends duplicates.
        """
        if not pairs:
            return []
        winners: Dict[int, Tuple[int, str, int]] = {}
        for cid, tag in pairs:
            try:
                d = int(self._apply_route_map(source_group=source_group, dest_channel_id=int(cid or 0)) or 0)
            except Exception:
                d = 0
            if d <= 0:
                continue
            pr = _dispatch_tag_priority(str(tag or ""))
            cur = winners.get(d)
            if cur is None or pr > cur[2]:
                winners[d] = (int(cid or 0), str(tag or ""), pr)
        out: List[Tuple[int, str]] = []
        emitted: Set[int] = set()
        for cid, tag in pairs:
            try:
                d = int(self._apply_route_map(source_group=source_group, dest_channel_id=int(cid or 0)) or 0)
            except Exception:
                d = 0
            if d <= 0:
                continue
            w = winners.get(d)
            if not w:
                continue
            if int(cid or 0) != int(w[0]) or str(tag or "") != str(w[1]):
                continue
            if d in emitted:
                continue
            out.append((w[0], w[1]))
            emitted.add(d)
        return out

    async def _send_to_destination(
        self,
        *,
        dest_channel_id: int,
        content: str,
        embeds: List[Dict[str, Any]],
        attachments: Optional[List[Dict[str, Any]]] = None,
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
            if i == 0:
                # Use channel.send if we need the message object (return_first_message) or have reference/view
                if return_first_message or reference is not None or view is not None:
                    # Reference/view/return_message path: use classic send to get message object
                    first_msg = await channel.send(
                        content=chunk,
                        embeds=embed_objs[:10],
                        allowed_mentions=allowed_mentions,
                        reference=reference,
                        view=view,
                    )
                else:
                    # Plain forward: use webhook for cleaner identity
                    await send_via_webhook_or_bot(
                        dest_channel=channel,
                        content=chunk,
                        embeds=embed_objs[:10],
                        attachments=attachments,
                        username=webhook_username,
                        avatar_url=webhook_avatar_url,
                        reason="MWDataManagerBot live forward",
                    )
                    first_msg = None
            else:
                # Only attach reference/view on the first chunk.
                if reference is None and view is None:
                    await send_via_webhook_or_bot(
                        dest_channel=channel,
                        content=chunk,
                        embeds=[],
                        attachments=None,
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
        _add("AMZ_DEALS", cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID, "Amazon • Deals (conversational)")

        # Global-trigger buckets
        _add("PRICE_ERROR", cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID, "Global • Price error/glitched")
        _add("PROFITABLE_FLIP", cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID, "Global • Profitable flip")
        _add("LUNCHMONEY_FLIP", cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID, "Global • Lunchmoney flip")
        _add("AMAZON_PROFITABLE_LEAD", cfg.SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID, "Global • Amazon profitable lead")
        _add("MAJOR_CLEARANCE", int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0), "Global • Major clearance")
        _add(
            "HD_TOTAL_INVENTORY",
            int(getattr(cfg, "HD_TOTAL_INVENTORY_DESTINATION_CHANNEL_ID", 0) or 0),
            "Clearance • HD total inventory (1:1)",
        )

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

    async def handle_message(self, message, *, is_edit: bool = False) -> None:
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
                is_webhook = getattr(message, "webhook_id", None) is not None
                author_obj = getattr(message, "author", None)
                is_bot_author = bool(getattr(author_obj, "bot", False))
                # Some source monitor apps post as bot-authored messages (not webhooks).
                # Allow those while still blocking regular user messages.
                if not is_webhook and not is_bot_author:
                    if cfg.VERBOSE and int(channel_id or 0) in getattr(
                        cfg, "SMART_SOURCE_CHANNELS_INSTORE", set()
                    ):
                        log_filter(
                            f"skip instore ch=<#{channel_id}>: not webhook and not bot author "
                            f"(MONITOR_WEBHOOK_MESSAGES_ONLY)"
                        )
                    return
            except Exception:
                return

        if getattr(message, "author", None) == self.bot.user:
            return

        # Same message id must not be processed twice on CREATE, but MESSAGE_UPDATE edits
        # (Discum hydration) need a full re-run so classification sees fields/description/image.
        mid = int(getattr(message, "id", 0) or 0)
        if mid in self.processed_ids and not is_edit:
            return
        if mid > 0 and mid not in self.processed_ids:
            self.processed_ids.add(mid)

        payload = self._to_filter_payload(message)
        message, payload = await self._hydrate_short_embed_message(message, payload)

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
                log_filter(f"skipped message {message.id} in channel <#{channel_id}>")
            await self._debug_react(message, allowed=False, reason="filter")
            return

        content_sig = generate_content_signature(content, embeds, attachments, for_cross_post_dedupe=True)
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
                log_info(f"Duplicate message detected (posted {round(now-last,1)}s ago) in channel <#{channel_id}>")
            await self._debug_react(message, allowed=False, reason="per_channel_duplicate")
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
                text_to_check, dealshacks_links = await augment_text_with_dealshacks_hiddendealsociety(text_to_check)
                text_to_check, affiliate_links = await augment_text_with_affiliate_redirects(text_to_check)
                # Merge and de-dupe while preserving order
                seen = set()
                merged: List[str] = []
                for u in (
                    (raw_links or [])
                    + (dmflip_links or [])
                    + (ring_links or [])
                    + (dealshacks_links or [])
                    + (affiliate_links or [])
                ):
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
        global_sig = generate_content_signature(content, embeds, attachments, for_cross_post_dedupe=True)
        if self._is_global_duplicate(global_sig):
            trace["decision"] = {"action": "skip", "reason": "global_duplicate", "global_sig": global_sig}
            try:
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_global(f"skip duplicate content signature in channel <#{channel_id}>", event="global_duplicate")
            await self._debug_react(message, allowed=False, reason="global_duplicate")
            return

        all_link_types = detect_all_link_types(
            text_to_check,
            attachments,
            self.keywords_list,
            embeds,
            source_channel_id=channel_id,
            trace=trace,
            message_content=content,
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

        try:
            mm_skip = bool((trace.get("classifier") or {}).get("matches", {}).get("mention_format_skip"))
        except Exception:
            mm_skip = False
        if mm_skip and not all_link_types:
            trace["decision"] = {"action": "skip", "reason": "mention_format_noise"}
            try:
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_filter(f"skipped mention-only / ping-shaped message msg={message.id} ch=<#{channel_id}>")
            await self._debug_react(message, allowed=False, reason="mention_format_noise")
            return

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
        # Discum-style output: when reuploading attachments as real files, do NOT convert them into embed images.
        use_files = bool(getattr(cfg, "FORWARD_ATTACHMENTS_AS_FILES", True))
        if not use_files:
            # Render image attachments as embeds for better Discord UX (legacy behavior).
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
                source_group = "clearance"
            elif int(channel_id) in cfg.SMART_SOURCE_CHANNELS_ONLINE:
                source_group = "online"
            elif int(channel_id) in cfg.SMART_SOURCE_CHANNELS_MISC:
                source_group = "misc"
        except Exception:
            source_group = "unknown"

        if not dispatch_link_types:
            fallback = select_target_channel_id(
                text_to_check,
                attachments,
                self.keywords_list,
                source_channel_id=channel_id,
                trace=trace,
                message_content=content,
                embeds=embeds,
            )
            if fallback:
                dispatch_link_types = [fallback]

        if dispatch_link_types:
            dispatch_link_types = self._collapse_dispatch_same_destination(
                dispatch_link_types,
                source_group=source_group,
            )

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

        # Special paired-flow: "major clearance" stock-monitor embeds + follow-up message.
        # IMPORTANT: Only on *clearance* source channels. If we also intercept instore sources,
        # Tempo-shaped embeds there return early and never run instore smartfilters (sneakers/seasonal/etc.).
        try:
            major_clearance_dest = int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0)
        except Exception:
            major_clearance_dest = 0
        # Major-clearance pairing sources:
        # - If `major_clearance_source_channel_ids` is configured, use it (explicit allowlist).
        # - Otherwise fall back to `source_channel_ids_clearance`.
        try:
            mc_sources = set(getattr(cfg, "MAJOR_CLEARANCE_SOURCE_CHANNEL_IDS", set()) or set())
        except Exception:
            mc_sources = set()
        if not mc_sources:
            try:
                mc_sources = set(getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()) or set())
            except Exception:
                mc_sources = set()
        is_major_clearance_source = bool(int(channel_id or 0) in mc_sources)
        if major_clearance_dest > 0 and is_major_clearance_source:
            now_ts = asyncio.get_event_loop().time()
            sender_key = self._sender_key(message, payload)
            source_ch_id = int(channel_id or 0)
            embed_msg_id = int(getattr(message, "id", 0) or 0)
            pair_key = (source_ch_id, embed_msg_id)
            try:
                ttl_s = float(getattr(cfg, "MAJOR_CLEARANCE_PAIR_TTL_SECONDS", 180) or 180)
            except Exception:
                ttl_s = 180.0
            if ttl_s < 10:
                ttl_s = 10.0
            send_single_on_timeout = bool(getattr(cfg, "MAJOR_CLEARANCE_SEND_SINGLE_ON_TIMEOUT", False))
            mc_filtered = self._major_clearance_attachments_without_barcode_noise(
                attachments if use_files else None,
                embeds,
            )

            # Flush expired pending candidates (optional single-send fallback).
            expired_first_items: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []
            expired_followup_items: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []
            try:
                for k, v in (self.pending_major_clearance or {}).items():
                    if (now_ts - float(v.get("timestamp", 0.0))) > ttl_s:
                        expired_first_items.append((k, v))
            except Exception:
                expired_first_items = []
            try:
                for k, v in (self.pending_major_clearance_followups or {}).items():
                    if (now_ts - float(v.get("timestamp", 0.0))) > ttl_s:
                        expired_followup_items.append((k, v))
            except Exception:
                expired_followup_items = []

            for k, _v in expired_first_items:
                try:
                    self.pending_major_clearance.pop(k, None)
                except Exception:
                    pass
            for k, _v in expired_followup_items:
                try:
                    self.pending_major_clearance_followups.pop(k, None)
                except Exception:
                    pass
            # Also prune sender-latest index if it points at an expired embed.
            if expired_first_items:
                try:
                    expired_ids = {int(k[1]) for k, _ in expired_first_items if isinstance(k, tuple)}
                except Exception:
                    expired_ids = set()
                if expired_ids:
                    try:
                        for sk, mid in list(self._pending_major_clearance_latest_by_sender.items()):
                            if int(mid or 0) in expired_ids:
                                self._pending_major_clearance_latest_by_sender.pop(sk, None)
                    except Exception:
                        pass

            if send_single_on_timeout and expired_first_items:
                try:
                    import discord

                    allowed_mentions = discord.AllowedMentions.none()
                    for _k, pending_item in expired_first_items[:20]:
                        # Safety guard: if the "first embed" match was created purely by an edit,
                        # don't send it alone on timeout (it tends to cause false positives).
                        if bool(pending_item.get("from_edit")):
                            continue
                        src_ch = int(pending_item.get("source_channel_id") or 0)
                        src_group = "instore" if src_ch in getattr(cfg, "SMART_SOURCE_CHANNELS_INSTORE", set()) else (
                            "clearance" if src_ch in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()) else "unknown"
                        )
                        dest_after_exp = self._apply_route_map(source_group=src_group, dest_channel_id=major_clearance_dest)
                        if dest_after_exp <= 0:
                            continue
                        await self._send_to_destination(
                            dest_channel_id=dest_after_exp,
                            content=str(pending_item.get("formatted_content") or ""),
                            embeds=list(pending_item.get("embeds_out") or []),
                            attachments=list(pending_item.get("attachments") or []) if isinstance(pending_item.get("attachments"), list) else None,
                            webhook_username=str(pending_item.get("webhook_username") or ""),
                            webhook_avatar_url=str(pending_item.get("webhook_avatar_url") or ""),
                            allowed_mentions=allowed_mentions,
                        )
                        if cfg.VERBOSE:
                            try:
                                log_explainable_major_clearance_send(
                                    variant="timeout_fallback",
                                    message_id=int(pending_item.get("source_message_id") or 0),
                                    source_channel_id=src_ch,
                                    dest_channel_id=int(dest_after_exp),
                                    route_map_applied=int(major_clearance_dest) != int(dest_after_exp),
                                )
                            except Exception:
                                pass
                except Exception as e:
                    if cfg.VERBOSE:
                        log_warn(f"major-clearance timeout fallback failed: {type(e).__name__}: {e}")

            try:
                hd_inv_src = int(getattr(cfg, "HD_TOTAL_INVENTORY_SOURCE_CHANNEL_ID", 0) or 0)
            except Exception:
                hd_inv_src = 0
            hd_exclusive_definitive = bool(
                hd_inv_src > 0
                and int(channel_id or 0) == hd_inv_src
                and is_definitive_major_clearance_embed(text_to_check)
            )
            is_candidate_embed = is_major_clearance_monitor_embed_blob(text_to_check)
            if is_candidate_embed and not hd_exclusive_definitive:
                is_definitive_embed = is_definitive_major_clearance_embed(text_to_check)
                if is_definitive_embed:
                    try:
                        import discord

                        allowed_mentions = discord.AllowedMentions.none()
                        dest_after = self._apply_route_map(source_group=source_group, dest_channel_id=major_clearance_dest)
                        if dest_after > 0:
                            await self._send_to_destination(
                                dest_channel_id=dest_after,
                                content=str(formatted_content or ""),
                                embeds=list(embeds_out or []),
                                attachments=list(mc_filtered or []) if use_files else None,
                                webhook_username=wh_username,
                                webhook_avatar_url=wh_avatar_url,
                                allowed_mentions=allowed_mentions,
                            )
                            trace["decision"] = {"action": "sent_major_clearance_single", "dest": int(dest_after)}
                            try:
                                write_trace_log(trace)
                            except Exception:
                                pass
                            if cfg.VERBOSE:
                                log_explainable_major_clearance_send(
                                    variant="single_embed",
                                    message_id=int(message.id),
                                    source_channel_id=int(channel_id),
                                    dest_channel_id=int(dest_after),
                                    route_map_applied=int(major_clearance_dest) != int(dest_after),
                                )
                            return
                    except Exception as e:
                        if cfg.VERBOSE:
                            log_warn(f"major-clearance single-send failed (msg={message.id}): {type(e).__name__}: {e}")
                        # Fall through to paired-flow cache behavior when immediate single-send fails.

                # If a follow-up arrived earlier and included a message-link, it may already be cached under our msg id.
                followup_cached = (self.pending_major_clearance_followups or {}).get(pair_key)
                # If follow-up arrived earlier, send the pair immediately (even if the first came from edit).
                if followup_cached:
                    try:
                        import discord

                        allowed_mentions = discord.AllowedMentions.none()
                        dest_after = self._apply_route_map(source_group=source_group, dest_channel_id=major_clearance_dest)

                        first_msg = await self._send_to_destination(
                            dest_channel_id=dest_after,
                            content=str(formatted_content or ""),
                            embeds=list(embeds_out or []),
                            attachments=list(mc_filtered or []) if use_files else None,
                            webhook_username=wh_username,
                            webhook_avatar_url=wh_avatar_url,
                            allowed_mentions=allowed_mentions,
                            return_first_message=True,
                        )

                        followup_embed = discord.Embed(title="Follow-up", color=0xE67E22)
                        # Use first's jump_url when available.
                        src_jump = str(getattr(message, "jump_url", "") or "").strip()
                        if src_jump:
                            followup_embed.add_field(name="Source message", value=f"[Jump to original]({src_jump})", inline=False)

                        _fu_att = followup_cached.get("attachments") if use_files else None
                        _fu_att = self._major_clearance_attachments_without_barcode_noise(
                            _fu_att if isinstance(_fu_att, list) else None,
                            list(followup_cached.get("embeds_out") or []),
                        )
                        await self._send_to_destination(
                            dest_channel_id=dest_after,
                            content=str(followup_cached.get("formatted_content") or "") or "\u200b",
                            embeds=[followup_embed.to_dict()] + list(followup_cached.get("embeds_out") or []),
                            attachments=_fu_att,
                            webhook_username=str(followup_cached.get("webhook_username") or ""),
                            webhook_avatar_url=str(followup_cached.get("webhook_avatar_url") or ""),
                            allowed_mentions=allowed_mentions,
                            reference=first_msg,
                        )

                        self.pending_major_clearance.pop(pair_key, None)
                        self.pending_major_clearance_followups.pop(pair_key, None)
                        self._pending_major_clearance_latest_by_sender.pop((source_ch_id, str(sender_key)), None)
                        trace["decision"] = {"action": "sent_major_clearance_pair_rev_order", "dest": int(dest_after or 0)}
                        try:
                            write_trace_log(trace)
                        except Exception:
                            pass
                        if cfg.VERBOSE:
                            log_explainable_major_clearance_send(
                                variant="pair_reverse_order",
                                message_id=int(message.id),
                                source_channel_id=int(channel_id),
                                dest_channel_id=int(dest_after),
                                route_map_applied=int(major_clearance_dest) != int(dest_after),
                            )
                        return
                    except Exception as e:
                        if cfg.VERBOSE:
                            log_warn(f"major-clearance pair(rev-order) failed (msg={message.id}): {type(e).__name__}: {e}")

                self.pending_major_clearance[pair_key] = {
                    "timestamp": now_ts,
                    "formatted_content": formatted_content,
                    "embeds_out": embeds_out,
                    "attachments": mc_filtered if use_files else None,
                    "webhook_username": wh_username,
                    "webhook_avatar_url": wh_avatar_url,
                    "source_jump_url": str(getattr(message, "jump_url", "") or ""),
                    "source_channel_id": source_ch_id,
                    "source_message_id": embed_msg_id,
                    "from_edit": bool(is_edit),
                }
                # Track "latest pending embed" by sender so follow-ups without an explicit link can still pair.
                try:
                    self._pending_major_clearance_latest_by_sender[(source_ch_id, str(sender_key))] = embed_msg_id
                except Exception:
                    pass
                trace["decision"] = {"action": "pending_major_clearance", "reason": "waiting_followup_same_sender"}
                try:
                    write_trace_log(trace)
                except Exception:
                    pass
                if cfg.VERBOSE:
                    log_filter(f"cached major-clearance candidate msg={message.id} ch=<#{channel_id}>")
                return

            # Follow-up: try to pair by explicit message link first; otherwise fall back to most recent pending for sender.
            followup_ref_mid = self._extract_discord_message_id_from_text(text_to_check)
            pending_key: Optional[Tuple[int, int]] = None
            if followup_ref_mid > 0:
                pending_key = (source_ch_id, int(followup_ref_mid))
            else:
                try:
                    last_mid = int(self._pending_major_clearance_latest_by_sender.get((source_ch_id, str(sender_key)), 0) or 0)
                except Exception:
                    last_mid = 0
                if last_mid > 0:
                    pending_key = (source_ch_id, last_mid)

            pending = (self.pending_major_clearance or {}).get(pending_key) if pending_key else None
            if is_major_clearance_followup_blob(text_to_check, message_content=content, embeds=embeds):
                try:
                    import discord

                    # If we already have the first embed cached, send the pair.
                    if pending:
                        allowed_mentions = discord.AllowedMentions.none()
                        dest_after = self._apply_route_map(source_group=source_group, dest_channel_id=major_clearance_dest)
                        first_msg = await self._send_to_destination(
                            dest_channel_id=dest_after,
                            content=str(pending.get("formatted_content") or ""),
                            embeds=list(pending.get("embeds_out") or []),
                            attachments=list(pending.get("attachments") or []) if isinstance(pending.get("attachments"), list) else None,
                            webhook_username=str(pending.get("webhook_username") or ""),
                            webhook_avatar_url=str(pending.get("webhook_avatar_url") or ""),
                            allowed_mentions=allowed_mentions,
                            return_first_message=True,
                        )
                        followup_embed = discord.Embed(title="Follow-up", color=0xE67E22)
                        src_jump = str(pending.get("source_jump_url") or "").strip()
                        if src_jump:
                            followup_embed.add_field(name="Source message", value=f"[Jump to original]({src_jump})", inline=False)
                        await self._send_to_destination(
                            dest_channel_id=dest_after,
                            content=formatted_content or "\u200b",
                            embeds=[followup_embed.to_dict()] + (embeds_out or []),
                            attachments=mc_filtered if use_files else None,
                            webhook_username=wh_username,
                            webhook_avatar_url=wh_avatar_url,
                            allowed_mentions=allowed_mentions,
                            reference=first_msg,
                        )
                        try:
                            if pending_key:
                                self.pending_major_clearance.pop(pending_key, None)
                                self.pending_major_clearance_followups.pop(pending_key, None)
                        except Exception:
                            pass
                        self._pending_major_clearance_latest_by_sender.pop((source_ch_id, str(sender_key)), None)
                        trace["decision"] = {"action": "sent_major_clearance_pair", "dest": int(dest_after or 0)}
                        try:
                            write_trace_log(trace)
                        except Exception:
                            pass
                        if cfg.VERBOSE:
                            log_explainable_major_clearance_send(
                                variant="pair",
                                message_id=int(message.id),
                                source_channel_id=int(channel_id),
                                dest_channel_id=int(dest_after),
                                route_map_applied=int(major_clearance_dest) != int(dest_after),
                            )
                        return

                    # Otherwise cache the follow-up so an edited/late candidate can still pair.
                    cache_key = pending_key if pending_key else pair_key
                    self.pending_major_clearance_followups[cache_key] = {
                        "timestamp": now_ts,
                        "formatted_content": formatted_content,
                        "embeds_out": embeds_out,
                        "attachments": mc_filtered if use_files else None,
                        "webhook_username": wh_username,
                        "webhook_avatar_url": wh_avatar_url,
                        "source_channel_id": source_ch_id,
                        "source_message_id": int(getattr(message, "id", 0) or 0),
                        "from_edit": bool(is_edit),
                        "ref_message_id": int(followup_ref_mid or 0),
                    }
                    trace["decision"] = {"action": "pending_major_clearance_followup", "reason": "waiting_first_same_sender"}
                    try:
                        write_trace_log(trace)
                    except Exception:
                        pass
                    if cfg.VERBOSE:
                        log_filter(f"cached major-clearance follow-up msg={message.id} ch=<#{channel_id}>")
                    return
                except Exception as e:
                    if cfg.VERBOSE:
                        log_warn(f"major-clearance pair send failed (msg={message.id}): {type(e).__name__}: {e}")
                    # Do not return; if pair-send fails, continue through normal routing.

        if not dispatch_link_types:
            # Strict clearance mode: do not spam UNCLASSIFIED with monitor-feed fragments.
            # These are either handled by major-clearance flow or intentionally dropped.
            text_lower = (text_to_check or "").lower()
            if source_group == "clearance" and (
                "home depot store clearance deals" in text_lower
                or "internet number" in text_lower
                or "total inventory" in text_lower
            ):
                trace["decision"] = {"action": "skip", "reason": "clearance_monitor_no_route"}
                try:
                    write_trace_log(trace)
                except Exception:
                    pass
                if cfg.VERBOSE:
                    log_filter(f"skipped clearance monitor fragment msg={message.id} ch=<#{channel_id}>")
                await self._debug_react(message, allowed=False, reason="clearance_monitor_no_route")
                return
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
                await self._debug_react(message, allowed=True, reason="unclassified_sent")
                return
            if cfg.VERBOSE:
                log_warn(f"No destination after classification (msg={message.id}) for source channel <#{channel_id}>")
            await self._debug_react(message, allowed=False, reason="no_destination")
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
                    attachments=attachments if use_files else None,
                    webhook_username=wh_username,
                    webhook_avatar_url=wh_avatar_url,
                    return_first_message=False,
                )
                self.sent_to_destinations[dest_key] = now
                try:
                    self.sent_to_destinations[(dest_channel_id, f"sig-{content_sig}")] = now
                except Exception:
                    pass
                forwarded += 1
                dest_trace["decision"] = {"action": "sent"}
                dest_traces.append(dest_trace)

                if stop_after_first:
                    break
            except Exception as e:
                dest_trace["decision"] = {"action": "error", "error": str(e), "error_type": type(e).__name__}
                dest_traces.append(dest_trace)
                log_error(f"Failed forwarding (msg={message.id}) to <#{dest_channel_id}> (tag={tag})", error=e)

        if dest_traces or forwarded > 0:
            log_explainable_forward_summary(
                message_id=int(getattr(message, "id", 0) or 0),
                source_channel_id=int(channel_id or 0),
                source_group=str(source_group or "unknown"),
                dest_traces=dest_traces,
                stop_after_first=bool(stop_after_first),
                content_preview=str(trace.get("content_preview") or ""),
                forwarded_count=int(forwarded),
                trace=trace,
            )
        elif forwarded == 0 and cfg.VERBOSE:
            log_warn(f"All destinations blocked or failed (msg={message.id})")
        await self._debug_react(message, allowed=(forwarded > 0), reason="forwarded" if forwarded > 0 else "no_forwarded")
        try:
            trace["forwarded_count"] = int(forwarded)
            trace["destinations"] = dest_traces
            trace.setdefault("decision", {"action": "processed"})
            write_trace_log(trace)
        except Exception:
            pass

    async def handle_edit(self, payload) -> None:
        # payload: discord.RawMessageUpdateEvent
        if not bool(getattr(cfg, "FORWARD_ON_EDIT", False)):
            return
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
        await self.handle_message(message, is_edit=True)


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

    tokens = settings.get("_tokens") if isinstance(settings.get("_tokens"), dict) else None
    forwarder = MessageForwarder(bot=bot, keywords_list=load_keywords(), tokens=tokens)
    try:
        from commands import register_commands

        register_commands(bot=bot, forwarder=forwarder)
    except Exception as e:
        log_warn(f"Failed to register commands: {e}")

    # Register /discum on this same bot so one tree.sync() pushes both DataManager commands and /discum.
    # (DiscumBot is a user-account; only this bot token can register slash commands. Single sync avoids overwriting.)
    try:
        import sys
        import importlib.util
        from pathlib import Path as _Path
        _live_dir = _Path(__file__).resolve().parent
        # Server: ROOT/MWDataManagerBot, ROOT/MWDiscumBot. Local: ROOT/MWBots/MWDataManagerBot, ROOT/MWBots/MWDiscumBot.
        _candidates = [
            _live_dir.parent / "MWDiscumBot",
            _live_dir.parent.parent / "MWBots" / "MWDiscumBot",
        ]
        _dcm = None
        for _mw_discum_dir in _candidates:
            if not _mw_discum_dir.is_dir():
                continue
            _py_file = _mw_discum_dir / "discum_command_bot.py"
            if not _py_file.exists():
                continue
            if str(_mw_discum_dir) not in sys.path:
                sys.path.insert(0, str(_mw_discum_dir))
            _spec = importlib.util.spec_from_file_location("discum_command_bot", _py_file)
            if _spec is None or _spec.loader is None:
                continue
            _mod = importlib.util.module_from_spec(_spec)
            sys.modules["discum_command_bot"] = _mod
            _spec.loader.exec_module(_mod)
            _dcm = _mod
            break
        if _dcm is not None and hasattr(_dcm, "register_discum_commands_to_bot"):
            _dcm.register_discum_commands_to_bot(bot)
            log_info("Registered /discum on this bot (single sync will include /discum).")
        elif _dcm is None:
            _tried = [str(p) for p in _candidates]
            log_warn("Could not register /discum: MWDiscumBot/discum_command_bot.py not found. Tried: " + "; ".join(_tried))
    except Exception as e:
        import traceback
        log_warn(f"Could not register /discum on this bot: {e}")
        try:
            log_warn("Traceback: " + "".join(traceback.format_exception(type(e), e, e.__traceback__)).replace("\n", " | ")[:500])
        except Exception:
            pass

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
            int(cfg.SMARTFILTER_AMZ_DEALS_CHANNEL_ID or 0),
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
            int(getattr(cfg, "SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID", 0) or 0),
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

        # Slash commands: copy global (e.g. /discum) into each guild and sync first, then clear global scope.
        # Order matters: copy_global_to + sync(guild=...) must run before clear_commands(guild=None), otherwise
        # /discum is removed from the tree before it is copied to the guild.
        try:
            dest_guild_ids = sorted(int(x) for x in (cfg.DESTINATION_GUILD_IDS or set()) if int(x) > 0)
        except Exception:
            dest_guild_ids = []
        if dest_guild_ids:
            try:
                synced = 0
                for gid in dest_guild_ids:
                    try:
                        gobj = discord.Object(id=int(gid))
                        try:
                            bot.tree.copy_global_to(guild=gobj)
                        except Exception as copy_err:
                            try:
                                log_warn(f"copy_global_to failed for guild={gid}: {type(copy_err).__name__}: {copy_err}")
                            except Exception:
                                pass
                        cmds = await bot.tree.sync(guild=gobj)
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

        # After guild sync: clear global scope so commands only appear in destination guild(s).
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

