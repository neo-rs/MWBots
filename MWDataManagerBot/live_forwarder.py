from __future__ import annotations

import asyncio
import hashlib
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
    build_raw_links_followup,
    collect_embed_strings,
    extract_all_raw_links_from_text,
    extract_urls_from_text,
    generate_content_signature,
    replace_single_url_with_raw,
)


def _format_embeds_for_forwarding(embeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Trim/clean embeds to a safe dict shape before forwarding."""
    out: List[Dict[str, Any]] = []
    for e in embeds or []:
        if not isinstance(e, dict):
            continue
        embed: Dict[str, Any] = {}
        if e.get("title"):
            embed["title"] = e.get("title")
        if e.get("url"):
            embed["url"] = e.get("url")
        desc = e.get("description") or ""
        fields = e.get("fields") if isinstance(e.get("fields"), list) else []
        if desc or fields:
            embed["description"] = desc or "\u200b"
            embed_fields = []
            for field in fields:
                if not isinstance(field, dict):
                    continue
                name = field.get("name") or "\u200b"
                value = field.get("value")
                if not value:
                    continue
                cleaned = {"name": name, "value": value}
                if field.get("inline") is not None:
                    cleaned["inline"] = field.get("inline")
                embed_fields.append(cleaned)
            if embed_fields:
                embed["fields"] = embed_fields
        if "image" in e and isinstance(e.get("image"), dict) and e["image"].get("url"):
            embed["image"] = {"url": e["image"]["url"]}
        if "thumbnail" in e and isinstance(e.get("thumbnail"), dict) and e["thumbnail"].get("url"):
            embed["thumbnail"] = {"url": e["thumbnail"]["url"]}
        if embed:
            out.append(embed)
    return out[:10]


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


def _chunk_text(text: str, limit: int = 2000) -> List[str]:
    if not text:
        return [""]
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        chunks.append(remaining[:limit])
        remaining = remaining[limit:]
    return chunks


def _is_image_attachment(att: Dict[str, Any]) -> bool:
    try:
        ct = str(att.get("content_type") or "").lower()
        if ct.startswith("image/"):
            return True
    except Exception:
        pass
    try:
        fn = str(att.get("filename") or "").lower()
        if fn.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
            return True
    except Exception:
        pass
    return False


def _append_image_attachments_as_embeds(
    embeds_out: List[Dict[str, Any]], attachments: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Render image attachments as embed images (better UX than appending CDN URLs)."""
    if not attachments:
        return embeds_out
    embeds_out = list(embeds_out or [])
    # Avoid duplicating images already present in embeds.
    existing_urls: Set[str] = set()
    for e in embeds_out:
        if not isinstance(e, dict):
            continue
        try:
            img = e.get("image") or {}
            if isinstance(img, dict) and img.get("url"):
                existing_urls.add(str(img.get("url")))
        except Exception:
            pass
        try:
            thumb = e.get("thumbnail") or {}
            if isinstance(thumb, dict) and thumb.get("url"):
                existing_urls.add(str(thumb.get("url")))
        except Exception:
            pass

    slots = max(0, 10 - len(embeds_out))
    if slots <= 0:
        return embeds_out

    added = 0
    for a in attachments:
        if added >= slots:
            break
        if not isinstance(a, dict):
            continue
        url = str(a.get("url") or a.get("proxy_url") or "").strip()
        if not url:
            continue
        if url in existing_urls:
            continue
        if not _is_image_attachment(a):
            continue
        embeds_out.append({"image": {"url": url}})
        existing_urls.add(url)
        added += 1
    return embeds_out


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

    async def _send_to_destination(self, *, dest_channel_id: int, content: str, embeds: List[Dict[str, Any]]) -> None:
        import discord

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

        chunks = _chunk_text(content, 2000)
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
                await channel.send(content=chunk, embeds=embed_objs[:10])
            else:
                await channel.send(content=chunk)
            try:
                self._last_send_ts[int(dest_channel_id)] = asyncio.get_event_loop().time()
            except Exception:
                pass

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
                text_to_check, affiliate_links = await augment_text_with_affiliate_redirects(text_to_check)
                # Merge and de-dupe while preserving order
                seen = set()
                merged: List[str] = []
                for u in (raw_links or []) + (dmflip_links or []) + (affiliate_links or []):
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

        if not dispatch_link_types:
            fallback = select_target_channel_id(
                text_to_check, attachments, self.keywords_list, source_channel_id=channel_id, trace=trace
            )
            if fallback:
                dispatch_link_types = [fallback]

        if not dispatch_link_types:
            trace["decision"] = {"action": "skip", "reason": "no_destination"}
            try:
                trace.setdefault("classifier", {})["dispatch_link_types"] = []
                write_trace_log(trace)
            except Exception:
                pass
            if cfg.VERBOSE:
                log_warn(f"No destination after classification (msg={message.id}) for source channel {channel_id}")
            return

        # Format output
        formatted_content = content
        replaced = False
        if cfg.ENABLE_RAW_LINK_UNWRAP and raw_links:
            try:
                formatted_content, replaced = replace_single_url_with_raw(formatted_content, raw_links)
            except Exception:
                replaced = False

        embeds_out = _format_embeds_for_forwarding(embeds)
        # Render image attachments as embeds for better Discord UX (no "image.png" link spam).
        try:
            embeds_out = _append_image_attachments_as_embeds(embeds_out, attachments)
        except Exception:
            embeds_out = embeds_out
        # For non-image attachments, append URLs (keeps access to files without reupload).
        try:
            non_image_urls = []
            for a in attachments:
                if not isinstance(a, dict):
                    continue
                if _is_image_attachment(a):
                    continue
                u = str(a.get("url") or "").strip()
                if u:
                    non_image_urls.append(u)
            if non_image_urls:
                formatted_content = (formatted_content + "\n\n" + "\n".join(non_image_urls[:10])).strip()
        except Exception:
            pass

        try:
            trace.setdefault("classifier", {})["dispatch_link_types"] = dispatch_link_types
            trace["stop_after_first"] = bool(stop_after_first)
            trace["raw_links_count"] = len(raw_links or [])
            trace["raw_link_replaced_in_content"] = bool(replaced)
        except Exception:
            pass

        forwarded = 0
        dest_traces: List[Dict[str, Any]] = []
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

        for dest_channel_id, tag in dispatch_link_types:
            dest_before = int(dest_channel_id or 0)
            dest_after = dest_before
            dest_trace: Dict[str, Any] = {"tag": str(tag or ""), "dest_before": dest_before}
            if dest_before <= 0:
                dest_trace["decision"] = {"action": "skip", "reason": "invalid_destination"}
                dest_traces.append(dest_trace)
                continue

            # Apply MIRRORWORLD routing maps (from legacy settings.env) so we don't forward into intermediate channels.
            try:
                if source_group == "online":
                    dest_after = int(getattr(cfg, "MIRRORWORLD_ROUTE_ONLINE", {}).get(dest_after, dest_after))
                elif source_group == "instore":
                    dest_after = int(getattr(cfg, "MIRRORWORLD_ROUTE_INSTORE", {}).get(dest_after, dest_after))
            except Exception:
                pass
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
                await self._send_to_destination(dest_channel_id=dest_channel_id, content=formatted_content, embeds=embeds_out)
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
                # Optional follow-up: send raw destination links (once per destination per message)
                if cfg.SEND_RAW_LINKS_FOLLOWUP and raw_links and not replaced:
                    try:
                        # Dedup follow-ups by the raw link signature (prevents spam across reposts).
                        raw_sig = hashlib.md5("|".join(raw_links).encode("utf-8", errors="ignore")).hexdigest()
                        follow_key = (dest_channel_id, f"rawlinks-{raw_sig}")
                        last_follow = self.sent_to_destinations.get(follow_key, 0.0)
                        # use a longer TTL for follow-ups so edits/retries don't spam
                        if not last_follow or (now - last_follow) > float(self.global_content_ttl_seconds or 300):
                            followup = build_raw_links_followup(raw_links, max_links=int(cfg.RAW_LINKS_FOLLOWUP_MAX or 5))
                            if followup and followup not in formatted_content:
                                await self._send_to_destination(dest_channel_id=dest_channel_id, content=followup, embeds=[])
                                self.sent_to_destinations[follow_key] = now
                    except Exception:
                        pass
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
            f"raw_unwrap={bool(cfg.ENABLE_RAW_LINK_UNWRAP)} raw_followup={bool(cfg.SEND_RAW_LINKS_FOLLOWUP)} ttl={cfg.RECENT_TTL_SECONDS}s"
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
            int(cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID or 0),
            int(cfg.SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID or 0),
            int(cfg.SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID or 0),
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

