from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

import discord

_log = logging.getLogger("instorebotforwarder")

# Must work in both layouts:
# - Imported as a package: `Instorebotforwarder.conversational_deals_forwarder`
# - Executed/used from the bot folder on Oracle where `Instorebotforwarder/` is on sys.path
try:
    from Instorebotforwarder.automatedParaphrase.gemini_paraphraser import (  # type: ignore
        rewrite_deal_post_keep_urls,
        gemini_status,
    )
except Exception:
    from automatedParaphrase.gemini_paraphraser import (  # type: ignore
        rewrite_deal_post_keep_urls,
        gemini_status,
    )

# Re-export so existing callers `from conversational_deals_forwarder import gemini_status`
# keep working. Canonical owner is gemini_paraphraser.py.
__all__ = ["gemini_status", "rewrite_deal_post_keep_urls"]

SOURCE_CHANNEL_ID = 1438970053352751215
DEST_CHANNEL_ID = 1484473267031904287

# Mavely Leads — affiliated rewrite+embed pipeline with food vs personal routing (config-driven).
AFFILIATED_LEADS_SOURCE_CHANNEL_ID = 1435308472639160522

# Canonical 1:1 conversational mapping only. Affiliated leads use `affiliated_leads` in config.json.
CHANNEL_MAP: Dict[int, int] = {
    int(SOURCE_CHANNEL_ID): int(DEST_CHANNEL_ID),
}

# Canonical multi-source export for the caller hard-stop (so these channels never fall back to Amazon routing).
SOURCE_CHANNEL_IDS = sorted({int(SOURCE_CHANNEL_ID), int(AFFILIATED_LEADS_SOURCE_CHANNEL_ID)})


_RE_URL = re.compile(r"(?i)\bhttps?://\S+")
_RE_DISCORD_CDN_ATTACHMENT = re.compile(r"(?i)\bhttps?://cdn\.discordapp\.com/attachments/\S+")

# Source-bot artifacts that must never appear in the destination embed, regardless
# of whether Gemini ran. Centralized here so `simple_message_block_from_*` produce
# already-cleaned text that is safe to (a) feed to Gemini and (b) post verbatim on
# the Gemini-failure fallback path.
_RE_AD_HASHTAG_LINE = re.compile(r"(?im)^\s*#\s*ad\s*$")
_RE_AD_HASHTAG_INLINE = re.compile(r"(?i)(?<![A-Za-z0-9])#\s*ad\b")
# "From: Divine | By: Divine Helper v2" style attribution. Some source bots put
# this in the embed body rather than the embed footer (which we already skip).
_RE_ATTRIBUTION_FROM_BY = re.compile(r"(?im)^\s*from:\s+.+?\s*\|\s*by:\s+.+?\s*$")
# "Powered by ...", "Sent by ..." style attribution lines, when source bots use
# them in the description.
_RE_ATTRIBUTION_POWERED_BY = re.compile(r"(?im)^\s*powered by\s+.+?$")
_RE_ATTRIBUTION_SENT_BY = re.compile(r"(?im)^\s*sent by\s+.+?$")


def _strip_source_artifacts(text: str) -> str:
    """
    Remove source-bot footer/disclosure artifacts from a message block.

    Canonical responsibility: any string this function returns is what the
    destination embed (and Gemini rewriter) should see. Cleanup is conservative
    on purpose - we only strip well-known patterns, never anything that could
    plausibly be deal content.
    """
    s = str(text or "")
    if not s.strip():
        return ""
    s = _RE_ATTRIBUTION_FROM_BY.sub("", s)
    s = _RE_ATTRIBUTION_POWERED_BY.sub("", s)
    s = _RE_ATTRIBUTION_SENT_BY.sub("", s)
    s = _RE_AD_HASHTAG_LINE.sub("", s)
    s = _RE_AD_HASHTAG_INLINE.sub("", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


def _safe_channel_id(raw: Any) -> Optional[int]:
    try:
        v = int(raw)
    except Exception:
        return None
    return v if v > 0 else None


def load_affiliated_leads_routes(cfg: Mapping[str, Any]) -> Optional[Dict[str, int]]:
    """
    Config block `affiliated_leads`: source_channel_id, dest_personal, dest_food.
    Returns None when misconfigured so affiliated messages are not half-routed.
    """
    raw = (cfg or {}).get("affiliated_leads")
    if not isinstance(raw, dict):
        return None
    src = _safe_channel_id(raw.get("source_channel_id") or AFFILIATED_LEADS_SOURCE_CHANNEL_ID)
    personal = _safe_channel_id(raw.get("dest_personal"))
    food = _safe_channel_id(raw.get("dest_food"))
    if not src or not personal or not food:
        return None
    return {"source_channel_id": src, "dest_personal": personal, "dest_food": food}


def _affiliated_food_keywords(cfg: Mapping[str, Any]) -> List[str]:
    raw = (cfg or {}).get("affiliated_food_keywords")
    if isinstance(raw, list) and raw:
        return [str(k).strip() for k in raw if str(k).strip()]
    # Fallback: reuse Amazon grocery keyword list when affiliated list is absent.
    raw2 = (cfg or {}).get("amazon_grocery_keywords")
    if isinstance(raw2, list):
        return [str(k).strip() for k in raw2 if str(k).strip()]
    return []


def classify_affiliated_food(text: str, cfg: Mapping[str, Any]) -> Tuple[bool, str]:
    """
    True when the lead is primarily human-consumable food (edible / grocery).

    Default is personal (batteries, games, electronics, etc.) unless a configured
    keyword matches. This is intentionally separate from Amazon department scraping.
    """
    low = (text or "").lower()
    if not low.strip():
        return False, "empty_text:personal"
    for kw in _affiliated_food_keywords(cfg):
        k = kw.strip().lower()
        if not k:
            continue
        if " " in k or "&" in k:
            if k in low:
                return True, f"keyword:{k}"
        else:
            if re.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", low):
                return True, f"keyword:{k}"
    return False, "default:personal"


def resolve_destination_channel_id(
    src_id: int,
    cfg: Mapping[str, Any],
    *,
    message_text: str,
) -> Tuple[Optional[int], str]:
    """
    Pick destination channel for conversational or affiliated sources.
    """
    fixed = CHANNEL_MAP.get(int(src_id))
    if fixed:
        return int(fixed), "conversational:fixed"

    routes = load_affiliated_leads_routes(cfg)
    if routes and int(src_id) == int(routes["source_channel_id"]):
        is_food, why = classify_affiliated_food(message_text, cfg)
        if is_food:
            return int(routes["dest_food"]), f"affiliated:food:{why}"
        return int(routes["dest_personal"]), f"affiliated:personal:{why}"

    return None, ""


def first_url_in_text(text: str) -> str:
    m = _RE_URL.search(text or "")
    if not m:
        return ""
    return str(m.group(0) or "").strip().rstrip(").,>")


def simple_message_block_from_discord_message(message: discord.Message) -> str:
    parts: list[str] = []
    content = (message.content or "").strip()
    if content:
        parts.append(content)
    # Only use embed text when the message has no plain content.
    if not content:
        try:
            for e in (message.embeds or []):
                eparts: list[str] = []
                t = (getattr(e, "title", None) or "").strip()
                if t:
                    eparts.append(t)
                d = (getattr(e, "description", None) or "").strip()
                if d:
                    eparts.append(d)
                for f in (getattr(e, "fields", None) or []):
                    fn = (getattr(f, "name", None) or "").strip()
                    fv = (getattr(f, "value", None) or "").strip()
                    row = "\n".join([x for x in (fn, fv) if x]).strip()
                    if row:
                        eparts.append(row)
                if eparts:
                    parts.append("\n\n".join(eparts))
        except Exception:
            pass
    try:
        for att in (message.attachments or []):
            u = str(getattr(att, "url", "") or "").strip()
            if u:
                parts.append(u)
    except Exception:
        pass
    block = "\n".join([x for x in parts if str(x).strip()]).strip()
    # Do not include raw attachment CDN URLs in the text body (image is carried via embed image).
    block = _RE_DISCORD_CDN_ATTACHMENT.sub("", block)
    block = re.sub(r"\n{3,}", "\n\n", block).strip()
    block = _strip_source_artifacts(block)
    return block


def media_url_from_discord_message(message: discord.Message) -> str:
    try:
        for e in (message.embeds or []):
            img = getattr(e, "image", None)
            u = str(getattr(img, "url", "") or "").strip()
            if u:
                return u
            th = getattr(e, "thumbnail", None)
            u2 = str(getattr(th, "url", "") or "").strip()
            if u2:
                return u2
    except Exception:
        pass
    try:
        for a in (message.attachments or []):
            ct = str(getattr(a, "content_type", "") or "").lower()
            u = str(getattr(a, "url", "") or "").strip()
            fn = str(getattr(a, "filename", "") or "").lower()
            if not u:
                continue
            if ct.startswith("image/") or fn.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                return u
    except Exception:
        pass
    return ""


def simple_message_block_from_rest(rest_msg: Dict[str, Any]) -> str:
    parts: list[str] = []
    content = str(rest_msg.get("content") or "").strip()
    if content:
        parts.append(content)
    # Only use embed text when the message has no plain content.
    if not content:
        for e in (rest_msg.get("embeds") or []):
            if not isinstance(e, dict):
                continue
            t = str(e.get("title") or "").strip()
            d = str(e.get("description") or "").strip()
            if t:
                parts.append(t)
            if d:
                parts.append(d)
            fields = e.get("fields") or []
            if isinstance(fields, list):
                for f in fields:
                    if not isinstance(f, dict):
                        continue
                    fn = str(f.get("name") or "").strip()
                    fv = str(f.get("value") or "").strip()
                    row = "\n".join([x for x in (fn, fv) if x]).strip()
                    if row:
                        parts.append(row)
    for a in (rest_msg.get("attachments") or []):
        if not isinstance(a, dict):
            continue
        u = str(a.get("url") or "").strip()
        if u:
            parts.append(u)
    block = "\n".join([x for x in parts if str(x).strip()]).strip()
    block = _RE_DISCORD_CDN_ATTACHMENT.sub("", block)
    block = re.sub(r"\n{3,}", "\n\n", block).strip()
    block = _strip_source_artifacts(block)
    return block


def media_url_from_rest(rest_msg: Dict[str, Any]) -> str:
    for e in (rest_msg.get("embeds") or []):
        if not isinstance(e, dict):
            continue
        img = e.get("image") if isinstance(e.get("image"), dict) else {}
        u = str((img or {}).get("url") or "").strip()
        if u:
            return u
        th = e.get("thumbnail") if isinstance(e.get("thumbnail"), dict) else {}
        u2 = str((th or {}).get("url") or "").strip()
        if u2:
            return u2
    for a in (rest_msg.get("attachments") or []):
        if not isinstance(a, dict):
            continue
        ct = str(a.get("content_type") or "").lower()
        u = str(a.get("url") or "").strip()
        fn = str(a.get("filename") or "").lower()
        if not u:
            continue
        if ct.startswith("image/") or fn.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
            return u
    return ""


async def rewrite_description(cfg: Dict[str, Any], text: str, *, no_gemini: bool = False) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if no_gemini:
        return ""
    key = str((cfg or {}).get("gemini_api_key") or "").strip()
    if not key:
        return ""
    model = str((cfg or {}).get("gemini_model") or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
    try:
        temp = float((cfg or {}).get("gemini_temperature") or 0.65)
    except Exception:
        temp = 0.65
    try:
        timeout_s = float((cfg or {}).get("gemini_timeout_s") or 12.0)
    except Exception:
        timeout_s = 12.0

    out = await rewrite_deal_post_keep_urls(
        text=raw,
        gemini_api_key=key,
        model=model,
        temperature=max(0.0, min(temp, 1.0)),
        timeout_s=max(5.0, timeout_s),
        neutralize_mentions_fn=lambda s: (s or "").replace("@", "@\u200b"),
        usage_accumulator=None,
    )
    out = str(out or "").strip()
    if not out:
        return ""
    # Strict: treat "unchanged" as failure.
    if out.strip() == raw.strip():
        return ""
    return out


def build_embed(description: str, *, media_url: str = "") -> discord.Embed:
    desc = str(description or "").strip()
    if len(desc) > 3900:
        desc = desc[:3897] + "..."
    u = first_url_in_text(desc)
    embed = discord.Embed(description=desc, url=(u or None))
    mu = str(media_url or "").strip()
    if mu:
        try:
            embed.set_image(url=mu)
        except Exception:
            pass
    return embed


async def forward_runtime_message(inst: Any, message: discord.Message) -> bool:
    """
    Returns True if this standalone forwarder handled the message.
    `inst` is expected to be the InstorebotForwarder instance (for config + bot access).
    """
    try:
        src_id = int(getattr(message.channel, "id", 0) or 0)
    except Exception:
        return False

    cfg = getattr(inst, "config", None) or {}
    bot = getattr(inst, "bot", None)
    if bot is None:
        return False

    src_block = simple_message_block_from_discord_message(message)
    if not src_block:
        # Nothing usable to forward (no content, no embed text, no attachments).
        return True

    dest_id, route_reason = resolve_destination_channel_id(
        int(src_id), cfg, message_text=src_block
    )
    if not dest_id:
        return False

    ch = bot.get_channel(int(dest_id))
    if ch is None:
        try:
            ch = await bot.fetch_channel(int(dest_id))
        except Exception:
            ch = None
    if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
        return True

    try:
        _log.info(
            "conversational_deals_forwarder route: src=%s dest=%s reason=%s",
            int(src_id),
            int(dest_id),
            route_reason[:120],
        )
    except Exception:
        pass

    desc = await rewrite_description(cfg, src_block, no_gemini=False)
    desc = str(desc or "").strip()
    gemini_ok = bool(desc)
    if not gemini_ok:
        # Gemini failed/unchanged/throttled/disabled -> post the cleaned original.
        # This honors the "no silent decisions" rule on the conversational/affiliated
        # path: a lead must either be rewritten or forwarded as-is, never dropped.
        desc = src_block
        try:
            _log.info(
                "conversational_deals_forwarder fallback: posting cleaned original "
                "(gemini=unavailable) src_channel=%s dest_channel=%s route=%s",
                int(src_id),
                int(dest_id),
                route_reason[:80],
            )
        except Exception:
            pass

    media = media_url_from_discord_message(message)
    embed = build_embed(desc, media_url=media)
    try:
        await ch.send(embeds=[embed], allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        # Even if sending fails, we still claim the message so the Amazon path never runs for this channel.
        return True
    return True

