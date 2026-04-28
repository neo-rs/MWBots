from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

import discord

from automatedParaphrase.gemini_paraphraser import minimal_rephrase_keep_urls  # type: ignore

SOURCE_CHANNEL_ID = 1438970053352751215
DEST_CHANNEL_ID = 1484473267031904287


_RE_URL = re.compile(r"(?i)\bhttps?://\S+")


def gemini_status(cfg: Dict[str, Any]) -> Dict[str, str]:
    key = str((cfg or {}).get("gemini_api_key") or "").strip()
    model = str((cfg or {}).get("gemini_model") or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
    try:
        temp = float((cfg or {}).get("gemini_temperature") or 0.65)
    except Exception:
        temp = 0.65
    return {
        "enabled": "yes" if bool(key) else "no",
        "api_key": "set" if bool(key) else "missing",
        "model": model,
        "temperature": f"{max(0.0, min(temp, 1.0)):.4f}",
    }


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
    return "\n".join([x for x in parts if str(x).strip()]).strip()


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
    return "\n".join([x for x in parts if str(x).strip()]).strip()


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
        return raw
    key = str((cfg or {}).get("gemini_api_key") or "").strip()
    if not key:
        return raw
    model = str((cfg or {}).get("gemini_model") or "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
    try:
        temp = float((cfg or {}).get("gemini_temperature") or 0.65)
    except Exception:
        temp = 0.65
    try:
        timeout_s = float((cfg or {}).get("gemini_timeout_s") or 12.0)
    except Exception:
        timeout_s = 12.0

    out = await minimal_rephrase_keep_urls(
        text=raw,
        gemini_api_key=key,
        model=model,
        temperature=max(0.0, min(temp, 1.0)),
        timeout_s=max(5.0, timeout_s),
        neutralize_mentions_fn=lambda s: (s or "").replace("@", "@\u200b"),
        usage_accumulator=None,
    )
    out = str(out or "").strip()
    return out or raw


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
        if int(getattr(message.channel, "id", 0) or 0) != int(SOURCE_CHANNEL_ID):
            return False
    except Exception:
        return False

    cfg = getattr(inst, "config", None) or {}
    bot = getattr(inst, "bot", None)
    if bot is None:
        return False

    ch = bot.get_channel(int(DEST_CHANNEL_ID))
    if ch is None:
        try:
            ch = await bot.fetch_channel(int(DEST_CHANNEL_ID))
        except Exception:
            ch = None
    if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.DMChannel)):
        return True

    src_block = simple_message_block_from_discord_message(message)
    desc = await rewrite_description(cfg, src_block, no_gemini=False)
    media = media_url_from_discord_message(message)
    embed = build_embed(desc, media_url=media)
    try:
        await ch.send(embeds=[embed], allowed_mentions=discord.AllowedMentions.none())
    except Exception:
        return True
    return True

