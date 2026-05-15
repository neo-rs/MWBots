"""DM notification embeds for MWPingBot (multi-image grid + text summary)."""

from __future__ import annotations

from typing import List, Sequence

import discord


_EMBED_DESC_MAX = 4096
_EMBED_TITLE_MAX = 256
_MAX_EMBEDS = 10


def extract_embed_image_urls(message: discord.Message) -> List[str]:
    """Collect unique image URLs from message embeds only (image + thumbnail)."""
    out: List[str] = []
    seen: set[str] = set()
    for emb in getattr(message, "embeds", None) or []:
        for attr in ("image", "thumbnail"):
            part = getattr(emb, attr, None)
            url = getattr(part, "url", None) if part else None
            if not url:
                continue
            s = str(url).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
            if len(out) >= _MAX_EMBEDS:
                return out
    return out


def _truncate(text: str, max_len: int) -> str:
    s = (text or "").strip()
    if len(s) <= max_len:
        return s
    if max_len <= 3:
        return s[:max_len]
    return s[: max_len - 3] + "..."


def build_notify_description(message: discord.Message, channel: discord.abc.GuildChannel) -> str:
    """Full content when short; truncate body to fit embed limits. Channel + jump link footer."""
    jump = message.jump_url
    ch_name = getattr(channel, "name", None) or str(getattr(channel, "id", ""))
    footer = (
        f"\n\n**Channel:** <#{channel.id}> (`{ch_name}`)\n"
        f"**Message:** [Jump to message]({jump})"
    )
    max_body = max(80, _EMBED_DESC_MAX - len(footer))
    content = (getattr(message, "content", None) or "").strip()
    if content:
        body = _truncate(content, max_body)
    else:
        body = "_(no text content)_"
    return (body + footer)[:_EMBED_DESC_MAX]


def build_ping_notify_embeds(
    *,
    channel_name: str,
    description: str,
    jump_url: str,
    image_urls: Sequence[str],
) -> List[discord.Embed]:
    """
    Build embed(s) for DM: one text embed, or multi-embed grid when 2+ embed images exist.
    All embeds share jump_url so Discord groups multi-image cards.
    """
    title = _truncate(f"Ping: #{channel_name}", _EMBED_TITLE_MAX)
    desc = description[:_EMBED_DESC_MAX]
    images = [u for u in image_urls if u][: _MAX_EMBEDS]

    if not images:
        emb = discord.Embed(title=title, description=desc, url=jump_url)
        return [emb]

    if len(images) == 1:
        emb = discord.Embed(title=title, description=desc, url=jump_url)
        emb.set_image(url=images[0])
        return [emb]

    embeds: List[discord.Embed] = []
    first = discord.Embed(title=title, description=desc, url=jump_url)
    first.set_image(url=images[0])
    embeds.append(first)
    for url in images[1:]:
        extra = discord.Embed(url=jump_url)
        extra.set_image(url=url)
        embeds.append(extra)
    return embeds


async def send_ping_dm_notifications(
    bot: discord.Client,
    message: discord.Message,
    user_ids: Sequence[int],
    *,
    log_info,
    log_warn,
    log_error,
    write_log,
) -> None:
    """Send DM alert to each configured user after a successful @everyone ping."""
    ids = [int(u) for u in user_ids if int(u) > 0]
    if not ids:
        return

    channel = message.channel
    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        log_warn("DM notify skipped: channel is not a text channel or thread")
        return

    ch_name = getattr(channel, "name", None) or str(channel.id)
    jump_url = message.jump_url
    description = build_notify_description(message, channel)
    image_urls = extract_embed_image_urls(message)
    embeds = build_ping_notify_embeds(
        channel_name=ch_name,
        description=description,
        jump_url=jump_url,
        image_urls=image_urls,
    )

    for uid in ids:
        try:
            user = bot.get_user(uid)
            if user is None:
                user = await bot.fetch_user(uid)
            await user.send(embeds=embeds)
            log_info(f"DM ping notify sent to <@{uid}> for <#{channel.id}>")
            try:
                write_log(
                    {
                        "event": "dm_notify_sent",
                        "user_id": uid,
                        "channel_id": int(channel.id),
                        "message_id": getattr(message, "id", None),
                        "embed_image_count": len(image_urls),
                        "bot_type": "pingbot",
                    }
                )
            except Exception:
                pass
        except discord.Forbidden:
            log_warn(f"DM ping notify failed for <@{uid}>: DMs disabled or bot blocked")
        except discord.HTTPException as e:
            log_error(f"DM ping notify HTTP error for user {uid}", error=e)
        except Exception as e:
            log_error(f"DM ping notify failed for user {uid}", error=e)
