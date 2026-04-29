"""
Channel route audit (local dry-run)

Replays MWDataManagerBot classification against existing messages in a Discord channel
(scan = read history there) using a *replay* source id for `source_channel_id`
(default: same id as the scan channel when it appears in `source_channel_ids_online`,
`source_channel_ids_instore`, or `source_channel_ids_clearance`, else the first online source in settings).
Highlights use `smartfilter_destinations.CONVERSATIONAL_DEALS`
from settings (`SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID`), not necessarily the scan channel.

Requires: discord.py, valid config/tokens.env (DATAMANAGER_BOT), bot must be able to read
the audit channel (Mirror World server).

Explainable output follows the repo explainable-logging section layout (ELI5 + routes + trace).

Optional: --mirror-category-id posts a non-destructive *preview copy* of each audited message into
text channels named `{prefix}{tag}` under that category (see _AUDIT_MIRROR_PREFIX). This does not
call live_forwarder (no pending-cache pairing simulation); per-message routes include
MAJOR_CLEARANCE for Tempo/non-HD-exclusive embeds, HD_TOTAL_INVENTORY for configured HD definitive-only source,
and matching follow-up shapes via classifier only.

By default, when mirroring is enabled, existing `{prefix}*` text channels in that category are deleted
first (staging reset). Pass --no-mirror-delete-staging to keep them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Prefix for staging text channels under --mirror-category-id (lowercase slug per route tag).
_AUDIT_MIRROR_PREFIX = "audit-"

_BOT_DIR = Path(__file__).resolve().parent
if str(_BOT_DIR) not in sys.path:
    sys.path.insert(0, str(_BOT_DIR))

from config import load_settings_and_tokens
from classifier import determine_source_group, detect_all_link_types, order_link_types, select_target_channel_id
from global_triggers import detect_global_triggers
from keywords import load_keywords
import settings_store as cfg
from utils import (
    append_image_attachments_as_embeds,
    collect_embed_strings,
    extract_urls_from_text,
    format_embeds_for_forwarding,
    generate_content_signature,
    is_image_attachment,
)


def _safe_console(s: str) -> str:
    """Avoid UnicodeEncodeError on Windows cp1252 when printing Discord names / embed text."""
    enc = getattr(sys.stdout, "encoding", None) or "cp1252"
    try:
        return (s or "").encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return (s or "").encode("ascii", errors="replace").decode("ascii")


def _line(*parts: object) -> None:
    print(_safe_console(" ".join(str(p) for p in parts)))


def _audit_same_channel_note(*, audit_channel_id: int, dest_channel_id: int) -> str:
    """Clarify when the simulated destination is the same channel we read the message from."""
    try:
        if int(audit_channel_id) > 0 and int(dest_channel_id) == int(audit_channel_id):
            return " — same channel as audit read (replay only; no send)"
    except Exception:
        pass
    return ""


def _banner(title: str) -> None:
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def _content_is_only_urls(content: str) -> bool:
    """Match `live_forwarder._content_is_only_urls` for outbound preview parity."""
    body = (content or "").strip()
    if not body:
        return True
    urls = extract_urls_from_text(body)
    if not urls:
        return False
    cleaned = body
    try:
        for u in sorted(set(urls), key=len, reverse=True):
            if u:
                cleaned = cleaned.replace(u, " ")
    except Exception:
        pass
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned == ""


def _build_outbound_forward_preview(
    raw_content: str,
    embeds: List[Dict[str, Any]],
    attachments: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Mirror `live_forwarder.handle_message` outbound shaping: visible content only (not classifier blob),
    formatted embeds, optional attachment URL append when not forwarding as files, URL-only strip when embeds.
    """
    formatted_content = str(raw_content or "")
    embeds_out: List[Dict[str, Any]] = []
    try:
        embeds_out = format_embeds_for_forwarding(embeds or [])
    except Exception:
        embeds_out = list(embeds or [])

    strip_applied = False
    use_files = bool(getattr(cfg, "FORWARD_ATTACHMENTS_AS_FILES", True))
    non_image_urls: List[str] = []

    if not use_files:
        try:
            embeds_out = append_image_attachments_as_embeds(embeds_out, attachments or [], max_embeds=10)
        except Exception:
            pass
        try:
            for a in attachments or []:
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

    if bool(getattr(cfg, "STRIP_URL_ONLY_CONTENT_WHEN_EMBEDS", True)):
        try:
            if _content_is_only_urls(formatted_content) and bool(embeds_out):
                formatted_content = ""
                strip_applied = True
        except Exception:
            pass

    embed_lines: List[str] = []
    for i, e in enumerate((embeds_out or [])[:6]):
        ed = e if isinstance(e, dict) else {}
        title = str(ed.get("title") or "")[:140]
        desc = str(ed.get("description") or "")[:200].replace("\n", " ")
        url_f = str(ed.get("url") or "")
        embed_lines.append(f"  [{i}] title={title!r} url={url_f!r} desc_snip={desc!r}")

    return {
        "content": formatted_content,
        "embed_count": len(embeds_out or []),
        "strip_url_only_content": strip_applied,
        "forward_attachments_as_files": use_files,
        "attachment_urls_inlined": list(non_image_urls[:10]),
        "embed_summary_lines": embed_lines,
    }


def _print_route_message_preview(*, raw_content: str, embeds: List[Dict[str, Any]], attachments: List[Dict[str, Any]]) -> None:
    prev = _build_outbound_forward_preview(raw_content, embeds, attachments)
    _line("")
    _line("   ROUTE MESSAGE (outbound preview — same payload live_forwarder sends to each destination)")
    _line("   (Routing picks channels; this is the message body + formatted embeds.)")
    _line(f"   forward_attachments_as_files={prev.get('forward_attachments_as_files')}  "
          f"strip_url_only_content={prev.get('strip_url_only_content')}")
    att_inl = prev.get("attachment_urls_inlined") or []
    if att_inl:
        _line(f"   non-image attachment URLs inlined in content: {len(att_inl)}")
    cprev = str(prev.get("content") or "")
    _line("   Content:")
    if cprev.strip():
        snap = cprev[:900] + ("..." if len(cprev) > 900 else "")
        for ln in snap.splitlines()[:25]:
            _line(f"      {ln}")
    else:
        _line("      (empty — e.g. URL-only body stripped when embeds carry the link)")
    ec = int(prev.get("embed_count") or 0)
    _line(f"   Formatted embeds: {ec}")
    for el in (prev.get("embed_summary_lines") or [])[:8]:
        _line(el)
    if ec > 6:
        _line(f"   ... ({ec - 6} more embeds not shown)")


def _build_dest_labels(settings: Dict[str, Any]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for key, val in (settings.get("smartfilter_destinations") or {}).items():
        try:
            cid = int(val)
            if cid > 0:
                out[cid] = f"smartfilter.{key}"
        except Exception:
            continue
    for key, val in (settings.get("global_trigger_destinations") or {}).items():
        try:
            cid = int(val)
            if cid > 0:
                out.setdefault(cid, f"global.{key}")
        except Exception:
            continue
    try:
        fb = int(settings.get("fallback_channel_id") or 0)
        if fb > 0:
            out.setdefault(fb, "fallback_channel_id")
    except Exception:
        pass
    try:
        hd_dest = int(settings.get("hd_total_inventory_destination_channel_id") or 0)
        if hd_dest > 0:
            out.setdefault(hd_dest, "hd_total_inventory_destination")
    except Exception:
        pass
    return out


def _message_to_classifier_inputs(message: Any) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    content = str(getattr(message, "content", "") or "")
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
    embed_texts = collect_embed_strings(embeds)
    text_to_check = (content + " " + " ".join(embed_texts)).strip()
    return text_to_check, embeds, attachments


def _eli5_amz_deals(
    *,
    amz_deals_dest_id: int,
    pairs: List[Tuple[int, str]],
    ordered: List[Tuple[int, str]],
    fallback: Optional[Tuple[int, str]],
    matches: Dict[str, Any],
) -> str:
    tags = [t for _, t in pairs]
    if "CONVERSATIONAL_DEALS" in tags:
        if ordered and ordered[0][1] == "CONVERSATIONAL_DEALS":
            return "CONVERSATIONAL_DEALS is in the route list and sorts first among returned buckets (unless PRICE_ERROR forces stop-after-first)."
        return "CONVERSATIONAL_DEALS is still among routed buckets, but another tag may send first depending on dispatch order / collapse."
    skip = matches.get("amz_deals_conversational_skip")
    if skip:
        return f"CONVERSATIONAL_DEALS conversational gate did not apply: skip reason `{skip}`."
    if not pairs and fallback and int(fallback[0]) == amz_deals_dest_id and str(fallback[1]) == "CONVERSATIONAL_DEALS":
        return "Single-target fallback still selects CONVERSATIONAL_DEALS (multi-route list was empty)."
    if not pairs and not fallback:
        return "No route matched for this replay context (same as forwarder having nothing to send)."
    return "CONVERSATIONAL_DEALS is not in the multi-route list; another classifier bucket matched instead."


def _mirror_slug_for_tag(tag: str) -> str:
    t = (tag or "unknown").strip().lower().replace("_", "-")
    out = f"{_AUDIT_MIRROR_PREFIX}{t}"
    if len(out) > 100:
        out = out[:100]
    return out


def _mirror_send_targets(
    *,
    ordered: List[Tuple[int, str]],
    fb: Optional[Tuple[int, str]],
    global_hits: List[Tuple[int, str]],
    include_globals: bool,
) -> List[Tuple[int, str, str]]:
    """
    (live_destination_channel_id, tag, kind) with kind ordered|fallback|global.
    Deduplicates by (cid, tag) so the same bucket is not posted twice for one message.
    """
    out: List[Tuple[int, str, str]] = []
    seen: set[Tuple[int, str]] = set()
    for cid, tag in ordered:
        key = (int(cid), str(tag))
        if key in seen:
            continue
        seen.add(key)
        out.append((int(cid), str(tag), "ordered"))
    if not ordered and fb:
        key = (int(fb[0]), str(fb[1]))
        if key not in seen:
            out.append((int(fb[0]), str(fb[1]), "fallback"))
    if include_globals:
        for cid, tag in global_hits:
            key = (int(cid), str(tag))
            if key in seen:
                continue
            seen.add(key)
            out.append((int(cid), str(tag), "global"))
    return out


def _parse_discord_message_link(link: str) -> Tuple[int, int, int]:
    """
    Parse a Discord message link into (guild_id, channel_id, message_id).
    Accepts discord.com, discordapp.com, ptb.discord.com, canary.discord.com.
    Returns (0,0,0) on failure.
    """
    raw = str(link or "").strip()
    if not raw:
        return 0, 0, 0
    m = re.search(
        r"https?://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)\b",
        raw,
        re.IGNORECASE,
    )
    if not m:
        return 0, 0, 0
    try:
        return int(m.group(1)), int(m.group(2)), int(m.group(3))
    except Exception:
        return 0, 0, 0


async def _ensure_audit_mirror_channel(
    *,
    discord_mod: Any,
    guild: Any,
    category: Any,
    tag: str,
    slug: str,
    cache: Dict[str, Any],
    create_missing: bool,
) -> Optional[Any]:
    if slug in cache:
        return cache[slug]
    for c in getattr(category, "channels", []) or []:
        try:
            if getattr(c, "name", None) == slug and c.type == discord_mod.ChannelType.text:
                cache[slug] = c
                return c
        except Exception:
            continue
    if not create_missing:
        return None
    ch = await guild.create_text_channel(
        slug,
        category=category,
        topic=f"Route audit mirror | tag={tag}",
        reason="MWDataManagerBot channel_route_audit mirror bucket",
    )
    cache[slug] = ch
    return ch


async def _post_route_mirror_copy(
    *,
    target_ch: Any,
    message: Any,
    outbound_preview: Dict[str, Any],
    tag: str,
    dest_cid: int,
    dest_label: str,
    kind: str,
) -> None:
    jump = str(getattr(message, "jump_url", "") or "")
    header = (
        f"**Route audit mirror** ({kind})\n"
        f"Tag: `{tag}` | Live dest: <#{dest_cid}> ({dest_label})\n"
        f"Original: {jump}\n"
        "---\n"
        "**Outbound preview** (same shaping as `live_forwarder` — not the classifier blob)\n"
    )
    att_lines: List[str] = []
    for a in getattr(message, "attachments", []) or []:
        u = getattr(a, "url", None)
        if u:
            att_lines.append(str(u))
    att_block = ""
    if att_lines:
        att_block = "\nAttachments:\n" + "\n".join(att_lines[:8])
    body_txt = str(outbound_preview.get("content") or "")
    emb_n = int(outbound_preview.get("embed_count") or 0)
    emb_lines = "\n".join(outbound_preview.get("embed_summary_lines") or [])
    meta = (
        f"forward_attachments_as_files={outbound_preview.get('forward_attachments_as_files')} "
        f"strip_url_only_content={outbound_preview.get('strip_url_only_content')}\n"
        f"Formatted embeds: {emb_n}\n"
    )
    mid = body_txt
    if emb_lines:
        mid = mid + "\n" + emb_lines
    room = max(0, 2000 - len(header) - len(meta) - len(att_block) - 40)
    mid = mid[:room]
    payload = header + meta + mid + att_block
    if len(payload) > 2000:
        payload = payload[:2000]
    await target_ch.send(payload)


async def _reset_staging_mirror_channels(
    *,
    discord_mod: Any,
    category: Any,
    delay_s: float,
) -> int:
    """
    Delete text channels directly under `category` whose names start with _AUDIT_MIRROR_PREFIX.
    Returns count deleted. Ignores non-text channels and any channel not matching the prefix.
    """
    deleted = 0
    kids = list(getattr(category, "channels", []) or [])
    kids.sort(key=lambda c: str(getattr(c, "name", "") or ""))
    for c in kids:
        try:
            if getattr(c, "type", None) != discord_mod.ChannelType.text:
                continue
            name = str(getattr(c, "name", "") or "")
            if not name.startswith(_AUDIT_MIRROR_PREFIX):
                continue
            await c.delete(reason="MWDataManagerBot channel_route_audit staging reset")
            deleted += 1
        except Exception as e:
            _line(f"MIRROR reset: skip/delete failed id={getattr(c, 'id', 0)} name={getattr(c, 'name', '?')}: {e}")
        if delay_s > 0:
            await asyncio.sleep(delay_s)
    return deleted


def main() -> int:
    p = argparse.ArgumentParser(description="Audit historical Discord messages against MWDataManagerBot routing.")
    p.add_argument(
        "--message-link",
        type=str,
        default="",
        help=(
            "Optional: audit exactly ONE message by Discord link (discord.com/ptb/canary). "
            "When provided, --channel-id/--limit are ignored and the channel_id is taken from the link."
        ),
    )
    p.add_argument(
        "--message-id",
        type=int,
        default=0,
        help=(
            "Optional: audit exactly ONE message id from the given --channel-id. "
            "If set (>0), fetches that message only (ignores --limit history scan)."
        ),
    )
    p.add_argument(
        "--channel-id",
        type=int,
        default=1438970053352751215,
        help="Channel to read history from (default: CONVERSATIONAL_DEALS).",
    )
    p.add_argument("--limit", type=int, default=30, help="Max messages to scan (newest first).")
    p.add_argument(
        "--source-channel-id",
        type=int,
        default=None,
        help=(
            "SOURCE channel id for replay (classifier `source_channel_id`). Omit or 0: if --channel-id is in "
            "source_channel_ids_online, source_channel_ids_instore, or source_channel_ids_clearance, use that same id; "
            "otherwise use the first online source in settings.json."
        ),
    )
    p.add_argument("--json-out", type=str, default="", help="Optional path to write full JSON results.")
    p.add_argument(
        "--mirror-category-id",
        type=int,
        default=0,
        help=(
            "Optional Discord category id (same guild as the audit channel). When set, posts preview copies "
            f"into text channels named `{_AUDIT_MIRROR_PREFIX}<tag>` under that category (staging only; "
            "not the live smartfilter destination channels). "
            f"Default: delete existing `{_AUDIT_MIRROR_PREFIX}*` text channels in that category first (see "
            "--no-mirror-delete-staging)."
        ),
    )
    p.add_argument(
        "--no-mirror-delete-staging",
        action="store_true",
        help=(
            f"With --mirror-category-id: do not delete existing `{_AUDIT_MIRROR_PREFIX}*` staging channels "
            "before this run."
        ),
    )
    p.add_argument(
        "--mirror-delete-delay-seconds",
        type=float,
        default=0.75,
        help="Delay between staging channel deletes (0 disables; reduces Discord rate-limit risk).",
    )
    p.add_argument(
        "--no-mirror-create-channels",
        action="store_true",
        help=f"If set, do not create missing `{_AUDIT_MIRROR_PREFIX}<tag>` channels; only post where they already exist.",
    )
    p.add_argument(
        "--mirror-post-delay-seconds",
        type=float,
        default=0.35,
        help="Delay between mirror Discord sends to reduce rate limits (0 disables).",
    )
    p.add_argument(
        "--mirror-skip-globals",
        action="store_true",
        help="If set, mirror only classifier ordered/fallback routes (skip global_trigger destinations).",
    )
    args = p.parse_args()

    settings, tokens = load_settings_and_tokens(_BOT_DIR / "config")
    cfg.init(settings)
    dest_labels = _build_dest_labels(settings)

    try:
        import discord
    except ImportError:
        print("ERROR: discord.py is required. Install from MWDataManagerBot/requirements.txt")
        return 2

    token = (tokens.get("DATAMANAGER_BOT") or "").strip()
    if not token:
        print("ERROR: DATAMANAGER_BOT missing in config/tokens.env")
        return 2

    # Single-message mode: message-link overrides channel-id; message-id may be provided directly
    msg_link = str(getattr(args, "message_link", "") or "").strip()
    msg_id = int(getattr(args, "message_id", 0) or 0)

    audit_channel_id = int(args.channel_id)
    if msg_link:
        link_gid, link_cid, link_mid = _parse_discord_message_link(msg_link)
        if link_cid > 0:
            audit_channel_id = int(link_cid)
        if link_mid > 0:
            msg_id = int(link_mid)
    focus_amz_deals_cid = int(getattr(cfg, "SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID", 0) or 0)

    replay_source = 0
    if args.source_channel_id is not None:
        try:
            replay_source = int(args.source_channel_id)
        except Exception:
            replay_source = 0
    if replay_source <= 0:
        if audit_channel_id in cfg.SMART_SOURCE_CHANNELS_ONLINE:
            replay_source = int(audit_channel_id)
        elif audit_channel_id in cfg.SMART_SOURCE_CHANNELS_INSTORE:
            replay_source = int(audit_channel_id)
        elif audit_channel_id in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()):
            replay_source = int(audit_channel_id)
        else:
            online = sorted(cfg.SMART_SOURCE_CHANNELS_ONLINE)
            if not online:
                print("ERROR: No source_channel_ids_online in settings.json; pass --source-channel-id")
                return 2
            replay_source = int(online[0])

    if replay_source in cfg.SMART_SOURCE_CHANNELS_ONLINE:
        pass
    elif replay_source in cfg.SMART_SOURCE_CHANNELS_INSTORE:
        print(
            "INFO: replay source is instore; conversational CONVERSATIONAL_DEALS uses online-only source/channel gates "
            "in the classifier (expected difference vs auditing an online source)."
        )
    elif replay_source in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()):
        print(
            "INFO: replay source is clearance; global_trigger paths are skipped in production for clearance "
            "(same as detect_global_triggers early return). HD_TOTAL_INVENTORY / MAJOR_CLEARANCE use classifier only."
        )
    elif (
        replay_source not in cfg.SMART_SOURCE_CHANNELS_ONLINE
        and replay_source not in cfg.SMART_SOURCE_CHANNELS_INSTORE
        and replay_source not in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set())
        and replay_source not in getattr(cfg, "SMART_SOURCE_CHANNELS_MISC", set())
    ):
        print(
            f"WARNING: replay source <#{replay_source}> is not in configured source_channel_ids_* lists; "
            "pass --source-channel-id to match production. CONVERSATIONAL_DEALS / source gates may differ."
        )

    keywords_list = load_keywords()

    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.message_content = True

    client = discord.Client(intents=intents)
    mirror_category_id = int(getattr(args, "mirror_category_id", 0) or 0)
    mirror_create = not bool(getattr(args, "no_mirror_create_channels", False))
    try:
        mirror_delay = float(getattr(args, "mirror_post_delay_seconds", 0.35) or 0.0)
    except Exception:
        mirror_delay = 0.35
    if mirror_delay < 0:
        mirror_delay = 0.0
    mirror_skip_globals = bool(getattr(args, "mirror_skip_globals", False))
    mirror_delete_staging_first = bool(mirror_category_id > 0) and (not bool(getattr(args, "no_mirror_delete_staging", False)))
    try:
        mirror_delete_delay = float(getattr(args, "mirror_delete_delay_seconds", 0.75) or 0.0)
    except Exception:
        mirror_delete_delay = 0.75
    if mirror_delete_delay < 0:
        mirror_delete_delay = 0.0

    state: Dict[str, Any] = {
        "rows": [],
        "summary": {},
        "audit_channel_id": audit_channel_id,
        "focus_amz_deals_cid": focus_amz_deals_cid,
        "replay_source": replay_source,
        "limit": int(args.limit),
        "json_out": str(args.json_out or "").strip(),
        "dest_labels": dest_labels,
        "settings": settings,
        "mirror_category_id": mirror_category_id,
        "mirror_create_channels": mirror_create,
        "mirror_post_delay_seconds": mirror_delay,
        "mirror_skip_globals": mirror_skip_globals,
        "mirror_delete_staging_first": mirror_delete_staging_first,
        "mirror_delete_delay_seconds": mirror_delete_delay,
    }

    @client.event
    async def on_ready() -> None:
        try:
            ch = client.get_channel(audit_channel_id)
            if ch is None:
                ch = await client.fetch_channel(audit_channel_id)
        except Exception as e:
            print(f"ERROR: cannot access channel {audit_channel_id}: {e}")
            await client.close()
            return

        mirror_cache: Dict[str, Any] = {}
        mirror_posts = 0
        mirror_skips = 0
        cat: Any = None
        guild: Any = None
        if mirror_category_id > 0:
            try:
                cat = client.get_channel(mirror_category_id)
                if cat is None:
                    cat = await client.fetch_channel(mirror_category_id)
            except Exception as e:
                print(f"ERROR: cannot access mirror category {mirror_category_id}: {e}")
                await client.close()
                return
            if not isinstance(cat, discord.CategoryChannel):
                print(f"ERROR: --mirror-category-id {mirror_category_id} is not a category channel")
                await client.close()
                return
            guild = cat.guild
            if cfg.DESTINATION_GUILD_IDS and guild.id not in cfg.DESTINATION_GUILD_IDS:
                print(
                    f"WARNING: mirror category guild id={guild.id} is not listed in destination_guild_ids; "
                    "continuing anyway."
                )

        mirror_staging_deleted = 0
        if mirror_category_id > 0 and cat is not None and mirror_delete_staging_first:
            print()
            print("MIRROR RESET (before scan)")
            print(
                f"   Deleting text channels under category <#{mirror_category_id}> whose names start with "
                f"`{_AUDIT_MIRROR_PREFIX}` ..."
            )
            mirror_staging_deleted = await _reset_staging_mirror_channels(
                discord_mod=discord,
                category=cat,
                delay_s=mirror_delete_delay,
            )
            _line(f"   Deleted: {mirror_staging_deleted} channel(s). delay_s={mirror_delete_delay}")
        elif mirror_category_id > 0 and cat is not None and not mirror_delete_staging_first:
            print()
            print("MIRROR RESET: skipped (--no-mirror-delete-staging)")

        _banner("MWDataManagerBot - CHANNEL ROUTE AUDIT")
        _line(f"Bot: {client.user} (id={client.user.id if client.user else 0})")
        _line(f"Audit channel (read history): <#{audit_channel_id}> name={getattr(ch, 'name', '?')}")
        replay_group = determine_source_group(int(replay_source))
        print(f"Replay as source ({replay_group}): <#{replay_source}>")
        if audit_channel_id == replay_source:
            if audit_channel_id in cfg.SMART_SOURCE_CHANNELS_ONLINE:
                print("   (Replay source = audit channel: listed in source_channel_ids_online.)")
            elif audit_channel_id in cfg.SMART_SOURCE_CHANNELS_INSTORE:
                print("   (Replay source = audit channel: listed in source_channel_ids_instore.)")
            elif audit_channel_id in getattr(cfg, "SMART_SOURCE_CHANNELS_CLEARANCE", set()):
                print("   (Replay source = audit channel: listed in source_channel_ids_clearance.)")
        elif audit_channel_id in cfg.SMART_SOURCE_CHANNELS_ONLINE or audit_channel_id in cfg.SMART_SOURCE_CHANNELS_INSTORE:
            print(
                "   (Note: audit channel is a configured smartfilter source but replay used a different id; "
                "pass --source-channel-id to match.)"
            )
        print(f"Message limit: {state['limit']}")
        print(f"Keywords loaded: {len(keywords_list)}")
        if focus_amz_deals_cid > 0:
            print(f"CONVERSATIONAL_DEALS destination (for highlights): <#{focus_amz_deals_cid}>")
        if mirror_category_id > 0 and cat is not None:
            _line(
                f"MIRROR: category #{cat.id} name={getattr(cat, 'name', '?')} guild={guild.id} "
                f"create_missing={mirror_create} delay_s={mirror_delay}"
            )
            print(f"   Mirror channel name pattern: `{_AUDIT_MIRROR_PREFIX}<tag>` (tag lowercased, _ -> -)")
        print()
        print("1) CONTEXT")
        print("   *Scan* = read messages from the audit channel above.")
        print("   *Replay source* = pretend each message arrived from that SOURCE channel id (classifier input).")
        print("   If you omit --source-channel-id and the audit channel is in source_channel_ids_online,")
        print("   source_channel_ids_instore, or source_channel_ids_clearance, replay defaults to the SAME id.")
        if mirror_category_id > 0 and cat is not None:
            print(
                f"   Mirror posts: ON (category <#{mirror_category_id}>). Preview copies go to "
                f"`{_AUDIT_MIRROR_PREFIX}<tag>` channels only (not live destination channels)."
            )
        else:
            print("   Read-only: no Discord posts (unless --mirror-category-id is set).")
        try:
            pe_dest = int(getattr(cfg, "SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID", 0) or 0)
        except Exception:
            pe_dest = 0
        if pe_dest > 0 and int(audit_channel_id) == pe_dest:
            print(
                f"   Note: audit channel is the configured PRICE_ERROR destination (<#{pe_dest}>). "
                "If routes show PRICE_ERROR → that channel, it only means classification matches this copy’s "
                "current home (not that the audit would post again)."
            )
        print()

        rows_out: List[Dict[str, Any]] = state["rows"]
        summary: Dict[str, int] = state["summary"]

        # Message iterator: either a single fetched message (by --message-id/--message-link),
        # or channel history.
        count = 0
        if msg_id > 0:
            try:
                message = await ch.fetch_message(int(msg_id))
            except Exception as e:
                print(
                    f"ERROR: cannot fetch message id={int(msg_id)} from channel <#{audit_channel_id}>: "
                    f"{type(e).__name__}: {e}"
                )
                await client.close()
                return
            messages_iter = [message]
        else:
            messages_iter = None

        if messages_iter is not None:
            for message in messages_iter:
                count += 1
                # fall through to common per-message logic below
                text_to_check, embeds, attachments = _message_to_classifier_inputs(message)
                raw_content = str(getattr(message, "content", "") or "")
                trace: Dict[str, Any] = {
                    "message_id": str(message.id),
                    "audit_channel_id": int(audit_channel_id),
                    "replay_source_channel_id": int(replay_source),
                }
                pairs = detect_all_link_types(
                    text_to_check,
                    attachments,
                    keywords_list=keywords_list,
                    embeds=embeds,
                    source_channel_id=int(replay_source),
                    trace=trace,
                    message_content=raw_content,
                )
                ordered, stop_after_first = order_link_types(list(pairs))
                fb = select_target_channel_id(
                    text_to_check,
                    attachments,
                    keywords_list=keywords_list,
                    source_channel_id=int(replay_source),
                    trace=trace,
                    message_content=raw_content,
                    embeds=embeds,
                )
                global_hits = detect_global_triggers(
                    text_to_check,
                    source_channel_id=int(replay_source),
                    link_tracking_cache={},
                    embeds=embeds,
                    attachments=attachments,
                )
                matches = (trace.get("classifier") or {}).get("matches") or {}
                sig = generate_content_signature(
                    str(getattr(message, "content", "") or ""),
                    embeds,
                    attachments,
                    for_cross_post_dedupe=True,
                )

                still_amz = bool(
                    focus_amz_deals_cid > 0
                    and any(int(cid) == focus_amz_deals_cid and tag == "CONVERSATIONAL_DEALS" for cid, tag in pairs)
                )
                primary_tag = ordered[0][1] if ordered else (fb[1] if fb else None)

                # Store row + update summary
                row: Dict[str, Any] = {
                    "message_id": str(message.id),
                    "jump_url": str(getattr(message, "jump_url", "") or ""),
                    "sig": sig,
                    "pairs": pairs,
                    "ordered": ordered,
                    "stop_after_first": bool(stop_after_first),
                    "fallback": fb,
                    "global_hits": global_hits,
                    "primary_tag": primary_tag,
                    "amz_deals_still_in_pairs": bool(still_amz),
                    "matches": matches,
                    "trace": trace,
                    "outbound_preview": _build_outbound_forward_preview(raw_content, embeds, attachments),
                }
                rows_out.append(row)
                try:
                    if primary_tag:
                        summary[str(primary_tag)] = int(summary.get(str(primary_tag), 0) or 0) + 1
                except Exception:
                    pass

                # Print explainable console output for the single message.
                _banner(f"MESSAGE {count}/{count}  (single-message audit)")
                _line(f"Message: {row.get('jump_url')}")
                _line(f"Author: {getattr(getattr(message,'author',None),'name',None) or getattr(getattr(message,'author',None),'display_name',None) or 'Unknown'}")
                _line(f"Channel: <#{int(audit_channel_id)}>  Replay source: <#{int(replay_source)}>")
                _line()
                _line("2) MESSAGE SNAPSHOT")
                _line((text_to_check or "")[:800])
                _line()
                _line("3) ELI5 SUMMARY")
                _line(_eli5_amz_deals(amz_deals_dest_id=focus_amz_deals_cid, pairs=pairs, ordered=ordered, fallback=fb, matches=matches))
                _line()
                _line("4) ROUTES (ordered)")
                for cid, tag in ordered:
                    label = dest_labels.get(int(cid), "")
                    _line(
                        f"- {tag} -> <#{int(cid)}> {label}"
                        f"{_audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(cid))}"
                    )
                if not ordered and fb:
                    _line(
                        f"- fallback: {fb[1]} -> <#{int(fb[0])}> {dest_labels.get(int(fb[0]), '')}"
                        f"{_audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(fb[0]))}"
                    )
                _print_route_message_preview(raw_content=raw_content, embeds=embeds, attachments=attachments)
                if global_hits:
                    _line()
                    _line("5) GLOBAL TRIGGERS")
                    for cid, tag in global_hits:
                        _line(
                            f"- {tag} -> <#{int(cid)}> {dest_labels.get(int(cid), '')}"
                            f"{_audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(cid))}"
                        )
                _line()
                _line("6) MATCHES (classifier.matches)")
                try:
                    _line(json.dumps(matches, indent=2, ensure_ascii=False)[:1800])
                except Exception:
                    _line(str(matches)[:1800])

                # Optional mirror preview for the one message
                if mirror_category_id > 0 and cat is not None:
                    out_prev = _build_outbound_forward_preview(raw_content, embeds, attachments)
                    ordered2, _ = order_link_types(list(pairs))
                    targets = _mirror_send_targets(
                        ordered=ordered2,
                        fb=fb,
                        global_hits=global_hits,
                        include_globals=(not mirror_skip_globals),
                    )
                    for dest_cid, tag, kind in targets:
                        slug = _mirror_slug_for_tag(tag)
                        target_ch = await _ensure_audit_mirror_channel(
                            discord_mod=discord,
                            guild=guild,
                            category=cat,
                            tag=tag,
                            slug=slug,
                            cache=mirror_cache,
                            create_missing=mirror_create,
                        )
                        if target_ch is None:
                            continue
                        await _post_route_mirror_copy(
                            target_ch=target_ch,
                            message=message,
                            outbound_preview=out_prev,
                            tag=tag,
                            dest_cid=int(dest_cid),
                            dest_label=str(dest_labels.get(int(dest_cid), "") or ""),
                            kind=kind,
                        )
                        mirror_posts += 1
                        if mirror_delay > 0:
                            await asyncio.sleep(mirror_delay)

                # End single-message run: flush JSON if requested and close.
            try:
                json_path = str(state.get("json_out") or "").strip()
                if json_path:
                    out_path = (_BOT_DIR / json_path).resolve()
                    out_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
                    _line(f"JSON written: {out_path}")
            except Exception:
                pass
            await client.close()
            return

        # History scan mode
        async for message in ch.history(limit=state["limit"], oldest_first=False):
            count += 1
            text_to_check, embeds, attachments = _message_to_classifier_inputs(message)
            raw_content = str(getattr(message, "content", "") or "")
            trace: Dict[str, Any] = {
                "message_id": str(message.id),
                "audit_channel_id": int(audit_channel_id),
                "replay_source_channel_id": int(replay_source),
            }
            pairs = detect_all_link_types(
                text_to_check,
                attachments,
                keywords_list=keywords_list,
                embeds=embeds,
                source_channel_id=int(replay_source),
                trace=trace,
                message_content=raw_content,
            )
            ordered, stop_after_first = order_link_types(list(pairs))
            fb = select_target_channel_id(
                text_to_check,
                attachments,
                keywords_list=keywords_list,
                source_channel_id=int(replay_source),
                trace=trace,
                message_content=raw_content,
                embeds=embeds,
            )
            global_hits = detect_global_triggers(
                text_to_check,
                source_channel_id=int(replay_source),
                link_tracking_cache={},
                embeds=embeds,
                attachments=attachments,
            )
            matches = (trace.get("classifier") or {}).get("matches") or {}
            sig = generate_content_signature(
                str(getattr(message, "content", "") or ""),
                embeds,
                attachments,
                for_cross_post_dedupe=True,
            )

            still_amz = bool(
                focus_amz_deals_cid > 0
                and any(int(cid) == focus_amz_deals_cid and tag == "CONVERSATIONAL_DEALS" for cid, tag in pairs)
            )
            primary_tag = ordered[0][1] if ordered else (fb[1] if fb else None)
            if primary_tag:
                summary[primary_tag] = summary.get(primary_tag, 0) + 1

            rows_out.append(
                {
                    "message_id": int(message.id),
                    "jump_url": getattr(message, "jump_url", None),
                    "sig_cross_post": sig,
                    "pairs": [
                        {"channel_id": int(c), "tag": t, "label": dest_labels.get(int(c), str(c))} for c, t in pairs
                    ],
                    "ordered": [{"channel_id": int(c), "tag": t} for c, t in ordered],
                    "stop_after_first": bool(stop_after_first),
                    "fallback_select": {"channel_id": int(fb[0]), "tag": fb[1]} if fb else None,
                    "global_triggers": [{"channel_id": int(c), "tag": t} for c, t in global_hits],
                    "amz_deals_still_in_pairs": bool(still_amz),
                    "classifier_matches": matches,
                    "outbound_preview": _build_outbound_forward_preview(raw_content, embeds, attachments),
                }
            )

            _banner(f"MESSAGE #{count}  id={message.id}")
            print("2) MESSAGE INFO")
            _line(f"   created: {getattr(message, 'created_at', '')}  author: {getattr(message.author, 'name', '?')}")
            _line(f"   jump: {getattr(message, 'jump_url', '')}")
            print()
            print("3) MESSAGE SNAPSHOT (truncated)")
            snap = (text_to_check or "")[:900]
            _line(snap + ("..." if len(text_to_check or "") > 900 else ""))
            print()
            print("4) ELI5 SUMMARY")
            _line(
                "   "
                + _eli5_amz_deals(
                    amz_deals_dest_id=focus_amz_deals_cid,
                    pairs=pairs,
                    ordered=ordered,
                    fallback=fb,
                    matches=matches,
                )
            )
            print()
            print("5) ROUTES (detect_all_link_types → order_link_types)")
            if not pairs:
                print("   (empty)")
            for cid, tag in ordered:
                lbl = dest_labels.get(int(cid), str(cid))
                mark = (
                    " <-- CONVERSATIONAL_DEALS bucket"
                    if focus_amz_deals_cid > 0 and int(cid) == focus_amz_deals_cid and tag == "CONVERSATIONAL_DEALS"
                    else ""
                )
                same = _audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(cid))
                print(f"   - {tag} → <#{cid}> ({lbl}){mark}{same}")
            print(f"   stop_after_first (PRICE_ERROR present): {stop_after_first}")
            _print_route_message_preview(raw_content=raw_content, embeds=embeds, attachments=attachments)
            print()
            print("6) SINGLE-TARGET FALLBACK (select_target_channel_id)")
            if fb:
                lbl = dest_labels.get(int(fb[0]), str(fb[0]))
                same = _audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(fb[0]))
                print(f"   {fb[1]} → <#{fb[0]}> ({lbl}){same}")
            else:
                print("   (none)")
            print()
            print("7) GLOBAL TRIGGERS (additive in live forwarder)")
            if not global_hits:
                print("   (none)")
            for cid, tag in global_hits:
                lbl = dest_labels.get(int(cid), str(cid))
                same = _audit_same_channel_note(audit_channel_id=int(audit_channel_id), dest_channel_id=int(cid))
                print(f"   - {tag} → <#{cid}> ({lbl}){same}")
            print()
            print("8) TECHNICAL TRACE (classifier.matches subset)")
            interesting = {
                k: v
                for k, v in matches.items()
                if str(k).startswith(
                    (
                        "amz_deals",
                        "conversational",
                        "amazon",
                        "instore",
                        "price_error",
                        "skip",
                        "flipflip",
                        "mention",
                        "hd_total",
                        "definitive_major",
                        "tempo_major",
                        "major_clearance",
                    )
                )
                or str(k).endswith("_template")
            }
            if interesting:
                for k, v in sorted(interesting.items()):
                    _line(f"   {k}: {v}")
            else:
                print("   (no conversational / amazon skip keys on this message)")
            print()
            print("9) DEDUPE SIGNATURE (cross-post mode)")
            print(f"   {sig}")

            posted_slugs: List[str] = []
            if mirror_category_id > 0 and cat is not None and guild is not None:
                out_prev = _build_outbound_forward_preview(raw_content, embeds, attachments)
                targets = _mirror_send_targets(
                    ordered=ordered,
                    fb=fb,
                    global_hits=global_hits,
                    include_globals=not mirror_skip_globals,
                )
                for dest_cid, tag, kind in targets:
                    slug = _mirror_slug_for_tag(tag)
                    try:
                        mch = await _ensure_audit_mirror_channel(
                            discord_mod=discord,
                            guild=guild,
                            category=cat,
                            tag=tag,
                            slug=slug,
                            cache=mirror_cache,
                            create_missing=mirror_create,
                        )
                        if mch is None:
                            mirror_skips += 1
                            _line(f"MIRROR skip (missing channel `{slug}`; use default create or remove --no-mirror-create-channels): {tag}")
                            continue
                        await _post_route_mirror_copy(
                            target_ch=mch,
                            message=message,
                            outbound_preview=out_prev,
                            tag=tag,
                            dest_cid=int(dest_cid),
                            dest_label=dest_labels.get(int(dest_cid), str(dest_cid)),
                            kind=kind,
                        )
                        posted_slugs.append(slug)
                        mirror_posts += 1
                        if mirror_delay > 0:
                            await asyncio.sleep(mirror_delay)
                    except Exception as e:
                        _line(f"MIRROR ERROR tag={tag} slug={slug}: {e}")
                try:
                    rows_out[-1]["mirror_posted_slugs"] = posted_slugs
                except Exception:
                    pass

        _banner("AUDIT SUMMARY (primary ordered tag per message)")
        for tag, n in sorted(summary.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"   {tag}: {n}")
        print(f"   total messages: {count}")
        if mirror_category_id > 0:
            print(f"   mirror posts: {mirror_posts}  mirror skips (missing channel): {mirror_skips}")
            print(f"   mirror staging channels deleted at start: {mirror_staging_deleted}")

        jo = state["json_out"]
        if jo:
            out_path = Path(jo)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "audit_channel_id": audit_channel_id,
                "replay_source_channel_id": replay_source,
                "amz_deals_destination_channel_id": focus_amz_deals_cid,
                "mirror_category_id": int(mirror_category_id or 0),
                "mirror_posts_total": int(mirror_posts),
                "mirror_skips_missing_channel": int(mirror_skips),
                "mirror_staging_channels_deleted": int(mirror_staging_deleted),
                "limit": state["limit"],
                "rows": rows_out,
                "summary_by_primary_ordered_tag": summary,
            }
            out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print()
            print(f"Wrote JSON: {out_path.resolve()}")

        await client.close()

    try:
        client.run(token)
    except Exception as e:
        print(f"ERROR: Discord client failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
