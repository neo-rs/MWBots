#!/usr/bin/env python3
"""
Standalone conversational-deals forwarder preview/sender for one mapping:
  source: 1438970053352751215
  dest:   1484473267031904287

This tool is intentionally **standalone** and does NOT import `instore_auto_mirror_bot.py`.

Usage:
  py -3 MWBots/Instorebotforwarder/audit_conversational_deals_forward.py --link "https://discord.com/channels/<guild>/<channel>/<message>"
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

import json
from urllib.request import Request, urlopen

from conversational_deals_forwarder import (  # type: ignore
    DEST_CHANNEL_ID,
    SOURCE_CHANNEL_ID,
    first_url_in_text,
    gemini_status,
    media_url_from_rest,
    rewrite_description,
    simple_message_block_from_rest,
)


_RE_DISCORD_MSG_LINK = re.compile(
    r"(?i)https?://(?:(?:ptb|canary)\.)?discord(?:app)?\.com/channels/(\d+|@me)/(\d+)/(\d+)"
)


def _configure_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def _print_safe(s: str = "") -> None:
    try:
        print(s)
    except UnicodeEncodeError:
        try:
            sys.stdout.buffer.write((s or "").encode("utf-8", errors="replace") + b"\n")
        except Exception:
            print((s or "").encode("ascii", errors="ignore").decode("ascii", errors="ignore"))


def _parse_discord_message_link(link: str) -> Tuple[Optional[int], int, int]:
    m = _RE_DISCORD_MSG_LINK.search((link or "").strip())
    if not m:
        raise SystemExit("Could not parse message link. Expected: https://discord.com/channels/<guild>/<channel>/<message>")
    guild_raw, ch_raw, msg_raw = m.group(1), m.group(2), m.group(3)
    guild_id = None if guild_raw == "@me" else int(guild_raw)
    return guild_id, int(ch_raw), int(msg_raw)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_config() -> Dict[str, Any]:
    cfg_path = _repo_root() / "MWBots" / "Instorebotforwarder" / "config.json"
    sec_path = _repo_root() / "MWBots" / "Instorebotforwarder" / "config.secrets.json"
    cfg: Dict[str, Any] = {}
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    try:
        sec = json.loads(sec_path.read_text(encoding="utf-8"))
        if isinstance(sec, dict):
            cfg.update(sec)
    except Exception:
        pass
    return cfg if isinstance(cfg, dict) else {}


def _discord_rest_get_json(url: str, *, headers: Dict[str, str], timeout_s: float = 30.0) -> Dict[str, Any]:
    req = Request(url, headers=headers, method="GET")
    with urlopen(req, timeout=max(5.0, float(timeout_s))) as resp:
        raw = resp.read()
    data = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
    return data if isinstance(data, dict) else {}


def _discord_rest_post_json(url: str, *, headers: Dict[str, str], payload: Dict[str, Any], timeout_s: float = 30.0) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, method="POST", headers={**headers, "Content-Type": "application/json"})
    with urlopen(req, timeout=max(5.0, float(timeout_s))) as resp:
        raw = resp.read()
    data = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
    return data if isinstance(data, dict) else {}


def main(argv: Optional[Sequence[str]] = None) -> int:
    _configure_stdout()
    ap = argparse.ArgumentParser()
    ap.add_argument("--link", default="", help="Discord message link")
    ap.add_argument("--no-gemini", action="store_true", help="Skip Gemini rewrite")
    ap.add_argument(
        "--gemini-check",
        action="store_true",
        help="Run Gemini API health check (can trigger rate limits).",
    )
    ap.add_argument("--send-preview", action="store_true", help="Offer to send preview to destination")
    ap.add_argument("--send-now", action="store_true", help="Send preview without prompt (dangerous)")
    args = ap.parse_args(argv)

    link = str(args.link or "").strip() or input("Discord message link: ").strip()
    guild_id, channel_id, message_id = _parse_discord_message_link(link)
    if int(channel_id) != int(SOURCE_CHANNEL_ID):
        raise SystemExit(f"This standalone tool only supports source_channel_id={SOURCE_CHANNEL_ID}.")

    cfg = _load_config()
    token = str((cfg or {}).get("bot_token") or "").strip()
    if not token:
        raise SystemExit("bot_token missing (check Instorebotforwarder/config.secrets.json).")

    url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}"
    headers = {"Authorization": f"Bot {token}", "User-Agent": "mirror-world-instoreaudit/1.0"}
    data = _discord_rest_get_json(url, headers=headers)

    _print_safe("=" * 78)
    _print_safe("CONVERSATIONAL DEALS STANDALONE (preview/sender)")
    _print_safe("=" * 78)

    gs = gemini_status(cfg)
    _print_safe("0) GEMINI STATUS")
    _print_safe(
        f"   enabled={gs.get('enabled')} api_key={gs.get('api_key')} model={gs.get('model')} temp={gs.get('temperature')}"
    )
    _print_safe("   Note: --gemini-check does a real API call (can rate limit).")
    _print_safe("")
    _print_safe("1) MESSAGE INFO")
    _print_safe(f"   link={link}")
    _print_safe(f"   guild_id={guild_id or ''}")
    _print_safe(f"   channel_id={channel_id}")
    _print_safe(f"   message_id={message_id}")
    _print_safe("")

    src_block = simple_message_block_from_rest(data)
    _print_safe("2) SOURCE SNAPSHOT (simple_message_block)")
    _print_safe(src_block)
    _print_safe("")

    desc = src_block
    if not args.no_gemini:
        desc = asyncio.run(rewrite_description(cfg, desc, no_gemini=False))
        desc = str(desc or "").strip() or src_block
    embed_url = first_url_in_text(desc)
    media_url = media_url_from_rest(data)

    _print_safe("3) OUTBOUND PREVIEW (embed.description)")
    _print_safe(desc)
    _print_safe("")
    _print_safe("4) EMBED PREVIEW META")
    _print_safe("   send_mode=embed")
    _print_safe(f"   embed_url={embed_url}")
    _print_safe(f"   image_url={media_url}")
    _print_safe("")

    if args.send_preview or args.send_now:
        dest_channel_id = int(DEST_CHANNEL_ID)

        do_send = bool(args.send_now)
        if not do_send:
            _print_safe("5) SEND PREVIEW (optional)")
            _print_safe("   Type SEND to confirm, or press Enter to skip.")
            resp = input("Confirm (SEND): ").strip()
            do_send = (resp.upper() == "SEND")
            _print_safe("")

        if do_send:
            embed_obj: Dict[str, Any] = {"description": desc}
            if embed_url:
                embed_obj["url"] = embed_url
            if media_url:
                embed_obj["image"] = {"url": media_url}
            post_payload: Dict[str, Any] = {"embeds": [embed_obj], "allowed_mentions": {"parse": []}}
            post_url = f"https://discord.com/api/v10/channels/{int(dest_channel_id)}/messages"
            posted = _discord_rest_post_json(post_url, headers=headers, payload=post_payload)
            mid = str(posted.get("id") or "").strip()
            _print_safe("5) SEND PREVIEW")
            _print_safe("   status=posted")
            _print_safe(f"   dest_channel_id={dest_channel_id}")
            _print_safe(f"   message_id={mid}")
            if guild_id:
                _print_safe(f"   message_link=https://discord.com/channels/{guild_id}/{dest_channel_id}/{mid}")
            _print_safe("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

