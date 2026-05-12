#!/usr/bin/env python3
"""
Standalone Gemini-rewrite forwarder preview/sender for the conversational-deals pipeline.

Supports every source mapped in `conversational_deals_forwarder.CHANNEL_MAP`, which is
currently:
  - 1438970053352751215 -> 1484473267031904287  (conversational_deals)
  - 1435308472639160522 -> 1484599902863622195  (affiliated_leads / Mavely Leads)

This tool is intentionally **standalone** and does NOT import `instore_auto_mirror_bot.py`.

Usage:
  py -3 MWBots/Instorebotforwarder/audit_conversational_deals_forward.py --link "https://discord.com/channels/<guild>/<channel>/<message>"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Tuple

from urllib.request import Request, urlopen

from conversational_deals_forwarder import (  # type: ignore
    CHANNEL_MAP,
    first_url_in_text,
    media_url_from_rest,
    rewrite_description,
    simple_message_block_from_rest,
)
from automatedParaphrase.gemini_paraphraser import (  # type: ignore
    gemini_status,
    probe_gemini_api,
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


def repo_root() -> Path:
    """
    Resolve the mirror-world repo root from this file's location.

    Two real layouts are in use:
      - Local Windows checkout:  `<repo>/MWBots/Instorebotforwarder/<this>.py`
        -> parents[2] is the repo root.
      - Oracle live tree (flat): `<repo>/Instorebotforwarder/<this>.py`
        -> parents[1] is the repo root.
    We pick the first parent that contains the canonical `MWBots/` directory
    OR a tracked top-level marker (`RSAdminBot/`); falling back to parents[2]
    keeps backward-compat with anything that previously relied on it.
    """
    here = Path(__file__).resolve().parent
    for p in (here.parents[0], here.parents[1] if len(here.parents) > 1 else here, here.parents[2] if len(here.parents) > 2 else here):
        if (p / "MWBots").is_dir() or (p / "RSAdminBot").is_dir():
            return p
    return Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) > 2 else Path(__file__).resolve().parent


def load_instore_config() -> Dict[str, Any]:
    """
    Canonical loader for Instorebotforwarder runtime config + secrets.

    Reads `config.json` and overlays `config.secrets.json` (latter wins, e.g.
    bot_token, gemini_api_key). The files are resolved relative to THIS file's
    directory, which works for both layouts:
      - Local:  `<repo>/MWBots/Instorebotforwarder/config*.json`
      - Oracle: `<repo>/Instorebotforwarder/config*.json`
    so every standalone Instore CLI tool gets the right config in either spot.
    """
    here = Path(__file__).resolve().parent
    cfg_path = here / "config.json"
    sec_path = here / "config.secrets.json"
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


def _probe_cache_path() -> Path:
    return Path(__file__).resolve().parent / ".gemini_probe_cache.json"


def _load_probe_cache() -> Dict[str, Any]:
    p = _probe_cache_path()
    try:
        o = json.loads(p.read_text(encoding="utf-8"))
        return o if isinstance(o, dict) else {}
    except Exception:
        return {}


def _save_probe_cache(o: Dict[str, Any]) -> None:
    try:
        _probe_cache_path().write_text(json.dumps(o, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def main(argv: Optional[Sequence[str]] = None) -> int:
    _configure_stdout()
    ap = argparse.ArgumentParser()
    ap.add_argument("--link", default="", help="Discord message link")
    ap.add_argument("--no-gemini", action="store_true", help="Skip Gemini rewrite")
    ap.add_argument("--probe-api", action="store_true", help="Probe Gemini API now (one real request; uses cooldown cache).")
    ap.add_argument("--send-preview", action="store_true", help="Offer to send preview to destination")
    ap.add_argument("--send-now", action="store_true", help="Send preview without prompt (dangerous)")
    args = ap.parse_args(argv)

    link = str(args.link or "").strip() or input("Discord message link: ").strip()
    guild_id, channel_id, message_id = _parse_discord_message_link(link)
    mapped_dest = CHANNEL_MAP.get(int(channel_id))
    if not mapped_dest:
        supported = ", ".join(str(k) for k in sorted(CHANNEL_MAP.keys()))
        raise SystemExit(
            f"Source channel {channel_id} is not in CHANNEL_MAP. Supported sources: {supported}"
        )
    dest_channel_id = int(mapped_dest)

    cfg = load_instore_config()
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
    _print_safe("0) GEMINI CONFIG (not an API OK)")
    _print_safe(
        f"   enabled={gs.get('enabled')} api_key={gs.get('api_key')} model={gs.get('model')} temp={gs.get('temperature')}"
    )
    _print_safe("   Tip: use --probe-api to do one real API request (cooldown cached).")
    _print_safe("")

    if args.probe_api:
        cache = _load_probe_cache()
        now_s = int(__import__("time").time())
        cooldown_s = 120
        last_ts = int(cache.get("ts") or 0) if isinstance(cache.get("ts"), (int, float, str)) else 0
        last_status = str(cache.get("status") or "").strip()
        last_detail = str(cache.get("detail") or "").strip()
        use_cache = bool(last_ts and (now_s - last_ts) < cooldown_s and last_status)
        if use_cache:
            st = {"status": last_status, "detail": last_detail}
            src = "cache"
        else:
            st = probe_gemini_api(cfg)
            src = "live"
            _save_probe_cache({"ts": now_s, "status": st.get("status"), "detail": st.get("detail")})
        _print_safe("0.1) GEMINI API PROBE")
        _print_safe(f"   source={src} status={st.get('status')} detail={st.get('detail')}")
        _print_safe("")
    _print_safe("1) MESSAGE INFO")
    _print_safe(f"   link={link}")
    _print_safe(f"   guild_id={guild_id or ''}")
    _print_safe(f"   source_channel_id={channel_id}")
    _print_safe(f"   dest_channel_id={dest_channel_id}")
    _print_safe(f"   message_id={message_id}")
    _print_safe("")

    src_block = simple_message_block_from_rest(data)
    _print_safe("2) SOURCE SNAPSHOT (simple_message_block)")
    _print_safe(src_block)
    _print_safe("")

    desc = src_block
    used_fallback = False
    if not args.no_gemini:
        before = desc
        gemini_out = asyncio.run(rewrite_description(cfg, desc, no_gemini=False))
        gemini_out = str(gemini_out or "").strip()
        changed = ("yes" if (gemini_out and (gemini_out != str(before).strip())) else "no")
        ok = ("yes" if bool(gemini_out) else "no")
        _print_safe("3) GEMINI RESULT")
        _print_safe(f"   ok={ok} changed={changed}")
        if gemini_out:
            desc = gemini_out
        else:
            # Mirror the runtime forwarder: Gemini unavailable -> post cleaned original.
            used_fallback = True
            _print_safe(
                "   fallback=cleaned_original  "
                "(runtime will post src_block as-is - no silent drop)"
            )
        _print_safe("")
    embed_url = first_url_in_text(desc)
    media_url = media_url_from_rest(data)

    _print_safe("4) OUTBOUND PREVIEW (embed.description)")
    _print_safe(desc)
    _print_safe("")
    _print_safe("5) EMBED PREVIEW META")
    _print_safe("   send_mode=embed")
    _print_safe(f"   embed_source={'gemini_fallback_cleaned_original' if used_fallback else 'gemini_rewrite'}")
    _print_safe(f"   embed_url={embed_url}")
    _print_safe(f"   image_url={media_url}")
    _print_safe("")

    if args.send_preview or args.send_now:
        do_send = bool(args.send_now)
        if not do_send:
            _print_safe("6) SEND PREVIEW (optional)")
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
            _print_safe("6) SEND PREVIEW")
            _print_safe("   status=posted")
            _print_safe(f"   dest_channel_id={dest_channel_id}")
            _print_safe(f"   message_id={mid}")
            if guild_id:
                _print_safe(f"   message_link=https://discord.com/channels/{guild_id}/{dest_channel_id}/{mid}")
            _print_safe("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

