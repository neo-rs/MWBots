"""
Link logic audit (local dry-run)

Purpose:
- Given a URL or a snippet of message text, replay the same link-unwrap augmentation and
  classifier routing that MWDataManagerBot uses in `live_forwarder.py`.
- This is a *local* tool: no Discord API calls, no sends.

Key rules:
- Single source of truth: reuse canonical helpers from `utils.py` + `classifier.py`.
- Never rewrite outbound content (this tool only audits classification inputs).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

_BOT_DIR = Path(__file__).resolve().parent
if str(_BOT_DIR) not in sys.path:
    sys.path.insert(0, str(_BOT_DIR))

from config import load_settings_and_tokens
from classifier import detect_all_link_types, order_link_types, select_target_channel_id
from global_triggers import detect_global_triggers
from keywords import load_keywords
import settings_store as cfg
from utils import (
    AFFILIATE_LINK_DOMAINS,
    _DISCORD_MEDIA_HOSTS,  # internal constant; ok for audit output only
    extract_all_raw_links_from_text,
    extract_urls_from_text,
    augment_text_with_affiliate_redirects,
    augment_text_with_dealshacks_hiddendealsociety,
    augment_text_with_dmflip,
    augment_text_with_ringinthedeals,
    augment_text_with_universal_resolver_fallback,
)


def _safe_console(s: str) -> str:
    enc = getattr(sys.stdout, "encoding", None) or "cp1252"
    try:
        return (s or "").encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return (s or "").encode("ascii", errors="replace").decode("ascii")


def _line(*parts: object) -> None:
    print(_safe_console(" ".join(str(p) for p in parts)))


def _banner(title: str) -> None:
    _line("")
    _line("=" * 78)
    _line(title)
    _line("=" * 78)


def _host(url: str) -> str:
    try:
        h = (urlparse(url).netloc or "").lower()
        if h.startswith("www."):
            h = h[4:]
        return h
    except Exception:
        return ""


def _is_discord_host(host: str) -> bool:
    h = (host or "").lower().strip()
    if not h:
        return False
    if h in _DISCORD_MEDIA_HOSTS:
        return True
    if h in {"discord.com", "discord.gg", "discordapp.com", "discord.me"}:
        return True
    if h.endswith(".discord.com"):
        return True
    return False


async def _build_text_to_check(raw_text: str) -> Tuple[str, Dict[str, Any]]:
    """
    Mirror the unwrap augmentation block from `live_forwarder.py` for a local text snippet.
    Returns (text_to_check, unwrap_debug)
    """
    base = str(raw_text or "").strip()
    dbg: Dict[str, Any] = {"enable_raw_link_unwrap": bool(cfg.ENABLE_RAW_LINK_UNWRAP)}
    if not cfg.ENABLE_RAW_LINK_UNWRAP:
        return base, dbg

    text_to_check = base
    original = base
    raw_links: List[str] = []
    dmflip_links: List[str] = []
    ring_links: List[str] = []
    dealshacks_links: List[str] = []
    affiliate_links: List[str] = []

    try:
        raw_links = extract_all_raw_links_from_text(text_to_check)
        text_to_check, dmflip_links = await augment_text_with_dmflip(text_to_check)
        text_to_check, ring_links = await augment_text_with_ringinthedeals(text_to_check)
        text_to_check, dealshacks_links = await augment_text_with_dealshacks_hiddendealsociety(text_to_check)
        text_to_check, affiliate_links = await augment_text_with_affiliate_redirects(text_to_check)
    except Exception as e:
        dbg["unwrap_error"] = str(e)
        return base, dbg

    merged: List[str] = []
    seen = set()
    for u in (raw_links or []) + (dmflip_links or []) + (ring_links or []) + (dealshacks_links or []) + (affiliate_links or []):
        if not u or not isinstance(u, str):
            continue
        if u in seen:
            continue
        seen.add(u)
        merged.append(u)

    # Keep only links not already visible in the original text.
    filtered: List[str] = []
    for u in merged:
        try:
            if u in original:
                continue
        except Exception:
            pass
        filtered.append(u)

    if filtered:
        text_to_check = (text_to_check + " " + " ".join(filtered)).strip()

    dbg.update(
        {
            "raw_links": raw_links,
            "dmflip_links": dmflip_links,
            "ring_links": ring_links,
            "dealshacks_links": dealshacks_links,
            "affiliate_redirect_links": affiliate_links,
            "filtered_new_links": filtered,
        }
    )
    return text_to_check, dbg


async def _audit_text(raw_text: str, *, source_channel_id: Optional[int]) -> Dict[str, Any]:
    trace: Dict[str, Any] = {}
    text_to_check, unwrap_dbg = await _build_text_to_check(raw_text)
    try:
        text_to_check = await augment_text_with_universal_resolver_fallback(
            str(raw_text or ""), text_to_check, trace=trace
        )
    except Exception:
        pass

    # Keyword list is optional; keep parity with bot by loading it.
    try:
        keywords_list = load_keywords()
    except Exception:
        keywords_list = []

    urls = extract_urls_from_text(raw_text or "")
    hosts = []
    for u in urls:
        h = _host(u)
        if h and h not in hosts:
            hosts.append(h)

    host_notes: Dict[str, Any] = {}
    for h in hosts:
        host_notes[h] = {
            "discord_excluded": bool(_is_discord_host(h)),
            "known_affiliate_wrapper": any(d in h for d in (AFFILIATE_LINK_DOMAINS or set())),
        }

    all_link_types = detect_all_link_types(
        text_to_check,
        attachments=[],
        keywords_list=keywords_list,
        embeds=[],
        source_channel_id=source_channel_id,
        trace=trace,
        message_content=str(raw_text or ""),
    )
    global_types = detect_global_triggers(
        text_to_check,
        source_channel_id=source_channel_id,
        link_tracking_cache=None,
        embeds=[],
        attachments=[],
    )
    if global_types:
        all_link_types.extend(global_types)

    dispatch_link_types: List[Tuple[int, str]] = []
    stop_after_first = False
    if all_link_types:
        dispatch_link_types, stop_after_first = order_link_types(all_link_types)

    if not dispatch_link_types:
        fallback = select_target_channel_id(
            text_to_check,
            attachments=[],
            keywords_list=keywords_list,
            source_channel_id=source_channel_id,
            trace=trace,
            message_content=str(raw_text or ""),
            embeds=[],
        )
        if fallback:
            dispatch_link_types = [fallback]

    out: Dict[str, Any] = {
        "input_text": raw_text,
        "text_to_check": text_to_check,
        "source_channel_id": int(source_channel_id or 0),
        "urls": urls,
        "hosts": hosts,
        "host_notes": host_notes,
        "unwrap": unwrap_dbg,
        "universal_resolver_fallback": (trace.get("classifier") or {})
        .get("matches", {})
        .get("universal_resolver_fallback"),
        "routes_ordered": [{"channel_id": int(cid), "tag": str(tag)} for (cid, tag) in (dispatch_link_types or [])],
        "stop_after_first": bool(stop_after_first),
        "classifier_matches": (trace.get("classifier") or {}).get("matches", {}),
    }
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit MWDataManagerBot link logic for a URL or message text.")
    p.add_argument("--text", help="Message text or URL(s) to audit. If omitted, reads from stdin.")
    p.add_argument("--url", help="Single URL shortcut (equivalent to --text <url>).")
    p.add_argument("--source-channel-id", type=int, default=0, help="Replay source channel id for routing context (online/instore/clearance).")
    p.add_argument("--json-out", help="Write full JSON output to a file (path).")
    return p.parse_args()


async def _amain() -> int:
    args = _parse_args()
    settings, _tokens = load_settings_and_tokens(_BOT_DIR / "config")
    cfg.init(settings)

    raw_text = args.text or args.url or ""
    if not raw_text.strip():
        _banner("MWDataManagerBot - Link logic audit (paste text, then Ctrl+Z Enter)")
        try:
            raw_text = sys.stdin.read()
        except Exception:
            raw_text = ""

    raw_text = str(raw_text or "").strip()
    if not raw_text:
        _line("ERROR: no input text/url.")
        return 2

    # Match channel_route_audit behavior: if source_channel_id not provided, use the first online source
    # from settings (keeps affiliate logic consistent, since it is online-only).
    src = int(args.source_channel_id or 0) or 0
    if src <= 0:
        try:
            src = int(next(iter(getattr(cfg, "SMART_SOURCE_CHANNELS_ONLINE", set()) or set())) or 0)
        except Exception:
            src = 0
    src_opt = src if src > 0 else None
    payload = await _audit_text(raw_text, source_channel_id=src_opt)

    _banner("LINK LOGIC AUDIT (local)")
    _line("Input:", payload.get("input_text", "")[:900] + ("..." if len(payload.get("input_text", "") or "") > 900 else ""))
    _line("")
    _line("URLs:")
    for u in payload.get("urls") or []:
        _line("-", u)
    if not (payload.get("urls") or []):
        _line("- (none found)")

    _line("")
    _line("Hosts:")
    for h in payload.get("hosts") or []:
        n = (payload.get("host_notes") or {}).get(h) or {}
        _line("-", h, f"(discord_excluded={bool(n.get('discord_excluded'))} known_wrapper={bool(n.get('known_affiliate_wrapper'))})")
    if not (payload.get("hosts") or []):
        _line("- (none)")

    _line("")
    _line("Unwrap enabled:", bool(((payload.get("unwrap") or {}).get("enable_raw_link_unwrap"))))
    filtered = (payload.get("unwrap") or {}).get("filtered_new_links") or []
    _line("Unwrapped new links appended to classifier text:", len(filtered))
    for u in filtered[:12]:
        _line("-", u)
    if len(filtered) > 12:
        _line(f"- ... ({len(filtered) - 12} more)")

    _line("")
    _line("Universal resolver fallback (optional, classification-only):")
    _line("- enabled in settings:", bool(getattr(cfg, "UNIVERSAL_RESOLVER_FALLBACK_ENABLED", False)))
    ufb = payload.get("universal_resolver_fallback")
    if isinstance(ufb, dict):
        _line("- ran:", bool(ufb.get("ran")))
        _line("- appended Amazon-ish URLs:", int(ufb.get("appended_count") or 0))
        try:
            _line(json.dumps(ufb, indent=2, ensure_ascii=False)[:1600])
        except Exception:
            _line(str(ufb)[:1600])
    else:
        _line("- (no trace)")

    _line("")
    _line("ROUTES (ordered):")
    routes = payload.get("routes_ordered") or []
    for r in routes:
        _line("-", r.get("tag"), "->", r.get("channel_id"))
    if not routes:
        _line("- (no route)")

    _line("")
    _line("MATCHES (classifier.matches):")
    try:
        _line(json.dumps(payload.get("classifier_matches") or {}, indent=2, ensure_ascii=False))
    except Exception:
        _line(str(payload.get("classifier_matches") or {}))

    if args.json_out:
        try:
            out_path = Path(args.json_out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            _line("")
            _line("Wrote JSON:", str(out_path))
        except Exception as e:
            _line("")
            _line("WARN: failed to write JSON:", e)

    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()

