from __future__ import annotations

import io
import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from logging_utils import log_info, log_warn

import settings_store as cfg

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"

# Secrets: this file contains webhook URLs and is intentionally ignored by git.
# Format: { "<destination_channel_id>": "<webhook_url>" }
_WEBHOOK_MAP_PATH = _CONFIG_DIR / "channel_map.json"

_LOCK = threading.RLock()
_CACHE: Dict[int, str] = {}

def _invalidate_webhook_for_channel(channel_id: int) -> None:
    """Remove a cached/stored webhook URL for a destination channel (e.g. if Discord says Unknown Webhook)."""
    try:
        cid = int(channel_id or 0)
    except Exception:
        cid = 0
    if cid <= 0:
        return
    try:
        with _LOCK:
            try:
                _CACHE.pop(cid, None)
            except Exception:
                pass
            m = _load_webhook_map()
            if cid in m:
                m.pop(cid, None)
                _save_webhook_map(m)
    except Exception:
        return


def _load_webhook_map() -> Dict[int, str]:
    try:
        with _LOCK:
            if not _WEBHOOK_MAP_PATH.exists():
                return {}
            raw = json.loads(_WEBHOOK_MAP_PATH.read_text(encoding="utf-8") or "{}")
            if not isinstance(raw, dict):
                return {}
            out: Dict[int, str] = {}
            for k, v in raw.items():
                try:
                    cid = int(str(k).strip())
                except Exception:
                    continue
                url = str(v or "").strip()
                if cid > 0 and url:
                    out[cid] = url
            return out
    except Exception:
        return {}


def _save_webhook_map(m: Dict[int, str]) -> None:
    try:
        with _LOCK:
            _WEBHOOK_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
            raw: Dict[str, str] = {str(int(k)): str(v) for k, v in (m or {}).items() if int(k) > 0 and str(v or "").strip()}
            tmp = Path(str(_WEBHOOK_MAP_PATH) + ".tmp")
            tmp.write_text(json.dumps(raw, indent=2, ensure_ascii=False), encoding="utf-8")
            tmp.replace(_WEBHOOK_MAP_PATH)
    except Exception:
        pass


def _is_webhook_url(url: str) -> bool:
    u = str(url or "").strip()
    return "/webhooks/" in u and len(u) > 20


async def _get_or_create_webhook_url(*, dest_channel, reason: str, force: bool = False, webhook_name: str = "MWDataManagerBot") -> str:
    """
    Return a webhook URL for a destination channel.
    Creates one if missing. Requires "Manage Webhooks" permission.
    """
    try:
        import discord  # type: ignore
    except Exception:
        return ""

    try:
        cid = int(getattr(dest_channel, "id", 0) or 0)
    except Exception:
        cid = 0
    if cid <= 0:
        return ""

    # Config gate (default False; must be enabled) unless force=True.
    enabled = bool(getattr(cfg, "USE_WEBHOOKS_FOR_FORWARDING", False))
    if not enabled and not bool(force):
        return ""

    # In-memory cache
    try:
        cached = _CACHE.get(cid) or ""
    except Exception:
        cached = ""
    if _is_webhook_url(cached):
        return cached

    # File cache
    m = _load_webhook_map()
    url = str(m.get(cid) or "").strip()
    if _is_webhook_url(url):
        try:
            _CACHE[cid] = url
        except Exception:
            pass
        return url

    # Create new webhook
    try:
        nm = str(webhook_name or "MWDataManagerBot").strip() or "MWDataManagerBot"
        nm = nm[:80]
        wh = await dest_channel.create_webhook(name=nm, reason=str(reason or "MWDataManagerBot webhook sender"))
        url2 = str(getattr(wh, "url", "") or "").strip()
    except Exception as e:
        try:
            log_warn(f"[WEBHOOK] create_webhook failed channel_id={cid} ({type(e).__name__}: {e})")
        except Exception:
            pass
        return ""

    if not _is_webhook_url(url2):
        return ""

    m[cid] = url2
    _save_webhook_map(m)
    try:
        _CACHE[cid] = url2
    except Exception:
        pass
    try:
        log_info(f"[WEBHOOK] created for channel_id={cid}")
    except Exception:
        pass
    return url2


async def send_via_webhook_or_bot(
    *,
    dest_channel,
    content: str,
    embeds,
    attachments: Optional[List[Dict[str, Any]]] = None,
    username: str = "",
    avatar_url: str = "",
    reason: str = "",
) -> None:
    """
    Send message to a destination channel:
    - Prefer webhook (if enabled and creatable)
    - Fallback to normal bot send
    """
    try:
        import discord  # type: ignore
    except Exception:
        # If discord isn't available, we can't send.
        return

    allowed_mentions = None
    try:
        allowed_mentions = discord.AllowedMentions.none()
    except Exception:
        allowed_mentions = None

    # Normalize username/avatar for webhook constraints
    uname = str(username or "").strip()
    if uname:
        uname = uname[:80]
    av = str(avatar_url or "").strip()

    use_files = bool(getattr(cfg, "FORWARD_ATTACHMENTS_AS_FILES", True))
    try:
        max_files = int(getattr(cfg, "FORWARD_ATTACHMENTS_MAX_FILES", 10) or 10)
    except Exception:
        max_files = 10
    max_files = max(0, min(10, max_files))
    try:
        max_bytes = int(getattr(cfg, "FORWARD_ATTACHMENTS_MAX_BYTES", 7_500_000) or 7_500_000)
    except Exception:
        max_bytes = 7_500_000
    if max_bytes < 0:
        max_bytes = 0

    try:
        cid = int(getattr(dest_channel, "id", 0) or 0)
    except Exception:
        cid = 0

    url = await _get_or_create_webhook_url(dest_channel=dest_channel, reason=reason, force=False, webhook_name="MWDataManagerBot")
    if url:
        # Webhook execute does not require bot token, but discord.py needs an aiohttp session.
        import aiohttp  # type: ignore

        async with aiohttp.ClientSession() as session:
            wh = discord.Webhook.from_url(url, session=session)  # type: ignore[arg-type]
            files: List[Any] = []
            skipped: List[str] = []
            if use_files and attachments and max_files > 0 and max_bytes > 0:
                files, skipped = await _download_attachment_files(
                    session=session, attachments=attachments, max_files=max_files, max_bytes=max_bytes
                )
            final_content = str(content or "")
            if skipped and len(final_content) < 1900:
                extra = "\n".join(skipped[:5]).strip()
                if extra:
                    final_content = (final_content + ("\n" if final_content else "") + extra)[:1950]

            kwargs: Dict[str, Any] = {
                "content": final_content,
                "embeds": list(embeds or [])[:10],
                "allowed_mentions": allowed_mentions,
            }
            if uname:
                kwargs["username"] = uname
            if av:
                kwargs["avatar_url"] = av
            if files:
                kwargs["files"] = files
            # wait=False keeps it fast (no message id needed).
            try:
                await wh.send(wait=False, **kwargs)
                return
            except discord.NotFound as e:
                # 10015: Unknown Webhook (deleted/invalid). Self-heal by recreating once.
                code = getattr(e, "code", None)
                if int(code or 0) == 10015 or "Unknown Webhook" in str(e):
                    if cid > 0:
                        _invalidate_webhook_for_channel(cid)
                    url2 = await _get_or_create_webhook_url(dest_channel=dest_channel, reason=reason or "recreate webhook")
                    if url2:
                        try:
                            wh2 = discord.Webhook.from_url(url2, session=session)  # type: ignore[arg-type]
                            await wh2.send(wait=False, **kwargs)
                            return
                        except Exception:
                            pass
                raise

    # Fallback: normal bot send
    files2: List[Any] = []
    skipped2: List[str] = []
    if use_files and attachments and max_files > 0 and max_bytes > 0:
        try:
            import aiohttp  # type: ignore

            async with aiohttp.ClientSession() as session:
                files2, skipped2 = await _download_attachment_files(
                    session=session, attachments=attachments, max_files=max_files, max_bytes=max_bytes
                )
        except Exception:
            files2, skipped2 = [], []
    final_content2 = str(content or "")
    if skipped2 and len(final_content2) < 1900:
        extra2 = "\n".join(skipped2[:5]).strip()
        if extra2:
            final_content2 = (final_content2 + ("\n" if final_content2 else "") + extra2)[:1950]
    kwargs2: Dict[str, Any] = {
        "content": final_content2,
        "embeds": list(embeds or [])[:10],
        "allowed_mentions": allowed_mentions,
    }
    if files2:
        kwargs2["files"] = files2
    await dest_channel.send(**kwargs2)


def _pick_attachment_url(a: Dict[str, Any]) -> str:
    try:
        u = str(a.get("url") or a.get("proxy_url") or "").strip()
    except Exception:
        u = ""
    return u


def _pick_attachment_filename(a: Dict[str, Any], url: str) -> str:
    try:
        fn = str(a.get("filename") or "").strip()
    except Exception:
        fn = ""
    if fn:
        return fn[:120]
    try:
        path = urlparse(url).path or ""
        base = path.rsplit("/", 1)[-1].strip()
    except Exception:
        base = ""
    return (base or "file")[:120]


async def _download_attachment_files(
    *,
    session,
    attachments: List[Dict[str, Any]],
    max_files: int,
    max_bytes: int,
) -> Tuple[List[Any], List[str]]:
    """
    Download attachment URLs and return (files, skipped_urls).
    Uses Discord CDN public URLs; no auth required.
    """
    files: List[Any] = []
    skipped: List[str] = []
    if not attachments or max_files <= 0 or max_bytes <= 0:
        return files, skipped

    try:
        import discord  # type: ignore
    except Exception:
        return files, skipped

    for a in attachments:
        if len(files) >= max_files:
            break
        if not isinstance(a, dict):
            continue
        url = _pick_attachment_url(a)
        if not url:
            continue
        try:
            async with session.get(url, timeout=15) as resp:
                if int(getattr(resp, "status", 0) or 0) != 200:
                    skipped.append(url)
                    continue
                try:
                    cl = int(resp.headers.get("Content-Length") or 0)
                except Exception:
                    cl = 0
                if cl and cl > max_bytes:
                    skipped.append(url)
                    continue
                data = await resp.read()
        except Exception:
            skipped.append(url)
            continue
        if not data:
            skipped.append(url)
            continue
        if len(data) > max_bytes:
            skipped.append(url)
            continue
        filename = _pick_attachment_filename(a, url)
        try:
            files.append(discord.File(fp=io.BytesIO(data), filename=filename))
        except Exception:
            skipped.append(url)
            continue
    return files, skipped

