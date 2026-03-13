"""Minimal utils for fetchall (MWDiscumBot). Only functions used by fetchall.py."""
from __future__ import annotations

from typing import Any, Dict, List, Set


def chunk_text(text: str, limit: int = 2000) -> List[str]:
    """Split text into Discord-safe message chunks."""
    if not text:
        return [""]
    try:
        lim = int(limit or 0)
    except Exception:
        lim = 2000
    if lim <= 0:
        lim = 2000
    if len(text) <= lim:
        return [text]
    chunks: List[str] = []
    remaining = text
    while remaining:
        chunks.append(remaining[:lim])
        remaining = remaining[lim:]
    return chunks


def is_image_attachment(att: Dict[str, Any]) -> bool:
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


def append_image_attachments_as_embeds(
    embeds_out: List[Dict[str, Any]], attachments: List[Dict[str, Any]], *, max_embeds: int = 10
) -> List[Dict[str, Any]]:
    """
    Render image attachments as embed images (better UX than appending CDN URLs).
    Returns a new embeds list capped to max_embeds.
    """
    if not attachments:
        return list(embeds_out or [])[: int(max_embeds or 10)]
    embeds_out = list(embeds_out or [])

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

    try:
        cap = int(max_embeds or 10)
    except Exception:
        cap = 10
    cap = max(1, min(10, cap))
    slots = max(0, cap - len(embeds_out))
    if slots <= 0:
        return embeds_out[:cap]

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
        if not is_image_attachment(a):
            continue
        embeds_out.append({"image": {"url": url}})
        existing_urls.add(url)
        added += 1
    return embeds_out[:cap]


def format_embeds_for_forwarding(embeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        if "author" in e and isinstance(e.get("author"), dict) and e["author"].get("name"):
            embed["author"] = {"name": e["author"].get("name"), "url": e["author"].get("url")}
        if "footer" in e and isinstance(e.get("footer"), dict) and e["footer"].get("text"):
            embed["footer"] = {"text": e["footer"].get("text")}
        if embed:
            out.append(embed)
    return out[:10]
