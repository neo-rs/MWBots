"""
Standalone Discord listener: expands backticked retailer product IDs into canonical store URLs.

Configured via config.json → retail_product_link_listener (see instore_auto_mirror_bot wiring).
Does not participate in Amazon routing, source_channel_ids, or simple_forward_mappings.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import discord

log = logging.getLogger("instorebotforwarder.retail_links")


@dataclass(frozen=True)
class RetailListenerSettings:
    enabled: bool
    channel_id: int


def _url_bestbuy(pid: str) -> str:
    return f"https://www.bestbuy.com/site/{pid}.p"


def _url_walmart(pid: str) -> str:
    return f"https://www.walmart.com/ip/{pid}"


def _url_target(pid: str) -> str:
    return f"https://www.target.com/p/-/A-{pid}"


def _url_costco(pid: str) -> str:
    return f"https://www.costco.com/.product.{pid}.html"


def _url_gamestop(pid: str) -> str:
    return f"https://www.gamestop.com/{pid}.html"


def _url_homedepot(pid: str) -> str:
    return f"https://www.homedepot.com/p/{pid}"


def _url_pokemon_center(pid: str) -> str:
    return f"https://www.pokemoncenter.com/product/{pid}"


def _url_samsclub(pid: str) -> str:
    return f"https://www.samsclub.com/ip/{pid}"


_URL_BUILDERS: Dict[str, Callable[[str], str]] = {
    "bestbuy": _url_bestbuy,
    "walmart": _url_walmart,
    "target": _url_target,
    "costco": _url_costco,
    "gamestop": _url_gamestop,
    "homedepot": _url_homedepot,
    "pokemon_center": _url_pokemon_center,
    "samsclub": _url_samsclub,
}

_TRIGGER_ALIASES: Dict[str, str] = {
    "bestbuy": "bestbuy",
    "best-buy": "bestbuy",
    "best_buy": "bestbuy",
    "costco": "costco",
    "gamestop": "gamestop",
    "walmart": "walmart",
    "target": "target",
    "homedepot": "homedepot",
    "home-depot": "homedepot",
    "home_depot": "homedepot",
    "pokemon": "pokemon_center",
    "pokemoncenter": "pokemon_center",
    "pokemon-center": "pokemon_center",
    "pokemon_center": "pokemon_center",
    "samsclub": "samsclub",
    "sams": "samsclub",
    "sams-club": "samsclub",
}

_RE_BACKTICK = re.compile(r"`([^`\n]+)`")


def _first_token(line: str) -> str:
    s = (line or "").strip().lower()
    if not s:
        return ""
    token = re.split(r"\s+", s, maxsplit=1)[0].strip()
    token = token.strip("*_:`")[0:64]
    return token


def _normalize_trigger(first_non_empty_line: str) -> Optional[str]:
    raw = first_non_empty_line.strip()
    if not raw:
        return None
    tok = _first_token(raw)
    if not tok:
        return None
    return _TRIGGER_ALIASES.get(tok)


_RE_NUMERIC_ID = re.compile(r"^\d{4,14}$")
_RE_HYPHEN_PRODUCT_ID = re.compile(r"^\d{2,}-\d[\d-]*\d$|^\d+-\d+-\d+$")


def _accept_backtick_inner(inner: str) -> Optional[str]:
    s = inner.strip()
    if not s or len(s) > 80:
        return None
    low = s.lower()
    if "http" in low or "/" in s or "@" in s:
        return None
    if "." in s:
        return None
    if _RE_NUMERIC_ID.match(s):
        return s
    if _RE_HYPHEN_PRODUCT_ID.match(s):
        return s
    return None


def extract_store_and_product_ids(content: str) -> Tuple[Optional[str], List[str]]:
    """
    First non-empty line sets store trigger (first word, alias map).
    Product IDs: every backtick segment that looks like a retailer product id (digits or hyphen SKU style).
    Order preserved; duplicates removed.
    """
    text = (content or "").replace("\r\n", "\n")
    lines = text.split("\n")
    first_line = ""
    for ln in lines:
        if ln.strip():
            first_line = ln
            break
    store = _normalize_trigger(first_line)
    if not store:
        return None, []

    ids_ordered: List[str] = []
    seen: set[str] = set()
    for m in _RE_BACKTICK.finditer(text):
        pid = _accept_backtick_inner(m.group(1))
        if not pid:
            continue
        if pid in seen:
            continue
        seen.add(pid)
        ids_ordered.append(pid)

    return store, ids_ordered


def build_product_urls(store_key: str, ids: Sequence[str]) -> List[str]:
    fn = _URL_BUILDERS.get(store_key)
    if not fn or not ids:
        return []
    out: List[str] = []
    for pid in ids:
        try:
            out.append(fn(pid.strip()))
        except Exception:
            continue
    return out


def load_listener_settings(cfg: dict) -> Optional[RetailListenerSettings]:
    raw = (cfg or {}).get("retail_product_link_listener")
    if not isinstance(raw, dict):
        return None
    enabled = raw.get("enabled", True)
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() not in {"0", "false", "no", "off"}
    if not bool(enabled):
        return None

    cid = raw.get("channel_id")
    env_id = (os.getenv("INSTORE_RETAIL_LINK_CHANNEL_ID", "") or "").strip()
    if env_id.isdigit():
        cid = int(env_id)

    try:
        channel_id = int(cid)  # type: ignore[arg-type]
    except Exception:
        return None
    if channel_id <= 0:
        return None
    return RetailListenerSettings(enabled=True, channel_id=channel_id)


async def maybe_reply_retail_product_links(client: discord.Client, message: discord.Message, cfg: dict) -> bool:
    """
    If message is in the configured channel and starts with a known store trigger,
    reply with one canonical URL per extracted backticked product id.

    Returns True when this handler owned the message (caller should skip other flows).
    """
    settings = load_listener_settings(cfg)
    if not settings:
        return False

    try:
        ch_id = int(message.channel.id)
    except Exception:
        return False
    if ch_id != settings.channel_id:
        return False

    try:
        if client.user and message.author.id == client.user.id:
            return False
    except Exception:
        pass

    store, ids = extract_store_and_product_ids(message.content or "")
    if not store:
        return False

    urls = build_product_urls(store, ids)
    if not urls:
        hint = (
            f"No product IDs found in backticks for `{store}`. "
            "Wrap each TCIN/SKU/UPC-style id in single backticks, e.g. `95120834`."
        )
        try:
            await message.reply(hint, mention_author=False)
        except Exception as e:
            log.warning("[retail_links] reply failed: %s", str(e)[:120])
        return True

    body = "\n".join(urls)
    try:
        await message.reply(body, mention_author=False)
    except Exception as e:
        log.warning("[retail_links] reply failed: %s", str(e)[:120])

    return True
