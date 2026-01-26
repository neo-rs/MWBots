from __future__ import annotations

import asyncio
import hashlib
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from patterns import THEATRE_CONTEXT_PATTERN, THEATRE_MERCH_PATTERN, THEATRE_STORE_PATTERN


def normalize_message(text: str) -> str:
    """Normalize message text for keyword scanning and signature generation."""
    if not text:
        return ""
    normalized = text.lower()
    # Custom emojis
    normalized = re.sub(r"<:[^:]+:\d+>", "", normalized)
    # Basic unicode emoji range removal
    normalized = re.sub(
        r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251]+",
        "",
        normalized,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def normalize_url(url: str) -> str:
    """Normalize URL for duplicate detection (remove query params, fragments)."""
    if not url:
        return ""
    try:
        if "?" in url:
            url = url.split("?", 1)[0]
        if "#" in url:
            url = url.split("#", 1)[0]
        url = url.rstrip("/")
        return url.lower()
    except Exception:
        return url.lower()


def extract_urls_from_text(text: str) -> List[str]:
    url_pattern = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE)
    urls = url_pattern.findall(text or "")
    return [normalize_url(u) for u in urls]


def collect_embed_strings(embeds: Optional[List[Dict[str, Any]]]) -> List[str]:
    """Flatten relevant embed fields into a list of strings for pattern checks."""
    if not embeds:
        return []
    collected: List[str] = []
    for embed in embeds:
        if not isinstance(embed, dict):
            continue
        for key in ("title", "description", "url"):
            value = embed.get(key)
            if value:
                collected.append(str(value))
        author = embed.get("author")
        if isinstance(author, dict):
            author_name = author.get("name")
            if author_name:
                collected.append(str(author_name))
            author_url = author.get("url")
            if author_url:
                collected.append(str(author_url))
        footer = embed.get("footer")
        if isinstance(footer, dict):
            footer_text = footer.get("text")
            if footer_text:
                collected.append(str(footer_text))
        fields = embed.get("fields")
        if isinstance(fields, list):
            for field in fields:
                if not isinstance(field, dict):
                    continue
                for field_key in ("name", "value"):
                    field_value = field.get(field_key)
                    if field_value:
                        collected.append(str(field_value))
    return collected


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


def format_embeds_for_forwarding(embeds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Trim/clean embeds to a safe dict shape before forwarding.

    This is canonical so fetchall + live forwarding render embeds consistently.
    """
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


def generate_content_signature(
    content: str,
    embeds: Optional[List[Dict[str, Any]]],
    attachments: Optional[List[Dict[str, Any]]],
) -> str:
    """Create a normalized signature for content + embeds + attachments."""
    components: List[str] = []
    components.append(normalize_message(content or ""))

    embed_strings = sorted(
        normalize_message(str(item))
        for item in collect_embed_strings(embeds or [])
        if item
    )
    components.extend(embed_strings)

    attachment_urls: List[str] = []
    for attachment in attachments or []:
        if not isinstance(attachment, dict):
            continue
        url = attachment.get("url") or attachment.get("proxy_url")
        if url:
            attachment_urls.append(normalize_url(str(url)))
    attachment_urls.sort()
    components.extend(attachment_urls)

    signature_source = "||".join(components).strip()
    return hashlib.md5(signature_source.encode("utf-8")).hexdigest()


def matches_instore_theatre(text: str, where_location: str = "") -> bool:
    if not text:
        return False
    if THEATRE_STORE_PATTERN.search(text):
        return True
    if where_location and THEATRE_STORE_PATTERN.search(where_location):
        return True
    if THEATRE_MERCH_PATTERN.search(text) and THEATRE_CONTEXT_PATTERN.search(text):
        return True
    return False


def has_product_and_marketplace_links(
    text: str,
    *,
    attachments: Optional[List[Dict[str, Any]]] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    resale_domains: Optional[Set[str]] = None,
    ignored_domains: Optional[Set[str]] = None,
) -> Tuple[bool, bool]:
    """Best-effort detection: at least one product link and one marketplace link."""
    resale_domains = set(resale_domains or [])
    ignored_domains = set(ignored_domains or [])

    urls: Set[str] = set(extract_urls_from_text(text or ""))
    if attachments:
        for a in attachments:
            if not isinstance(a, dict):
                continue
            url = a.get("url") or a.get("proxy_url") or ""
            if url:
                urls.add(normalize_url(str(url)))
    if embeds:
        for e in embeds:
            if not isinstance(e, dict):
                continue
            if e.get("url"):
                urls.add(normalize_url(str(e.get("url"))))
            desc = e.get("description")
            if desc:
                for u in extract_urls_from_text(str(desc)):
                    urls.add(u)

    marketplace_urls: Set[str] = set()
    product_urls: Set[str] = set()

    for url in urls:
        if not url:
            continue
        try:
            parsed = urlparse(url)
        except Exception:
            parsed = None
        domain = parsed.netloc.lower() if parsed and parsed.netloc else ""
        if domain.startswith("www."):
            domain = domain[4:]
        if not domain:
            continue
        if domain in ignored_domains:
            continue
        path = parsed.path if parsed else ""
        is_market = False
        if domain in resale_domains:
            is_market = True
        if domain == "facebook.com" and path.lower().startswith("/marketplace"):
            is_market = True
        if is_market:
            marketplace_urls.add(url)
        else:
            product_urls.add(url)

    return bool(marketplace_urls), bool(product_urls)


# =============================================================================
# Raw link extraction + unwrapping (ported from legacy datamanagerbot.py)
# =============================================================================

RAW_URL_REGEX = re.compile(r'https?://[^\s<>"\'\)\]]+', re.IGNORECASE)
PERCENT_ENCODED_URL_REGEX = re.compile(r'https?%3A%2F%2F[^\s<>"\'\)\]]+', re.IGNORECASE)
MARKDOWN_LINK_REGEX = re.compile(r"\[[^\]]+\]\((https?://[^\s<>\)]+)\)", re.IGNORECASE)
COMMON_REDIRECT_KEYS = ("url", "link", "redirect", "target", "u", "r", "to", "dest", "destination", "out", "q", "l", "s", "o")

AFFILIATE_LINK_DOMAINS_REDIRECT = {"howl.link", "mavely.app.link", "go.magik.ly", "magik.ly"}
AFFILIATE_LINK_DOMAINS_QUERY = {"galaxydeals.net"}
AFFILIATE_LINK_DOMAINS_HTML = {"dmflip.com", "ringinthedeals.com"}
AFFILIATE_LINK_DOMAINS = AFFILIATE_LINK_DOMAINS_REDIRECT | AFFILIATE_LINK_DOMAINS_QUERY | AFFILIATE_LINK_DOMAINS_HTML

_AMAZON_HOST_RE = re.compile(r"(?:^|\.)amazon\.[a-z.]{2,}$", re.IGNORECASE)
_AMZN_SHORT_RE = re.compile(r"^https?://(?:www\.)?(?:amzn\.to|a\.co)/[A-Za-z0-9]+", re.IGNORECASE)
_ASIN_RE = re.compile(r"\b([A-Z0-9]{10})\b")

_REDIRECT_CACHE: Dict[str, Tuple[float, str]] = {}
_REDIRECT_CACHE_TTL_SECONDS = 12 * 60 * 60

# Don't include Discord CDN/media links as "raw links"
_DISCORD_MEDIA_HOSTS = {
    "cdn.discordapp.com",
    "media.discordapp.net",
    "cdn.discordapp.net",
}


def _is_affiliate_domain(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
        return any(d in host for d in AFFILIATE_LINK_DOMAINS)
    except Exception:
        return False


def _is_discord_media_url(url: str) -> bool:
    try:
        host = (urlparse(url).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host in _DISCORD_MEDIA_HOSTS
    except Exception:
        return False


def canonicalize_amazon_url(url: str) -> str:
    """Best-effort canonical Amazon URL (strip tracking, normalize /dp/<ASIN>)."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]

        if _AMZN_SHORT_RE.match(url):
            return normalize_url(url)

        if not _AMAZON_HOST_RE.search(host):
            return normalize_url(url)

        path = parsed.path or ""
        asin = None
        m = re.search(r"/dp/([A-Z0-9]{10})(?:[/?]|$)", path, re.IGNORECASE)
        if m:
            asin = m.group(1).upper()
        if not asin:
            m = re.search(r"/gp/product/([A-Z0-9]{10})(?:[/?]|$)", path, re.IGNORECASE)
            if m:
                asin = m.group(1).upper()
        if not asin:
            m = _ASIN_RE.search(url)
            if m:
                asin = m.group(1).upper()

        if asin:
            return f"https://{host}/dp/{asin}"
        return normalize_url(url)
    except Exception:
        return normalize_url(url)


def canonicalize_amazon_url_keep_tag(url: str) -> str:
    """
    Canonical Amazon URL but preserve ?tag= when present.
    - Normalizes to https://{host}/dp/{ASIN}
    - If tag exists, returns https://{host}/dp/{ASIN}?tag=XXXX
    """
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query or "")
        tag = ""
        if "tag" in qs and qs["tag"]:
            tag = (qs["tag"][0] or "").strip()

        base = canonicalize_amazon_url(url)
        if not base:
            return ""
        return f"{base}?tag={tag}" if tag else base
    except Exception:
        return canonicalize_amazon_url(url)


def unwrap_single_url(value: str, *, depth: int = 0, prefer_domains: Optional[Set[str]] = None) -> Optional[str]:
    """
    Unwrap embedded/affiliate/redirected URLs via markdown + query params.
    Redirect-based affiliates require async redirect following; see `extract_link_from_redirect_affiliate`.
    """
    if not value or depth > 4:
        return None
    try:
        md = MARKDOWN_LINK_REGEX.search(value)
        if md:
            inner = md.group(1)
            return unwrap_single_url(inner, depth=depth + 1, prefer_domains=prefer_domains) or inner

        m = RAW_URL_REGEX.search(value)
        encoded_m = PERCENT_ENCODED_URL_REGEX.search(value)
        candidate = m.group(0) if m else (encoded_m.group(0) if encoded_m else None)
        if candidate:
            decoded = unquote(candidate)
            if decoded != candidate and RAW_URL_REGEX.search(decoded):
                rec = unwrap_single_url(decoded, depth=depth + 1, prefer_domains=prefer_domains)
                if rec:
                    return rec

            parsed = urlparse(decoded)
            if parsed.query:
                q = parse_qs(parsed.query)
                for key in COMMON_REDIRECT_KEYS:
                    for val in q.get(key, []):
                        rec = unwrap_single_url(val, depth=depth + 1, prefer_domains=prefer_domains)
                        if rec:
                            return rec
                for vals in q.values():
                    for val in vals:
                        rec = unwrap_single_url(val, depth=depth + 1, prefer_domains=prefer_domains)
                        if rec:
                            return rec

            if prefer_domains:
                host = (parsed.netloc or "").lower()
                if any(pref in host for pref in prefer_domains):
                    return decoded
            return decoded

        dec = unquote(value)
        if dec and dec != value:
            return unwrap_single_url(dec, depth=depth + 1, prefer_domains=prefer_domains)
    except Exception:
        return None
    return None


def extract_all_raw_links_from_text(text: str) -> List[str]:
    """
    Extract *hidden* destination URLs from affiliate wrapper links (query/encoded).

    This is intentionally conservative:
    - It does NOT return normal "already-visible" links (e.g. ebay.com, mattel.com)
    - It does NOT return Discord CDN/media URLs
    - Redirect-based affiliates (mavely/howl/magik) and dmflip require async resolution
      and are handled elsewhere.
    """
    if not text:
        return []
    seen: Set[str] = set()
    results: List[str] = []

    # Collect wrapper candidates (raw urls + markdown inner urls + encoded urls)
    candidates: List[str] = []
    try:
        for md in MARKDOWN_LINK_REGEX.finditer(text):
            candidates.append(md.group(1))
    except Exception:
        pass
    try:
        for m in RAW_URL_REGEX.finditer(text):
            candidates.append(m.group(0))
    except Exception:
        pass
    try:
        for m in PERCENT_ENCODED_URL_REGEX.finditer(text):
            candidates.append(m.group(0))
    except Exception:
        pass

    for raw in candidates:
        if not raw:
            continue
        try:
            decoded = unquote(str(raw))
        except Exception:
            decoded = str(raw)

        # Only attempt unwrap for known affiliate wrapper domains (query-style).
        try:
            host = (urlparse(decoded).netloc or "").lower()
            if host.startswith("www."):
                host = host[4:]
        except Exception:
            host = ""

        if not host or not any(d in host for d in AFFILIATE_LINK_DOMAINS_QUERY):
            continue

        unwrapped = unwrap_single_url(decoded) or ""
        if not unwrapped or unwrapped == decoded:
            continue
        if _is_affiliate_domain(unwrapped):
            continue
        if _is_discord_media_url(unwrapped):
            continue

        try:
            uhost = (urlparse(unwrapped).netloc or "").lower()
            if uhost.startswith("www."):
                uhost = uhost[4:]
            if _AMAZON_HOST_RE.search(uhost):
                unwrapped = canonicalize_amazon_url(unwrapped)
        except Exception:
            pass

        if unwrapped and unwrapped not in seen:
            seen.add(unwrapped)
            results.append(unwrapped)

    return results[:25]


async def extract_link_from_redirect_affiliate(affiliate_url: str) -> Optional[str]:
    """Follow redirects for mavely/howl/magik and return final destination URL."""
    if not affiliate_url:
        return None
    try:
        if not affiliate_url.startswith(("http://", "https://")):
            affiliate_url = "https://" + affiliate_url
    except Exception:
        return None

    now = time.time()
    cached = _REDIRECT_CACHE.get(affiliate_url)
    if cached and (now - cached[0]) < _REDIRECT_CACHE_TTL_SECONDS:
        return cached[1] or None

    try:
        import aiohttp  # type: ignore
    except Exception:
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(affiliate_url, allow_redirects=True, max_redirects=10) as response:
                final_url = str(getattr(response, "url", "") or "")
                if not final_url:
                    return None
                final_host = (urlparse(final_url).netloc or "").lower()
                if not any(d in final_host for d in AFFILIATE_LINK_DOMAINS):
                    _REDIRECT_CACHE[affiliate_url] = (now, final_url)
                    # prune occasionally
                    if len(_REDIRECT_CACHE) > 2000:
                        cutoff = now - _REDIRECT_CACHE_TTL_SECONDS
                        for k, (ts, _) in list(_REDIRECT_CACHE.items()):
                            if ts < cutoff:
                                _REDIRECT_CACHE.pop(k, None)
                    return final_url
    except Exception:
        return None
    return None


async def extract_amazon_link_from_dmflip(dmflip_url: str) -> Optional[str]:
    """Fetch dmflip page and extract an Amazon URL (regex-based, no bs4 dependency)."""
    if not dmflip_url or not isinstance(dmflip_url, str):
        return None
    try:
        if not dmflip_url.startswith(("http://", "https://")):
            dmflip_url = "https://" + dmflip_url
    except Exception:
        return None

    try:
        import aiohttp  # type: ignore
    except Exception:
        return None

    html = ""
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(dmflip_url, allow_redirects=True) as response:
                if int(getattr(response, "status", 0) or 0) != 200:
                    return None
                html = await response.text()
    except Exception:
        return None

    amazon_url_pattern = re.compile(r'https?://[^/\s]*amazon\.[a-z.]{2,}/[^\s<>"\']+', re.IGNORECASE)
    amzn_pattern = re.compile(r'https?://(?:www\.)?(?:amzn\.to|a\.co)/[A-Za-z0-9]+', re.IGNORECASE)

    candidates: List[str] = []
    try:
        candidates.extend(amazon_url_pattern.findall(html)[:80])
        candidates.extend(PERCENT_ENCODED_URL_REGEX.findall(html)[:80])
        candidates.extend(amzn_pattern.findall(html)[:20])
    except Exception:
        candidates = []

    for raw in candidates:
        unwrapped = unwrap_single_url(raw, prefer_domains={"amazon.", "amzn.to", "a.co"}) or raw
        host = (urlparse(unwrapped).netloc or "").lower()
        if _AMAZON_HOST_RE.search(host) or _AMZN_SHORT_RE.match(unwrapped):
            return canonicalize_amazon_url(unwrapped)
    return None


async def extract_amazon_link_from_ringinthedeals(url: str) -> Optional[str]:
    """
    Resolve ringinthedeals.com deal pages and extract the first Amazon URL found in HTML.
    Supports inputs that are missing scheme (ringinthedeals.com/deal/xxx).
    """
    if not url or not isinstance(url, str):
        return None

    u = url.strip()
    if u.startswith("ringinthedeals.com/") or u.startswith("www.ringinthedeals.com/"):
        u = "https://" + u
    if not u.startswith(("http://", "https://")):
        u = "https://" + u

    try:
        import aiohttp  # type: ignore
    except Exception:
        return None

    html = ""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.get(u, allow_redirects=True) as response:
                if int(getattr(response, "status", 0) or 0) >= 400:
                    return None
                html = await response.text(errors="ignore")
    except Exception:
        return None

    if not html:
        return None

    amazon_url_pattern = re.compile(r'https?://[^/\s]*amazon\.[a-z.]{2,}/[^\s<>"\']+', re.IGNORECASE)
    amzn_pattern = re.compile(r'https?://(?:www\.)?(?:amzn\.to|a\.co)/[A-Za-z0-9]+', re.IGNORECASE)

    candidates: List[str] = []
    try:
        candidates.extend(amazon_url_pattern.findall(html)[:80])
        candidates.extend(PERCENT_ENCODED_URL_REGEX.findall(html)[:80])
        candidates.extend(amzn_pattern.findall(html)[:20])
    except Exception:
        candidates = []

    for raw in candidates:
        unwrapped = unwrap_single_url(raw, prefer_domains={"amazon.", "amzn.to", "a.co"}) or raw
        host = (urlparse(unwrapped).netloc or "").lower()
        if _AMAZON_HOST_RE.search(host):
            return canonicalize_amazon_url_keep_tag(unwrapped)
        if _AMZN_SHORT_RE.match(unwrapped):
            # Expand amzn.to/a.co to final destination
            final = await extract_link_from_redirect_affiliate(unwrapped)
            if final:
                fhost = (urlparse(final).netloc or "").lower()
                if _AMAZON_HOST_RE.search(fhost):
                    return canonicalize_amazon_url_keep_tag(final)
            # fall back to the short link if we couldn't expand
            return normalize_url(unwrapped)
    return None


async def augment_text_with_dmflip(text: str) -> Tuple[str, List[str]]:
    """Expand dmflip.com URLs into extracted Amazon URLs."""
    if not text:
        return text, []
    dmflip_pattern = re.compile(r'https?://(?:www\.)?dmflip\.com/[^\s<>"\']+', re.IGNORECASE)
    matches = dmflip_pattern.findall(text)
    if not matches:
        return text, []
    tasks = [extract_amazon_link_from_dmflip(u) for u in matches[:5]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    extracted: List[str] = []
    for r in results:
        if isinstance(r, str) and r:
            extracted.append(r)
    if extracted:
        text = (text + " " + " ".join(extracted)).strip()
    return text, extracted


async def augment_text_with_ringinthedeals(text: str) -> Tuple[str, List[str]]:
    """Expand ringinthedeals.com deal URLs into extracted Amazon URLs."""
    if not text:
        return text, []
    ring_pattern = re.compile(
        r"(?:https?://(?:www\.)?)?ringinthedeals\.com/deal/[^\s<>'\"\)\]]+",
        re.IGNORECASE,
    )
    matches = ring_pattern.findall(text)
    if not matches:
        return text, []
    tasks = [extract_amazon_link_from_ringinthedeals(u) for u in matches[:5]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    extracted: List[str] = []
    for r in results:
        if isinstance(r, str) and r:
            extracted.append(r)
    if extracted:
        text = (text + " " + " ".join(extracted)).strip()
    return text, extracted


async def augment_text_with_affiliate_redirects(text: str) -> Tuple[str, List[str]]:
    """Expand redirect-based affiliate links into destination URLs."""
    if not text:
        return text, []
    redirect_affiliate_pattern = re.compile(
        r'https?://(?:www\.)?(?:howl\.link|mavely\.app\.link|go\.magik\.ly|magik\.ly)/[^\s<>"\']+',
        re.IGNORECASE,
    )
    matches = redirect_affiliate_pattern.findall(text)
    if not matches:
        return text, []
    tasks = [extract_link_from_redirect_affiliate(u) for u in matches[:8]]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    extracted: List[str] = []
    for r in results:
        if isinstance(r, str) and r:
            extracted.append(r)
    if extracted:
        text = (text + " " + " ".join(extracted)).strip()
    return text, extracted


def _pick_best_raw_url(raw_urls: List[str]) -> Optional[str]:
    candidates = [u for u in (raw_urls or []) if isinstance(u, str) and u.startswith("http")]
    if not candidates:
        return None
    # Prefer non-affiliate destination URLs
    for u in candidates:
        if not _is_affiliate_domain(u):
            # Prefer canonical amazon for amazon hosts
            host = (urlparse(u).netloc or "").lower()
            if _AMAZON_HOST_RE.search(host):
                return canonicalize_amazon_url_keep_tag(u)
            if _AMZN_SHORT_RE.match(u):
                return normalize_url(u)
            return u
    return candidates[0]


def _split_trailing_url_punct(token: str) -> Tuple[str, str]:
    """
    Split "url-like token" from trailing punctuation that often follows links in chat.
    Example: "https://x.y/z)." -> ("https://x.y/z", ").")
    """
    s = (token or "").strip()
    if not s:
        return "", ""
    tail = ""
    while s and s[-1] in ".,);]}>":
        tail = s[-1] + tail
        s = s[:-1]
    return s, tail


async def rewrite_affiliate_links_in_message(content: str, raw_urls: Optional[List[str]] = None) -> Tuple[str, List[str], bool]:
    """
    Rewrite wrapper links in-place so the forwarded message contains the destination link(s),
    and remove any existing "Raw links:" blocks that older logic may have appended.

    Returns:
      (new_content, extracted_urls, did_replace_any)
    """
    if not isinstance(content, str) or not content.strip():
        return content, [], False

    did_replace = False
    extracted: List[str] = []

    # Remove any existing "Raw links:" block from older logic.
    cleaned = re.sub(
        r"\n+Raw links:\s*\n(?:<?https?://\S+>?\s*)+",
        "",
        content,
        flags=re.IGNORECASE,
    )
    if cleaned != content:
        did_replace = True
        content = cleaned

    dmflip_re = re.compile(r'https?://(?:www\.)?dmflip\.com/[^\s<>"\']+', re.IGNORECASE)
    ring_re = re.compile(
        r"(?:https?://(?:www\.)?)?ringinthedeals\.com/deal/[^\s<>'\"\)\]]+",
        re.IGNORECASE,
    )

    # dmflip -> amazon
    dmflip_matches = dmflip_re.findall(content)
    if dmflip_matches:
        uniques: List[str] = []
        seen: Set[str] = set()
        for token in dmflip_matches:
            clean, _tail = _split_trailing_url_punct(token)
            if clean and clean not in seen:
                seen.add(clean)
                uniques.append(clean)
        tasks = [extract_amazon_link_from_dmflip(u) for u in uniques[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        resolved: Dict[str, str] = {}
        for src, res in zip(uniques[:5], results):
            if isinstance(res, str) and res:
                resolved[src] = canonicalize_amazon_url_keep_tag(res)
        for token in dmflip_matches:
            clean, tail = _split_trailing_url_punct(token)
            raw = resolved.get(clean or "")
            if raw:
                extracted.append(raw)
                rep = f"<{raw}>{tail}"
                if token in content and rep not in content:
                    content = content.replace(token, rep)
                    did_replace = True

    # ringinthedeals -> amazon
    ring_matches = ring_re.findall(content)
    if ring_matches:
        uniques = []
        seen = set()
        for token in ring_matches:
            clean, _tail = _split_trailing_url_punct(token)
            if clean and clean not in seen:
                seen.add(clean)
                uniques.append(clean)
        tasks = [extract_amazon_link_from_ringinthedeals(u) for u in uniques[:5]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        resolved = {}
        for src, res in zip(uniques[:5], results):
            if isinstance(res, str) and res:
                resolved[src] = canonicalize_amazon_url_keep_tag(res)
        for token in ring_matches:
            clean, tail = _split_trailing_url_punct(token)
            raw = resolved.get(clean or "")
            if raw:
                extracted.append(raw)
                rep = f"<{raw}>{tail}"
                if token in content and rep not in content:
                    content = content.replace(token, rep)
                    did_replace = True

    # Legacy behavior: if message has exactly one *affiliate wrapper* URL, rewrite it inline.
    if not did_replace and raw_urls:
        target = _pick_best_raw_url(list(raw_urls or []))
        if target:
            urls = RAW_URL_REGEX.findall(content)
            if len(urls) == 1:
                src_token = urls[0]
                src, tail = _split_trailing_url_punct(src_token)
                try:
                    if src and _is_affiliate_domain(src) and target != src and target not in content:
                        content = content.replace(src_token, f"<{target}>{tail}")
                        did_replace = True
                except Exception:
                    pass

    # Dedupe extracted while preserving order
    seen_out: Set[str] = set()
    deduped: List[str] = []
    for u in extracted:
        if not u or u in seen_out:
            continue
        seen_out.add(u)
        deduped.append(u)

    return content, deduped, did_replace

