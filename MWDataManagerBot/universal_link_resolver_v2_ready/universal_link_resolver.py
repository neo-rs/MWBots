#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import html
import json
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import requests

DEFAULT_TIMEOUT = 20
DEFAULT_MAX_DEPTH = 10
DEFAULT_SETTLE_MS = 6000
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/132.0.0.0 Safari/537.36"
)

# Universal resolver strategy:
# 1. Maintain a list of known redirector/tracker/intermediate hosts.
# 2. Treat anything outside those hosts as a likely merchant/final destination.
# 3. Recursively unwrap query params, HTTP redirects, HTML/meta/JS redirects.
# 4. Use Playwright only when requests/html extraction gets stuck.

REDIRECTOR_EXACT_HOSTS = {
    "mavely.app.link",
    "www.mavely.app.link",
    "mavelyinfluencer.com",
    "www.mavelyinfluencer.com",
    "joinmavely.com",
    "www.joinmavely.com",
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "lnk.to",
    "linktr.ee",
    "go.skimresources.com",
    "click.linksynergy.com",
    "www.anrdoezrs.net",
    "www.tkqlhce.com",
    "www.dpbolvw.net",
    "goto.target.com",
    "target.georiot.com",
    "shop-links.co",
    "pricedoffers.com",
    "www.pricedoffers.com",
    "app.link",
    "branch.io",
    "www.branch.io",
}

REDIRECTOR_KEYWORDS = [
    "mavely",
    "app.link",
    "branch",
    "bit.ly",
    "tinyurl",
    "skimlinks",
    "skimresources",
    "linksynergy",
    "impact.com",
    "impactradius",
    "rakuten",
    "pepperjam",
    "shareasale",
    "cj.com",
    "anrdoezrs",
    "tkqlhce",
    "dpbolvw",
    "redirectingat",
    "awin1",
    "go.redirectingat",
    "shop-links",
    "pricedoffers",
    "pricedoffer",
    "clickbank",
    "refersion",
    "partnerize",
    "pntra",
    "viglink",
    "sovrn",
]

BLOCK_OR_INFRA_EXACT_HOSTS = {
    "cloudflare.com",
    "www.cloudflare.com",
    "captcha-delivery.com",
    "www.captcha-delivery.com",
    "challenges.cloudflare.com",
    "errors.edgesuite.net",
    "akamaihd.net",
}

BLOCK_PATH_KEYWORDS = [
    "5xx-error-landing",
    "access-denied",
    "captcha",
    "challenge",
    "blocked",
]

COMMON_QUERY_KEYS = [
    "url", "u", "uri", "target", "dest", "destination", "redirect", "redirect_url",
    "returnUrl", "return_url", "merchant_url", "out", "to", "r", "q", "link", "href",
    "deep_link_value", "$fallback_url", "fallback_url", "$canonical_url", "canonical_url",
    "af_dp", "af_web_dp", "clickurl", "adurl", "camp", "murl", "redirectUrl",
]

NOISY_DOMAINS = {
    "facebook.com", "www.facebook.com",
    "instagram.com", "www.instagram.com",
    "twitter.com", "x.com", "www.x.com",
    "youtube.com", "www.youtube.com",
    "google.com", "www.google.com",
    "doubleclick.net", "www.doubleclick.net",
    "googletagmanager.com", "www.googletagmanager.com",
    # Namespace / spec URLs that often appear inside inline SVG/HTML and are not destinations.
    "w3.org", "www.w3.org",
    # JSON-LD vocabulary URLs commonly embedded in merchant HTML (not destinations).
    "schema.org", "www.schema.org",
}


def is_namespace_or_spec_url(url: str) -> bool:
    """
    Filter out URLs that show up in HTML/SVG namespaces (not click destinations).
    Example: http://www.w3.org/2000/svg
    """
    try:
        host = host_of(url)
        path = path_of(url)
        if host in {"w3.org", "www.w3.org"} and ("/2000/svg" in path or path.endswith("/svg")):
            return True
        # schema.org vocabulary endpoints (JSON-LD), not destinations.
        if host in {"schema.org", "www.schema.org"}:
            return True
        return False
    except Exception:
        return False

@dataclass
class Step:
    depth: int
    method: str
    url: str
    host: str
    kind: str
    status_code: Optional[int] = None
    elapsed_ms: Optional[int] = None
    note: Optional[str] = None

@dataclass
class ResolveResult:
    input_url: str
    final_url: Optional[str]
    final_kind: str
    confidence: str
    method_used: str
    reason: str
    elapsed_ms: int
    chain: List[Step]
    error: Optional[str] = None


def normalize_url(url: str) -> str:
    url = (url or "").strip().strip("<>")
    if not url:
        return url
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return html.unescape(url)


def clean_candidate_url(url: str) -> str:
    url = html.unescape(unquote(url or "")).strip().strip("'\"<>),;]")
    # Some HTML/JS blobs leave escaped slashes.
    url = url.replace("\\/", "/")
    return normalize_url(url)


def host_of(url: Optional[str]) -> str:
    try:
        return (urlparse(url or "").netloc or "").lower()
    except Exception:
        return ""


def path_of(url: Optional[str]) -> str:
    try:
        return (urlparse(url or "").path or "").lower()
    except Exception:
        return ""


def _looks_like_http_url(url: str) -> bool:
    """
    Prevent nonsense tokens (e.g., base64 blobs) from being normalized into bogus hosts.

    Requires http/https + a plausible hostname containing a dot.
    """
    u = str(url or "").strip()
    if not u.startswith(("http://", "https://")):
        return False
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if not host or "." not in host:
            return False
        if len(host) > 200:
            return False
        return True
    except Exception:
        return False


def _decode_samsclub_encoded_path_token(raw: str) -> Optional[str]:
    """
    Sam's Club anti-bot pages embed the intended path under ?url=<base64>.
    If decoded path starts with '/', rebuild https://www.samsclub.com{path}{query...}.
    """
    s = (raw or "").strip().strip("'\"")
    if not s:
        return None

    def _b64decode(x: str) -> Optional[bytes]:
        pad = "=" * ((4 - (len(x) % 4)) % 4)
        # Try URL-safe first, then standard.
        for variant in (x + pad, x.translate(str.maketrans("-_", "+/")) + pad):
            try:
                return base64.b64decode(variant, validate=False)
            except Exception:
                continue
        return None

    b = _b64decode(s)
    if not b:
        return None
    try:
        decoded = b.decode("utf-8", errors="ignore").strip()
    except Exception:
        return None

    if not decoded.startswith("/"):
        return None

    low = decoded.lower()
    if not any(low.startswith(p) for p in ("/ip/", "/p/", "/product")):
        return None

    return "https://www.samsclub.com" + decoded


def try_upgrade_samsclub_interstitial(url: str) -> Optional[str]:
    """Upgrade Sam's Club '/are-you-human' pages to the embedded destination URL when possible."""
    try:
        u = str(url or "").strip()
        if not u.startswith(("http://", "https://")):
            return None
        p = urlparse(u)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").lower()
        if host != "samsclub.com":
            return None
        if "/are-you-human" not in path:
            return None
        qs = parse_qs(p.query or "", keep_blank_values=True)
        vals = qs.get("url") or []
        if not vals:
            return None
        raw = str(vals[0] or "").strip()
        out = _decode_samsclub_encoded_path_token(raw)
        if out and _looks_like_http_url(out):
            return out
        return None
    except Exception:
        return None


def try_extract_linksynergy_murl(url: str) -> Optional[str]:
    """
    LinkSynergy deeplinks embed the real merchant destination in `murl=`.
    Prefer extracting this early to avoid noisy intermediate merchant bot checks.
    """
    try:
        u = str(url or "").strip()
        if not u.startswith(("http://", "https://")):
            return None
        host = host_of(u)
        if "linksynergy.com" not in host:
            return None
        qs = parse_qs(urlparse(u).query or "", keep_blank_values=True)
        vals = qs.get("murl") or []
        if not vals:
            return None
        raw = unquote(str(vals[0] or "").strip())
        c = clean_candidate_url(raw)
        return c if _looks_like_http_url(c) else None
    except Exception:
        return None


def try_extract_murl_from_linksynergy_response(resp: "requests.Response") -> Optional[str]:
    """
    After `requests` follows redirects, the LinkSynergy hop may only appear in `resp.history`.
    Scan history + final URL for an `murl=` destination.
    """
    try:
        candidates: List[str] = []
        for h in getattr(resp, "history", []) or []:
            try:
                candidates.append(str(getattr(h, "url", "") or ""))
            except Exception:
                continue
        try:
            candidates.append(str(getattr(resp, "url", "") or ""))
        except Exception:
            pass

        # De-dupe while preserving order (most recent link hops last).
        seen = set()
        ordered: List[str] = []
        for u in candidates:
            uu = str(u or "").strip()
            if not uu or uu in seen:
                continue
            seen.add(uu)
            ordered.append(uu)

        for u in reversed(ordered):
            m = try_extract_linksynergy_murl(u)
            if m:
                return m
        return None
    except Exception:
        return None


def host_matches_keyword(host: str, keyword: str) -> bool:
    host = host.lower()
    keyword = keyword.lower()
    return host == keyword or host.endswith("." + keyword) or keyword in host


def is_redirector_host(host: str) -> bool:
    if not host:
        return False
    if host in REDIRECTOR_EXACT_HOSTS:
        return True
    return any(host_matches_keyword(host, kw) for kw in REDIRECTOR_KEYWORDS)


def is_block_or_infra(url: Optional[str]) -> bool:
    host = host_of(url)
    path = path_of(url)
    if host in BLOCK_OR_INFRA_EXACT_HOSTS:
        return True
    if any(host.endswith("." + h) for h in BLOCK_OR_INFRA_EXACT_HOSTS):
        return True
    if any(k in path for k in BLOCK_PATH_KEYWORDS):
        return True
    # Bot/WAF interstitials are not reliable destinations for programmatic resolution.
    try:
        if host == "samsclub.com" and "/are-you-human" in path:
            return True
    except Exception:
        pass
    return False


def is_probably_file_or_asset(url: str) -> bool:
    path = path_of(url)
    return bool(re.search(r"\.(png|jpg|jpeg|gif|webp|svg|css|js|ico|woff|woff2|ttf|mp4|webm|pdf)(\?|$)", path))


def is_noisy_url(url: str) -> bool:
    host = host_of(url)
    return host in NOISY_DOMAINS or any(host.endswith("." + d) for d in NOISY_DOMAINS)


def classify_url(url: Optional[str]) -> str:
    if not url:
        return "empty"
    if is_block_or_infra(url):
        return "blocked_or_infra"
    host = host_of(url)
    if is_redirector_host(host):
        return "redirector_or_affiliate"
    if is_probably_file_or_asset(url):
        return "asset"
    return "likely_final_destination"


def is_final_destination(url: Optional[str]) -> bool:
    if not url:
        return False
    host = host_of(url)
    if not host:
        return False
    if is_block_or_infra(url):
        return False
    if is_redirector_host(host):
        return False
    if is_probably_file_or_asset(url):
        return False
    return True


def score_candidate(url: str) -> int:
    if not url or not url.startswith(("http://", "https://")):
        return -100
    if is_block_or_infra(url):
        return -90
    if is_namespace_or_spec_url(url):
        return -80
    if is_probably_file_or_asset(url):
        return -60
    if is_noisy_url(url):
        return -40
    host = host_of(url)
    score = 0
    if is_redirector_host(host):
        score -= 20
    else:
        score += 40
    path = path_of(url)
    if any(x in path for x in ["/ip/", "/p/", "/product", "/products/", "/dp/", "/itm/", "/sku", "/shop/"]):
        score += 25
    if len(path) > 5:
        score += 5
    return score


def find_urls_in_text(text: str) -> List[str]:
    if not text:
        return []
    candidates: List[str] = []
    patterns = [
        r"https?://[^\s\"'<>\\]+",
        r"window\.location(?:\.href)?\s*=\s*[\"'](https?://[^\"']+)[\"']",
        r"location\.replace\(\s*[\"'](https?://[^\"']+)[\"']\s*\)",
        r"location\.assign\(\s*[\"'](https?://[^\"']+)[\"']\s*\)",
        r"<meta[^>]+http-equiv=[\"']refresh[\"'][^>]+content=[\"'][^\"']*url=(https?://[^\"'>]+)",
        r"href=[\"'](https?://[^\"']+)[\"']",
        r"https%3A%2F%2F[^\"' <>{}\\]+",
        r"http%3A%2F%2F[^\"' <>{}\\]+",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            if isinstance(match, tuple):
                match = match[0]
            candidate = clean_candidate_url(match)
            if _looks_like_http_url(candidate):
                candidates.append(candidate)
    return dedupe_keep_order(candidates)


def dedupe_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def extract_any_url_from_query(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        candidates: List[str] = []

        # Prefer known redirect keys first.
        for key in COMMON_QUERY_KEYS:
            for actual_key, vals in qs.items():
                if actual_key == key or actual_key.lower() == key.lower():
                    for raw in vals:
                        c = clean_candidate_url(raw)
                        if _looks_like_http_url(c):
                            candidates.append(c)

        # Then scan all query values for embedded URLs.
        for vals in qs.values():
            for raw in vals:
                candidates.extend(find_urls_in_text(raw))

        candidates = dedupe_keep_order(candidates)
        if not candidates:
            return None
        return sorted(candidates, key=score_candidate, reverse=True)[0]
    except Exception:
        return None


def extract_best_url_from_html(text: str, base_url: str = "") -> Optional[str]:
    candidates = find_urls_in_text(text)
    if not candidates:
        return None

    # Also unwrap query params inside discovered URLs.
    expanded = []
    for c in candidates:
        q = extract_any_url_from_query(c)
        if q:
            expanded.append(q)
        expanded.append(c)

    expanded = [
        u
        for u in dedupe_keep_order(expanded)
        if _looks_like_http_url(u) and host_of(u) and host_of(u) != host_of(base_url)
    ]
    if not expanded:
        return None
    return sorted(expanded, key=score_candidate, reverse=True)[0]


def request_once(url: str, timeout: int) -> Tuple[Optional[requests.Response], List[Step], int, Optional[str]]:
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    session = requests.Session()
    session.headers.update(headers)
    start = time.perf_counter()
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
    except Exception as e:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return None, [], elapsed_ms, str(e)

    steps: List[Step] = []
    for hist in resp.history:
        steps.append(Step(
            depth=-1,
            method="http_redirect",
            url=hist.url,
            host=host_of(hist.url),
            kind=classify_url(hist.url),
            status_code=hist.status_code,
        ))
    steps.append(Step(
        depth=-1,
        method="requests_final",
        url=resp.url,
        host=host_of(resp.url),
        kind=classify_url(resp.url),
        status_code=resp.status_code,
        elapsed_ms=elapsed_ms,
    ))
    return resp, steps, elapsed_ms, None


def playwright_follow(url: str, timeout_ms: int, profile_dir: str, headed: bool, settle_ms: int) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None, None, "Playwright not installed. Run install_requirements.bat first."

    start = time.perf_counter()
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(Path(profile_dir).resolve()),
                headless=not headed,
                args=["--disable-blink-features=AutomationControlled", "--disable-http2"],
                user_agent=DEFAULT_UA,
                viewport={"width": 1400, "height": 900},
            )
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(settle_ms)

            final_url = page.url
            content = page.content() or ""

            poll_end = time.time() + 10
            while time.time() < poll_end:
                if is_final_destination(page.url):
                    final_url = page.url
                    break
                page.wait_for_timeout(1000)
                final_url = page.url
                content = page.content() or ""

            if not is_final_destination(final_url):
                extracted = extract_best_url_from_html(content, final_url)
                if extracted:
                    final_url = extracted

            context.close()

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return final_url, elapsed_ms, None
    except Exception as e:
        return None, None, f"Playwright error: {e}"


def confidence_for(final_url: Optional[str], method: str, chain: List[Step]) -> Tuple[str, str]:
    if not final_url:
        return "low", "No final URL was resolved."
    if is_block_or_infra(final_url):
        return "low", "Resolved URL is a block/infra page, not a merchant destination."
    if is_final_destination(final_url):
        if method in {"query_extract", "html_extract", "playwright", "samsclub_path_decode", "linksynergy_murl"}:
            return "high", "Final URL no longer looks like a redirector/tracker and was extracted through a strong unwrap method."
        return "high", "HTTP redirects ended on a likely merchant/final destination."
    if is_redirector_host(host_of(final_url)):
        return "low", "Still appears to be a redirector/tracker. Browser fallback or manual inspect is needed."
    return "medium", "Resolved to a non-block URL, but confidence is not perfect."


def resolve_universal(
    input_url: str,
    timeout: int = DEFAULT_TIMEOUT,
    max_depth: int = DEFAULT_MAX_DEPTH,
    use_playwright: bool = False,
    headed: bool = False,
    profile_dir: str = "./pw_profile",
    settle_ms: int = DEFAULT_SETTLE_MS,
) -> ResolveResult:
    start_total = time.perf_counter()
    original = normalize_url(input_url)
    current = original
    seen = set()
    chain: List[Step] = []
    last_method = "input"
    error_notes: List[str] = []

    for depth in range(max_depth):
        if not current:
            break
        if current in seen:
            chain.append(Step(depth, "loop_detected", current, host_of(current), classify_url(current), note="Already seen this URL."))
            break
        seen.add(current)

        chain.append(Step(depth, "current", current, host_of(current), classify_url(current)))

        # 1. Query-param unwrap before doing a network request.
        embedded = extract_any_url_from_query(current)
        if embedded and embedded != current:
            chain.append(Step(depth, "query_extract", embedded, host_of(embedded), classify_url(embedded), note="Embedded destination found in query params."))
            current = embedded
            last_method = "query_extract"
            continue

        # 2. HTTP redirects with requests.
        resp, req_steps, req_elapsed, req_err = request_once(current, timeout)
        for s in req_steps:
            s.depth = depth
            chain.append(s)
        if req_err:
            error_notes.append(f"requests error at depth {depth}: {req_err}")
            break
        if not resp:
            break

        # 2a. If we passed through a LinkSynergy deeplink, prefer its embedded `murl=` destination.
        ls_murl = try_extract_murl_from_linksynergy_response(resp)
        if ls_murl and ls_murl != current:
            chain.append(
                Step(
                    depth,
                    "linksynergy_murl",
                    ls_murl,
                    host_of(ls_murl),
                    classify_url(ls_murl),
                    note="Extracted embedded merchant URL from LinkSynergy deeplink (murl=).",
                )
            )
            current = ls_murl
            last_method = "linksynergy_murl"
            continue

        final = resp.url

        # 3a. Sam's Club anti-bot interstitial: decode embedded destination path when present.
        sams_up = try_upgrade_samsclub_interstitial(final)
        if sams_up and sams_up != final:
            chain.append(
                Step(
                    depth,
                    "samsclub_path_decode",
                    sams_up,
                    host_of(sams_up),
                    classify_url(sams_up),
                    note="Decoded Sam's Club destination from are-you-human url= parameter.",
                )
            )
            current = sams_up
            last_method = "samsclub_path_decode"
            continue

        # 3. Query unwrap after redirects.
        embedded = extract_any_url_from_query(final)
        if embedded and embedded != final:
            chain.append(Step(depth, "query_extract_after_redirect", embedded, host_of(embedded), classify_url(embedded), note="Embedded destination found after redirects."))
            current = embedded
            last_method = "query_extract"
            continue

        # 4. HTML/meta/JS unwrap.
        content_type = (resp.headers.get("content-type") or "").lower()
        if "text/html" in content_type or resp.text:
            html_url = extract_best_url_from_html(resp.text or "", final)
            if html_url and html_url != final and score_candidate(html_url) > score_candidate(final):
                chain.append(Step(depth, "html_extract", html_url, host_of(html_url), classify_url(html_url), note="Better destination found in HTML/meta/JS."))
                current = html_url
                last_method = "html_extract"
                continue

        # 5. Stop when likely final.
        if is_final_destination(final):
            current = final
            last_method = "requests"
            break

        # 6. If stuck and Playwright is enabled, try browser resolution.
        if use_playwright:
            pw_url, pw_ms, pw_err = playwright_follow(final, timeout * 1000, profile_dir, headed, settle_ms)
            if pw_err:
                error_notes.append(pw_err)
            if pw_url:
                chain.append(Step(depth, "playwright", pw_url, host_of(pw_url), classify_url(pw_url), elapsed_ms=pw_ms, note="Browser-based fallback."))
                current = pw_url
                last_method = "playwright"
                if is_final_destination(current):
                    break
                continue

        current = final
        last_method = "requests_stuck"
        break

    elapsed_total = int((time.perf_counter() - start_total) * 1000)
    confidence, reason = confidence_for(current, last_method, chain)

    return ResolveResult(
        input_url=original,
        final_url=current,
        final_kind=classify_url(current),
        confidence=confidence,
        method_used=last_method,
        reason=reason,
        elapsed_ms=elapsed_total,
        chain=chain,
        error=" | ".join(error_notes) if error_notes else None,
    )


def print_report(results: List[ResolveResult]) -> None:
    print("=" * 96)
    print("UNIVERSAL LINK RESOLVER REPORT V2")
    print("=" * 96)
    for idx, r in enumerate(results, 1):
        print(f"\n[{idx}] INPUT")
        print(f"  URL         : {r.input_url}")
        print("\n  FINAL")
        print(f"  URL         : {r.final_url}")
        print(f"  KIND        : {r.final_kind}")
        print(f"  CONFIDENCE  : {r.confidence}")
        print(f"  METHOD      : {r.method_used}")
        print(f"  ELAPSED MS  : {r.elapsed_ms}")
        print(f"  REASON      : {r.reason}")
        if r.error:
            print(f"  ERROR/NOTE  : {r.error}")
        print("\n  CHAIN")
        for s in r.chain:
            status = f" status={s.status_code}" if s.status_code is not None else ""
            elapsed = f" {s.elapsed_ms}ms" if s.elapsed_ms is not None else ""
            note = f" | {s.note}" if s.note else ""
            print(f"    - depth={s.depth} method={s.method}{status}{elapsed}")
            print(f"      {s.url}")
            print(f"      host={s.host} kind={s.kind}{note}")
        print("-" * 96)


def save_json(results: List[ResolveResult], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Universal affiliate/shortlink/tracker resolver.")
    p.add_argument("--url", action="append", default=[], help="URL to resolve. Can be used multiple times.")
    p.add_argument("--urls-file", help="Text file with one URL per line.")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    p.add_argument("--max-depth", type=int, default=DEFAULT_MAX_DEPTH)
    p.add_argument("--use-playwright", action="store_true", help="Use persistent browser fallback when requests gets stuck.")
    p.add_argument("--headed", action="store_true", help="Show browser window when using Playwright.")
    p.add_argument("--profile-dir", default="./pw_profile", help="Persistent Playwright profile directory.")
    p.add_argument("--settle-ms", type=int, default=DEFAULT_SETTLE_MS)
    p.add_argument("--json-out", help="Save full results to a JSON file.")
    return p.parse_args()


def load_urls(args: argparse.Namespace) -> List[str]:
    urls: List[str] = []
    urls.extend(args.url or [])
    if args.urls_file:
        with open(args.urls_file, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if raw and not raw.startswith("#"):
                    urls.append(raw)
    urls = [normalize_url(u) for u in urls if u.strip()]
    if not urls:
        raise SystemExit("No URLs provided. Use --url or --urls-file.")
    return urls


def main() -> int:
    args = parse_args()
    results = [
        resolve_universal(
            u,
            timeout=args.timeout,
            max_depth=args.max_depth,
            use_playwright=args.use_playwright,
            headed=args.headed,
            profile_dir=args.profile_dir,
            settle_ms=args.settle_ms,
        )
        for u in load_urls(args)
    ]
    print_report(results)
    if args.json_out:
        save_json(results, args.json_out)
        print(f"\nSaved JSON report to: {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
