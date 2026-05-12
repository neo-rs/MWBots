#!/usr/bin/env python3
"""
Canonical eBay sold-comps DOM scraper for Instorebotforwarder.

This replaces the old aiohttp/HTML/three-condition scrape path. It opens one
sold-listings grid search, reads the first real result cards from the DOM, and
returns structured price data. No alternate fetch/parsing fallback is attempted:
if first-8 DOM cards or prices are unavailable, callers must treat that as no
usable comps.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, quote_plus, urlencode, urlparse, urlunparse


DEFAULT_CDP_URL = "http://127.0.0.1:9222"


def _cfg_bool(cfg: Mapping[str, Any], key: str, default: bool) -> bool:
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _cfg_int(cfg: Mapping[str, Any], key: str, default: int, *, lo: int, hi: int) -> int:
    try:
        v = int(cfg.get(key, default))
    except Exception:
        v = int(default)
    return max(int(lo), min(int(v), int(hi)))


def _cfg_float(cfg: Mapping[str, Any], key: str, default: float, *, lo: float, hi: float) -> float:
    try:
        v = float(cfg.get(key, default))
    except Exception:
        v = float(default)
    return max(float(lo), min(float(v), float(hi)))


def safe_name(text: str, max_len: int = 80) -> str:
    text = re.sub(r"https?://", "", text.strip(), flags=re.I)
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
    text = text.strip("._-") or "ebay"
    return text[:max_len]


def ebay_keyword_from_title(title: str, *, max_words: int = 0) -> str:
    s = (title or "").strip()
    if not s:
        return ""
    s = re.sub(r"[^\w\s:|+-]", "", s, flags=re.IGNORECASE)
    s = " ".join(s.split()).strip()
    if max_words > 0:
        words = s.split()
        s = " ".join(words[:max_words]) if words else s
    return s


def ebay_sold_search_url(keyword: str) -> str:
    q = (keyword or "").strip()
    if not q:
        return ""
    encoded_nkw = quote_plus(q)
    return (
        "https://www.ebay.com/sch/i.html"
        f"?_nkw={encoded_nkw}&LH_Sold=1&LH_Complete=1&LH_PrefLoc=1&_sop=15&_dmd=2"
    )


def is_ebay_search(url: str) -> bool:
    try:
        p = urlparse(url)
        h = (p.hostname or "").lower()
        return ("ebay." in h or h.endswith("ebay.com")) and "/sch/" in (p.path or "").lower()
    except Exception:
        return False


def ensure_ebay_grid_sold(url: str) -> str:
    if not is_ebay_search(url):
        return url
    try:
        p = urlparse(url)
        pairs = parse_qsl(p.query, keep_blank_values=True)
        existing = {k.lower(): i for i, (k, _) in enumerate(pairs)}

        def set_param(k: str, v: str) -> None:
            idx = existing.get(k.lower())
            if idx is None:
                existing[k.lower()] = len(pairs)
                pairs.append((k, v))
            else:
                pairs[idx] = (pairs[idx][0], v)

        set_param("_dmd", "2")
        set_param("LH_Sold", "1")
        set_param("LH_Complete", "1")
        return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(pairs, doseq=True), p.fragment))
    except Exception:
        return url


def parse_price(raw: str) -> Optional[float]:
    s = (raw or "").strip()
    if not s:
        return None
    # Prefer USD-style tokens. Keep comma thousands separators valid.
    m = re.search(r"(?:US\s*)?\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+(?:\.\d{2})?)", s, re.I)
    if not m:
        m = re.search(r"(\d{1,3}(?:,\d{3})*(?:\.\d{2})?|\d+(?:\.\d{2})?)", s)
    if not m:
        return None
    try:
        v = float((m.group(1) or "").replace(",", ""))
    except Exception:
        return None
    return v if 0.01 <= v <= 1_000_000 else None


def format_price(v: float) -> str:
    return f"${float(v):,.2f}"


def ebay_html_looks_blocked(text: str) -> bool:
    t = (text or "").lower()
    return any(
        n in t
        for n in (
            "access denied",
            "you don't have permission to access",
            "pardon our interruption",
            "why have i been blocked",
            "reference #",
            "robot check",
            "verify you are human",
            "attention required",
            "request blocked",
        )
    )


def pick_chrome_exe() -> Optional[str]:
    if sys.platform.startswith("win"):
        candidates = [
            Path(os.environ.get("PROGRAMFILES", "C:/Program Files")) / "Google/Chrome/Application/chrome.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Google/Chrome/Application/chrome.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Google/Chrome/Application/chrome.exe",
        ]
    elif sys.platform.startswith("darwin"):
        candidates = [Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")]
    else:
        candidates = [
            Path("/opt/google/chrome/google-chrome"),
            Path("/usr/bin/google-chrome-stable"),
            Path("/usr/bin/google-chrome"),
            Path("/usr/bin/chromium"),
            Path("/usr/bin/chromium-browser"),
        ]
    for c in candidates:
        try:
            if c and c.exists():
                return str(c)
        except Exception:
            continue
    return None


def resolve_chrome_launch_kwargs(cfg: Mapping[str, Any]) -> Tuple[Dict[str, Any], str]:
    kwargs: Dict[str, Any] = {"headless": _cfg_bool(cfg, "ebay_first8_headless", True)}
    args: List[str] = []
    if sys.platform.startswith("linux"):
        args.extend(["--no-sandbox", "--disable-dev-shm-usage"])
    if _cfg_bool(cfg, "ebay_first8_stealth_launch", True):
        args.extend(["--disable-blink-features=AutomationControlled", "--disable-infobars"])

    exe = str(cfg.get("ebay_first8_chrome_exe") or "").strip()
    exe = exe or (os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_EXE") or "").strip()
    if exe:
        kwargs["executable_path"] = exe
        resolved = exe
    else:
        picked = pick_chrome_exe()
        if picked:
            kwargs["executable_path"] = picked
            resolved = picked
        elif _cfg_bool(cfg, "ebay_first8_allow_playwright_chromium", False):
            resolved = "playwright bundled chromium"
        else:
            kwargs["channel"] = "chrome"
            resolved = "channel=chrome"

    if args:
        kwargs["args"] = args
    return kwargs, resolved


def _ensure_headed_linux_display(cfg: Mapping[str, Any]) -> Tuple[Optional[subprocess.Popen[Any]], Optional[str]]:
    """
    Match Windows visible-Chrome mode on headless Linux services by providing a display.

    `run_interactive_visible_chrome.bat` launches installed Chrome with headless=False.
    Windows already has a desktop. Oracle/systemd does not, so the exact Linux equivalent
    is headed Chrome on an X display. If DISPLAY is missing, start Xvfb and set DISPLAY.
    """
    if not sys.platform.startswith("linux"):
        return None, None
    if _cfg_bool(cfg, "ebay_first8_headless", True):
        return None, None
    if os.environ.get("DISPLAY"):
        return None, None
    if not _cfg_bool(cfg, "ebay_first8_xvfb_enabled", True):
        return None, "DISPLAY is not set and ebay_first8_xvfb_enabled=false"

    xvfb = shutil.which("Xvfb")
    if not xvfb:
        return None, "Xvfb not installed; install xvfb for headed Chrome under systemd"

    display = str(cfg.get("ebay_first8_xvfb_display") or ":99").strip() or ":99"
    screen = str(cfg.get("ebay_first8_xvfb_screen") or "1680x1500x24").strip() or "1680x1500x24"
    wait_s = _cfg_float(cfg, "ebay_first8_xvfb_wait_s", 3.0, lo=0.2, hi=20.0)
    poll_s = _cfg_float(cfg, "ebay_first8_xvfb_poll_s", 0.1, lo=0.05, hi=1.0)
    socket_path = Path(f"/tmp/.X11-unix/X{display.lstrip(':')}")
    if socket_path.exists():
        os.environ["DISPLAY"] = display
        return None, None

    proc = subprocess.Popen(  # noqa: S603 - controlled executable/args from local config for bot runtime.
        [xvfb, display, "-screen", "0", screen, "-nolisten", "tcp"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return None, f"Xvfb exited early with code {proc.returncode}"
        if socket_path.exists():
            os.environ["DISPLAY"] = display
            return proc, None
        time.sleep(poll_s)
    if proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
    return None, f"Xvfb did not create {socket_path} within {wait_s:.1f}s"


async def _wait_for_results_or_empty(page: Any, timeout_ms: int) -> str:
    try:
        await page.wait_for_selector("body", timeout=timeout_ms)
    except Exception:
        return "timeout"
    try:
        await page.wait_for_function(
            """
            () => {
              const cards = document.querySelectorAll('ul.srp-results li.s-card').length;
              const txt = (document.body.innerText || '').toLowerCase();
              return cards > 0 || txt.includes('0 results') || txt.includes('no exact matches') || txt.includes('no results');
            }
            """,
            timeout=timeout_ms,
        )
    except Exception:
        return "timeout"
    count = await page.locator("ul.srp-results li.s-card").count()
    return "results" if count > 0 else "empty"


async def _extract_cards(page: Any, limit: int, min_price: float) -> Tuple[List[Dict[str, Any]], int]:
    rows = await page.evaluate(
        """
        (lim) => {
          const nodes = Array.from(document.querySelectorAll('ul.srp-results li.s-card')).slice(0, lim);
          return nodes.map((card, idx) => {
            const textOf = (sel) => {
              const el = card.querySelector(sel);
              return el ? (el.innerText || el.textContent || '').trim() : '';
            };
            const title = textOf('.s-item__title') || textOf('.s-card__title') || textOf('h3');
            const price = textOf('.s-item__price') || textOf('.s-card__price') || textOf('[class*="price"]');
            const condition = textOf('.SECONDARY_INFO') || textOf('[class*="condition"]');
            const shipping = textOf('.s-item__shipping, .s-card__shipping, [class*="shipping"]');
            const sold = textOf('.s-item__caption, .s-card__caption, [class*="sold"]');
            const a = card.querySelector('a[href*="/itm/"]') || card.querySelector('a[href]');
            const r = card.getBoundingClientRect();
            return {
              rank: idx + 1,
              title,
              price_text: price,
              condition,
              shipping,
              sold,
              url: a ? a.href : '',
              raw_text: (card.innerText || '').trim().slice(0, 1200),
              box: {x: r.x, y: r.y, width: r.width, height: r.height}
            };
          });
        }
        """,
        int(limit),
    )
    total_count = await page.locator("ul.srp-results li.s-card").count()
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        price = parse_price(str(row.get("price_text") or row.get("raw_text") or ""))
        if price is None or price < float(min_price):
            continue
        item = dict(row)
        item["price"] = price
        item["price_display"] = format_price(price)
        out.append(item)
    return out, int(total_count or 0)


async def _get_first_card_boxes(page: Any, limit: int) -> Tuple[List[Dict[str, float]], int]:
    """
    Product cards only (`li.s-card`); excludes river banners like "Have one to sell?".

    Measure all cards in one DOM pass at scroll 0. Per-card scrolling mixes
    coordinates from different scroll states and can cut off lower rows.
    """
    total_count = await page.locator("ul.srp-results li.s-card").count()
    take = min(int(limit), int(total_count or 0))
    if take <= 0:
        return [], int(total_count or 0)
    boxes_raw = await page.evaluate(
        """
        (lim) => {
          const nodes = document.querySelectorAll('ul.srp-results li.s-card');
          const out = [];
          const n = Math.min(lim, nodes.length);
          for (let i = 0; i < n; i++) {
            const r = nodes[i].getBoundingClientRect();
            out.push({x: r.x, y: r.y, width: r.width, height: r.height});
          }
          return out;
        }
        """,
        take,
    )
    boxes: List[Dict[str, float]] = []
    for box in boxes_raw or []:
        try:
            w = float(box.get("width", 0))
            h = float(box.get("height", 0))
            if w > 20 and h > 20:
                boxes.append({"x": float(box["x"]), "y": float(box["y"]), "width": w, "height": h})
        except Exception:
            continue
    return boxes, int(total_count or 0)


def merge_boxes(boxes: List[Dict[str, float]], padding: int) -> Dict[str, float]:
    """
    Merge a list of bounding boxes (`x`, `y`, `width`, `height`) into a single
    enclosing rectangle, expanded by `padding` pixels on every side. The result
    is clamped to non-negative origin so it is safe to pass directly to
    Playwright's `page.screenshot(clip=...)`.

    Public API: imported by sibling DOM-screenshot scrapers (e.g. the Amazon
    buybox screenshot module) so we don't duplicate the merging math.
    """
    x = min(b["x"] for b in boxes) - padding
    y = min(b["y"] for b in boxes) - padding
    right = max(b["x"] + b["width"] for b in boxes) + padding
    bottom = max(b["y"] + b["height"] for b in boxes) + padding
    return {"x": max(0, x), "y": max(0, y), "width": max(1, right - max(0, x)), "height": max(1, bottom - max(0, y))}


async def fetch_first8_sold_comps(title: str, cfg: Mapping[str, Any], *, bot_dir: Path) -> Dict[str, Any]:
    keyword = ebay_keyword_from_title(
        title,
        max_words=_cfg_int(cfg, "ebay_max_search_words", 20, lo=0, hi=80),
    )
    if not keyword:
        return {"status": "no_keyword", "reason": "empty keyword", "keyword": "", "listings": []}

    url = ensure_ebay_grid_sold(ebay_sold_search_url(keyword))
    limit = _cfg_int(cfg, "ebay_first8_limit", 8, lo=1, hi=8)
    min_price = _cfg_float(cfg, "ebay_min_sold_price", 5.0, lo=0.01, hi=100000.0)
    result: Dict[str, Any] = {
        "status": "unknown",
        "keyword": keyword,
        "url": url,
        "opened_url": url,
        "listings": [],
        "prices": [],
        "screenshot_path": "",
        "detected_cards": 0,
        "used_cards": 0,
    }

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        result.update({"status": "browser_error", "reason": f"playwright import failed: {str(exc)[:160]}"})
        return result

    viewport_width = _cfg_int(cfg, "ebay_first8_viewport_width", 1680, lo=800, hi=3200)
    viewport_height = _cfg_int(cfg, "ebay_first8_viewport_height", 1500, lo=700, hi=3200)
    goto_timeout_ms = _cfg_int(cfg, "ebay_first8_goto_timeout_ms", 90000, lo=5000, hi=180000)
    networkidle_timeout_ms = _cfg_int(cfg, "ebay_first8_networkidle_timeout_ms", 15000, lo=1000, hi=60000)
    results_timeout_ms = _cfg_int(cfg, "ebay_first8_results_timeout_ms", 20000, lo=1000, hi=90000)
    extra_wait_s = _cfg_float(cfg, "ebay_first8_extra_wait_s", 2.0, lo=0.0, hi=30.0)
    padding = _cfg_int(cfg, "ebay_first8_screenshot_padding", 12, lo=0, hi=80)
    screenshot_enabled = _cfg_bool(cfg, "ebay_first8_screenshot_enabled", True)
    xvfb_proc: Optional[subprocess.Popen[Any]] = None
    xvfb_err: Optional[str] = None
    if not _cfg_bool(cfg, "ebay_first8_connect_cdp", False):
        xvfb_proc, xvfb_err = _ensure_headed_linux_display(cfg)
        if xvfb_err:
            result.update({"status": "browser_error", "reason": xvfb_err})
            return result
        if os.environ.get("DISPLAY"):
            result["display"] = os.environ.get("DISPLAY")

    async with async_playwright() as p:
        browser = None
        page = None
        try:
            if _cfg_bool(cfg, "ebay_first8_connect_cdp", False):
                browser = await p.chromium.connect_over_cdp(str(cfg.get("ebay_first8_cdp_url") or DEFAULT_CDP_URL))
            else:
                launch_kwargs, resolved = resolve_chrome_launch_kwargs(cfg)
                result["browser"] = resolved
                browser = await p.chromium.launch(**launch_kwargs)

            context = browser.contexts[0] if browser.contexts else await browser.new_context(
                viewport={"width": viewport_width, "height": viewport_height},
                device_scale_factor=1,
                is_mobile=False,
            )
            page = await context.new_page()
            try:
                await page.set_viewport_size({"width": viewport_width, "height": viewport_height})
            except Exception:
                pass
            await page.goto(url, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            result["opened_url"] = page.url
            try:
                await page.wait_for_load_state("networkidle", timeout=networkidle_timeout_ms)
            except Exception:
                pass
            if extra_wait_s > 0:
                await page.wait_for_timeout(int(extra_wait_s * 1000))
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(300)

            body_txt = await page.evaluate(
                "() => (document.body && document.body.innerText) ? document.body.innerText.slice(0, 16000) : ''"
            )
            if ebay_html_looks_blocked(body_txt):
                result.update({"status": "blocked", "reason": "ebay returned bot/interstitial page"})
                return result

            state = await _wait_for_results_or_empty(page, results_timeout_ms)
            listings, detected_cards = await _extract_cards(page, limit, min_price)
            result["state"] = state
            result["detected_cards"] = detected_cards
            result["used_cards"] = len(listings)
            result["listings"] = listings
            result["prices"] = [x["price"] for x in listings]

            if not listings:
                status = "no_cards" if detected_cards <= 0 else "no_prices"
                result.update({"status": status, "reason": f"detected_cards={detected_cards}; prices_above_min=0"})
                return result

            # Mark success BEFORE the screenshot/meta write, so the on-disk JSON
            # sidecar reflects the real outcome instead of "unknown".
            result["status"] = "ok"

            if screenshot_enabled:
                boxes, screenshot_total_count = await _get_first_card_boxes(page, limit)
                if boxes:
                    clip_try = merge_boxes(boxes, padding)
                    try:
                        vh = int(await page.evaluate("() => window.innerHeight"))
                        need_h = int(clip_try["y"] + clip_try["height"]) + 24
                        if need_h > vh:
                            vw = int(await page.evaluate("() => window.innerWidth"))
                            new_h = min(max(need_h, viewport_height), 3200)
                            await page.set_viewport_size({"width": vw, "height": new_h})
                            await page.wait_for_timeout(150)
                            await page.evaluate("window.scrollTo(0, 0)")
                            await page.wait_for_timeout(200)
                            boxes, screenshot_total_count = await _get_first_card_boxes(page, limit)
                    except Exception:
                        pass
                if boxes:
                    out_dir_raw = str(cfg.get("ebay_first8_output_dir") or "ebay_screenshots").strip()
                    out_dir = Path(out_dir_raw)
                    if not out_dir.is_absolute():
                        out_dir = bot_dir / out_dir
                    out_dir.mkdir(parents=True, exist_ok=True)
                    base = out_dir / f"{safe_name(url)}_{int(time.time())}"
                    clip = merge_boxes(boxes, padding)
                    dims = await page.evaluate(
                        """
                        () => ({
                          width: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth, window.innerWidth),
                          height: Math.max(document.documentElement.scrollHeight, document.body.scrollHeight, window.innerHeight)
                        })
                        """
                    )
                    clip["width"] = min(clip["width"], max(1, float(dims.get("width", clip["width"])) - clip["x"]))
                    clip["height"] = min(clip["height"], max(1, float(dims.get("height", clip["height"])) - clip["y"]))
                    shot = str(base) + f"_first_{len(boxes)}.png"
                    await page.screenshot(path=shot, full_page=False, clip=clip)
                    result["screenshot_path"] = shot
                    result["screenshot_cards"] = len(boxes)
                    result["screenshot_total_cards"] = screenshot_total_count
                    meta_path = str(base) + ".json"
                    Path(meta_path).write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
                    result["meta_path"] = meta_path
            return result
        except Exception as exc:
            result.update({"status": "browser_error", "reason": str(exc)[:220]})
            return result
        finally:
            # Always close the scrape's tab, including the CDP-attach case where
            # the trusted Chrome must stay alive. This is the only place that
            # closes `page`, so every code path (ok, blocked, no_cards, exception)
            # goes through exactly one close.
            try:
                if page is not None:
                    await page.close()
            except Exception:
                pass
            try:
                if browser is not None and not _cfg_bool(cfg, "ebay_first8_connect_cdp", False):
                    await browser.close()
            except Exception:
                pass
            if xvfb_proc is not None and xvfb_proc.poll() is None:
                try:
                    xvfb_proc.terminate()
                except Exception:
                    pass
