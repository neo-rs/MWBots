#!/usr/bin/env python3
"""
Canonical Amazon product-page price/buybox DOM screenshot for Instorebotforwarder.

Companion to `ebay_first8_dom_comps.py`. Where the eBay module captures the
sold-listings grid, this module opens an Amazon product detail page and saves
a rectangular screenshot containing the price/coupon/savings block and the
buybox column (Add to Cart / Buy Now / Ships from / Sold by / etc.).

Default capture regions (see `amazon_buybox_screenshot_layout`):
  - **Left (`compact_pdp`):** main image + thumbnail strip.
  - **Center (`compact_pdp`):** title, ratings, price (center column).
  - **Right:** from `#availability` through Add to Cart / Buy Now / Ships from — no
    protection-plan block below.
  - **Left (`side_by_side` legacy):** price block only + buybox.

Layout modes:
  - `side_by_side` (default) — price strip + buybox (legacy; white gap under price).
  - `compact_pdp` / `product_card` — three columns like a net PDP card (image | info | buybox).
  - `separate` — two PNGs (price + in-stock) for the Discord image grid.
  - `merged` — legacy single bounding box (pulls in extra page chrome).

This module ALWAYS prefers the same trusted CDP Chrome the eBay scraper uses
(127.0.0.1:9222 by default). That keeps a single warmed profile across
scrapes and avoids spawning a second Chrome per Amazon hit. If CDP is not
enabled, it falls back to the same launch path the eBay module uses
(`resolve_chrome_launch_kwargs`) so we don't duplicate Chrome-binary
discovery logic.

Public surface:
  - `capture_amazon_buybox_screenshot(url, cfg, *, bot_dir) -> Dict[str, Any]`
  - `amazon_html_looks_blocked(text) -> bool`

Status values returned in the result dict:
  ok            - screenshot saved successfully
  no_targets    - none of the configured selectors resolved to a visible element
  blocked       - Amazon returned a captcha / robot-check / interstitial page
  browser_error - playwright import / CDP connect / launch failure
  unknown       - default; should never appear in a saved JSON sidecar
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# Reuse canonical helpers from the eBay scraper (single source of truth for
# Chrome-binary discovery, headed-Linux display, and bounding-box merging).
from ebay_first8_dom_comps import (  # type: ignore
    _cfg_bool,
    _cfg_float,
    _cfg_int,
    _ensure_headed_linux_display,
    acquire_scrape_browser_context,
    connect_chromium_over_cdp,
    merge_boxes,
    pick_chrome_exe,  # noqa: F401  re-exported for callers if needed
    prune_screenshot_dir,
    resolve_chrome_launch_kwargs,
    safe_name,
)


DEFAULT_CDP_URL = "http://127.0.0.1:9222"
# Tight price block first; full #apex_desktop is last-resort (includes variant grids).
DEFAULT_LEFT_SELECTORS: Tuple[str, ...] = (
    "#corePriceDisplay_desktop_feature_div",
    "#corePrice_feature_div",
    "#apex_desktop",
)
LEGACY_RIGHT_SELECTOR = "form#addToCart"
# Legacy: outlined every buybox control (In Stock, qty, buttons) — too noisy.
HIGHLIGHT_RIGHT_FROM_IN_STOCK: Tuple[str, ...] = (
    "#availability",
    "#selectQuantity",
    "#addToCart_feature_div",
    "#buyNow_feature_div",
    "#add-to-cart-button",
    "#buy-now-button",
)
# Single parent used for the one red outline (not per-field outlines).
DEFAULT_HIGHLIGHT_PRICE_BLOCK_SELECTORS: Tuple[str, ...] = (
    "#corePriceDisplay_desktop_feature_div",
    "#corePrice_feature_div",
    "#apex_desktop",
)
DEFAULT_HIGHLIGHT_COLOR = "#e3382f"
DEFAULT_HIGHLIGHT_THICKNESS_PX = 3
DEFAULT_OUTPUT_DIR = "amazon_screenshots"


# ---------------------------------------------------------------------------
# Config getters that gracefully fall back to the eBay CDP settings so a user
# who already has the trusted Chrome wired up for eBay does not need to
# duplicate keys to enable Amazon screenshots.
# ---------------------------------------------------------------------------

def _connect_cdp_enabled(cfg: Mapping[str, Any]) -> bool:
    if "amazon_buybox_connect_cdp" in cfg:
        return _cfg_bool(cfg, "amazon_buybox_connect_cdp", False)
    return _cfg_bool(cfg, "ebay_first8_connect_cdp", False)


def _connect_cdp_url(cfg: Mapping[str, Any]) -> str:
    raw = str(cfg.get("amazon_buybox_cdp_url") or "").strip()
    if raw:
        return raw
    raw = str(cfg.get("ebay_first8_cdp_url") or "").strip()
    return raw or DEFAULT_CDP_URL


def _right_from_in_stock_enabled(cfg: Mapping[str, Any]) -> bool:
    """When true (default), right column clip starts at In Stock, not full form."""
    v = cfg.get("amazon_buybox_right_from_in_stock", True)
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return True


def _screenshot_layout_mode(cfg: Mapping[str, Any]) -> str:
    """
    compact_pdp   — image | title+price | compact buybox (three columns, top-aligned).
    product_card  — alias for compact_pdp.
    side_by_side  — legacy price strip + buybox.
    merged        — legacy single bounding box (not recommended).
    separate      — part PNGs only, no stitch.
    """
    raw = str(cfg.get("amazon_buybox_screenshot_layout") or "side_by_side").strip().lower()
    if raw in {"merged", "merge", "single"}:
        return "merged"
    if raw in {"separate", "dual", "split"}:
        return "separate"
    if raw in {"side_by_side", "side-by-side", "price_buybox"}:
        return "side_by_side"
    if raw in {"product_card", "compact_pdp", "compact", "net_pdp"}:
        return "compact_pdp"
    return "side_by_side"


def _layout_uses_compact_pdp(cfg: Mapping[str, Any]) -> bool:
    return _screenshot_layout_mode(cfg) == "compact_pdp"


def _resolve_left_selectors(cfg: Mapping[str, Any]) -> List[str]:
    """Left/center price block selectors (`form#addToCart` is never used here)."""
    raw = cfg.get("amazon_buybox_selectors", None)
    if isinstance(raw, (list, tuple)) and raw:
        out = [
            str(s).strip()
            for s in raw
            if str(s).strip() and str(s).strip() != LEGACY_RIGHT_SELECTOR
        ]
        if out:
            return out
    return list(DEFAULT_LEFT_SELECTORS)


def _highlight_price_only_enabled(cfg: Mapping[str, Any]) -> bool:
    """When true (default), red outlines only price / discount / coupon DOM — not buy buttons."""
    v = cfg.get("amazon_buybox_highlight_price_only", True)
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return True


def _highlight_block_selectors(cfg: Mapping[str, Any]) -> List[str]:
    raw = cfg.get("amazon_buybox_highlight_selectors")
    if isinstance(raw, (list, tuple)) and raw:
        return [str(s).strip() for s in raw if str(s).strip()]
    return list(DEFAULT_HIGHLIGHT_PRICE_BLOCK_SELECTORS)


def _highlight_selectors_for_capture(cfg: Mapping[str, Any]) -> List[str]:
    if _highlight_price_only_enabled(cfg):
        return _highlight_block_selectors(cfg)
    left = _resolve_left_selectors(cfg)
    if _right_from_in_stock_enabled(cfg):
        return left + list(HIGHLIGHT_RIGHT_FROM_IN_STOCK)
    return left + [LEGACY_RIGHT_SELECTOR]


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

async def _dismiss_amazon_page_chrome(page: Any) -> bool:
    """
    Best-effort clicks for Amazon soft gates (Continue shopping, zip modal, glow).
    Returns True when a control was clicked.
    """
    try:
        clicked = await page.evaluate(
            """
            () => {
              const clickEl = (el) => {
                if (!el) return false;
                try { el.click(); return true; } catch (e) { return false; }
              };
              const byText = () => {
                const want = ['continue shopping', 'continue'];
                for (const el of document.querySelectorAll('button, a, input[type=submit], span.a-button-inner')) {
                  const t = ((el.innerText || el.value || '') + '').trim().toLowerCase();
                  if (want.includes(t)) return clickEl(el.closest('button, a, input') || el);
                }
                return false;
              };
              const picks = [
                'form[action*="validateCaptcha"] button',
                'button[type=submit]',
                '#GLUXConfirmClose',
                'button[name="glowDoneButton"]',
                '#a-popover-close',
                'input[data-action-type="SELECT_LOCATION"]',
                'a[href="/ref=cs_503_logo"]',
              ];
              if (byText()) return true;
              for (const sel of picks) {
                const el = document.querySelector(sel);
                if (clickEl(el)) return true;
              }
              return false;
            }
            """
        )
        return bool(clicked)
    except Exception:
        return False


def amazon_html_looks_blocked(text: str) -> bool:
    """
    Best-effort Amazon interstitial / robot-check detector. Matches what
    Amazon shows on automation/datacenter IPs and inside their CAPTCHA flow.
    """
    t = (text or "").lower()
    return any(
        n in t
        for n in (
            "to discuss automated access to amazon data",
            "sorry, we just need to make sure you're not a robot",
            "type the characters you see in this image",
            "enter the characters you see below",
            "your browser does not have javascript enabled",
            "automated access to our data",
            "robot check",
            "click the button below to continue shopping",
        )
    )


async def _highlight_targets(page: Any, selectors: Sequence[str], color: str, thickness: int) -> None:
    """Inject an outline on each matching element so the captured PNG shows it."""
    safe_color = (color or DEFAULT_HIGHLIGHT_COLOR).strip() or DEFAULT_HIGHLIGHT_COLOR
    safe_thickness = max(1, int(thickness or DEFAULT_HIGHLIGHT_THICKNESS_PX))
    try:
        await page.evaluate(
            """
            (args) => {
              const { selectors, color, thickness } = args;
              const style = `${thickness}px solid ${color}`;
              selectors.forEach((sel) => {
                document.querySelectorAll(sel).forEach((el) => {
                  el.style.outline = style;
                  el.style.outlineOffset = '2px';
                });
              });
            }
            """,
            {"selectors": list(selectors), "color": safe_color, "thickness": safe_thickness},
        )
    except Exception:
        # Outline is cosmetic; never fail the screenshot because the CSS inject
        # threw (e.g. CSP-restricted page would still allow inline styles).
        pass


async def _highlight_single_price_block(
    page: Any,
    cfg: Mapping[str, Any],
    color: str,
    thickness: int,
) -> None:
    """
    One red rectangle around the whole price block (discount + current + list)
    and an adjacent clip-coupon line when present — never per-field outlines.
    """
    safe_color = (color or DEFAULT_HIGHLIGHT_COLOR).strip() or DEFAULT_HIGHLIGHT_COLOR
    safe_thickness = max(1, int(thickness or DEFAULT_HIGHLIGHT_THICKNESS_PX))
    try:
        await page.evaluate(
            """
            (args) => {
              const { pickOrder, color, thickness } = args;
              const old = document.getElementById('aw-price-highlight-overlay');
              if (old) old.remove();
              let root = null;
              for (const sel of (pickOrder || [])) {
                const el = document.querySelector(sel);
                if (!el) continue;
                const r = el.getBoundingClientRect();
                if (r.width < 80 || r.height < 20) continue;
                if (sel === '#apex_desktop' && r.height > 520) continue;
                root = el;
                break;
              }
              if (!root) return;
              let x1 = root.getBoundingClientRect().left;
              let y1 = root.getBoundingClientRect().top;
              let x2 = x1 + root.getBoundingClientRect().width;
              let y2 = y1 + root.getBoundingClientRect().height;
              const mergeRect = (el) => {
                if (!el) return;
                const cr = el.getBoundingClientRect();
                if (cr.width < 20 || cr.height < 8) return;
                if (cr.top > y2 + 96) return;
                if (Math.abs(cr.left - x1) > Math.max(x2 - x1, 160)) return;
                x1 = Math.min(x1, cr.left);
                y1 = Math.min(y1, cr.top);
                x2 = Math.max(x2, cr.right);
                y2 = Math.max(y2, cr.bottom);
              };
              [
                '#promoPriceBlockMessage',
                '#couponBlockVertical',
                '.couponLabelText',
                '#applyClippableCoupon_couponSubText',
              ].forEach((sel) => mergeRect(document.querySelector(sel)));
              const pad = 2;
              const box = document.createElement('div');
              box.id = 'aw-price-highlight-overlay';
              box.style.position = 'fixed';
              box.style.left = (x1 - pad) + 'px';
              box.style.top = (y1 - pad) + 'px';
              box.style.width = Math.max(20, x2 - x1 + pad * 2) + 'px';
              box.style.height = Math.max(20, y2 - y1 + pad * 2) + 'px';
              box.style.outline = `${thickness}px solid ${color}`;
              box.style.outlineOffset = '0px';
              box.style.pointerEvents = 'none';
              box.style.zIndex = '2147483646';
              box.style.background = 'transparent';
              box.style.boxSizing = 'border-box';
              document.body.appendChild(box);
            }
            """,
            {
                "pickOrder": _highlight_block_selectors(cfg),
                "color": safe_color,
                "thickness": safe_thickness,
            },
        )
    except Exception:
        pass


def _is_unit_price_text(text: str) -> bool:
    """True when the string is a per-unit quote (e.g. $0.89 / ounce), not item price."""
    low = (text or "").lower().strip()
    if not low:
        return False
    if re.search(r"/\s*(ounce|ounces|oz\.?|lb\.?|pounds?|fl\s*oz|count|ct)\b", low):
        return True
    if re.search(r"\bper\s+(ounce|ounces|oz|lb|unit|count)\b", low):
        return True
    if "(" in low and "/" in low and re.search(r"\$\s*\d", low):
        return True
    return False


def _price_float_from_text(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = str(text).replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    return float(m.group(1)) if m else None


def evaluate_amazon_buybox_price_gate(
    buybox_result: Mapping[str, Any],
    *,
    source_current_value: Optional[float],
    source_before_value: Optional[float],
    promo_discount_pct: Optional[int],
    cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Strict Amazon price gate (single owner for buybox pass/fail rules).

    Returns dict with keys: ok, reason, failure_line, page_current_value,
    source_current_value, notes (list).
    """
    notes: List[str] = []
    pct_max = _cfg_float(cfg, "amazon_price_mismatch_max_pct", 10.0, lo=0.0, hi=50.0)
    abs_max = _cfg_float(cfg, "amazon_price_mismatch_max_abs_usd", 1.0, lo=0.0, hi=100.0)

    def _fail(reason: str, line: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "reason": reason,
            "failure_line": line,
            "page_current_value": page_cur,
            "source_current_value": source_current_value,
            "notes": notes,
        }

    page_cur: Optional[float] = None
    status = str(buybox_result.get("status") or "")
    if status != "ok":
        line = f"Amazon Gate: buybox capture failed ({status or 'unknown'}); sent to review."
        detail = str(buybox_result.get("reason") or "").strip()
        if detail:
            line = f"Amazon Gate: buybox capture failed ({status}: {detail[:120]}); sent to review."
        return {
            "ok": False,
            "reason": f"buybox_{status or 'failed'}",
            "failure_line": line,
            "page_current_value": None,
            "source_current_value": source_current_value,
            "notes": notes,
        }

    prices = buybox_result.get("prices") or {}
    if not isinstance(prices, dict):
        return _fail("buybox_no_prices", "Amazon Gate: no price data from live page; sent to review.")

    if prices.get("out_of_stock"):
        notes.append("oos_detected_on_page")
        return _fail(
            "amazon_out_of_stock",
            "Amazon Gate: product is out of stock on the live page; sent to review.",
        )

    if prices.get("unit_price_rejected"):
        notes.append("unit_price_only")
        return _fail(
            "amazon_unit_price",
            "Amazon Gate: could not read a trusted item price (unit price only); sent to review.",
        )

    try:
        page_cur = float(prices.get("current_price_value")) if prices.get("current_price_value") is not None else None
    except Exception:
        page_cur = None
    cur_text = str(prices.get("current_price_text") or "").strip()
    if page_cur is None or page_cur <= 0:
        if cur_text and not _is_unit_price_text(cur_text):
            page_cur = _price_float_from_text(cur_text)
        if page_cur is None or page_cur <= 0:
            return _fail(
                "amazon_no_price",
                "Amazon Gate: live page has no buyable item price; sent to review.",
            )

    if _is_unit_price_text(cur_text):
        return _fail(
            "amazon_unit_price",
            "Amazon Gate: extracted price looks like a per-unit quote, not item price; sent to review.",
        )

    notes.append(f"page_current=${page_cur:,.2f}")

    if source_current_value is None:
        notes.append("no_source_current_skip_mismatch")
        return {
            "ok": True,
            "reason": "ok_no_source_compare",
            "failure_line": "",
            "page_current_value": page_cur,
            "source_current_value": None,
            "notes": notes,
        }

    src = float(source_current_value)
    notes.append(f"source_current=${src:,.2f}")

    # Source higher than page: never fail (stale/high deal-site price is OK).
    if src > page_cur:
        notes.append("source_higher_than_page_ok")
        return {
            "ok": True,
            "reason": "ok_source_higher",
            "failure_line": "",
            "page_current_value": page_cur,
            "source_current_value": src,
            "notes": notes,
        }

    def _within_tolerance(candidate: float) -> bool:
        if candidate > page_cur:
            return True
        delta = page_cur - candidate
        if delta <= abs_max:
            return True
        if page_cur > 0 and (delta / page_cur) * 100.0 <= pct_max:
            return True
        return False

    try:
        page_list = float(prices.get("list_price_value")) if prices.get("list_price_value") is not None else None
    except Exception:
        page_list = None
    if page_list is None:
        page_list = source_before_value

    candidates: List[float] = [src]
    if promo_discount_pct and promo_discount_pct > 0:
        pct = float(promo_discount_pct)
        if page_list and page_list > 0:
            candidates.append(round(page_list * (1.0 - pct / 100.0), 2))
        if source_before_value and source_before_value > 0:
            candidates.append(round(float(source_before_value) * (1.0 - pct / 100.0), 2))

    for cand in candidates:
        if _within_tolerance(cand):
            notes.append(f"mismatch_ok_candidate=${cand:,.2f}")
            return {
                "ok": True,
                "reason": "ok_within_tolerance",
                "failure_line": "",
                "page_current_value": page_cur,
                "source_current_value": src,
                "notes": notes,
            }

    return _fail(
        "amazon_price_mismatch",
        (
            f"Amazon Gate: source ${src:,.2f} vs live page ${page_cur:,.2f} "
            f"(>{pct_max:.0f}% / >${abs_max:,.2f} apart); sent to review."
        ),
    )


_GATE_REASON_LABELS: Dict[str, str] = {
    "buybox_blocked": "Amazon blocked the page (captcha / robot check).",
    "buybox_no_targets": "Buybox screenshot anchors did not load in time.",
    "buybox_browser_error": "Browser/CDP could not open the product page.",
    "buybox_no_prices": "Screenshot ok but price JSON was missing.",
    "amazon_out_of_stock": "Availability says out of stock.",
    "amazon_unit_price": "Only a per-unit price was found (e.g. per ounce), not item price.",
    "amazon_no_price": "No item price matched our buybox DOM selectors.",
    "amazon_price_mismatch": "Source price is lower than the live page beyond tolerance.",
    "buybox_required_missing": "Strict mode requires buybox capture but it is disabled or URL missing.",
}


def format_amazon_gate_review_lines(
    *,
    gate: Mapping[str, Any],
    buybox_result: Mapping[str, Any],
    source_current: str = "",
    source_before: str = "",
    price_src: str = "",
    before_src: str = "",
    source_channel_label: str = "",
    asin: str = "",
    final_url: str = "",
) -> List[str]:
    """
    Operator-facing lines for enrich_failed embeds (source + live page diagnostics).
    """
    reason = str(gate.get("reason") or "unknown").strip()
    lines: List[str] = [
        "**Amazon Gate** — sent to review",
        str(gate.get("failure_line") or _GATE_REASON_LABELS.get(reason, reason)).strip(),
        f"Reason code: `{reason}`",
    ]
    if source_channel_label:
        lines.append(f"Source channel: {source_channel_label}")
    if asin:
        lines.append(f"ASIN: `{asin}`")
    src_cur = (source_current or "").strip()
    src_bef = (source_before or "").strip()
    if src_cur:
        lines.append(f"Source current ({price_src or 'message'}): **{src_cur}**")
    else:
        lines.append(f"Source current ({price_src or 'message'}): *(none parsed)*")
    if src_bef:
        lines.append(f"Source before ({before_src or 'message'}): **{src_bef}**")
    b_status = str(buybox_result.get("status") or "not_run").strip()
    lines.append(f"Buybox capture: `{b_status}`")
    detail = str(buybox_result.get("reason") or "").strip()
    if detail:
        lines.append(f"Buybox detail: {detail[:160]}")
    prices = buybox_result.get("prices") if isinstance(buybox_result.get("prices"), dict) else {}
    if prices:
        pt = str(prices.get("current_price_text") or "").strip()
        lt = str(prices.get("list_price_text") or "").strip()
        if pt:
            lines.append(f"Page current (DOM): **{pt}**")
        else:
            lines.append("Page current (DOM): *(empty)*")
        if lt:
            lines.append(f"Page list/before (DOM): **{lt}**")
        if prices.get("out_of_stock"):
            lines.append("Page availability: **out of stock**")
        if prices.get("unit_price_rejected"):
            lines.append("Page price probe: **unit price only rejected**")
        dom_notes = prices.get("notes") if isinstance(prices.get("notes"), list) else []
        if dom_notes:
            lines.append("DOM: " + " | ".join(str(x) for x in dom_notes[:6])[:400])
        probe = prices.get("price_probe") if isinstance(prices.get("price_probe"), list) else []
        if probe:
            lines.append("Prices seen in buybox: " + ", ".join(str(x) for x in probe[:8])[:400])
    page_cur = gate.get("page_current_value")
    if page_cur is not None:
        try:
            lines.append(f"Gate page current (numeric): **${float(page_cur):,.2f}**")
        except Exception:
            pass
    src_gate = gate.get("source_current_value")
    if src_gate is not None:
        try:
            lines.append(f"Gate source current (numeric): **${float(src_gate):,.2f}**")
        except Exception:
            pass
    gate_notes = gate.get("notes") if isinstance(gate.get("notes"), list) else []
    if gate_notes:
        lines.append("Gate: " + " | ".join(str(x) for x in gate_notes[:8])[:400])
    if final_url:
        lines.append(f"URL: {final_url[:200]}")
    return [ln for ln in lines if str(ln).strip()]


async def _extract_buybox_prices(page: Any) -> Dict[str, Any]:
    """
    Read the canonical price strings off the live Amazon product page DOM.

    Returns a dict shaped like:
      {
        "current_price_text": "$107.99",
        "current_price_value": 107.99,
        "list_price_text":    "$179.00",
        "list_price_value":   179.0,
        "discount_pct_text":  "-40%",
        "discount_pct_value": 40,
        "coupon_text":        "Apply 10% coupon",   # may be ""
        "notes":              ["current_from:#apex_desktop ...", ...]
      }

    Empty strings / None on fields that could not be resolved. Never raises.

    Why we extract here: the same Playwright session that captures the buybox
    PNG is the only place we know the exact price the screenshot shows. Pulling
    these values inside the same load means the embed text and the screenshot
    are guaranteed to agree (single source of truth for deal economics).
    """
    js = r"""
    () => {
      const out = {
        current_price_text: '', current_price_value: null,
        list_price_text: '',    list_price_value: null,
        discount_pct_text: '',  discount_pct_value: null,
        coupon_text: '',
        out_of_stock: false,
        unit_price_rejected: false,
        price_probe: [],
        notes: []
      };
      const text = (el) => el ? (el.textContent || '').replace(/\s+/g, ' ').trim() : '';
      const num = (s) => {
        if (!s) return null;
        const cleaned = String(s).replace(/[,]/g, '');
        const m = cleaned.match(/(\d+(?:\.\d+)?)/);
        return m ? parseFloat(m[1]) : null;
      };
      const isUnitPriceText = (t) => {
        if (!t) return false;
        const low = t.toLowerCase();
        if (/\/\s*(ounce|ounces|oz\.?|lb\.?|pounds?|fl\s*oz|count|ct)\b/.test(low)) return true;
        if (/\bper\s+(ounce|ounces|oz|lb|unit|count)\b/.test(low)) return true;
        if (low.indexOf('(') >= 0 && low.indexOf('/') >= 0 && /\$\s*\d/.test(low)) return true;
        return false;
      };
      const firstMatch = (selectors, key) => {
        for (const sel of selectors) {
          const el = document.querySelector(sel);
          const t = text(el);
          if (t && !isUnitPriceText(t)) {
            out.notes.push(key + '_from:' + sel);
            return t;
          }
        }
        return '';
      };
      const bestItemPriceFromBlock = (root, key) => {
        if (!root) return '';
        const off = root.querySelector('.a-offscreen');
        const t0 = text(off);
        if (t0 && !isUnitPriceText(t0)) {
          out.notes.push(key + '_from:priceToPay_offscreen');
          return t0;
        }
        let best = '';
        let bestVal = -1;
        root.querySelectorAll('.a-offscreen').forEach((el) => {
          const t = text(el);
          if (!t || isUnitPriceText(t)) return;
          const v = num(t);
          if (v != null && v > bestVal) {
            bestVal = v;
            best = t;
          }
        });
        if (best) out.notes.push(key + '_from:priceToPay_max_offscreen');
        return best;
      };

      const availEl = document.querySelector('#availability');
      const availTxt = text(availEl).toLowerCase();
      const oosPhrases = ['out of stock', 'temporarily out of stock'];
      out.out_of_stock = oosPhrases.some((p) => availTxt.includes(p));

      const priceFromWholeFraction = (root) => {
        if (!root) return '';
        const whole = root.querySelector('.a-price-whole');
        if (!whole) return '';
        const w = (whole.textContent || '').replace(/[^\d]/g, '');
        const fracEl = root.querySelector('.a-price-fraction');
        const f = fracEl ? (fracEl.textContent || '').replace(/[^\d]/g, '') : '00';
        if (!w) return '';
        return '$' + w + '.' + (f || '00');
      };

      const collectProbe = (root, tag) => {
        if (!root) return;
        root.querySelectorAll('.a-offscreen').forEach((el) => {
          const t = text(el);
          if (t) out.price_probe.push(tag + ':' + t);
        });
      };

      // CURRENT PRICE — buybox column first (true purchase price), then center column.
      const priceRoots = [
        document.querySelector('form#addToCart'),
        document.querySelector('#buybox'),
        document.querySelector('#rightCol'),
        document.querySelector('#apex_desktop .priceToPay'),
        document.querySelector('#apex_desktop #apexPriceToPay'),
        document.querySelector('#corePriceDisplay_desktop_feature_div .priceToPay'),
        document.querySelector('#corePriceDisplay_desktop_feature_div #apexPriceToPay'),
        document.querySelector('#corePrice_feature_div .priceToPay'),
        document.querySelector('#apex_desktop'),
      ].filter(Boolean);
      for (const root of priceRoots) {
        collectProbe(root, 'offscreen');
        let t = bestItemPriceFromBlock(root, 'current');
        if (!t) t = priceFromWholeFraction(root);
        if (!t) {
          const pt = root.querySelector('.priceToPay, #apexPriceToPay');
          if (pt) {
            t = bestItemPriceFromBlock(pt, 'current') || priceFromWholeFraction(pt);
          }
        }
        if (t) {
          out.current_price_text = t;
          break;
        }
      }
      if (!out.current_price_text) {
        const currentSel = [
          'form#addToCart .priceToPay .a-offscreen',
          'form#addToCart #apexPriceToPay .a-offscreen',
          'form#addToCart span.a-price[data-a-strike="false"] .a-offscreen',
          '#buybox .priceToPay .a-offscreen',
          '#buybox span.a-price[data-a-strike="false"] .a-offscreen',
          '#apex_desktop .priceToPay .a-offscreen',
          '#apex_desktop #apexPriceToPay .a-offscreen',
          '#corePriceDisplay_desktop_feature_div .priceToPay .a-offscreen',
          '#corePriceDisplay_desktop_feature_div #apexPriceToPay .a-offscreen',
          '#apex_desktop span.a-price[data-a-strike="false"] .a-offscreen',
          '#corePriceDisplay_desktop_feature_div .a-price[data-a-strike="false"] .a-offscreen',
          '#priceblock_dealprice',
          '#priceblock_ourprice',
          '#priceblock_saleprice',
        ];
        out.current_price_text = firstMatch(currentSel, 'current');
      }
      if (out.price_probe.length > 12) out.price_probe = out.price_probe.slice(0, 12);
      out.current_price_value = num(out.current_price_text);
      if (!out.current_price_text && out.out_of_stock) {
        out.notes.push('oos_no_item_price');
      }
      if (out.current_price_text && isUnitPriceText(out.current_price_text)) {
        out.unit_price_rejected = true;
        out.current_price_text = '';
        out.current_price_value = null;
      }

      // LIST / "Before" PRICE — strike-through.
      const listSel = [
        '#apex_desktop span.a-price[data-a-strike="true"] .a-offscreen',
        '#corePriceDisplay_desktop_feature_div .basisPrice .a-offscreen',
        '#corePriceDisplay_desktop_feature_div span.a-price[data-a-strike="true"] .a-offscreen',
        '.a-text-price[data-a-strike="true"] .a-offscreen',
        'span.a-price[data-a-strike="true"] .a-offscreen',
        '#priceblock_strikePrice',
        '#listPrice .a-offscreen',
        '#listPriceLegalMessage .a-offscreen',
        '.a-text-strike',
      ];
      out.list_price_text = firstMatch(listSel, 'list');
      out.list_price_value = num(out.list_price_text);

      // DISCOUNT % — Amazon shows e.g. "-40%" or "(15% off)".
      const discountSel = [
        '#apex_desktop .savingsPercentage',
        '#corePriceDisplay_desktop_feature_div .savingsPercentage',
        '.savingsPercentage',
        '#regularprice_savings .a-color-price',
        '.priceBlockSavingsString',
      ];
      out.discount_pct_text = firstMatch(discountSel, 'discount');
      out.discount_pct_value = num(out.discount_pct_text);

      // COUPON line (informational, not used to drive price math).
      const couponSel = [
        '#promoPriceBlockMessage .a-text-bold',
        '.couponLabelText',
        '#applyClippableCoupon_couponSubText',
      ];
      out.coupon_text = firstMatch(couponSel, 'coupon');

      return out;
    }
    """
    try:
        raw = await page.evaluate(js)
    except Exception as exc:
        return {
            "current_price_text": "", "current_price_value": None,
            "list_price_text": "", "list_price_value": None,
            "discount_pct_text": "", "discount_pct_value": None,
            "coupon_text": "",
            "out_of_stock": False,
            "unit_price_rejected": False,
            "price_probe": [],
            "notes": [f"extract_exc:{str(exc)[:120]}"],
        }
    return raw or {}


def _valid_boxes_from_targets(targets: Sequence[Dict[str, Any]]) -> List[Dict[str, float]]:
    """Convert measured target dicts into boxes safe for merge_boxes / clip."""
    valid: List[Dict[str, float]] = []
    for t in targets:
        if not t.get("found"):
            continue
        try:
            w = float(t.get("width") or 0)
            h = float(t.get("height") or 0)
        except Exception:
            w = h = 0.0
        if w > 20 and h > 20:
            valid.append({
                "x": float(t["x"]),
                "y": float(t["y"]),
                "width": w,
                "height": h,
            })
    return valid


def _clamp_clip(box: Dict[str, float], dims: Mapping[str, Any], padding: int) -> Dict[str, float]:
    clip = merge_boxes([box], padding)
    clip["width"] = min(clip["width"], max(1, float(dims.get("width", clip["width"])) - clip["x"]))
    clip["height"] = min(clip["height"], max(1, float(dims.get("height", clip["height"])) - clip["y"]))
    return clip


def _stitch_png_vertical(top: bytes, bottom: bytes, *, gap: int = 8) -> Optional[bytes]:
    """Stack two PNGs vertically; returns None when Pillow is unavailable."""
    try:
        import io
        from PIL import Image
    except ImportError:
        return None
    try:
        img_t = Image.open(io.BytesIO(top)).convert("RGB")
        img_b = Image.open(io.BytesIO(bottom)).convert("RGB")
        gap = max(0, int(gap))
        width = max(img_t.width, img_b.width)
        height = img_t.height + gap + img_b.height
        canvas = Image.new("RGB", (width, height), (255, 255, 255))
        canvas.paste(img_t, (0, 0))
        canvas.paste(img_b, (0, img_t.height + gap))
        out = io.BytesIO()
        canvas.save(out, format="PNG")
        return out.getvalue()
    except Exception:
        return None


def _stitch_png_row_aligned(
    panels: Sequence[Tuple[bytes, float]],
    *,
    gap: int = 12,
    bg: Tuple[int, int, int] = (255, 255, 255),
) -> Optional[bytes]:
    """Horizontally stitch panels; each panel's `y` is page top used for vertical alignment."""
    if not panels:
        return None
    try:
        import io
        from PIL import Image
    except ImportError:
        return None
    try:
        loaded: List[Tuple[Any, float]] = []
        for png, top_y in panels:
            if not png:
                continue
            loaded.append((Image.open(io.BytesIO(png)).convert("RGB"), float(top_y)))
        if not loaded:
            return None
        align_y = min(y for _, y in loaded)
        gap = max(0, int(gap))
        total_w = sum(im.width for im, _ in loaded) + gap * (len(loaded) - 1)
        total_h = max(im.height + int(y - align_y) for im, y in loaded)
        canvas = Image.new("RGB", (total_w, total_h), bg)
        x = 0
        for im, top_y in loaded:
            canvas.paste(im, (x, int(top_y - align_y)))
            x += im.width + gap
        out = io.BytesIO()
        canvas.save(out, format="PNG")
        return out.getvalue()
    except Exception:
        return None


def _stitch_png_bytes(left: bytes, right: bytes, *, gap: int = 12) -> Optional[bytes]:
    """Horizontally stitch two PNG byte blobs; returns None when Pillow is unavailable."""
    try:
        import io
        from PIL import Image
    except ImportError:
        return None
    try:
        img_l = Image.open(io.BytesIO(left)).convert("RGB")
        img_r = Image.open(io.BytesIO(right)).convert("RGB")
        gap = max(0, int(gap))
        height = max(img_l.height, img_r.height)
        width = img_l.width + gap + img_r.width
        canvas = Image.new("RGB", (width, height), (255, 255, 255))
        canvas.paste(img_l, (0, 0))
        canvas.paste(img_r, (img_l.width + gap, 0))
        out = io.BytesIO()
        canvas.save(out, format="PNG")
        return out.getvalue()
    except Exception:
        return None


async def _measure_product_image_block(page: Any) -> Dict[str, Any]:
    raw = await page.evaluate(
        r"""
        () => {
          const img = document.querySelector('#imgTagWrapperId')
            || document.querySelector('#landingImage')
            || document.querySelector('#main-image-container');
          if (!img) return { found: false, reason: 'no main image anchor' };
          const r = img.getBoundingClientRect();
          if (r.width < 60 || r.height < 60) {
            return { found: false, reason: 'image box too small' };
          }
          let y2 = r.bottom;
          const alt = document.querySelector('#altImages');
          if (alt) {
            const ar = alt.getBoundingClientRect();
            if (ar.height > 20 && ar.top >= r.top) y2 = Math.max(y2, ar.bottom);
          }
          const h = Math.max(80, y2 - r.y);
          return {
            found: true,
            selector: 'compact_pdp:image',
            x: r.x, y: r.y, width: r.width, height: Math.max(80, h),
            tag: 'img',
          };
        }
        """
    )
    return raw if isinstance(raw, dict) else {"found": False, "reason": "evaluate failed"}


async def _measure_center_title_price(page: Any, cfg: Mapping[str, Any]) -> Dict[str, Any]:
    price_selectors = list(_resolve_left_selectors(cfg))
    max_h = _cfg_int(cfg, "amazon_buybox_compact_center_max_height", 520, lo=120, hi=900)
    raw = await page.evaluate(
        """
        (args) => {
          const priceSels = args.priceSelectors || [
            '#corePriceDisplay_desktop_feature_div', '#corePrice_feature_div'];
          const maxH = args.maxH || 520;
          const title = document.querySelector('#productTitle') || document.querySelector('#title');
          if (!title) return { found: false, reason: 'no #productTitle' };
          const titleSection = document.querySelector('#titleSection') || title;
          const tr = titleSection.getBoundingClientRect();
          let y1 = tr.y;
          let y2 = tr.bottom;
          let x1 = tr.x;
          let x2 = tr.right;
          for (const sel of priceSels) {
            const el = document.querySelector(sel);
            if (!el) continue;
            const r = el.getBoundingClientRect();
            if (r.width < 80 || r.height > 520) continue;
            y2 = Math.max(y2, r.bottom);
            x1 = Math.min(x1, r.x);
            x2 = Math.max(x2, r.right);
            break;
          }
          const twister = document.querySelector('#twister_feature_div, #variation_size_name');
          if (twister) {
            const wr = twister.getBoundingClientRect();
            if (wr.height > 10 && wr.top < y1 + maxH + 80) {
              y2 = Math.min(Math.max(y2, wr.bottom), y1 + maxH);
            }
          }
          const center = document.querySelector('#centerCol');
          if (center) {
            const cr = center.getBoundingClientRect();
            x1 = cr.x;
            x2 = cr.right;
          }
          const rightCol = document.querySelector('#rightCol');
          if (rightCol) {
            const rr = rightCol.getBoundingClientRect();
            if (rr.x > x1 + 60) x2 = Math.min(x2, rr.x - 10);
          }
          if (y2 - y1 > maxH) y2 = y1 + maxH;
          const width = Math.max(120, x2 - x1);
          const height = Math.max(60, y2 - y1);
          if (width < 80 || height < 40) {
            return { found: false, reason: 'title/price region too small' };
          }
          return {
            found: true,
            selector: 'compact_pdp:center',
            x: x1, y: y1, width, height,
            tag: 'region',
          };
        }
        """,
        {"priceSelectors": price_selectors, "maxH": max_h},
    )
    return raw if isinstance(raw, dict) else {"found": False, "reason": "evaluate failed"}


async def _measure_left_product_card(page: Any, cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Desktop PDP left panel: main image + product title + price block (one clip).
    Stays left of #rightCol so the buybox column can sit beside it without a
    huge blank band under a tiny price strip.
    """
    price_selectors = list(_resolve_left_selectors(cfg))
    raw = await page.evaluate(
        """
        (priceSelectors) => {
          const priceSels = (priceSelectors && priceSelectors.length)
            ? priceSelectors
            : [
                '#corePriceDisplay_desktop_feature_div',
                '#corePrice_feature_div',
              ];
          let x1 = Infinity, y1 = Infinity, x2 = -Infinity, y2 = -Infinity;
          let matched = 0;
          const addRect = (r) => {
            if (!r || r.width < 24 || r.height < 12) return;
            matched += 1;
            x1 = Math.min(x1, r.x);
            y1 = Math.min(y1, r.y);
            x2 = Math.max(x2, r.right);
            y2 = Math.max(y2, r.bottom);
          };
          const img = document.querySelector('#imgTagWrapperId')
            || document.querySelector('#landingImage')
            || document.querySelector('#main-image-container');
          const title = document.querySelector('#productTitle') || document.querySelector('#title');
          if (img) addRect(img.getBoundingClientRect());
          if (title) addRect(title.getBoundingClientRect());
          for (const sel of priceSels) {
            const el = document.querySelector(sel);
            if (!el) continue;
            const r = el.getBoundingClientRect();
            if (r.width < 80 || r.height > 520) continue;
            addRect(r);
            break;
          }
          if (!matched || !isFinite(x1)) {
            return { found: false, reason: 'no product card anchors matched' };
          }
          const alt = document.querySelector('#altImages');
          if (alt) {
            const at = alt.getBoundingClientRect().top;
            if (at > y1 + 40) y2 = Math.min(y2, at - 6);
          }
          const rightCol = document.querySelector('#rightCol');
          if (rightCol) {
            const rr = rightCol.getBoundingClientRect();
            if (rr.x > x1 + 80) x2 = Math.min(x2, rr.x - 10);
          }
          const maxH = 720;
          if (y2 - y1 > maxH) y2 = y1 + maxH;
          const width = Math.max(120, x2 - x1);
          const height = Math.max(80, y2 - y1);
          if (width < 80 || height < 60) {
            return { found: false, reason: 'product card box too small after clamp' };
          }
          return {
            found: true,
            selector: 'product_card:left_panel',
            x: x1, y: y1, width, height,
            tag: 'region',
          };
        }
        """,
        price_selectors,
    )
    return raw if isinstance(raw, dict) else {"found": False, "reason": "evaluate failed"}


async def _measure_left_price_block(page: Any, cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Tight bounding box for the current-price block only (discount %, $ price,
    list price). Prefers #corePriceDisplay_desktop_feature_div over full
    #apex_desktop so size/variation grids are not captured.
    """
    configured = _resolve_left_selectors(cfg)
    raw = await page.evaluate(
        """
        (configured) => {
          const pick = [];
          const seen = new Set();
          const addSel = (sel) => {
            if (!sel || seen.has(sel)) return;
            seen.add(sel);
            pick.push(sel);
          };
          for (const sel of (configured || [])) addSel(sel);
          [
            '#corePriceDisplay_desktop_feature_div',
            '#corePrice_feature_div',
            '#apex_desktop .priceToPay',
            '#apex_desktop',
          ].forEach(addSel);
          for (const sel of pick) {
            const el = document.querySelector(sel);
            if (!el) continue;
            const r = el.getBoundingClientRect();
            if (r.width < 80 || r.height < 20) continue;
            if (sel === '#apex_desktop' && r.height > 520) continue;
            return {
              found: true,
              selector: sel,
              x: r.x, y: r.y, width: r.width, height: r.height,
              tag: el.tagName.toLowerCase(),
            };
          }
          return { found: false, reason: 'no left price block matched' };
        }
        """,
        list(configured),
    )
    return raw if isinstance(raw, dict) else {"found": False, "reason": "evaluate failed"}


async def _measure_right_column_from_in_stock(page: Any) -> Dict[str, Any]:
    """
    Bounding box for the right buybox column starting at In Stock (#availability)
    through purchase controls and seller/ships-from lines below it.
    """
    raw = await page.evaluate(
        r"""
        () => {
          const avail = document.querySelector('#availability');
          if (!avail) return { found: false, reason: 'no #availability' };
          const ar = avail.getBoundingClientRect();
          if (ar.width < 20 || ar.height < 4) {
            return { found: false, reason: 'availability box too small' };
          }
          const rightRoot =
            document.querySelector('#rightCol') ||
            document.querySelector('#desktop_buybox') ||
            document.querySelector('#buybox') ||
            avail.closest('#buybox') ||
            avail.parentElement;
          const rr = rightRoot ? rightRoot.getBoundingClientRect() : ar;
          let bottom = ar.bottom;
          const extendSel = [
            '#selectQuantity', '#quantity',
            '#addToCart_feature_div', '#buyNow_feature_div',
            '#add-to-cart-button', '#buy-now-button',
            '#submit.add-to-cart', '#buybox',
            '#merchant-info', '#sellerProfileTriggerId',
            '#amazonMerchant', '#shipsFromSoldBy_feature_div',
            '#tabular-buybox', '#offerDisplayFeatures',
          ];
          for (const sel of extendSel) {
            document.querySelectorAll(sel).forEach((el) => {
              if (!rightRoot || rightRoot.contains(el) || el === rightRoot) {
                const r = el.getBoundingClientRect();
                if (r.width > 20 && r.height > 8) bottom = Math.max(bottom, r.bottom);
              }
            });
          }
          const stopSel = [
            '#protectionPlanIngressFeature',
            '#attachAccessoryModal_feature_div',
            '#offer-display-features',
            '#product-support-desktop',
          ];
          for (const sel of stopSel) {
            const el = document.querySelector(sel);
            if (!el || !rightRoot || !rightRoot.contains(el)) continue;
            const sr = el.getBoundingClientRect();
            if (sr.top > ar.top + 20) bottom = Math.min(bottom, sr.top - 6);
          }
          let n = avail.nextElementSibling;
          let steps = 0;
          while (n && rightRoot && rightRoot.contains(n) && steps < 24) {
            const txt = ((n.innerText || '') + '').toLowerCase();
            if (txt.includes('protection plan') || txt.includes('complete protect')) break;
            const r = n.getBoundingClientRect();
            if (r.height > 5) bottom = Math.max(bottom, r.bottom);
            n = n.nextElementSibling;
            steps += 1;
          }
          const x = rr.x;
          const width = Math.max(rr.width, ar.width, 220);
          const height = Math.max(80, Math.min(bottom - ar.y, 520));
          return {
            found: true,
            selector: 'buybox:from_in_stock',
            x, y: ar.y, width, height,
            tag: 'region',
          };
        }
        """
    )
    return raw if isinstance(raw, dict) else {"found": False, "reason": "evaluate failed"}


async def _measure_capture_targets(page: Any, cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """
    Diagnostic target list: left panel + right buybox (from In Stock).
    """
    targets: List[Dict[str, Any]] = []
    if _layout_uses_compact_pdp(cfg):
        img_t = await _measure_product_image_block(page)
        info_t = await _measure_center_title_price(page, cfg)
        for t in (img_t, info_t):
            if t.get("found"):
                targets.append(t)
            else:
                targets.append({
                    "selector": str(t.get("selector") or "product_card:part"),
                    "found": False,
                    "reason": str(t.get("reason") or "unavailable"),
                })
        if not img_t.get("found") and not info_t.get("found"):
            left = await _measure_left_product_card(page, cfg)
            if left.get("found"):
                targets.append(left)
    else:
        left = await _measure_left_price_block(page, cfg)
        if left.get("found"):
            targets.append(left)
        else:
            targets.append({
                "selector": "price:block",
                "found": False,
                "reason": str(left.get("reason") or "unavailable"),
            })
    if _right_from_in_stock_enabled(cfg):
        right = await _measure_right_column_from_in_stock(page)
        if right.get("found"):
            targets.append(right)
        else:
            legacy = await _measure_target_boxes(page, [LEGACY_RIGHT_SELECTOR])
            targets.extend(legacy)
            targets.append({
                "selector": "buybox:from_in_stock",
                "found": False,
                "reason": str(right.get("reason") or "unavailable"),
                "fallback": LEGACY_RIGHT_SELECTOR if legacy and legacy[0].get("found") else "",
            })
    else:
        targets.extend(await _measure_target_boxes(page, [LEGACY_RIGHT_SELECTOR]))
    return targets


def _region_boxes_from_targets(
    targets: Sequence[Dict[str, Any]],
) -> Tuple[
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
    Optional[Dict[str, float]],
]:
    """Split measured targets into (image, title_price, legacy_left, right_buybox)."""
    image_box: Optional[Dict[str, float]] = None
    info_box: Optional[Dict[str, float]] = None
    left_box: Optional[Dict[str, float]] = None
    right_box: Optional[Dict[str, float]] = None
    for t in targets:
        if not t.get("found"):
            continue
        try:
            w = float(t.get("width") or 0)
            h = float(t.get("height") or 0)
        except Exception:
            continue
        if w <= 20 or h <= 20:
            continue
        box = {"x": float(t["x"]), "y": float(t["y"]), "width": w, "height": h}
        sel = str(t.get("selector") or "")
        if sel == "buybox:from_in_stock" or sel == LEGACY_RIGHT_SELECTOR or sel.startswith("form#"):
            right_box = box
        elif sel in {"compact_pdp:image", "product_card:image"}:
            image_box = box
        elif sel in {"compact_pdp:center", "product_card:title_price"}:
            info_box = box
        elif sel == "product_card:left_panel" or left_box is None:
            left_box = box
    return image_box, info_box, left_box, right_box


async def _measure_target_boxes(
    page: Any, selectors: Sequence[str]
) -> List[Dict[str, Any]]:
    """
    For each selector, return the bounding box of the FIRST visible match.

    Returns a list of dicts: {selector, found, x, y, width, height} for
    diagnostic logging. Only entries with width>20 and height>20 are valid
    targets - everything else is treated as "selector did not resolve".
    """
    raw = await page.evaluate(
        """
        (sels) => {
          return sels.map((sel) => {
            const el = document.querySelector(sel);
            if (!el) return { selector: sel, found: false };
            const r = el.getBoundingClientRect();
            return {
              selector: sel,
              found: true,
              x: r.x, y: r.y, width: r.width, height: r.height,
              tag: el.tagName.toLowerCase(),
            };
          });
        }
        """,
        list(selectors),
    )
    return list(raw or [])


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def capture_amazon_buybox_screenshot(
    url: str,
    cfg: Mapping[str, Any],
    *,
    bot_dir: Path,
) -> Dict[str, Any]:
    """
    Open `url` in a Playwright-controlled Chrome (CDP-attached when configured,
    otherwise a fresh launch matching the eBay scraper's settings), measure the
    configured selectors, highlight them, and save a clipped PNG.

    Returns a dict shaped like the eBay scraper's output so call sites can
    treat both flows uniformly:
      {
        "status": ok|no_targets|blocked|browser_error|unknown,
        "url": <input url>,
        "opened_url": <final url after redirects>,
        "selectors": [...],
        "targets": [ {selector, found, x, y, width, height} ],
        "screenshot_path": <abs path or "">,
        "meta_path": <abs path or "">,
        "prices": {                                # only on status == "ok"
            "current_price_text", "current_price_value",
            "list_price_text",    "list_price_value",
            "discount_pct_text",  "discount_pct_value",
            "coupon_text", "notes",
        },
        "reason": <short string on non-ok status>,
        ... (other diagnostic fields)
      }
    """
    u = (url or "").strip()
    left_selectors = _resolve_left_selectors(cfg)
    right_from_in_stock = _right_from_in_stock_enabled(cfg)
    highlight_selectors = _highlight_selectors_for_capture(cfg)
    viewport_width = _cfg_int(cfg, "amazon_buybox_viewport_width", 1680, lo=800, hi=3200)
    viewport_height = _cfg_int(cfg, "amazon_buybox_viewport_height", 1500, lo=700, hi=3200)
    goto_timeout_ms = _cfg_int(cfg, "amazon_buybox_goto_timeout_ms", 45000, lo=5000, hi=180000)
    networkidle_timeout_ms = _cfg_int(cfg, "amazon_buybox_networkidle_timeout_ms", 10000, lo=1000, hi=60000)
    selector_timeout_ms = _cfg_int(cfg, "amazon_buybox_selector_timeout_ms", 15000, lo=1000, hi=90000)
    extra_wait_s = _cfg_float(cfg, "amazon_buybox_extra_wait_s", 1.5, lo=0.0, hi=30.0)
    padding = _cfg_int(cfg, "amazon_buybox_screenshot_padding", 12, lo=0, hi=80)
    highlight_color = str(cfg.get("amazon_buybox_highlight_color") or DEFAULT_HIGHLIGHT_COLOR)
    highlight_thickness = _cfg_int(
        cfg, "amazon_buybox_highlight_thickness_px", DEFAULT_HIGHLIGHT_THICKNESS_PX, lo=1, hi=12,
    )
    output_dir_raw = str(cfg.get("amazon_buybox_output_dir") or DEFAULT_OUTPUT_DIR).strip() or DEFAULT_OUTPUT_DIR

    result: Dict[str, Any] = {
        "status": "unknown",
        "url": u,
        "opened_url": u,
        "selectors": list(left_selectors) + (
            ["buybox:from_in_stock"] if right_from_in_stock else [LEGACY_RIGHT_SELECTOR]
        ),
        "right_column_mode": "from_in_stock" if right_from_in_stock else "full_form",
        "targets": [],
        "screenshot_path": "",
        "meta_path": "",
        "viewport": {"width": viewport_width, "height": viewport_height},
        "connected_via": "",
    }

    if not u:
        result.update({"status": "browser_error", "reason": "missing url"})
        return result

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        result.update({"status": "browser_error", "reason": f"playwright import failed: {str(exc)[:160]}"})
        return result

    # Only manage Xvfb when we are launching our own Chrome. CDP-attach
    # piggybacks on the long-lived trusted Chrome that already owns its X
    # display (started by mirror-world-instorebotforwarder-chrome-cdp.service
    # via Chromerrunner/start_chrome_oracle_cdp.sh --with-xvfb).
    xvfb_proc: Optional[subprocess.Popen[Any]] = None
    use_cdp = _connect_cdp_enabled(cfg)
    if not use_cdp:
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
            if use_cdp:
                cdp_url = _connect_cdp_url(cfg)
                browser = await connect_chromium_over_cdp(p.chromium, cdp_url)
                result["connected_via"] = f"cdp:{cdp_url}"
            else:
                launch_kwargs, resolved = resolve_chrome_launch_kwargs(cfg)
                browser = await p.chromium.launch(**launch_kwargs)
                result["connected_via"] = f"launch:{resolved}"

            context = await acquire_scrape_browser_context(
                browser,
                use_cdp=use_cdp,
                viewport_width=viewport_width,
                viewport_height=viewport_height,
            )
            page = await context.new_page()
            try:
                await page.set_viewport_size({"width": viewport_width, "height": viewport_height})
            except Exception:
                pass

            await page.goto(u, wait_until="domcontentloaded", timeout=goto_timeout_ms)
            result["opened_url"] = page.url
            try:
                await page.wait_for_load_state("networkidle", timeout=networkidle_timeout_ms)
            except Exception:
                pass
            if extra_wait_s > 0:
                await page.wait_for_timeout(int(extra_wait_s * 1000))
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(250)
            try:
                await page.wait_for_selector(
                    "#productTitle, #title, #availability",
                    timeout=min(selector_timeout_ms, 20000),
                    state="visible",
                )
            except Exception:
                pass
            await page.wait_for_timeout(300)
            for _ in range(2):
                if await _dismiss_amazon_page_chrome(page):
                    await page.wait_for_timeout(600)
                    try:
                        await page.wait_for_load_state("domcontentloaded", timeout=8000)
                    except Exception:
                        pass

            body_txt = await page.evaluate(
                "() => (document.body && document.body.innerText) ? document.body.innerText.slice(0, 16000) : ''"
            )
            if amazon_html_looks_blocked(body_txt):
                if await _dismiss_amazon_page_chrome(page):
                    await page.wait_for_timeout(800)
                    body_txt = await page.evaluate(
                        "() => (document.body && document.body.innerText) ? document.body.innerText.slice(0, 16000) : ''"
                    )
            if amazon_html_looks_blocked(body_txt):
                result.update({"status": "blocked", "reason": "amazon returned bot/captcha page"})
                return result

            # Wait until left panel / price block or right buybox anchor exists.
            wait_selectors = list(left_selectors)
            if _layout_uses_compact_pdp(cfg):
                wait_selectors = ["#productTitle", "#imgTagWrapperId", "#imageBlock"] + wait_selectors
            wait_selectors.append(
                "#availability" if right_from_in_stock else LEGACY_RIGHT_SELECTOR
            )
            seen_any = False
            for sel in wait_selectors:
                try:
                    await page.wait_for_selector(sel, timeout=selector_timeout_ms, state="attached")
                    seen_any = True
                    break
                except Exception:
                    continue
            if not seen_any:
                result.update({
                    "status": "no_targets",
                    "reason": f"none of capture anchors resolved within {selector_timeout_ms}ms",
                })
                return result

            targets = await _measure_capture_targets(page, cfg)
            result["targets"] = targets
            valid_boxes = _valid_boxes_from_targets(targets)

            if not valid_boxes:
                result.update({
                    "status": "no_targets",
                    "reason": "selectors matched but had no visible bounding box",
                })
                return result

            # Tentative clip so we can grow the viewport if the merged box
            # extends below the current visible area (mirrors the eBay flow).
            clip_try = merge_boxes(valid_boxes, padding)
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
                    # Re-measure after viewport change (positions can shift).
                    targets = await _measure_capture_targets(page, cfg)
                    result["targets"] = targets
                    valid_boxes = _valid_boxes_from_targets(targets)
            except Exception:
                pass

            if not valid_boxes:
                result.update({
                    "status": "no_targets",
                    "reason": "viewport resize lost all target boxes",
                })
                return result

            layout_pre = _screenshot_layout_mode(cfg)
            if layout_pre != "compact_pdp":
                if _highlight_price_only_enabled(cfg):
                    await _highlight_single_price_block(page, cfg, highlight_color, highlight_thickness)
                else:
                    await _highlight_targets(page, highlight_selectors, highlight_color, highlight_thickness)

            dims = await page.evaluate(
                """
                () => ({
                  width: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth, window.innerWidth),
                  height: Math.max(document.documentElement.scrollHeight, document.body.scrollHeight, window.innerHeight)
                })
                """
            )
            image_box, info_box, left_box, right_box = _region_boxes_from_targets(targets)
            layout = _screenshot_layout_mode(cfg)
            gap_px = _cfg_int(cfg, "amazon_buybox_side_by_side_gap_px", 12, lo=0, hi=80)
            v_gap_px = _cfg_int(cfg, "amazon_buybox_product_card_v_gap_px", 8, lo=0, hi=40)

            out_dir = Path(output_dir_raw)
            if not out_dir.is_absolute():
                out_dir = bot_dir / out_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            base = out_dir / f"{safe_name(u)}_{int(time.time())}_buybox"
            shot = str(base) + ".png"
            result["status"] = "ok"
            result["screenshot_layout"] = layout

            async def _capture_clip(box: Dict[str, float]) -> Tuple[bytes, Dict[str, float]]:
                clip = _clamp_clip(box, dims, padding)
                png = await page.screenshot(full_page=False, clip=clip, type="png")
                return png, clip

            left_png: Optional[bytes] = None
            left_clip: Dict[str, Any] = {}
            product_left_box = left_box
            if layout == "compact_pdp" and image_box and info_box and right_box:
                img_png, img_clip = await _capture_clip(image_box)
                info_png, info_clip = await _capture_clip(info_box)
                right_png, right_clip = await _capture_clip(right_box)
                stitched = _stitch_png_row_aligned(
                    [
                        (img_png, float(image_box["y"])),
                        (info_png, float(info_box["y"])),
                        (right_png, float(right_box["y"])),
                    ],
                    gap=gap_px,
                )
                result["clip"] = {
                    "layout": layout,
                    "image": img_clip,
                    "center": info_clip,
                    "right": right_clip,
                }
                left_part = str(base) + "_image.png"
                mid_part = str(base) + "_center.png"
                right_part = str(base) + "_buybox.png"
                Path(left_part).write_bytes(img_png)
                Path(mid_part).write_bytes(info_png)
                Path(right_part).write_bytes(right_png)
                if stitched:
                    Path(shot).write_bytes(stitched)
                    result["screenshot_path"] = shot
                    result["screenshot_paths"] = [shot]
                else:
                    result["screenshot_path"] = mid_part
                    result["screenshot_paths"] = [left_part, mid_part, right_part]
                    result["stitch_note"] = "Pillow unavailable; using 3 separate PNGs"
            elif layout in {"compact_pdp", "side_by_side", "separate"} and right_box and (
                product_left_box is not None or left_png is not None
            ):
                if product_left_box is not None:
                    left_png, left_clip = await _capture_clip(product_left_box)
                right_png, right_clip = await _capture_clip(right_box)
                result["clip"] = {"layout": layout, "left": left_clip, "right": right_clip}
                left_part = str(base) + "_product.png"
                right_part = str(base) + "_instock.png"
                Path(left_part).write_bytes(left_png)
                Path(right_part).write_bytes(right_png)
                screenshot_paths = [left_part, right_part]
                if layout == "side_by_side":
                    stitched = _stitch_png_bytes(left_png, right_png, gap=gap_px)
                    if stitched:
                        Path(shot).write_bytes(stitched)
                        result["screenshot_path"] = shot
                        result["screenshot_paths"] = [shot]
                    else:
                        result["screenshot_path"] = left_part
                        result["screenshot_paths"] = screenshot_paths
                        result["stitch_note"] = "Pillow unavailable; using separate product + in-stock PNGs"
                else:
                    result["screenshot_path"] = left_part
                    result["screenshot_paths"] = screenshot_paths
            elif layout in {"side_by_side", "separate"} and left_box and right_box:
                left_png, left_clip = await _capture_clip(left_box)
                right_png, right_clip = await _capture_clip(right_box)
                result["clip"] = {"layout": layout, "left": left_clip, "right": right_clip}
                left_part = str(base) + "_price.png"
                right_part = str(base) + "_instock.png"
                Path(left_part).write_bytes(left_png)
                Path(right_part).write_bytes(right_png)
                screenshot_paths = [left_part, right_part]
                if layout == "side_by_side":
                    stitched = _stitch_png_bytes(left_png, right_png, gap=gap_px)
                    if stitched:
                        Path(shot).write_bytes(stitched)
                        result["screenshot_path"] = shot
                        result["screenshot_paths"] = [shot]
                    else:
                        result["screenshot_path"] = left_part
                        result["screenshot_paths"] = screenshot_paths
                        result["stitch_note"] = "Pillow unavailable; using separate price + in-stock PNGs"
                else:
                    result["screenshot_path"] = left_part
                    result["screenshot_paths"] = screenshot_paths
            else:
                clip = merge_boxes(valid_boxes, padding)
                clip["width"] = min(clip["width"], max(1, float(dims.get("width", clip["width"])) - clip["x"]))
                clip["height"] = min(clip["height"], max(1, float(dims.get("height", clip["height"])) - clip["y"]))
                result["clip"] = clip
                await page.screenshot(path=shot, full_page=False, clip=clip)
                result["screenshot_path"] = shot
                result["screenshot_paths"] = [shot]

            # Extract canonical price/list/discount strings from the SAME page
            # load that produced the screenshot. The caller uses these to
            # override the source-message price so embed text and screenshot
            # never disagree (single source of truth for deal economics).
            result["prices"] = await _extract_buybox_prices(page)
            meta_path = str(base) + ".json"
            Path(meta_path).write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
            result["meta_path"] = meta_path
            # Retention sweep on the Amazon screenshot directory so output
            # never grows unbounded. Canonical owner is prune_screenshot_dir
            # in ebay_first8_dom_comps; both scrapers use the same helper.
            try:
                result["prune"] = prune_screenshot_dir(
                    out_dir,
                    keep_files=_cfg_int(cfg, "amazon_buybox_keep_files", 100, lo=0, hi=100000),
                    max_age_days=_cfg_float(cfg, "amazon_buybox_max_age_days", 7.0, lo=0.0, hi=365.0),
                    max_total_mb=_cfg_float(cfg, "amazon_buybox_max_total_mb", 100.0, lo=0.0, hi=100000.0),
                )
            except Exception:
                pass
            return result

        except Exception as exc:
            result.update({"status": "browser_error", "reason": str(exc)[:220]})
            return result
        finally:
            # Single close site for the tab, on every code path. Mirrors the
            # eBay scraper's finally block so CDP-attached trusted Chrome
            # never leaks orphan tabs.
            try:
                if page is not None:
                    await page.close()
            except Exception:
                pass
            try:
                if browser is not None and not use_cdp:
                    await browser.close()
            except Exception:
                pass
            if xvfb_proc is not None and xvfb_proc.poll() is None:
                try:
                    xvfb_proc.terminate()
                except Exception:
                    pass
