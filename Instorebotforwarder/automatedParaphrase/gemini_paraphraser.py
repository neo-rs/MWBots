from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen

_gem_log = logging.getLogger("instorebotforwarder")


# Canonical default model id used when config does not specify one. Keep in sync with
# `rewrite_deal_post_keep_urls` (same default below) and any Instore config defaults.
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash-lite"
# One sync HTTP call's hard ceiling for probe_gemini_api(). Independent of the async
# `timeout_s` used by `rewrite_deal_post_keep_urls`, because probes are short.
PROBE_TIMEOUT_S = 20.0


def gemini_status(cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    Summarize Gemini configuration (NOT a live API call).

    Returns a small string dict suitable for direct logging/printing:
      enabled, api_key, model, temperature.
    """
    key = str((cfg or {}).get("gemini_api_key") or "").strip()
    model = str((cfg or {}).get("gemini_model") or DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    try:
        temp = float((cfg or {}).get("gemini_temperature") or 0.65)
    except Exception:
        temp = 0.65
    return {
        "enabled": "yes" if bool(key) else "no",
        "api_key": "set" if bool(key) else "missing",
        "model": model,
        "temperature": f"{max(0.0, min(temp, 1.0)):.4f}",
    }


def _classify_gemini_error_body(body_text: str) -> Dict[str, str]:
    """
    Parse Gemini's JSON error body into a small, operator-friendly dict.

    Gemini's 4xx/5xx responses follow Google's protobuf-error shape:
      {"error": {"code", "message", "status", "details": [...]}}
    where `details[*]` can include a QuotaFailure (with quotaMetric / quotaId)
    and a RetryInfo (with retryDelay like "30s"). We pull the bits an operator
    actually needs to decide "wait a minute" vs "wait til midnight UTC".

    Returns keys (all strings, all optional):
      reason        e.g. "RESOURCE_EXHAUSTED"
      message       short Gemini message
      quota_scope   "per_minute" | "per_day" | "other" | ""  (best-effort)
      quota_id      raw quotaMetric/quotaId string from the QuotaFailure
      retry_after   e.g. "30s" from RetryInfo
    """
    out: Dict[str, str] = {
        "reason": "", "message": "", "quota_scope": "", "quota_id": "", "retry_after": "",
    }
    try:
        obj = json.loads(body_text) if body_text else {}
    except Exception:
        return out
    err = (obj or {}).get("error") or {}
    if not isinstance(err, dict):
        return out
    out["reason"] = str(err.get("status") or "").strip()
    out["message"] = str(err.get("message") or "").strip()[:200]
    for d in (err.get("details") or []):
        if not isinstance(d, dict):
            continue
        t = str(d.get("@type") or "")
        if "QuotaFailure" in t:
            for v in (d.get("violations") or []):
                if not isinstance(v, dict):
                    continue
                qid = str(v.get("quotaMetric") or v.get("quotaId") or "").strip()
                if qid and not out["quota_id"]:
                    out["quota_id"] = qid
                ql = qid.lower()
                if not out["quota_scope"]:
                    if "perminute" in ql or "per_minute" in ql or "per-minute" in ql:
                        out["quota_scope"] = "per_minute"
                    elif "perday" in ql or "per_day" in ql or "per-day" in ql:
                        out["quota_scope"] = "per_day"
                    elif qid:
                        out["quota_scope"] = "other"
        elif "RetryInfo" in t:
            rd = d.get("retryDelay")
            if rd and not out["retry_after"]:
                out["retry_after"] = str(rd).strip()

    # Fall back to RetryInfo as the ground truth when the quota_id is ambiguous
    # ("other"). Gemini's RetryInfo.retryDelay is a google.protobuf.Duration
    # serialized as "{seconds}s" (e.g. "42s", "86400s"):
    #   <=  120s -> per_minute bucket refill (transient)
    #   >= 3600s -> per_day quota (won't clear until next UTC day)
    if out["retry_after"] and (not out["quota_scope"] or out["quota_scope"] == "other"):
        try:
            ra = out["retry_after"].strip().lower()
            if ra.endswith("s"):
                secs = float(ra[:-1])
                if secs <= 120:
                    out["quota_scope"] = "per_minute"
                elif secs >= 3600:
                    out["quota_scope"] = "per_day"
        except Exception:
            pass
    return out


def probe_gemini_api(cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    Live one-shot Gemini :generateContent probe.

    Sends a tiny prompt to the same endpoint `rewrite_deal_post_keep_urls` uses,
    so a success here means the bot's rewrite path will reach Gemini too.

    Returns:
      {
        "status":       "200" | "<http_code>" | "no_key" | "error",
        "detail":       short human-readable string suitable for one log line,
        "model":        resolved model id,
        "reply":        raw text Gemini returned (best-effort, may be empty),
        "error_reason": "RESOURCE_EXHAUSTED" / "" (Gemini's status field on 4xx),
        "quota_scope":  "per_minute" | "per_day" | "other" | "" (best-effort),
        "quota_id":     raw quotaMetric string from QuotaFailure (best-effort),
        "retry_after":  e.g. "30s" from RetryInfo (best-effort, ""),
      }
    Extra keys are "" on success (200) responses.
    """
    key = str((cfg or {}).get("gemini_api_key") or "").strip()
    model = str((cfg or {}).get("gemini_model") or DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    blank_err: Dict[str, str] = {"error_reason": "", "quota_scope": "", "quota_id": "", "retry_after": ""}
    if not key:
        return {"status": "no_key", "detail": "missing gemini_api_key", "model": model, "reply": "", **blank_err}

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": "Reply with exactly: OK"}]}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 16, "topP": 1.0},
    }
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "mirror-world-instore-gemini-probe/1.0",
        },
    )
    try:
        with urlopen(req, timeout=PROBE_TIMEOUT_S) as resp:
            status = str(getattr(resp, "status", 200))
            txt = resp.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(txt) if txt else {}
        except Exception:
            data = {}
        reply = ""
        try:
            reply = (
                (((data or {}).get("candidates") or [])[0] or {})
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                or ""
            )
            reply = str(reply).strip()
        except Exception:
            reply = ""
        return {
            "status": status,
            "detail": (txt.strip()[:140] if txt else ""),
            "model": model,
            "reply": reply,
            **blank_err,
        }
    except Exception as e:
        code = getattr(e, "code", None)
        status = str(code) if code is not None else "error"
        # Pull the error response body if this was an HTTPError (it carries .read()).
        body_text = ""
        try:
            if hasattr(e, "read"):
                raw_b = e.read()  # type: ignore[attr-defined]
                if raw_b:
                    body_text = raw_b.decode("utf-8", errors="replace")
        except Exception:
            body_text = ""
        parsed = _classify_gemini_error_body(body_text)
        bits = []
        if parsed.get("reason"):
            bits.append(parsed["reason"])
        if parsed.get("quota_scope"):
            bits.append(f"scope={parsed['quota_scope']}")
        if parsed.get("retry_after"):
            bits.append(f"retry_after={parsed['retry_after']}")
        if parsed.get("message"):
            bits.append(parsed["message"][:100])
        detail = " | ".join(bits) if bits else f"{type(e).__name__}: {str(e)[:140]}"
        return {
            "status": status,
            "detail": detail,
            "model": model,
            "reply": "",
            "error_reason": parsed.get("reason", ""),
            "quota_scope": parsed.get("quota_scope", ""),
            "quota_id": parsed.get("quota_id", ""),
            "retry_after": parsed.get("retry_after", ""),
        }


def accumulate_gemini_response_usage(data: Any, sink: Dict[str, int]) -> None:
    """
    Sum usageMetadata from a generateContent JSON body into sink (mutates sink).

    Keys (all optional on input; sink defaults to 0):
    - prompt_token_count, candidates_token_count, total_token_count
    - generate_content_calls (incremented by 1 when usageMetadata is present)
    """
    if not isinstance(sink, dict) or not isinstance(data, dict):
        return
    um = data.get("usageMetadata")
    if not isinstance(um, dict):
        return
    try:
        p = int(um.get("promptTokenCount") or 0)
        c = int(um.get("candidatesTokenCount") or 0)
        t = int(um.get("totalTokenCount") or 0)
    except (TypeError, ValueError):
        return
    sink["prompt_token_count"] = int(sink.get("prompt_token_count") or 0) + p
    sink["candidates_token_count"] = int(sink.get("candidates_token_count") or 0) + c
    sink["total_token_count"] = int(sink.get("total_token_count") or 0) + t
    sink["generate_content_calls"] = int(sink.get("generate_content_calls") or 0) + 1


def _replace_unicode_dashes_with_hyphen(s: str) -> str:
    """Em dash (—) / en dash (–) -> ASCII hyphen-minus (-); keeps Discord copy plain."""
    if not s:
        return s
    return s.replace("\u2014", "-").replace("\u2013", "-")


async def rewrite_deal_post_keep_urls(
    *,
    text: str,
    gemini_api_key: str,
    model: str,
    temperature: float,
    timeout_s: float,
    neutralize_mentions_fn,
    usage_accumulator: Optional[Dict[str, int]] = None,
) -> str:
    """
    Gemini-based Discord deal-post rewriter.

    Contract:
    - Returns a natural rewrite of `text` in a clean RS-style Discord deal-post tone.
    - URLs are pinned: same characters, same order (prompted; caller may still validate).
    - Always safe-fallback: on any failure returns the original `text`.
    - Caller is responsible for treating an unchanged output as a drop signal if desired.
    """
    raw = (text or "").strip()
    if not raw:
        return ""

    key = (gemini_api_key or "").strip()
    if not key:
        return raw

    # Rewrite prompt: RS-style Discord deal post. URLs must remain exact strings.
    sys_prompt = (
        "You rewrite short Discord deal/lead messages in a clean RS-style tone.\n"
        "\n"
        "Goal:\n"
        "- Rewrite the post so it sounds like a real person sharing a deal in Discord.\n"
        "- Make it cleaner, sharper, and easier to skim.\n"
        "- Improve wording and flow, not just punctuation or line breaks.\n"
        "- Keep it casual and deal-focused, not corporate or salesy.\n"
        "- Rewrite naturally even if the source message is already clean.\n"
        "- Prefer improving wording and flow over making tiny cosmetic edits.\n"
        "\n"
        "Title rule:\n"
        "- ALWAYS start the post with a single bold headline on its own line, using Discord bold formatting: **like this**.\n"
        "- The headline must be short (3 to 8 words) and high-impact: lead with the price, % off, or strongest hook plus the product name.\n"
        "- Do NOT put a URL inside the headline.\n"
        "- After the headline, leave one blank line, then write the rewritten body.\n"
        "- Put the main link on its own line at the end of the body.\n"
        "- Use **bold** ONLY for the headline. Do not bold anything else, and do not use other markdown (no headers `#`, no bullets, no code fences).\n"
        "\n"
        "Style:\n"
        "- Lead with the strongest hook: product + price, discount, savings, coupon, or comparison value.\n"
        "- Prefer short readable lines instead of one long paragraph.\n"
        "- Use natural deal-channel wording.\n"
        "- Avoid robotic extracted-data formatting unless the source is very list-heavy.\n"
        "- Do not over-explain. Keep the post compact.\n"
        "- Sound like a reseller posting a quick find, not an ad.\n"
        "\n"
        "Examples of tone (body line only, shown without the bold headline for brevity):\n"
        "- Bad: 'Save $330 at Best Buy. MSRP $659.99.'\n"
        "- Better: '50% OFF at Best Buy from the usual $659.99 MSRP.'\n"
        "- Bad: 'Now only $3 with Subscribe & Save.'\n"
        "- Better: '$3 with sub/save which is way below most stores right now.'\n"
        "- Bad: 'Limited time deal. Great savings available now.'\n"
        "- Better: 'Nice price drop if you were waiting on this one.'\n"
        "\n"
        "Full output examples (headline + body + link, in the exact shape to produce):\n"
        "Example 1 input:\n"
        "  Stacking Offer (6-Pack Dog Food)\n"
        "  $5 at checkout after doing below:\n"
        "  1 Clip coupon for the 'One-time purchase'\n"
        "  2 Switch to 'Sub & Save' + Clip Coupon\n"
        "  https://pricedoffers.com/3b9j6\n"
        "Example 1 output:\n"
        "  **$5 Blue Buffalo 6-Pack Dog Food**\n"
        "  \n"
        "  Down to $5 after clipping the coupon, then switching to Sub & Save and clipping again.\n"
        "  \n"
        "  https://pricedoffers.com/3b9j6\n"
        "\n"
        "Example 2 input:\n"
        "  Beats Studio Pro headphones, Best Buy clearance, $329.99 reg $349.99.\n"
        "  https://bestbuy.com/abc\n"
        "Example 2 output:\n"
        "  **Beats Studio Pro - $329.99 at Best Buy**\n"
        "  \n"
        "  Clearance price from the usual $349.99 MSRP.\n"
        "  \n"
        "  https://bestbuy.com/abc\n"
        "\n"
        "Accuracy rules:\n"
        "- Keep all product names, brands, prices, codes, discounts, warranty details, and store comparisons accurate.\n"
        "- Do not add new claims, fake urgency, fake stock info, fake resale info, or extra product details.\n"
        "- Mention coupon/code instructions clearly if present.\n"
        "- Mention comparison pricing only if it exists in the input.\n"
        "\n"
        "URL rules:\n"
        "- Keep every URL exactly the same: same characters, same order.\n"
        "- If the same URL appears more than once, remove duplicate copies when it is clearly the same link repeated.\n"
        "- Put the main link on its own line at the end.\n"
        "\n"
        "Cleanup rules:\n"
        "- Remove filler, repeated lines, hashtag ad markers, Prime trial promos, and messy sales wording.\n"
        "- Avoid overhype like 'must cop', 'insane', 'steal', 'don't miss', or 'crazy deal'.\n"
        "- Do not use emojis unless they already exist in the input and fit naturally.\n"
        "- Do not use em dashes or en dashes. Use normal hyphens only.\n"
        "- Output only the rewritten plain text plus the **bold** headline. No code fences, no preamble, no other markdown.\n"
    )

    clipped = raw
    if len(clipped) > 6000:
        clipped = clipped[:5997] + "..."

    # Encourage "main link on its own line" formatting without changing URL characters.
    # This especially helps when the input uses Discord's embed-hiding "<https://...>" form.
    try:
        clipped = re.sub(r"\s*(<https?://[^>\s]+>)\s*", r"\n\1\n", clipped)
        clipped = re.sub(r"\n{3,}", "\n\n", clipped).strip()
    except Exception:
        pass

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": f"{sys_prompt}\n{clipped}"}],
            }
        ],
        "generationConfig": {
            "temperature": max(0.0, min(float(temperature), 1.0)),
            "topP": 0.9,
            "maxOutputTokens": 1024,
        },
    }

    try:
        import aiohttp

        timeout_s = max(5.0, float(timeout_s))
        model = (model or "").strip() or "gemini-1.5-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
            async with session.post(url, json=payload) as resp:
                txt = await resp.text(errors="replace")
                if int(resp.status) >= 400:
                    return raw

                data = json.loads(txt) if txt else {}
                if usage_accumulator is not None:
                    accumulate_gemini_response_usage(data, usage_accumulator)
                out = (
                    (((data or {}).get("candidates") or [])[0] or {})
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                    or ""
                )
                out = str(out).strip()
                if not out:
                    return raw
                out = _replace_unicode_dashes_with_hyphen(out)

                # Prevent pings if Gemini ever outputs mentions.
                try:
                    return neutralize_mentions_fn(out) or raw
                except Exception:
                    return out
    except Exception:
        return raw


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    s = (text or "").strip()
    if not s:
        return None
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```$", "", s)
    try:
        o = json.loads(s)
        if isinstance(o, dict):
            return o
    except Exception:
        pass
    i = s.find("{")
    j = s.rfind("}")
    if i < 0 or j <= i:
        return None
    try:
        o = json.loads(s[i : j + 1])
        if isinstance(o, dict):
            return o
    except Exception:
        pass
    return None


