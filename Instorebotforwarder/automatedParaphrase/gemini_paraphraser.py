from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional

_gem_log = logging.getLogger("instorebotforwarder")


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


async def minimal_rephrase_keep_urls(
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
    Gemini-based minimal paraphraser.

    Contract:
    - Returns a rewritten version of `text` that is minimally different.
    - URLs should remain unchanged (prompted, but the caller must still validate if needed).
    - Always safe-fallback: on any failure returns the original `text`.
    """
    raw = (text or "").strip()
    if not raw:
        return ""

    key = (gemini_api_key or "").strip()
    if not key:
        return raw

    # Rewrite prompt: RS-style, clean, readable. URLs must remain exact strings.
    sys_prompt = (
        "You rewrite short Discord deal/lead messages in a clean RS-style tone.\n"
        "Rules:\n"
        "- Rewrite the message so it reads cleaner, sharper, and easier to post.\n"
        "- You may restructure the message for better flow; do not only swap words.\n"
        "- Keep all product names, brands, prices, codes, discounts, warranty details, and store comparisons accurate.\n"
        "- Keep every URL exactly the same: same characters, same order.\n"
        "- If the same URL appears more than once, remove duplicate copies when it is clearly the same link repeated.\n"
        "- Remove filler, repeated lines, hashtag ad markers, Prime trial promos, and messy sales wording.\n"
        "- Do not add new claims, fake urgency, fake stock info, or extra resale claims.\n"
        "- Keep it casual and deal-focused, not corporate.\n"
        "- Avoid overhype like 'must cop', 'insane', 'steal', 'don't miss', or 'crazy deal'.\n"
        "- Prefer short readable lines instead of one long paragraph.\n"
        "- Put the main link on its own line when possible.\n"
        "- Lead with the strongest hook: price + product, discount, or comparison value.\n"
        "- Mention coupon/code instructions clearly if present.\n"
        "- Mention comparison pricing only if it exists in the input.\n"
        "- Do not use emojis unless they already exist in the input and fit naturally.\n"
        "- Do not use em dashes or en dashes. Use normal hyphens only.\n"
        "- Output only the rewritten plain text. No markdown fences, no preamble.\n"
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


