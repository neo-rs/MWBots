from __future__ import annotations

import difflib
import json
import logging
import re
from typing import Any, Dict, Optional, Tuple

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


def _norm_similarity(s: str) -> str:
    t = (s or "").lower()
    t = re.sub(r"\s+", " ", t).strip()
    t = t.replace("**", "").replace("|", " ")
    return t


def _replace_unicode_dashes_with_hyphen(s: str) -> str:
    """Em dash (—) / en dash (–) -> ASCII hyphen-minus (-); keeps Discord copy plain."""
    if not s:
        return s
    return s.replace("\u2014", "-").replace("\u2013", "-")


def _amz_deal_rewrite_too_close_to_source(h0: str, b0: str, ho: str, bo: str, *, threshold: float = 0.84) -> bool:
    """True when combined output is almost the same string as input (cosmetic edit)."""
    src = _norm_similarity(f"{h0}\n{b0}")
    out = _norm_similarity(f"{ho}\n{bo}")
    if len(src) < 12:
        return False
    r = difflib.SequenceMatcher(None, src, out).ratio()
    return r >= threshold


_DEAL_WRITER_STYLE_EXAMPLES = """
Style reference (fabricated products; do NOT reuse these names/numbers in your output, match HEADER_INPUT/BODY_INPUT only):

INPUT header: 412-PIECE TOOL KIT $35 ON AMAZON
INPUT body: Was $100; similar kits often ~$80
OUTPUT header: 412-Piece Tool Kit | $35 Shipped
OUTPUT body: Down to $35 when comparable kits still hang near $80-$100. Worth grabbing if you want a full homeowner set without paying toolbox prices.

INPUT header: 53% OFF Laundry Pods | Lowest EVER on Amazon
INPUT body: $12 with clip coupon (listed up to $28 at other stores). Good stock-up before travel season.
OUTPUT header: Laundry Pods | $12 After Coupon (53% Off)
OUTPUT body: Amazon is sitting at a real low on these pods. Big-box prices are still up around $28, so $12 after the clip is a solid pantry refill.
""".strip()


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

    # Keep this prompt small: URLs must match exactly; prose must change so output != input.
    sys_prompt = (
        "You are rewriting a short Discord deal/lead message.\n"
        "Rules:\n"
        "- Change the wording (sentence order, synonyms, punctuation) so the result is NOT identical "
        "to the input, but keep the same meaning, numbers, prices, and product/brand names.\n"
        "- Keep every URL exactly as in the input: same characters, same order, same count "
        "(do not add, remove, shorten, or edit any URL).\n"
        "- Do not add new claims.\n"
        "- Remove hashtag ad markers (e.g. trailing #ad) and standalone Prime free-trial promo lines; "
        "keep one clear product-focused flow.\n"
        "- Output only the rewritten plain text (no markdown fences, no preamble).\n"
    )

    clipped = raw
    if len(clipped) > 6000:
        clipped = clipped[:5997] + "..."

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


async def minimal_rephrase_amz_deal_header_body_json(
    *,
    header: str,
    body: str,
    gemini_api_key: str,
    model: str,
    temperature: float,
    timeout_s: float,
    neutralize_mentions_fn,
    usage_accumulator: Optional[Dict[str, int]] = None,
) -> Tuple[str, str, Optional[str]]:
    """
    Deal-writer pass: JSON {"header": "...", "body": "..."} for Discord embeds (not minimal paraphrase).
    Coupon/checkout lines and short price stubs stay out of the prompt (caller parses them into kept_lines).
    May run a second API call when output is unchanged or too close to source (cosmetic edit).

    Returns (header_out, body_out, api_err). On failure, returns (h_in, b_in, reason).
    """
    h_in = (header or "").strip()
    b_in = (body or "").strip()
    if not h_in and not b_in:
        return "", "", None

    key = (gemini_api_key or "").strip()
    if not key:
        return h_in, b_in, "no_api_key"

    h_clip = h_in[:400]
    b_clip = b_in[:2000]

    sys_prompt_main = (
        "You write scannable Discord DEAL COPY for resellers (not a timid paraphrase).\n"
        "Return JSON only with keys \"header\" and \"body\". No markdown fences; no ** or __ in values; no emojis; no URLs.\n"
        "STRICT: Never use Unicode em dash (—) or en dash (–), including spaced forms like \" — \". "
        "Use ASCII hyphen-minus (-), pipe (|), comma, or colon instead (e.g. \"Raid | $3\" or \"Raid - $3 After Sub/Save\").\n"
        f"{_DEAL_WRITER_STYLE_EXAMPLES}\n\n"
        "Your job for the real inputs below:\n"
        "- HEADER: one tight line with product identity + price hook + percent-off if present. "
        "Prefer formats like \"Brand Product | $X (Y% Off)\", \"Brand - $X Shipped\", or \"… After Code\". "
        "You may drop low-value tail phrases such as \"on Amazon\" / \"Lowest EVER on Amazon\" if the deal still reads clearly.\n"
        "- BODY: If BODY_INPUT is empty, body must be \"\". Otherwise write 2-4 sentences in a natural deal-poster voice: "
        "lead with why the price matters, weave in compare-to-other-stores or stack context from the input, light personality OK. "
        "Sound human (\"sharp stock-up\", \"real low\", \"worth a look\"), not corporate SEO.\n"
        "FACTS (strict): Copy every $ amount, percent off, and product/brand name from HEADER_INPUT/BODY_INPUT. "
        "Do not invent MSRPs, store counts, or features not implied by the inputs. "
        "Do not add coupon/checkout codes unless they appear in BODY_INPUT.\n"
        "BANNED: \"available for\", \"currently listed\", \"is listed on Amazon\", \"can be purchased\", "
        "\"for sale at\", robotic filler.\n"
        "HEADER length: if HEADER_INPUT has more than 18 characters, your header may be up to 90%% longer than HEADER_INPUT "
        "(rebalancing words is OK); never exceed 130 characters.\n\n"
        "===HEADER_INPUT===\n"
        f"{h_clip}\n"
        "===BODY_INPUT===\n"
        f"{b_clip}\n"
    )

    sys_prompt_push = (
        "Same JSON task as before, but your last draft was too close to the source (minor word shuffles only).\n"
        "REFRAME: new sentence structures and rhythm; keep every price, %, and product fact identical.\n"
        "Header: lead with product + price (or % off), not a clone of the shouty source line. No em dash (—) or en dash (–); use - or |.\n"
        "Body: open with a fresh angle (e.g. compare-store framing, timing/use-case), 2-4 sentences.\n"
        "No ** no emojis no URLs. Keys: header, body.\n\n"
        "===HEADER_INPUT===\n"
        f"{h_clip}\n"
        "===BODY_INPUT===\n"
        f"{b_clip}\n"
    )

    response_json_schema = {
        "type": "object",
        "properties": {
            "header": {"type": "string", "description": "Scannable deal headline, product + price hook."},
            "body": {
                "type": "string",
                "description": "2-4 sentence deal copy; empty if BODY_INPUT was empty. No em or en dash characters.",
            },
        },
        "required": ["header", "body"],
    }

    try:
        import aiohttp

        timeout_s = max(5.0, float(timeout_s))
        model = (model or "").strip() or "gemini-1.5-flash"
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

        async def _one_round(session: Any, temp_use: float, prompt_text: str) -> Tuple[str, str, Optional[str]]:
            """Returns (h_out, b_out, err). err is None on HTTP 200 + parseable JSON."""
            payload = {
                "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
                "generationConfig": {
                    "temperature": max(0.0, min(float(temp_use), 1.0)),
                    "topP": 0.92,
                    "maxOutputTokens": 768,
                    "responseMimeType": "application/json",
                    "responseJsonSchema": response_json_schema,
                },
            }
            async with session.post(url, json=payload) as resp:
                txt = await resp.text(errors="replace")
                st = int(resp.status)
                if st >= 400:
                    _gem_log.warning(
                        "[FLOW:GEMINI] generateContent HTTP status=%s body_preview=%r",
                        st,
                        (txt or "")[:400],
                    )
                    return h_in, b_in, f"http_{st}"

                data = json.loads(txt) if txt else {}
                if usage_accumulator is not None:
                    accumulate_gemini_response_usage(data, usage_accumulator)
                raw_out = (
                    (((data or {}).get("candidates") or [])[0] or {})
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                    or ""
                )
                raw_out = str(raw_out).strip()
                if not raw_out:
                    return h_in, b_in, "empty_model_output"

                obj = _extract_json_object(raw_out)
                if not isinstance(obj, dict):
                    _gem_log.info(
                        "[FLOW:GEMINI] invalid_json_shape raw_preview=%r",
                        (raw_out or "")[:320],
                    )
                    return h_in, b_in, "invalid_json_shape"

                _h = obj.get("header", None)
                ho = h_in if _h is None else str(_h).strip() or h_in
                _b = obj.get("body", None)
                if _b is None:
                    bo = b_in
                else:
                    bo = str(_b).strip()
                    if not bo and not b_in:
                        bo = ""

                try:
                    ho = neutralize_mentions_fn(ho) or h_in
                except Exception:
                    pass
                try:
                    bo = neutralize_mentions_fn(bo) or bo
                except Exception:
                    pass
                ho = _replace_unicode_dashes_with_hyphen(ho)
                bo = _replace_unicode_dashes_with_hyphen(bo)
                return ho, bo, None

        base_t = max(0.0, min(float(temperature), 1.0))
        to = aiohttp.ClientTimeout(total=timeout_s)
        async with aiohttp.ClientSession(timeout=to) as session:
            h_out, b_out, err1 = await _one_round(session, base_t, sys_prompt_main)
            if err1 is not None:
                return h_in, b_in, err1

            unchanged = h_out == h_in and b_out == b_in
            if unchanged and (h_in or b_in):
                h2, b2, err2 = await _one_round(session, min(0.95, base_t + 0.35), sys_prompt_main)
                if err2 is None:
                    h_out, b_out = h2, b2
            elif (h_in or b_in) and _amz_deal_rewrite_too_close_to_source(h_in, b_in, h_out, b_out):
                push_t = min(0.93, base_t + 0.22)
                h2, b2, err2 = await _one_round(session, push_t, sys_prompt_push)
                if err2 is None:
                    h_out, b_out = h2, b2
                    _gem_log.info(
                        "[FLOW:GEMINI] amz_deals_struct deal_writer_push_retry ratio_was_high=1 temp=%.2f",
                        push_t,
                    )

            return h_out, b_out, None
    except Exception:
        return h_in, b_in, "exception"

