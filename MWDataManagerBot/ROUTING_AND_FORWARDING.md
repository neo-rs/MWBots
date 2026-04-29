# MWDataManagerBot — Routing & Forwarding Reference

This document describes how **MWDataManagerBot** decides where a monitored Discord message should go, and how **`live_forwarder.py`** turns the source message into an outbound post.

Canonical code:

| Concern | Primary files |
|--------|----------------|
| Source group, instore buckets, online buckets, `select_target_channel_id` / `detect_all_link_types` | `classifier.py`, `patterns.py`, `keywords.py` |
| Global triggers (price error glitched, flips, Amazon profitable from globals) | `global_triggers.py`, `patterns.py`, `utils.py` |
| Dispatch order, pairing, dedupe, message shaping, send | `live_forwarder.py`, `webhook_sender.py`, `utils.py` |
| Configuration loaded at runtime | `config/settings.json` → `settings_store.py` → `config.py` (loader) |

---

## 1. Configuration map (where channel IDs live)

### 1.1 Smartfilter destinations (`smartfilter_destinations` in `config/settings.json`)

These keys map to **`cfg.SMARTFILTER_*_CHANNEL_ID`** in `settings_store.py`. Each key corresponds to a **dispatch tag** (see §3).

Examples (keys are stable; IDs are per deployment):

- `AMAZON`, `AMAZON_FALLBACK`, `CONVERSATIONAL_DEALS`, `AFFILIATED_LINKS`, `UPCOMING`
- `INSTORE_LEADS`, `INSTORE_SEASONAL`, `INSTORE_SNEAKERS`, `INSTORE_CARDS`, `INSTORE_THEATRE`
- `MAJOR_STORES`, `DISCOUNTED_STORES`, `MONITORED_KEYWORD`
- `UNCLASSIFIED`, `DEFAULT`

### 1.2 Global trigger destinations (`global_trigger_destinations`)

Loaded as **`cfg.SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID`**, **`SMARTFILTER_FLIPS_*`**, **`SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID`**, **`SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID`**, etc.

Tags from globals include: **`PRICE_ERROR`**, **`PROFITABLE_FLIP`**, **`LUNCHMONEY_FLIP`**, **`AMAZON_PROFITABLE_LEAD`**.

### 1.3 Top-level routing knobs (not under `smartfilter_destinations`)

- **HD total inventory (1:1):** `hd_total_inventory_source_channel_id`, `hd_total_inventory_destination_channel_id`, `hd_total_inventory_min_total`, optional `hd_total_inventory_delete_source_on_success`
- **Major clearance pairing:** `major_clearance_pair_ttl_seconds`, `major_clearance_send_single_on_timeout`, `major_clearance_source_channel_ids`
- **Forwarding / shaping:** `use_webhooks_for_forwarding`, `forward_attachments_as_files`, `enable_raw_link_unwrap`, `affiliate_skip_link_only_messages`, `strip_url_only_message_content_when_embeds`, `send_min_interval_seconds`, etc.

### 1.4 MirrorWorld route maps

- `mirrorworld_route_online`, `mirrorworld_route_instore` — applied in `live_forwarder.py` via `_apply_route_map` **after** classification yields a destination channel id.

---

## 2. Source groups (`determine_source_group`)

Derived from **`source_channel_ids_*`** lists in `config/settings.json`:

| `source_group` | Meaning |
|----------------|---------|
| `online` | Channel id ∈ `source_channel_ids_online` |
| `instore` | Channel id ∈ `source_channel_ids_instore` |
| `clearance` | Channel id ∈ `source_channel_ids_clearance` |
| `misc` | Channel id ∈ `source_channel_ids_misc` |
| `unknown` | Anything else (usually should not be monitored unless policy widens) |

Many classifier gates are **scoped to one group** (e.g. conversational deals are **online-only** and require the replay/source channel to be in the online list).

---

## 3. Dispatch tags (classifier + globals)

Each processed message can produce one or more **`(destination_channel_id, tag)`** pairs. **`order_link_types`** may reorder and set **`stop_after_first`** when **`PRICE_ERROR`** is present.

### 3.1 HD one-to-one inventory

| Tag | When | Destination |
|-----|------|---------------|
| **`HD_TOTAL_INVENTORY`** | `qualifies_hd_total_inventory_route(...)` in `classifier.py`: correct **source** channel, definitive HD clearance embed, **Total inventory ≥ min** | `hd_total_inventory_destination_channel_id` |

`detect_all_link_types` returns **only** this pair when it qualifies (early exit).

### 3.2 Major clearance (Tempo / HD monitor embeds)

| Tag | When | Destination |
|-----|------|---------------|
| **`MAJOR_CLEARANCE`** | Clearance sources: `is_major_clearance_monitor_embed_blob`. Instore sources: monitor blob or `is_major_clearance_followup_blob` (and pairing logic in `live_forwarder.py`). | `SMARTFILTER_MAJOR_CLEARANCE_CHANNEL_ID` (`global_trigger_destinations.MAJOR_CLEARANCE`) |

**Live vs audit:** `channel_route_audit.py` classifies only; **pairing / pending cache / timeout single-send** are **`live_forwarder.py`** only.

### 3.3 Price error / glitched

| Tag | When | Destination |
|-----|------|---------------|
| **`PRICE_ERROR`** | `PRICE_ERROR_PATTERN` matches; not the rigid AMZ “price errors” monitor template (`is_amz_price_errors_monitor_blob`); not instore-only path in `select_target_channel_id`; **global triggers skip entirely for `clearance` sources** | `SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID` |

**Dispatch order:** `order_link_types` puts **`PRICE_ERROR` first** and sets **`stop_after_first`** so other tags on the same message do not send unless you change that helper.

### 3.4 Amazon family (strict + profitable + fallback + “conversational deals”)

| Tag | When (summary) | Destination |
|-----|----------------|-------------|
| **`AMAZON`** | `AMAZON_LINK_PATTERN` + `_is_amazon_primary` (kills Woot / non-Amazon-primary “comp” cases); not skipped for clearance major-clearance branch when applicable | `SMARTFILTER_AMAZON_CHANNEL_ID` |
| **`AMAZON_FALLBACK`** | When primary Amazon id missing but link pattern matches; also empty-route fallback in `detect_all_link_types` when `amazon_detected` | `SMARTFILTER_AMAZON_FALLBACK_CHANNEL_ID` |
| **`AMAZON_PROFITABLE_LEAD`** | Profitable signals + not monitor-template-blocked + not “complicated monitor” path; also global trigger path for Amazon-heavy content blocked from flip channels | `SMARTFILTER_AMAZON_PROFITABLE_LEADS_CHANNEL_ID` |
| **`CONVERSATIONAL_DEALS`** | `_looks_like_conversational_amazon_deal(...)` on **content+embed** blob (`pe_sel` / `pe_check_blob`), **online** + configured online channel, many explicit skips; **or** forced branch from `is_amz_deals_affiliate_bridge_blob` in multi-route Amazon logic when that helper is true and conversational dest is configured | `SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID` (`smartfilter_destinations.CONVERSATIONAL_DEALS`) |

**Patterns / helpers** live mainly in `patterns.py` (Amazon link regex, monitor blobs, RingInTheDeals detector, etc.).

### 3.5 Monitored keywords

| Tag | When | Destination |
|-----|------|-------------|
| **`MONITORED_KEYWORD`** | Keyword scan hit (`keywords.py`); optional per-keyword override channel id (still same tag string) | `SMARTFILTER_MONITORED_KEYWORD_CHANNEL_ID` and/or override id |

### 3.6 Instore buckets (`classify_instore_destination`)

Order inside the function:

1. `INSTORE_SEASONAL` — `SEASONAL_PATTERN`
2. `INSTORE_CARDS` — `CARDS_PATTERN` + TCG context; toy suppress can zero this
3. `INSTORE_SNEAKERS` — `SNEAKERS_PATTERN` + footwear gating helpers
4. `INSTORE_THEATRE` — theatre merch / venue context
5. **Suppress** instore store buckets when `is_major_clearance_monitor_embed_blob` matches (major clearance owns that shape)
6. `MAJOR_STORES` / `DISCOUNTED_STORES` — store list patterns + `store_category` tie-break
7. `INSTORE_LEADS` — general instore catch-all

Each returns **`(channel_id, tag)`** using the matching **`SMARTFILTER_INSTORE_*`** / **`MAJOR_STORES`** / **`DISCOUNTED_STORES`** id.

### 3.7 Affiliate / “other store” online bucket

| Tag | When | Destination |
|-----|------|-------------|
| **`AFFILIATED_LINKS`** | Online; http present; known store domain regex **or** `mavely.app` in blob; not “New Deal Found + GRAB IT HERE” template; not Discord-media-only; optional skip for link-only body when enabled | `SMARTFILTER_AFFILIATED_LINKS_CHANNEL_ID` |

### 3.8 Upcoming

| Tag | When | Destination |
|-----|------|-------------|
| **`UPCOMING`** | `TIMESTAMP_PATTERN` on `text_to_check` + `is_truly_upcoming_explain` passes | `SMARTFILTER_UPCOMING_CHANNEL_ID` |

### 3.9 Global flip triggers

| Tag | When (very short) | Destination |
|-----|-------------------|-------------|
| **`PROFITABLE_FLIP`** | Online, not instore, not clearance; ROI / marketplace / keyword paths in `detect_global_triggers` | `SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID` |
| **`LUNCHMONEY_FLIP`** | Fallback / lower bar paths in the same evaluator | `SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID` |

**Amazon content** is explicitly steered away from flip channels into **`AMAZON_PROFITABLE_LEAD`** when gates pass (see `global_triggers.py`).

### 3.10 Default fallback

| Tag | When | Destination |
|-----|------|---------------|
| **`DEFAULT`** | `detect_all_link_types` produced **no** routes, `enable_default_fallback` is **true**, and default channel configured | `SMARTFILTER_DEFAULT_CHANNEL_ID` |

### 3.11 Unclassified (not a normal multi-route tag)

When **no route** is found, **`live_forwarder.py`** may forward to **`UNCLASSIFIED`** and attach the interactive picker (`_send_unclassified_with_picker`). This is **operational UX**, not part of `detect_all_link_types`’s primary list.

---

## 4. `live_forwarder.py` — message build & send pipeline

High-level order inside **`MessageForwarder.handle_message`**:

1. **Eligibility:** destination guild, monitored source channel (and category-based monitoring if configured), optional **`MONITOR_WEBHOOK_MESSAGES_ONLY`** (webhook or bot author).
2. **Dedupe:** `processed_ids` for non-edit deliveries.
3. **Payload snapshot:** `_to_filter_payload` (content, embed dicts, attachment meta).
4. **Embed hydration:** `_hydrate_short_embed_message` for “thin” embed placeholders.
5. **Hard filter:** `_should_filter_message` (empty / mention-only).
6. **Per-channel dedupe:** `recent_hashes` + `RECENT_TTL_SECONDS`.
7. **Classification text:** `content + collect_embed_strings(embeds)` (attachment CDN URLs excluded from classifier text).
8. **Raw-link augmentation (optional):** unwrap / augment `text_to_check` when `enable_raw_link_unwrap` — **classifier only**; forwarded message text stays the original visible `content` (no inline URL replacement).
9. **Link host samples (optional):** when `link_host_samples_enabled` is true, record one sample URL per host (last-seen wins) into a runtime JSON file for later review. Discord links are excluded; cap enforced by `link_host_samples_max_hosts`.
9. **Link tracking:** `_track_link_occurrences`.
10. **Global duplicate skip:** `_is_global_duplicate` on cross-channel signature.
11. **Routes:** `detect_all_link_types` + `detect_global_triggers` + `order_link_types`; if empty, **`select_target_channel_id`** fallback.
12. **Per-destination collapse:** `_collapse_dispatch_same_destination` after **`_apply_route_map`** so the same final channel is not spammed with multiple tags.
13. **Outbound shaping:**
    - Start from **`content`** (no affiliate unwrap rewrite in the outbound body)
    - **`format_embeds_for_forwarding(embeds)`**
    - Attachment handling depends on **`FORWARD_ATTACHMENTS_AS_FILES`**
    - If **`strip_url_only_message_content_when_embeds`**: when content is **URL-only** and **`embeds_out`** is non-empty, **clear content** (embed carries the deal card).
14. **Major-clearance pairing block** (clearance + unioned source ids): pending caches, TTL, optional timeout send, attachment filtering for barcode noise, HD inventory guard.
15. **Send loop:** for each `(dest, tag)` after route map + dedupe guards → **`_send_to_destination`** (webhook-first unless a path needs `channel.send` return value / reference / view).
16. **Tag-specific post-send:**
    - **`PRICE_ERROR`:** if outbound content empty, may append first **non-Discord-CDN** URL found in embed text so the post is not “cut”.
    - **`HD_TOTAL_INVENTORY`:** optional **delete source message** after successful send when `hd_total_inventory_delete_source_on_success` is true.

---

## 5. Audit vs production

**`channel_route_audit.py`** replays **`detect_all_link_types`**, **`select_target_channel_id`**, and **`detect_global_triggers`** against history. It does **not** execute the full **`live_forwarder`** pipeline (pairing, dedupe timing, webhook send, URL-only strip unless you mirror posts separately).

---

## 6. Operational checklist

1. **`config/settings.json`** ids match the Discord channels you intend.
2. **Source channel lists** include every mirror/monitored source id.
3. **`CONVERSATIONAL_DEALS`** key exists (legacy `AMZ_DEALS` key is not read by `settings_store` anymore).
4. **Bot permissions:** send + embed in destinations; **Manage Messages** in HD inventory **source** channel if delete-on-success is enabled.
5. After changes: restart **MWDataManagerBot** so `settings_store.init()` reloads JSON.
