# MWDataManagerBot — Project Rules (This Bot)

This file is **scoped to `MWBots/MWDataManagerBot`**. Repo-wide rules still apply from the root **`CANONICAL_RULES.md`**; this document adds **bot-specific non‑negotiables** so routing, naming, and config stay coherent.

---

## 1. Single source of truth (no parallel routing logic)

| Responsibility | Canonical owner | Do not duplicate in |
|----------------|-----------------|---------------------|
| Slash / prefix commands for this bot | `commands.py` (+ `datamanagerbot.py` wiring) | Other bots |
| Classification & bucket tags | `classifier.py` + `global_triggers.py` | `live_forwarder.py` (forwarder may only *call* these) |
| Regex / monitor “blob” detectors | `patterns.py` | Random inline regex in forwarder except trivial guards |
| URL collect / flatten embed text | `utils.collect_embed_strings` | Ad-hoc embed parsing in multiple places |
| Outbound send (webhook vs channel) | `webhook_sender.send_via_webhook_or_bot` | Duplicate send helpers |
| Runtime settings | `config/settings.json` → `settings_store.init()` | Hardcoded channel ids in Python |

If you need a new bucket: **add one tag string**, wire **one** destination key in `settings_store.py` + `config/settings.json`, branch **once** in `classifier.py` / `global_triggers.py`, and document it in **`ROUTING_AND_FORWARDING.md`**.

---

## 2. Naming & config keys (avoid “AMZ_DEALS” drift)

- The conversational bucket is **`CONVERSATIONAL_DEALS`**.
- Settings key is **`smartfilter_destinations.CONVERSATIONAL_DEALS`** → `cfg.SMARTFILTER_CONVERSATIONAL_DEALS_CHANNEL_ID`.
- **`settings_store.py` does not fall back** to a legacy `AMZ_DEALS` key. If the key is missing, the destination id is **0** and nothing routes there.

---

## 3. Clearance vs online vs instore (hard boundaries)

1. **`source_group == "clearance"`**
   - **`detect_global_triggers` returns `[]`** — no global PRICE_ERROR / flip leakage on clearance feeds.
   - **`detect_all_link_types`**: only **`MAJOR_CLEARANCE`** when `is_major_clearance_monitor_embed_blob` matches; otherwise **no routes** (`[]`).

2. **`HD_TOTAL_INVENTORY`** is **not** `MAJOR_CLEARANCE`. It is a **separate** 1:1 route gated by **`qualifies_hd_total_inventory_route`** and top-level HD inventory settings.

3. **Instore** buckets must never be “accidentally” satisfied by clearance-only context; instore classification requires instore source + field / context gates (`classify_instore_destination`).

---

## 4. Forwarded message shaping (operator-visible rules)

These are **product** rules, not silent surprises:

1. **`strip_url_only_message_content_when_embeds`** (default **true** in `settings.json`):  
   If outbound `content` is **only URL(s)** and we are sending **non-empty formatted embeds**, **clear `content`** so Discord does not show a duplicate link line above the embed.

2. **`enable_raw_link_unwrap`**: may **augment classifier text** (`text_to_check`) with unwrapped URLs; it does **not** replace the original visible message body for forwards. It must not introduce a **second** competing classification implementation.

6. **`link_host_samples_enabled`** (default **false**): when enabled, the bot writes a runtime JSON map of **one sample URL per host** (last-seen wins) for operational review. Discord links (`discord.com`, `discord.gg`, and Discord CDN/media) are excluded. Optional settings: `link_host_samples_path`, `link_host_samples_max_hosts`.

3. **`affiliate_skip_link_only_messages`**: blocks **AFFILIATED_LINKS** for “bare URL in body, no embeds/attachments” noise.

4. **`PRICE_ERROR` empty-body helper** (forwarder only): if tag is **`PRICE_ERROR`** and content is still empty after shaping, append the first **non-Discord-media** URL from embed text so price-error posts are not “cut”.

5. **`hd_total_inventory_delete_source_on_success`**: optional **delete source** after a confirmed **`HD_TOTAL_INVENTORY`** send — **only** when explicitly enabled and the bot has **Manage Messages** in the **source** channel.

---

## 5. Major clearance pairing (live system)

- Pairing / pending state / TTL / optional timeout single-send live **only** in **`live_forwarder.py`**.
- Source channel set for pairing must include **clearance sources unioned** with any explicit `major_clearance_source_channel_ids` (see `_major_clearance_pairing_source_channel_ids`).
- **Do not** “fix” pairing in audit scripts; update **`live_forwarder.py`** only.

---

## 6. Dispatch collision rules

1. **`order_link_types`:** when **`PRICE_ERROR`** is among tags, it is **primary** and **`stop_after_first`** is set — know this when testing multi-route messages.

2. **`_collapse_dispatch_same_destination`:** after **`mirrorworld_route_*`** remap, **one outbound post per final destination** per source message — highest `_dispatch_tag_priority` wins per destination.

3. **Amazon suppression in `detect_all_link_types`:** when an Amazon-family tag is present, non-Amazon store routes are filtered from the multi-route list (see classifier tail — **AFFILIATED_LINKS** must not sneak past Amazon wins).

---

## 7. Trace / explainability

- Forwarder builds a **`trace`** dict per message (ids, preview, classifier matches, decisions).
- Prefer **`write_trace_log(trace)`** on skip/send paths when touching routing.
- When adding a new gate, add a **trace key** (e.g. `classifier.matches.my_feature = reason`) — no silent drops.

---

## 8. JSON-only / runtime data

- This bot’s **runtime** trace logs and picker state must follow repo storage rules: **no SQLite**, prefer existing JSON trace mechanisms already used by the bot.

---

## 9. When you change routing

1. Update **`classifier.py` / `global_triggers.py` / `patterns.py`** (detectors) — not the forwarder — unless the change is strictly “how we send”.
2. Update **`config/settings.json`** keys / ids.
3. Update **`ROUTING_AND_FORWARDING.md`** tables (this folder).
4. Run **`channel_route_audit.py`** for regression on real channels (remember: audit ≠ pairing).

---

## 10. Mandatory cleanup report (when you edit this bot)

Per root **`CANONICAL_RULES.md`**, every change must state:

- **Removed:** what dead path / key / branch was deleted  
- **Replaced:** what now owns that behavior  
- **Canonical:** the single module + setting keys that are now authoritative  

Paste that block into the PR / commit description for routing work.
