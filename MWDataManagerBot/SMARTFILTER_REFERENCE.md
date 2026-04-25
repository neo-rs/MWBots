# MWDataManagerBot – Smart Filters Reference

All rules, triggers, and source/destination channels.  
Source = which message origins are used; destinations are from `config/settings.json`.

---

## Source channel groups (where messages come from)

| Group | Config key | Channel IDs (from settings.json) |
|-------|------------|----------------------------------|
| **Online** | `source_channel_ids_online` | 1434967523601940655, 1434971502817706035, 1435277188839768206, 1435277272105091197, 1435268379521847369, 1435268468457996482, 1434974844444741653, 1435268203231314011, 1438910711978922216 |
| **Instore** | `source_channel_ids_instore` | 1434967990406873169, 1435277398886060073, 1434974855215714354, 1435268708632363200 |
| **Clearance** | `source_channel_ids_clearance` | 1435308747416141857, 1435308842387767356, 1435308770128298044, 1435308863795626014 |
| **Misc** | `source_channel_ids_misc` | 744273602924511283 |

**Instore classification** (major / discounted / theatre / sneakers / seasonal / instore_leads) runs **only** for channels in **`source_channel_ids_instore`**. **Clearance** sources are excluded from this pipeline (they still skip Amazon routing where configured).

---

## 1. Local classifier (classifier.py)

Evaluated in order; first match wins (except multi-destination path which collects all then applies Amazon suppression).

| # | Filter / tag | Trigger (rule) | Source restriction | Destination channel ID |
|---|--------------|----------------|--------------------|------------------------|
| **0** | **PRICE_ERROR** | Text matches: `bugged`, `wrong price`, `accidental drop`, `underpriced`, `checkout working`, `error price`, `price error`, `messed up`, `mispriced`, `glitched price`, `stacked glitch`, `glitch(ed)` | Any monitored source | **1435985494356918333** |
| **1a** | **AMAZON_PROFITABLE_LEADS** | Amazon link/ASIN + primary store is Amazon + (PROFITABLE_FLIP_PATTERN or AMAZON_PROFITABLE_INDICATOR_PATTERN). Profitable patterns: `200%`, `300%`, …, `high roi`, `great flip`, `easy money`, `quick flip`; or `avg 30`, `average 30`, `% drop`, `amazon sold`, `flip alert` | Any except clearance (clearance skips Amazon) | **1438969997178306661** |
| **1b** | **AMAZON_FALLBACK** | Amazon link/ASIN + primary Amazon, not profitable | Any except clearance | **1438433667067150416** |
| **1c** | **AMAZON** | Amazon link/ASIN + primary Amazon (used if no AMAZON_FALLBACK configured) | Any except clearance | **1435066421133443174** |
| **2** | **MONITORED_KEYWORD** | Message matches a keyword from the loaded keywords list; optional per-keyword channel overrides via `/keywordchannel` | Any | **1434974878833967227** (or override channel) |
| **3** | **INSTORE_SEASONAL** | Instore source + flip-lead shape + SEASONAL_PATTERN | **`source_channel_ids_instore` only** | **1435984551217205309** |
| **4** | **INSTORE_SNEAKERS** | Instore + flip-lead shape + SNEAKERS_PATTERN | **instore only** | **1435986120574894130** |
| **5** | **INSTORE_CARDS** | Instore + flip-lead shape + CARDS_PATTERN | **instore only** | **1435990107747516586** |
| **6** | **INSTORE_THEATRE** | Instore + flip-lead shape + theatre store + context | **instore only** | **1438996999793021008** |
| **7** | **MAJOR_STORES** | Instore + flip-lead shape + major retailer in text or Where | **instore only** | **1434974833019457707** |
| **8** | **DISCOUNTED_STORES** | Instore + flip-lead shape + discounted retailer (Ross, Ollie's, etc.) | **instore only** | **1434974822311661719** |
| **9** | **INSTORE_LEADS** | Instore + flip-lead shape + no higher-priority instore match | **instore only** | **1438433642408841329** |
| **10** | **UPCOMING** | Source = online + TIMESTAMP_PATTERN in text + `is_truly_upcoming` (future indicators like “coming soon”, “drops on”, “pre-order”, etc.; excludes “price drop”, “restock”, etc.) | Online only | **1434974811695747173** |
| **11** | **AFFILIATED_LINKS** | Source = online + `http` in (content + attachment URLs); store domain or mavely.app → AFFILIATED_LINKS; else any `http` → AFFILIATED_LINKS. Not applied if already instore-classified or Amazon. | Online only | **1435308472639160522** |
| **12** | **AMAZON** (fallback step) | No prior match + Amazon ASIN in text + primary Amazon + AMAZON channel configured | Any except clearance | **1435066421133443174** |
| **13** | **AMAZON_FALLBACK** (fallback step) | No prior match + Amazon link + primary Amazon + AMAZON_FALLBACK configured | Any except clearance | **1438433667067150416** |
| **14** | **DEFAULT** | No other match + `enable_default_fallback` = true | Any | **1438970053352751215** |

---

## 2. Global triggers (global_triggers.py)

Run after classifier; results are merged. **Source = online only**, and **not** from any **`source_channel_ids_instore`** channel.

| Filter / tag | Trigger (rule) | Source restriction | Destination channel ID |
|--------------|----------------|--------------------|------------------------|
| **PRICE_ERROR** | PRICE_ERROR_PATTERN in normalized text (same keywords as classifier) | Online, and **not** instore-allowed | **1435985494356918333** |
| **AMAZON_PROFITABLE_LEAD** | Message has Amazon content + marketplace link + product link + no DYOR block → route to Amazon profitable leads instead of flip channels | Online, not instore | **1438969997178306661** |
| **PROFITABLE_FLIP** | No Amazon content. “Flip shape”: marketplace hit (stockx, goat, ebay, etc.) + resell indicators (comps, sold for, flip, profit, roi) + marketplace link + product link. Then: (A) Labeled retail/resell prices + ROI meets thresholds → PROFITABLE_FLIP, or (B) Marketplace path + explicit ROI (e.g. 200%) or PROFITABLE_FLIP_PATTERN + ROI pass (or backcompat no prices) → PROFITABLE_FLIP. Blocked if “dyor” or strict “instore only” without product link. | Online, not instore | **1435998778871119942** |
| **LUNCHMONEY_FLIP** | Same flip shape as above but ROI below threshold or no explicit profitable trigger; or marketplace path without profitable trigger. | Online, not instore | **1439912388555182151** |

---

## 3. Destination channel ID quick reference

From `config/settings.json`:

**smartfilter_destinations**

| Tag | Channel ID |
|-----|------------|
| AMAZON | 1435066421133443174 |
| AMAZON_FALLBACK | 1438433667067150416 |
| AFFILIATED_LINKS | 1435308472639160522 |
| UPCOMING | 1434974811695747173 |
| INSTORE_LEADS | 1438433642408841329 |
| INSTORE_SEASONAL | 1435984551217205309 |
| INSTORE_SNEAKERS | 1435986120574894130 |
| INSTORE_CARDS | 1435990107747516586 |
| INSTORE_THEATRE | 1438996999793021008 |
| MAJOR_STORES | 1434974833019457707 |
| DISCOUNTED_STORES | 1434974822311661719 |
| MONITORED_KEYWORD | 1434974878833967227 |
| DEFAULT | 1438970053352751215 |

**global_trigger_destinations**

| Tag | Channel ID |
|-----|------------|
| PRICE_ERROR | 1435985494356918333 |
| PROFITABLE_FLIP | 1435998778871119942 |
| LUNCHMONEY_FLIP | 1439912388555182151 |
| AMAZON_PROFITABLE_LEADS | 1438969997178306661 |

---

## 4. Pattern reference (patterns.py)

- **PRICE_ERROR_PATTERN**: `bugged|wrong price|accidental drop|underpriced|checkout working|error price|price error|price/checkout/cart/listing messed up OR messed up price/checkout/cart/listing|mispriced|glitched price|stacked glitch|glitch(ed)` (standalone “messed up” does not match).
- **PRICE_ERROR substance gate** (`passes_deal_substance_gate` in `patterns.py`): even when `PRICE_ERROR_PATTERN` matches, routing requires either **deal signals** (http(s) URL, `/dp/`, Amazon-ish host, `B0…` ASIN, `$`/`£`/`€` + digits, `% off`, etc.) **or** enough **core** characters after stripping a trailing `---` block, a final `… From: … | By: …` line, or an inline `… From: … | By: …` suffix. Default minimum core length: **`price_error_min_substance_chars`** in settings (module default **52**). Trace key when blocked: `price_error_substance_gate` = `blocked_thin_placeholder`.
- **PROFITABLE_FLIP_PATTERN**: `200%|300%|400%|500%|\d{3,}%|3x|4x|5x|\d+x retail|high roi|exceptional margin|great flip|easy money|quick flip`.
- **AMAZON_PROFITABLE_INDICATOR_PATTERN**: `avg 30|average 30|avg 365|\d+% drop|\d+% off|amazon sold|flip alert`.
- **INSTORE**: Requires lines matching Retail:…, Resell:…, Where:… (or Location:…). SEASONAL, SNEAKERS, CARDS, THEATRE, MAJOR_STORES, DISCOUNTED_STORES use their respective pattern lists; INSTORE_LEADS is the catch-all instore.

---

## 5. Evaluation order (single-target fallback)

1. PRICE_ERROR  
2. AMAZON (profitable → AMAZON_PROFITABLE_LEADS; else AMAZON_FALLBACK or AMAZON)  
3. MONITORED_KEYWORD  
4. Instore (Seasonal → Sneakers → Cards → Theatre → Major → Discounted → INSTORE_LEADS)  
5. UPCOMING (online only)  
6. AFFILIATED_LINKS (online only)  
7. AMAZON / AMAZON_FALLBACK / DEFAULT fallbacks  

Global triggers run separately and are merged with classifier results; PRICE_ERROR can come from either. When both classifier and global triggers run, destinations are deduplicated and PRICE_ERROR is ordered first (primary) when present.
