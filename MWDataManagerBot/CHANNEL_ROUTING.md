# Channel routing reference (selected IDs)

Messages from **all** `source_channel_ids_online` are classified by the live forwarder (classifier + global triggers); the resulting destinations include the three channels below.

## Three primary online filter destinations

| Channel ID        | Filter / tag | How it receives messages |
|-------------------|--------------|---------------------------|
| 1435308472639160522 | **AFFILIATED_LINKS** | Online sources: message has `http` (store domain, mavely, or any link) → this channel. |
| 1435985494356918333 | **PRICE_ERROR** | Text matches glitch/price-error pattern (e.g. "price error", "glitch", "wrong price", "mispriced") → this channel. |
| 1435066509860012073 | **DEFAULT** | Online sources: no other classification (no Amazon, keyword, instore, upcoming, affiliate, etc.) → this channel when `enable_default_fallback` is true. |

All three are in `smartfilter_destinations` or `global_trigger_destinations` in `config/settings.json`. They also appear in `mirrorworld_route_online` for backward compatibility.

## Removed / not in use
- **FULL_SEND** and **MAJOR_CLEARANCE** are not in `settings_store` or `settings.json`. **FULL_SEND_PATTERN** was removed from `patterns.py`. **PRICE_ERROR_PATTERN** is the single pattern used for glitch/price-error routing.
- **No neonxt:** MWDataManagerBot runs standalone from `config/settings.json` and `config/tokens.env`; no imports from `neonxt`.
