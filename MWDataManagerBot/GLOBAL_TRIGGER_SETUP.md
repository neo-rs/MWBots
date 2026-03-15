# Global Trigger Channels Setup

This document describes how to enable routing to **#price-error-glitched** and **#amz-profitable-leads**.

## 1. Add `global_trigger_destinations` to your config

The bot loads settings from its config (e.g. `settings.json` or `config.json`). Add a `global_trigger_destinations` block:

```json
{
  "global_trigger_destinations": {
    "PRICE_ERROR": 1435985494356918333,
    "AMAZON_PROFITABLE_LEADS": 1438969997178306661
  }
}
```

- **PRICE_ERROR** (1435985494356918333) → #price-error-glitched
- **AMAZON_PROFITABLE_LEADS** (1438969997178306661) → #amz-profitable-leads

## 2. Ensure channel webhooks in `channel_map.json`

`config/channel_map.json` must map each destination channel ID to a Discord webhook URL:

- **1435985494356918333** (price-error-glitched) – already present
- **1438969997178306661** (amz-profitable-leads) – add if missing:
  1. In Discord: #amz-profitable-leads → Edit Channel → Integrations → Create Webhook
  2. Add the entry: `"1438969997178306661": "https://discord.com/api/webhooks/..."`
  3. Ensure source channels are listed in `source_channel_ids_online` (or your source config) so the bot monitors them

## 3. Routing logic (already implemented)

| Channel               | Trigger patterns |
|-----------------------|------------------|
| **price-error-glitched** | `price error`, `glitch`, `bugged`, `wrong price`, `accidental drop`, `underpriced`, `mispriced`, `glitched price`, `stacking glitch`, etc. |
| **amz-profitable-leads** | Amazon link + (`200%`, `300%`, `3x`, `4x`, `high roi`, `great flip`, `easy money`, `quick flip`, `avg 30`, `average 30`, `% drop`, `amazon sold`, `flip alert`, etc.) |

## 4. Restart the bot

After updating config and `channel_map.json`, restart the DataManagerBot service so the new routing is applied.
