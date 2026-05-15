# MWPingBot

PingBot for Mirror World: pings in configured channels with cooldown and dedupe.  
**Settings schema matches the live bot** at `mirror-world/MWPingBot/` on Oracle (same `config/settings.json`).

## Live bot (pingbot.py)

- **Path on server:** `/home/rsadmin/bots/mirror-world/MWPingBot/`
- **Config:** `config/settings.json`, `config/tokens.env`
- **Keys in settings.json:**
  - `mirrorworld_server_id` (string) – Mirror World guild ID
  - `ping_channel_ids` (list) – channel IDs where the bot pings
  - `cooldown_seconds` – per-channel cooldown before next ping
  - `dedupe_ttl_seconds` – TTL for content dedupe
  - `dm_notify_user_ids` (list) – Discord user IDs to DM after each successful `@everyone` ping (embed summary + embed images)
  - `verbose` (optional bool)
- **Token:** `PING_BOT` in `config/tokens.env`

If `mirrorworld_server_id` is missing or `ping_channel_ids` is empty, the bot logs warnings and will not ping.

## Slash commands (/ping settings)

This repo adds **ping_command_bot.py** (and **ping_config.py**) so you can manage the same settings from Discord:

- **/ping settings** – UI to view and edit:
  - **View channels & timings** – current ping channels, cooldown, dedupe TTL
  - **Add channels** – select server channels to add for pinging
  - **Remove channel** – remove one channel from the list
  - **Set cooldown (s)** – cooldown_seconds (0–86400)
  - **Set dedupe TTL (s)** – dedupe_ttl_seconds (0–86400)

Writes go to the same `config/settings.json` the main pingbot reads.

## Deployment

- **Live bot** runs from `mirror-world/MWPingBot/` (run_bot.sh pingbot → pingbot.py).
- **This code** lives under `MWBots/MWPingBot/`. To use it on the server you can:
  1. Copy `ping_config.py`, `ping_command_bot.py`, and `config/settings.json` (and optional `config/tokens.env.example`) into the live `MWPingBot/` folder so the main bot and slash command bot share one config dir; or
  2. Deploy the whole `MWBots/MWPingBot/` tree and run the command bot from there (then both must point at the same `config/settings.json` path, or you sync that file).

Ensure **config/settings.json** on the server has at least:

```json
{
  "verbose": true,
  "mirrorworld_server_id": "1431314516364230689",
  "cooldown_seconds": 30,
  "dedupe_ttl_seconds": 30,
  "ping_channel_ids": [],
  "dm_notify_user_ids": [971528709876113478]
}
```

Then set `ping_channel_ids` via /ping settings or by editing the file. The main bot does not hot-reload; restart the service after editing if it only reads settings at startup.
