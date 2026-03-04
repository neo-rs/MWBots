# MWDiscumBot (Discord2Discord Bridge)

Standalone forwarder: messages from configured source channels are sent to Discord webhooks. Uses a **user account token** (discum) for reading and an optional **bot token** (discord.py) for the `/discum browse` slash command.

## Run

- **Windows:** `run_discumbot.bat` or `python discumbot.py`
- **Linux/Oracle:** `run_bot.sh discumbot` (from repo root)

## Config (canonical paths)

All under `MWDiscumBot/config/`:

- `tokens.env` – secrets (user token, optional bot token). Copy from `tokens.env.example`.
- `settings.json` – non-secret settings (source_guild_ids, mirrorworld_server_id, etc.).
- `settings.runtime.json` – runtime-only overrides (e.g. source_guild_ids added by `/discum browse`).
- `channel_map.json` – source channel ID → webhook URL. Created empty if missing; edit or use `/discum browse` to add mappings.

Config loading is centralized in `discum_config.py` (single source of truth for paths and parsing).

## Slash command

- **`/discum browse`** – View and manage channel mappings (requires bot token in `tokens.env` as `DISCORD_BOT_TOKEN` or `BOT_TOKEN`). Synced to the guild set in `settings.json` as `mirrorworld_server_id` (canonical Mirror World ID: `1431314516364230689`).

## Guild IDs (from CANONICAL_RULES.md)

- **Mirror World:** `1431314516364230689` – set as `mirrorworld_server_id` in config.
