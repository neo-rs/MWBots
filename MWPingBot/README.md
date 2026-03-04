# MWPingBot

PingBot for Mirror World: pings in configured channels with a configurable delay.

## Slash commands (same idea as /discum for DiscumBot)

- **/ping settings** – Open the settings UI to:
  - **View** current ping channels and delay
  - **Add channels** – select server channels to add for pinging
  - **Remove channel** – remove a channel from the ping list
  - **Set delay (seconds)** – set the ping delay (0–86400)

Settings are stored in `config/settings.json`:
- `ping_channel_ids`: list of channel IDs that get pings
- `ping_delay_seconds`: delay in seconds
- `mirrorworld_guild_id`: target guild (Mirror World)

## Running the command bot

1. **Bot token**  
   Put `BOT_TOKEN` or `DISCORD_BOT_TOKEN` in `config/tokens.env` (same file as any other secrets).

2. **Standalone**  
   To only run the slash-command bot (no main pingbot):
   ```bash
   cd MWBots/MWPingBot
   python -u ping_command_bot.py
   ```

3. **With main pingbot**  
   If you have a main `pingbot.py` that runs the pinging logic, start the command bot in a background thread (same pattern as DiscumBot):
   ```python
   import ping_command_bot as _ping_cmd
   if getattr(_ping_cmd, "BOT_TOKEN", None):
       import threading
       threading.Thread(target=lambda: __import__("asyncio").run(_ping_cmd.bot.start(_ping_cmd.BOT_TOKEN)), daemon=True).start()
   ```
   The main pingbot should read `ping_config.load_settings()` to get `ping_channel_ids` and `ping_delay_seconds` and apply them.

## Config paths

- `config/settings.json` – ping channels and delay (edited via /ping settings or manually).
- `config/tokens.env` – `BOT_TOKEN`, optional `mirrorworld_server_id`.
