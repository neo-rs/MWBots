# MWDiscumBot Commands

Commands are served by the **discord.py command bot** (`discum_command_bot.py`): slash commands on the app command tree, prefix commands with **`!`** when the bot token is configured (`config/tokens.env`). The selfbot (`discumbot.py`) starts this bot in a background thread when `BOT_TOKEN` / `DISCORD_BOT_DISCUMBOT` is set.

---

## Slash commands (Discord)

#### `/discum`
- **Description:** Browse and manage Discum bot channel mappings (D2D webhook map).
- **Parameters:** `action` (Choice: `browse`) — action to perform.
- **Admin only:** No (any member can invoke; mapping UI is owner-scoped per interaction).
- **Usage:** `/discum action:browse` then use buttons to view mappings or browse source guilds and map channels to webhooks.
- **Returns:** Ephemeral embed with "View Current Mappings" and "Browse source & map" buttons.

---

## Prefix commands (`!`, Mirror World server)

These require **`Manage Channels`** in the server where you run them. They use **`DISCUM_USER_DISCUMBOT`** (user token) only where source-guild reads are needed; **`!fetchclear`** uses the bot client only for deletes.

#### `!fetchall`
- **Description:** Create or update **mirror text channels** under each mapping’s destination category from `config/fetchall_mappings.json` (and optional `fetchall_mappings.runtime.json`).
- **Parameters:** `source_guild_id` (integer, optional) — default `0` = all mappings; otherwise only that source guild.
- **Admin only:** Yes (`Manage Channels`).
- **Usage:** `!fetchall` or `!fetchall 667532381376217089`
- **Returns:** Progress edits on one bot message; final summary with ok count. Uses default **`prune_inactive=True`** inside `run_fetchall` (orphaned / date-stale / inactive mirror channels may be deleted — see `fetchall.py`).

#### `!fetchsync`
- **Description:** Mirror recent messages from source channels into existing mirror channels (webhook or bot send per `settings.json`).
- **Parameters:** `source_guild_id` (integer, optional) — default `0` = all mappings.
- **Admin only:** Yes (`Manage Channels`).
- **Usage:** `!fetchsync` or `!fetchsync 667532381376217089`
- **Returns:** Progress edits; final sent message totals.

#### `!fetchcycle`
- **Description:** One pass matching the **auto-poller** order: for each mapping, `run_fetchall` then `run_fetchsync` with **`prune_inactive=False`** (no inactive-2d prune on that pass).
- **Parameters:** `source_guild_id` (integer, optional) — default `0` = all mappings.
- **Admin only:** Yes (`Manage Channels`).
- **Usage:** `!fetchcycle` or `!fetchcycle 667532381376217089`
- **Returns:** Progress edits; final summary with ok count and `total_sent`.

#### `!fetchclear`
- **Description:** **Manual** cleanup of channels under fetchall **destination** categories: same deletion rules as optional **startup clear**, but does **not** require `fetchall_startup_clear_enabled`. Uses `fetchall_startup_clear_category_ids` in `config/settings.json` and, if that set is empty, each mapping’s **`destination_category_id`**. Honors `fetchall_startup_clear_only_mirror_channels` and `fetchall_startup_clear_all_channels`. Sets the fetchall maintenance lock so `fetchsync` stops cleanly during deletes. Afterward removes **empty** `…-overflow-N` categories tied to those base categories when possible.
- **Parameters:** None.
- **Admin only:** Yes (`Manage Channels`).
- **Usage:** `!fetchclear`
- **Returns:** One summary message (`deleted` / `skipped` / `errors` / `guilds_hit` / overflow category removals). Detailed lines use the `[FETCHALL] fetchclear` log tag.

---

## Quick map (message pattern, not a registered command)

If a message matches `!g<guild_id> s<source_id …> d<dest_channel_id>` (see `_parse_quick_map_message` in `discum_command_bot.py`), the bot updates `channel_map.json` (webhook in destination). Requires appropriate channel/webhook permissions (typically **Manage Webhooks** on the destination channel).

---

**Total slash commands:** 1  
**Total prefix commands:** 4 (`!fetchall`, `!fetchsync`, `!fetchcycle`, `!fetchclear`)  
**Admin prefix commands:** 4  
**Public slash commands:** 1  
