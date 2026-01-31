# MWDataManagerBot — COMMANDS (Canonical)

This file is the **single source of truth** for MWDataManagerBot commands.

Notes:
- MWDataManagerBot uses **slash commands only**.
- Slash commands are **guild-scoped** to Mirror World via `destination_guild_ids` and require the bot to be invited with `applications.commands`.
- Slash commands are admin-only via `manage_guild` permissions unless stated otherwise.

---

## Slash commands

### `/fetchall`

#### `/fetchall [source_guild_id]`
- **Description**: Create/ensure Mirror World mirror channels for each configured mapping (channel setup).
- **Permissions**: `manage_channels`
- **Usage**:
  - `/fetchall` (all mappings)
  - `/fetchall source_guild_id:<id>` (single mapping)

### `/fetchclear`

#### `/fetchclear [category_ids_csv] [confirm] [delete_all]`
- **Description**: Delete mirror/separator channels inside a Mirror World destination category (dryrun by default).
- **Permissions**: `manage_channels`
- **Safety**:
  - Dryrun by default (no deletes unless `confirm=true`)
  - By default deletes only channels with topic `MIRROR:` or separator channels
  - Set `delete_all=true` to delete everything in the category
- **Category selection**:
  - If `category_ids_csv` is omitted, the bot shows a **dropdown** so you can pick **one or more categories**.

### `/status`

#### `/status`
- **Description**: Show current monitoring/destination configuration summary.
- **Permissions**: none

### `/whereami`

#### `/whereami`
- **Description**: Basic runtime proof (guild/channel id) for debugging.
- **Permissions**: none

### `/fetchmap`

#### `/fetchmap list`
- **Description**: Paginated embed list of configured mappings (Prev/Next).
- **Permissions**: `manage_guild`

#### `/fetchmap browse [source_guild_id]`
- **Description**: Interactive browser for a source guild’s categories/channels:
  - pick mapping via dropdown (if no `source_guild_id`)
  - toggle category included in mapping
  - toggle ignored channels via multi-select
- **Permissions**: `manage_guild`

Note: **source categories are required**. If `source_category_ids` is empty, fetchall/fetchsync will return `missing_source_category_ids` to prevent mirroring an entire server by accident.

Fetchsync filtering + live mode:
- Fetchsync will **skip low-signal messages** like pure role/user mention blasts or very short "ping for attention" messages (configurable).
- Mirrored messages are attributed by either:
  - webhook identity (recommended; `use_webhooks_for_forwarding=true`), or
  - a small header embed showing the **source server name + icon** (when webhooks are disabled).
- Consecutive **attachment-only** messages posted by the same user are grouped into a **single** mirrored output (so multi-image drops don't spam).
- Fetchsync can run continuously in the background (auto-poller) using the user token to keep mirror channels up to date.

Config knobs (`config/settings.json`):
- `fetchsync_initial_backfill_limit` (default 20, max 50): how many recent messages to seed the cursor when a source channel has no cursor yet.
- `fetchsync_min_content_chars` (default 1): minimum non-mention text length to mirror (messages with embeds/attachments/URLs are still mirrored).
- `fetchsync_auto_poll_seconds` (default 0; set to e.g. 60 to enable): background polling interval for live updates.

#### `/fetchmap upsert`
- **Description**: Add/update a mapping entry.
- **Parameters**:
  - `source_guild_id` (int)
  - `destination_category` (CategoryChannel in Mirror World)
  - `name` (optional)
  - `source_category_ids_csv` (optional CSV)
  - `ignored_channel_ids_csv` (optional CSV)
  - `require_date` (bool)
- **Permissions**: `manage_guild`

#### `/fetchmap ignore_add`
- **Description**: Add an ignored source channel id to a mapping.
- **Permissions**: `manage_guild`

#### `/fetchmap ignore_remove`
- **Description**: Remove an ignored source channel id from a mapping.
- **Permissions**: `manage_guild`

### `/discum`

#### `/discum browse [source_guild_id]`
- **Description**: Interactive browser for **MWDiscumBot** source servers/channels using the configured user token:
  - pick a source guild (if no `source_guild_id`)
  - browse categories/channels
  - see a **preview** (latest message snippet + embed/file/link indicators)
  - select channels and map them to a destination channel in Mirror World
- **Permissions**: `manage_guild`
- **Writes runtime config**:
  - `MWDiscumBot/config/channel_map.json` (source_channel_id → destination webhook URL)
  - `MWDiscumBot/config/settings.runtime.json` (adds `source_guild_ids` so DiscumBot can cache names; takes effect on DiscumBot restart)

### `/fetchsync`

#### `/fetchsync dryrun [source_guild_id]`
- **Description**: Show what would be fetched/sent without sending (includes progress).
- **Permissions**: `manage_guild`

#### `/fetchsync run [source_guild_id]`
- **Description**: Pull and mirror messages (includes progress).
- **Permissions**: `manage_guild`

### `/keywords`

#### `/keywords list`
- **Description**: List monitored keywords.
- **Permissions**: `manage_guild`

#### `/keywords add keyword:<text>`
- **Description**: Add a monitored keyword.
- **Permissions**: `manage_guild`

#### `/keywords remove keyword:<text>`
- **Description**: Remove a monitored keyword.
- **Permissions**: `manage_guild`

#### `/keywords reload`
- **Description**: Reload keywords from disk.
- **Permissions**: `manage_guild`

#### `/keywords test text:<text> send_output:<bool>`
- **Description**: Test a sample text against monitored keywords; optionally post output to the MONITORED_KEYWORD channel.
- **Permissions**: `manage_guild`

### `/keywordchannel`

#### `/keywordchannel set keyword:<keyword> channel:<TextChannel>`
- **Description**: Route matches for a specific monitored keyword to an **extra** channel (in addition to the default monitored keyword channel).
- **Permissions**: `manage_guild`

#### `/keywordchannel clear keyword:<keyword>`
- **Description**: Remove the extra channel override for a keyword.
- **Permissions**: `manage_guild`

#### `/keywordchannel list`
- **Description**: List keyword -> extra channel overrides.
- **Permissions**: `manage_guild`

---

## Command summary

- **Prefix commands**: 0
- **Slash commands**: 20
