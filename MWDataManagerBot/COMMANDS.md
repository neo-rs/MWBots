# MWDataManagerBot — COMMANDS (Canonical)

This file is the **single source of truth** for MWDataManagerBot commands.

Notes:
- Prefix commands use the configured prefix (default `!`).
- Slash commands are **guild-scoped** to Mirror World via `destination_guild_ids` and require the bot to be invited with `applications.commands`.
- Slash commands are admin-only via `manage_guild` permissions unless stated otherwise.

---

## Prefix commands

#### `!fetchall`
- **Description**: Create/ensure Mirror World mirror channels for each configured fetchall mapping (channel setup).
- **Admin-only**: No (but should be used by admins)
- **Usage**: `!fetchall`
- **Output**: Live progress bar + per-guild results.

#### `!fetchsync [source_guild_id]`
- **Description**: Pull messages via user token and mirror into Mirror World (no writes to sources).
- **Admin-only**: No (but should be used by admins)
- **Usage**:
  - `!fetchsync` (all mappings)
  - `!fetchsync 123456789012345678` (single source guild)
- **Output**: Live progress bar + totals.

#### `!fetchauth <source_guild_id>`
- **Description**: Debug fetch selection/token access without exposing tokens (runs fetchsync dryrun diagnostics).
- **Admin-only**: No (but should be used by admins)
- **Usage**: `!fetchauth 123456789012345678`
- **Output**: HTTP status + channel-type counts + category preview.

#### `!fetch <source_guild_id>`
- **Description**: Run fetchall for a single mapping entry (mirror channel setup only).
- **Admin-only**: No (but should be used by admins)
- **Usage**: `!fetch 123456789012345678`

#### `!setfetchguild <source_guild_id> <destination_category_id>`
- **Description**: Create/update a mapping entry in `config/fetchall_mappings.json`.
- **Admin-only**: No (but should be used by admins)
- **Usage**: `!setfetchguild 123456789012345678 987654321098765432`

#### `!keywords [list|add|remove|reload] <value?>`
- **Description**: Manage monitored keywords (`config/keywords.json`) from Discord.
- **Admin-only**: No (but should be used by admins)
- **Usage**:
  - `!keywords list`
  - `!keywords add valentines`
  - `!keywords remove valentines`
  - `!keywords reload`

#### `!status`
- **Description**: Show current monitoring/destination configuration.
- **Admin-only**: No
- **Usage**: `!status`

#### `!whereami`
- **Description**: Basic runtime proof (guild/channel id) for debugging.
- **Admin-only**: No
- **Usage**: `!whereami`

#### `!slashstatus`
- **Description**: Debug: show slash commands registered in the bot tree for the current guild.
- **Admin-only**: No
- **Usage**: `!slashstatus`

#### `!slashsync`
- **Description**: Debug: force sync of slash commands to destination guild(s).
- **Admin-only**: No
- **Usage**: `!slashsync`

---

## Slash commands

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

- **Prefix commands**: 9
- **Slash commands**: 18
