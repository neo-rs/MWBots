# Channel Map Dashboard

Lives inside **MWDiscumBot**. UI for viewing and editing channel→webhook mappings. Data is this bot’s `config/channel_map.json` and `config/source_channels.json` (see `discum_config.py`).

## Structure

- **index.html** – Single-page channel map UI (source/dest servers, webhook mappings).
- **config.js** – Single source of truth for `CHANNELMAP_API_BASE` and `CHANNELMAP_ASSETS`. Loaded first.
- **channel-bridge.js** – Discord API bridge; uses config for API base and asset paths.
- **shared-styles.css** – Shared header/button/status styles.
- **assets/images/** – Optional: rs-logo.png, mirrorworld-default.jpg, rs-default.png, discord-default.png.

## Configuration (CANONICAL_RULES: no hardcoded values)

- **API base**: Set `window.CHANNELMAP_API_BASE` in `config.js` (or before loading scripts). Empty string = same origin. When the API is on another origin (e.g. Oracle), set that origin in `config.js`.
- **Assets**: Set `window.CHANNELMAP_ASSETS` to the base path for images (default `'./assets'`).

## Serving (including on Oracle)

1. Serve this folder as static files (e.g. nginx/Apache docroot, or a small HTTP server from `MWDiscumBot`).
2. Backend must serve `/api/discord/*`, `/config/channel_map.json`, `/config/source_channels.json`, `/api/destination_channels`, `/save_channel_map`, `/discord/create_webhook`, `/api/channel_map`, etc., reading/writing **MWDiscumBot/config/** (same repo: `../config/channel_map.json` relative to this folder).
3. Open `index.html` in a browser; all API calls use the configured base.

## Canonical data

Channel mapping data is owned by **MWDiscumBot**: `config/channel_map.json`, `config/source_channels.json`, and `discum_config.py` (`load_channel_map`, `save_channel_map`, `CHANNEL_MAP_PATH`). This dashboard is the UI only; no duplicate mapping logic.
