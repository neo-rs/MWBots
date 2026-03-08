/**
 * Channel Map Dashboard - Configuration (CANONICAL)
 * ==================================================
 * Single source of truth for API base and asset paths.
 * When deployed on Oracle server, set API_BASE to the backend origin if different.
 * Empty string = same origin (recommended when static and API are served together).
 */
(function () {
    'use strict';
    /** API base URL. Empty = same origin. Set when static and API are on different origins (e.g. Oracle). */
    window.CHANNELMAP_API_BASE = window.CHANNELMAP_API_BASE ?? '';
    /** Base path for images (logo, server icons). */
    window.CHANNELMAP_ASSETS = window.CHANNELMAP_ASSETS ?? './assets';
})();
