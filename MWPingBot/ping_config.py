"""Canonical config for MWPingBot.

Paths and settings for ping channels and delay. Used by pingbot and ping_command_bot.
Settings file: config/settings.json
- ping_channel_ids: list of channel IDs where the bot sends pings
- ping_delay_seconds: delay (seconds) between or before pings
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = str(_ROOT / "config")
SETTINGS_PATH = str(_ROOT / "config" / "settings.json")
TOKENS_ENV_PATH = str(_ROOT / "config" / "tokens.env")

DEFAULT_SETTINGS: Dict[str, Any] = {
    "ping_channel_ids": [],
    "ping_delay_seconds": 60,
    "mirrorworld_guild_id": 0,
}


def load_env_file(path: str) -> Dict[str, str]:
    """Load KEY=VALUE from file. No python-dotenv dependency."""
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip().strip('"').strip("'")
                if key:
                    out[key] = value
    except (FileNotFoundError, Exception):
        pass
    return out


def load_settings(path: str | None = None) -> Dict[str, Any]:
    """Load settings.json. Returns dict with ping_channel_ids, ping_delay_seconds, etc."""
    p = path or SETTINGS_PATH
    try:
        with open(p, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return dict(DEFAULT_SETTINGS)
    if not isinstance(data, dict):
        return dict(DEFAULT_SETTINGS)
    out = dict(DEFAULT_SETTINGS)
    if "ping_channel_ids" in data:
        raw = data["ping_channel_ids"]
        out["ping_channel_ids"] = [int(x) for x in raw if isinstance(x, (int, str)) and str(x).strip().isdigit()]
    if "ping_delay_seconds" in data:
        try:
            out["ping_delay_seconds"] = int(data["ping_delay_seconds"])
        except (TypeError, ValueError):
            pass
    if "mirrorworld_guild_id" in data:
        try:
            out["mirrorworld_guild_id"] = int(data["mirrorworld_guild_id"])
        except (TypeError, ValueError):
            pass
    return out


def save_settings(settings: Dict[str, Any], path: str | None = None) -> bool:
    """Write settings.json. Returns True on success."""
    p = path or SETTINGS_PATH
    try:
        data = {
            "ping_channel_ids": settings.get("ping_channel_ids", []),
            "ping_delay_seconds": settings.get("ping_delay_seconds", 60),
            "mirrorworld_guild_id": settings.get("mirrorworld_guild_id", 0),
        }
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False
