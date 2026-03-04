"""Canonical config and channel-map loading for MWDiscumBot.

Single source of truth for paths, env loading, and channel_map.json.
Used by discumbot.py and discum_command_bot.py. No duplication of parsing logic.

Paths are relative to this module's folder (MWDiscumBot), so the same code runs
on Oracle and locally; config and channel_map.json stay in MWDiscumBot/config/.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict

# Project root = folder containing this module (MWDiscumBot)
_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = str(_ROOT / "config")
CHANNEL_MAP_PATH = str(_ROOT / "config" / "channel_map.json")
TOKENS_ENV_PATH = str(_ROOT / "config" / "tokens.env")
SETTINGS_JSON_PATH = str(_ROOT / "config" / "settings.json")
SETTINGS_RUNTIME_PATH = str(_ROOT / "config" / "settings.runtime.json")


def load_env_file(path: str) -> Dict[str, str]:
    """Load KEY=VALUE from .env; return dict. No python-dotenv dependency."""
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("-"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    out[key] = value
    except (FileNotFoundError, Exception):
        pass
    return out


def load_channel_map(path: str) -> Dict[int, str]:
    """Load channel map JSON ({source_channel_id: webhook_url}). Returns dict with int keys."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (FileNotFoundError, Exception):
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[int, str] = {}
    for k, v in data.items():
        try:
            cid = int(str(k).strip())
        except (ValueError, TypeError):
            continue
        url = str(v or "").strip()
        if url:
            out[cid] = url
    return out


def save_channel_map(path: str, channel_map: Dict[int, str]) -> bool:
    """Write channel map JSON. Returns True on success."""
    try:
        data = {str(k): v for k, v in channel_map.items()}
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception:
        return False
