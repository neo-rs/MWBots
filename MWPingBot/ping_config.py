"""Canonical config for MWPingBot – matches live pingbot.py schema.

Live bot (mirror-world/MWPingBot/pingbot.py) reads config/settings.json with:
  - mirrorworld_server_id: str (Mirror World guild ID)
  - ping_channel_ids: list of channel IDs where the bot pings
  - cooldown_seconds: per-channel cooldown before next ping
  - dedupe_ttl_seconds: TTL for content dedupe
  - verbose: bool (optional)
This module uses the same keys so /ping settings and the main bot share one file.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = str(_ROOT / "config")
SETTINGS_PATH = str(_ROOT / "config" / "settings.json")
TOKENS_ENV_PATH = str(_ROOT / "config" / "tokens.env")
PINGBOT_JOURNAL_PATH = _ROOT / "logs" / "Botlogs" / "pingbotlogs.json"


def append_pingbot_journal(entry: Dict[str, Any]) -> None:
    """Append one JSONL line to the shared PingBot journal (command registration, sync, invocations)."""
    if "timestamp" not in entry:
        entry = {**entry, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")}
    try:
        PINGBOT_JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(PINGBOT_JOURNAL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass

# Same schema as live pingbot.py
DEFAULT_SETTINGS: Dict[str, Any] = {
    "mirrorworld_server_id": "1431314516364230689",
    "ping_channel_ids": [],
    "cooldown_seconds": 30,
    "dedupe_ttl_seconds": 30,
    "verbose": True,
    "dm_notify_user_ids": [],
}


def _parse_user_id_list(raw: Any) -> List[int]:
    if isinstance(raw, str):
        raw_list = [x.strip() for x in raw.split(",") if x.strip()]
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raw_list = []
    out: List[int] = []
    for x in raw_list:
        try:
            out.append(int(str(x).strip()))
        except (TypeError, ValueError):
            continue
    return out


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
    """Load settings.json. Same format as live pingbot.py."""
    p = path or SETTINGS_PATH
    try:
        with open(p, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, Exception):
        return dict(DEFAULT_SETTINGS)
    if not isinstance(data, dict):
        return dict(DEFAULT_SETTINGS)
    out = dict(DEFAULT_SETTINGS)
    if "mirrorworld_server_id" in data:
        out["mirrorworld_server_id"] = str(data["mirrorworld_server_id"] or "").strip()
    if "ping_channel_ids" in data:
        raw = data["ping_channel_ids"]
        if isinstance(raw, list):
            out["ping_channel_ids"] = [int(x) for x in raw if isinstance(x, (int, str)) and str(x).strip().isdigit()]
        else:
            out["ping_channel_ids"] = []
    if "cooldown_seconds" in data:
        try:
            out["cooldown_seconds"] = max(0, int(float(data["cooldown_seconds"])))
        except (TypeError, ValueError):
            pass
    if "dedupe_ttl_seconds" in data:
        try:
            out["dedupe_ttl_seconds"] = max(0, int(float(data["dedupe_ttl_seconds"])))
        except (TypeError, ValueError):
            pass
    if "verbose" in data:
        v = data["verbose"]
        out["verbose"] = bool(v) if isinstance(v, bool) else str(v).strip().lower() in ("1", "true", "yes", "on")
    if "dm_notify_user_ids" in data:
        out["dm_notify_user_ids"] = _parse_user_id_list(data.get("dm_notify_user_ids"))
    return out


def save_settings(settings: Dict[str, Any], path: str | None = None) -> bool:
    """Write settings.json. Preserves keys the main pingbot expects."""
    p = path or SETTINGS_PATH
    try:
        prior = load_settings(p)
        dm_ids = settings.get("dm_notify_user_ids")
        if dm_ids is None:
            dm_ids = prior.get("dm_notify_user_ids") or []
        data = {
            "verbose": settings.get("verbose", True),
            "mirrorworld_server_id": str(settings.get("mirrorworld_server_id") or "").strip() or "0",
            "cooldown_seconds": max(0, int(settings.get("cooldown_seconds", 30))),
            "dedupe_ttl_seconds": max(0, int(settings.get("dedupe_ttl_seconds", 30))),
            "ping_channel_ids": list(settings.get("ping_channel_ids") or []),
            "dm_notify_user_ids": _parse_user_id_list(dm_ids),
        }
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception:
        return False
