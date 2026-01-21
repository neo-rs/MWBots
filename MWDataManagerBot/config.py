from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple


def _load_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip()
                if not k:
                    continue
                out[k] = v
    except FileNotFoundError:
        return out
    except Exception:
        return out
    return out


def _load_settings_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


_ENV_OVERRIDE_MAP: Dict[str, Tuple[str, str]] = {
    # Global triggers
    "SMARTFILTER_PRICE_ERROR_GLITCHED_CHANNEL_ID": ("global_trigger_destinations", "PRICE_ERROR"),
    "SMARTFILTER_FLIPS_PROFITABLE_CHANNEL_ID": ("global_trigger_destinations", "PROFITABLE_FLIP"),
    "SMARTFILTER_FLIPS_LUNCHMONEY_CHANNEL_ID": ("global_trigger_destinations", "LUNCHMONEY_FLIP"),
    # Local destinations
    "SMARTFILTER_UPCOMING_CHANNEL_ID": ("smartfilter_destinations", "UPCOMING"),
}


def _apply_env_overrides(settings: Dict[str, Any], env: Dict[str, str]) -> Dict[str, Any]:
    """
    Apply whitelisted env overrides to settings.json structure.

    IMPORTANT: We intentionally ONLY read the specific SMARTFILTER_* ids here,
    so secrets remain exclusive to MWDataManagerBot/config/tokens.env.
    """
    out = dict(settings or {})
    for env_key, (section_key, dict_key) in _ENV_OVERRIDE_MAP.items():
        raw = (env.get(env_key) or "").strip()
        if not raw:
            continue
        try:
            value = int(float(raw))
        except Exception:
            continue
        if value <= 0:
            continue
        section = out.get(section_key)
        if not isinstance(section, dict):
            section = {}
            out[section_key] = section
        section[dict_key] = value
    return out


def load_settings_and_tokens(config_dir: Path) -> Tuple[Dict[str, Any], Dict[str, str]]:
    settings = _load_settings_json(config_dir / "settings.json")
    # Standalone rule: secrets must live inside MWDataManagerBot/config/tokens.env only.
    tokens: Dict[str, str] = _load_env_file(config_dir / "tokens.env")
    # Allow non-secret channel id overrides via OS env or a local .env file (whitelisted keys only).
    try:
        env_overrides = dict(os.environ)
        # Optional local env file (non-secret overrides only; we whitelist keys above)
        env_overrides.update(_load_env_file(config_dir / ".env"))
        settings = _apply_env_overrides(settings, env_overrides)
    except Exception:
        pass
    return settings, tokens

