from __future__ import annotations

import json
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


def load_settings_and_tokens(config_dir: Path) -> Tuple[Dict[str, Any], Dict[str, str]]:
    settings = _load_settings_json(config_dir / "settings.json")
    # Standalone rule: secrets must live inside MWDataManagerBot/config/tokens.env only.
    tokens: Dict[str, str] = _load_env_file(config_dir / "tokens.env")
    return settings, tokens

