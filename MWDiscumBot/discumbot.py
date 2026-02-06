"""Discord2Discord Bridge (v3.4) -- Pure Forwarder Mode

Environment/config is loaded from:
- `config/tokens.env` (secrets only)
- `config/settings.json` (non-secret settings)
- `config/channel_map.json` (source-channel → webhook URL)

This script forwards messages from specific channels (CHANNEL_MAP) to target webhooks.
Content classification and "filtered" routing are DISABLED here by design.

IMPORTANT: This uses a USER ACCOUNT TOKEN (selfbot), not a bot token.
- Uses discum library (designed for user account automation)
- DISCUM_BOT is a user account token from browser DevTools
- Different from datamanagerbot.py and pingbot.py which use bot tokens (discord.py)

This bot writes operational logs to `logs/Botlogs/discumlogs.json`.
"""

import sys
import os

# Standalone root = MWDiscumBot folder (no outside references).
try:
    from pathlib import Path as _Path

    _project_root = str(_Path(__file__).resolve().parent)
except Exception:
    _project_root = os.path.dirname(os.path.abspath(__file__))

import signal
import sys
import os
import time
import atexit
import hashlib
import json
from typing import Optional, Dict, Tuple, Any, List

import warnings
import requests
import discum
import re
import threading
import platform

import subprocess

# Suppress GIL warning from _brotli module (harmless)
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*GIL.*')

# Fix Windows console encoding for Unicode/emoji support
if platform.system().lower() == "windows":
    try:
        # Set console to UTF-8 encoding
        if sys.stdout.encoding != 'utf-8':
            import codecs
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
            sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except Exception:
        pass  # Fallback if encoding setup fails

if platform.system().lower().startswith("win"):
    try:
        # Reset console to default colors; our ANSI segments will color only tagged parts
        os.system("color 07")
    except Exception:
        pass

# --- Logging system ---
_discumbot_logger = None  # Will be initialized after config import

# --- Legacy colorama helpers (backward compatibility) ---
try:
    import importlib  # lightweight, stdlib
    _colorama = importlib.import_module("colorama")
    _cinit = getattr(_colorama, "init", lambda **kwargs: None)
    _F = getattr(_colorama, "Fore", None)
    _S = getattr(_colorama, "Style", None)
    if callable(_cinit):
        _cinit(autoreset=True)
    if _F is None or _S is None:
        raise ImportError("colorama missing symbols")
except Exception:
    class _Dummy:
        def __getattr__(self, k): return ""
    _F = _S = _Dummy()

import re as _re
import builtins as _builtins

_ANSI_ESC = "\x1b["
_CONSOLE_BLOCK_LOCK = threading.RLock()

def _colorize_line(text: str) -> str:
    # If already contains ANSI, assume it's colored
    if _ANSI_ESC in text:
        return text
    s = text
    # Color leading tags
    s = _re.sub(r"^\[INFO\]", f"{_F.GREEN}[INFO]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[WARN(?:ING)?\]", f"{_F.YELLOW}[WARN]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[ERROR\]", f"{_F.RED}[ERROR]{_S.RESET_ALL}", s)
    s = _re.sub(r"^\[DEBUG\]", f"{_F.WHITE}[DEBUG]{_S.RESET_ALL}", s)
    # Channel-like tokens (#channel-name) to blue
    s = _re.sub(r"(?P<prefix>\s|^)#([a-z0-9\-_]+)", lambda m: f"{m.group('prefix')}{_F.BLUE}#{m.group(2)}{_S.RESET_ALL}", s, flags=_re.IGNORECASE)
    # User-like tokens (@name) to magenta (avoid emails)
    s = _re.sub(r"(?P<prefix>\s|^)@([A-Za-z0-9_][A-Za-z0-9_\.\-]{1,30})", lambda m: f"{m.group('prefix')}{_F.MAGENTA}@{m.group(2)}{_S.RESET_ALL}", s)
    # 'Connected:' server label (word-ish) to cyan if not already colored
    s = _re.sub(r"(Connected:\s)([^|\n]+)", lambda m: f"{m.group(1)}{_F.CYAN}{m.group(2).strip()}{_S.RESET_ALL}", s)
    return s

def _print_colorized(*args, **kwargs):
    try:
        with _CONSOLE_BLOCK_LOCK:
            if not args:
                return _builtins.print(*args, **kwargs)
            text = " ".join(str(a) for a in args)
            # Replace problematic Unicode characters for Windows console (cp1252)
            text = text.replace('→', '->').replace('←', '<-').replace('↔', '<->').replace('•', '*').replace('✓', '[OK]').replace('✗', '[X]')
            try:
                _builtins.print(_colorize_line(text), **kwargs)
            except UnicodeEncodeError:
                # Fallback: replace problematic Unicode characters with ?
                safe_text = text.encode('ascii', errors='replace').decode('ascii')
                _builtins.print(_colorize_line(safe_text), **kwargs)
    except Exception:
        # Final fallback: print without colorization
        try:
            _builtins.print(*args, **kwargs)
        except UnicodeEncodeError:
            safe_args = [str(a).encode('ascii', errors='ignore').decode('ascii') for a in args]
            _builtins.print(*safe_args, **kwargs)

# Intercept raw prints to apply colors to legacy lines
print = _print_colorized  # type: ignore

def _c(txt: str, color: str) -> str:
    return f"{color}{txt}{_S.RESET_ALL}"

def _fmt_server(name: str) -> str:
    return _c(name or "unknown-server", _F.CYAN)

def _fmt_channel(name: str) -> str:
    label = name if name.startswith("#") else f"#{name}"
    return _c(label or "#unknown", _F.BLUE)

def _fmt_user(name: str) -> str:
    label = name if name.startswith("@") else f"@{name}"
    return _c(label or "@unknown", _F.MAGENTA)

def _fmt_arrow() -> str:
    return _c(" > ", _F.WHITE)

def log_info(msg: str, context: dict = None) -> None:
    """Log info message using unified logger."""
    try:
        if _discumbot_logger is not None:
            _discumbot_logger.info(msg)  # Logger only accepts message, not context
        else:
            print(f"{_F.GREEN}[INFO]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    except UnicodeEncodeError:
        safe_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(f"[INFO] {safe_msg}", flush=True)
    except Exception:
        print(f"[INFO] {msg}", flush=True)

def log_warn(msg: str, context: dict = None) -> None:
    """Log warning message using unified logger."""
    try:
        if _discumbot_logger is not None:
            _discumbot_logger.warn(msg)  # Logger only accepts message, not context
        else:
            print(f"{_F.YELLOW}[WARN]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    except UnicodeEncodeError:
        safe_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(f"[WARN] {safe_msg}", flush=True)
    except Exception:
        print(f"[WARN] {msg}", flush=True)

def log_error(msg: str, error: Exception = None, context: dict = None) -> None:
    """Log error message using unified logger."""
    try:
        if _discumbot_logger is not None:
            _discumbot_logger.error(msg, error=error)  # Don't pass context
        else:
            print(f"{_F.RED}[ERROR]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    except UnicodeEncodeError:
        safe_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(f"[ERROR] {safe_msg}", flush=True)
    except Exception:
        print(f"[ERROR] {msg}", flush=True)

def log_debug(msg: str, context: dict = None) -> None:
    """Log debug message using unified logger."""
    if _discumbot_logger is None:
        print(f"{_F.WHITE}[DEBUG]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    else:
        _discumbot_logger.debug(msg, context)

def log_forwarder(msg: str, channel_name: str = None, user_name: str = None, action: str = "detect") -> None:
    """Log forwarder operations with context - writes to live feed."""
    parts = []
    if channel_name:
        parts.append(f"in {_fmt_channel(channel_name)}")
    if user_name:
        parts.append(f"from {_fmt_user(user_name)}")
    context = f" ({', '.join(parts)})" if parts else ""
    print(f"{_F.MAGENTA}[FORWARDER]{_S.RESET_ALL} {_F.WHITE}{msg}{context}{_S.RESET_ALL}", flush=True)

def log_d2d(msg: str, channel_name: str = None, dest_channel_name: str = None) -> None:
    """Log D2D bridge operations."""
    parts = []
    if channel_name:
        parts.append(f"from {_fmt_channel(channel_name)}")
    if dest_channel_name:
        parts.append(f"to {_fmt_channel(dest_channel_name)}")
    context = f" ({', '.join(parts)})" if parts else ""
    print(f"{_F.CYAN}[D2D]{_S.RESET_ALL} {_F.WHITE}{msg}{context}{_S.RESET_ALL}", flush=True)

def log_webhook(msg: str, channel_name: str = None, user_name: str = None, status_code: int = None) -> None:
    """Log webhook operations."""
    parts = []
    if channel_name:
        parts.append(f"in {_fmt_channel(channel_name)}")
    if user_name:
        parts.append(f"from {_fmt_user(user_name)}")
    if status_code:
        status_color = _F.GREEN if status_code in (200, 204) else _F.YELLOW if status_code < 500 else _F.RED
        parts.append(f"HTTP {status_color}{status_code}{_S.RESET_ALL}")
    context = f" ({', '.join(parts)})" if parts else ""
    print(f"{_F.BLUE}[WEBHOOK]{_S.RESET_ALL} {_F.WHITE}{msg}{context}{_S.RESET_ALL}", flush=True)

def startup_banner(bot_name: str, lines: list[str]) -> None:
    """Print startup banner using unified logger (colorized)."""
    bar = "=" * 55
    with _CONSOLE_BLOCK_LOCK:
        print(_F.WHITE + bar + _S.RESET_ALL)
        print(f"{_F.GREEN}[START]{_S.RESET_ALL} {_F.WHITE}{bot_name}{_S.RESET_ALL}")
        for line in lines:
            # Colorize the line content
            print(f"{_F.WHITE}{line}{_S.RESET_ALL}")
        print(_F.WHITE + bar + _S.RESET_ALL + "\n")

def uname(user) -> str:
    try:
        return getattr(user, "name", None) or getattr(user, "display_name", None) or str(user)
    except Exception:
        return "unknown"

def gname(guild) -> str:
    try:
        return getattr(guild, "name", None) or "unknown"
    except Exception:
        return "unknown"

def cname(channel) -> str:
    try:
        return getattr(channel, "name", None) or "unknown"
    except Exception:
        return "unknown"
# --- end helpers ---

# Ensure default Windows console color so ANSI colors show properly
try:
    import platform as _plat, os as _os
    if _plat.system().lower().startswith("win"):
        _os.system("color 07")
except Exception:
    pass

# ================= Standalone config (no neonxt.* deps) =================
# This file is intended to run standalone (as a single Python script) without importing
# other bot modules or central "runner" systems.

_CONFIG_DIR = os.path.join(_project_root, "config")
_TOKENS_ENV_PATH = os.path.join(_CONFIG_DIR, "tokens.env")
_SETTINGS_JSON_PATH = os.path.join(_CONFIG_DIR, "settings.json")
_SETTINGS_RUNTIME_JSON_PATH = os.path.join(_CONFIG_DIR, "settings.runtime.json")

_CONFIG_RAW: Dict[str, str] = {}

def _load_env_file(path: str) -> None:
    """Minimal .env reader (no python-dotenv dependency)."""
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
                if not key:
                    continue
                _CONFIG_RAW[key] = value
    except FileNotFoundError:
        return
    except Exception:
        return

def _load_settings_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on", "enabled"}

def _set_raw(key: str, value: Any) -> None:
    if value is None:
        return
    try:
        if isinstance(value, (dict, list)):
            _CONFIG_RAW[key] = json.dumps(value, ensure_ascii=False)
        else:
            _CONFIG_RAW[key] = str(value)
    except Exception:
        return

_load_env_file(_TOKENS_ENV_PATH)

# Load settings (JSON base + optional runtime overrides).
# - settings.json is tracked (code/config defaults)
# - settings.runtime.json is runtime-only (NOT tracked) so tools can add guild ids without losing them on update
_settings = _load_settings_json(_SETTINGS_JSON_PATH) or {}
_runtime_settings = _load_settings_json(_SETTINGS_RUNTIME_JSON_PATH) or {}
try:
    if isinstance(_runtime_settings, dict) and _runtime_settings:
        # Merge: runtime overlays base.
        merged = dict(_settings)
        merged.update(_runtime_settings)
        # For source guild ids, we union to avoid accidentally clobbering.
        base_g = merged.get("source_guild_ids")
        rt_g = _runtime_settings.get("source_guild_ids")
        gids: List[str] = []
        if isinstance(base_g, list):
            gids += [str(x).strip() for x in base_g if str(x).strip()]
        if isinstance(rt_g, list):
            gids += [str(x).strip() for x in rt_g if str(x).strip()]
        if gids:
            # de-dupe preserving order
            seen = set()
            out_gids: List[str] = []
            for x in gids:
                if x in seen:
                    continue
                seen.add(x)
                out_gids.append(x)
            merged["source_guild_ids"] = out_gids
        _settings = merged
except Exception:
    _settings = _settings or {}

if _settings:
    _set_raw("GLOBAL_VERBOSE", "true" if _to_bool(_settings.get("verbose"), True) else "false")
    _set_raw("SOURCE_GUILD_IDS", _settings.get("source_guild_ids"))
    _set_raw("SOURCE_GUILD_ID", _settings.get("source_guild_id"))
    _set_raw("MIRRORWORLD_SERVER", _settings.get("mirrorworld_server_id"))
    _set_raw("DUPLICATE_TTL_SECONDS", _settings.get("duplicate_ttl_seconds"))
    _set_raw("SHORT_EMBED_CHAR_THRESHOLD", _settings.get("short_embed_char_threshold"))
    _set_raw("SHORT_EMBED_RETRY_DELAY_SECONDS", _settings.get("short_embed_retry_delay_seconds"))
    _set_raw("SHORT_EMBED_MAX_WAIT_SECONDS", _settings.get("short_embed_max_wait_seconds"))
    _set_raw("CHANNEL_CACHE_MIN_INTERVAL_SECONDS", _settings.get("channel_cache_min_interval_seconds"))
else:
    _settings = {}

# Relay/noise filtering (canonical: controlled via config/settings.json)
try:
    SKIP_WEBHOOK_MESSAGES: bool = _to_bool(_settings.get("skip_webhook_messages"), False)
except Exception:
    SKIP_WEBHOOK_MESSAGES = False
try:
    raw_names = _settings.get("skip_webhook_usernames")
    if isinstance(raw_names, list):
        SKIP_WEBHOOK_USERNAMES = {str(x).strip().lower() for x in raw_names if str(x).strip()}
    elif isinstance(raw_names, str) and raw_names.strip():
        SKIP_WEBHOOK_USERNAMES = {x.strip().lower() for x in raw_names.split(",") if x.strip()}
    else:
        SKIP_WEBHOOK_USERNAMES = {"rerouter"}
except Exception:
    SKIP_WEBHOOK_USERNAMES = {"rerouter"}

# Mention rewriting (prevents @unknown-role / #unknown-channel in destination server)
# Default ON for role/channel mentions because the destination guild will not have source IDs.
try:
    REWRITE_ROLE_MENTIONS: bool = _to_bool(_settings.get("rewrite_role_mentions"), True)
except Exception:
    REWRITE_ROLE_MENTIONS = True
try:
    REWRITE_CHANNEL_MENTIONS: bool = _to_bool(_settings.get("rewrite_channel_mentions"), True)
except Exception:
    REWRITE_CHANNEL_MENTIONS = True
try:
    REWRITE_USER_MENTIONS: bool = _to_bool(_settings.get("rewrite_user_mentions"), False)
except Exception:
    REWRITE_USER_MENTIONS = False

def cfg_get(key: str, fallback: Optional[str] = None) -> Optional[str]:
    """Get config value by key with basic backwards-compat matching."""
    if key in _CONFIG_RAW:
        return _CONFIG_RAW.get(key)
    env_val = os.getenv(key)
    if env_val is not None and str(env_val).strip() != "":
        return env_val
    key_upper = key.upper()
    for k, v in _CONFIG_RAW.items():
        if k.upper() == key_upper:
            return v
    # Partial match fallback (helps when labels change but values don't)
    for k, v in _CONFIG_RAW.items():
        ku = k.upper()
        if key_upper in ku or ku in key_upper:
            return v
    return fallback

def cfg_get_bool(key: str, default: bool = False) -> bool:
    v = cfg_get(key)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on", "enabled"}

def _parse_guild_ids(value: str) -> List[str]:
    """Parse comma-separated or JSON array guild IDs."""
    if not value or not str(value).strip():
        return []
    value = str(value).strip()
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(gid).strip() for gid in parsed if str(gid).strip()]
        if isinstance(parsed, (str, int)):
            return [str(parsed).strip()]
    except Exception:
        pass
    return [gid.strip() for gid in value.split(",") if gid.strip()]

# Core settings / IDs (mirror neonxt.core.config behavior, but locally)
VERBOSE: bool = cfg_get_bool("GLOBAL_VERBOSE", True)

DISCUM_BOT: str = str(
    cfg_get("DISCUM_USER_DISCUMBOT")
    or cfg_get("DISCUM_BOT")
    or cfg_get("DISCORD_TOKEN")
    or ""
).strip()

_source_guild_raw = str(
    cfg_get("SOURCE_GUILD_IDS")
    or cfg_get("SOURCE_GUILD_ID")
    or cfg_get("DISCORD_GUILD_ID")
    or ""
).strip()
SOURCE_GUILD_IDS: List[str] = _parse_guild_ids(_source_guild_raw)
SOURCE_GUILD_ID: str = SOURCE_GUILD_IDS[0] if SOURCE_GUILD_IDS else _source_guild_raw

MIRRORWORLD_SERVER: str = str(
    cfg_get("MIRRORWORLD_SERVER")
    or cfg_get("MIRRORWORLD_GUILD_ID")
    or cfg_get("MIRRORWORLD_GUILD")
    or ""
).strip()

MIRRORWORLD_SERVERS: List[str] = _parse_guild_ids(MIRRORWORLD_SERVER)
MIRRORWORLD_SERVER = MIRRORWORLD_SERVERS[0] if MIRRORWORLD_SERVERS else MIRRORWORLD_SERVER

# Legacy/back-compat: DISCORD_GUILD_ID maps to source guild id
DISCORD_GUILD_ID: str = SOURCE_GUILD_ID or str(cfg_get("RS_SERVER_GUILD_ID") or "").strip()

# Channel map (source channel -> webhook url)
CHANNEL_MAP_PATH = os.path.join(_CONFIG_DIR, "channel_map.json")

def load_channel_map(path: str) -> Dict[int, str]:
    """Load channel map JSON ({source_channel_id: webhook_url})."""
    try:
        # Some Windows editors save JSON with a UTF-8 BOM; utf-8-sig handles both.
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[int, str] = {}
    for k, v in data.items():
        try:
            cid = int(str(k).strip())
        except Exception:
            continue
        url = str(v or "").strip()
        if url:
            out[cid] = url
    return out

CHANNEL_MAP: Dict[int, str] = load_channel_map(CHANNEL_MAP_PATH)

# Standalone log writers (compatible paths)
_LOGS_DIR = os.path.join(_project_root, "logs")
_DISCUM_LOGS_PATH = os.path.join(_LOGS_DIR, "Botlogs", "discumlogs.json")
_SYSTEM_LOGS_PATH = os.path.join(_CONFIG_DIR, "systemlogs.json")

def _ensure_parent_dir(file_path: str) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
    except Exception:
        pass

def _append_json_line(file_path: str, entry: Dict[str, Any]) -> None:
    _ensure_parent_dir(file_path)
    try:
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass

def write_discum_log(entry: Dict[str, Any]) -> None:
    _append_json_line(_DISCUM_LOGS_PATH, entry)

def write_system_log(entry: Dict[str, Any]) -> None:
    """Write to config/systemlogs.json (JSON array)."""
    _ensure_parent_dir(_SYSTEM_LOGS_PATH)
    try:
        entry = dict(entry)
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        logs: List[Dict[str, Any]] = []
        try:
            if os.path.exists(_SYSTEM_LOGS_PATH):
                with open(_SYSTEM_LOGS_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        logs = loaded
        except Exception:
            logs = []
        logs.append(entry)
        logs = logs[-500:]
        tmp = _SYSTEM_LOGS_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
        try:
            os.replace(tmp, _SYSTEM_LOGS_PATH)
        except Exception:
            with open(_SYSTEM_LOGS_PATH, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

def write_error_log(entry: Dict[str, Any], bot_type: str = "unknown") -> None:
    entry = dict(entry)
    entry.setdefault("event", entry.get("event", "error"))
    entry["bot_type"] = bot_type
    entry["level"] = "ERROR"
    write_system_log(entry)

# Ensure logger stays local-only.
_discumbot_logger = None

try:
    _DUPLICATE_TTL_SECONDS = max(0, int(float(cfg_get("DUPLICATE_TTL_SECONDS", "10") or "10")))
except Exception:
    _DUPLICATE_TTL_SECONDS = 10
_RECENT_MESSAGE_HASHES: Dict[str, float] = {}  # Key: "channel_id:hash", Value: timestamp
try:
    _SHORT_EMBED_CHAR_THRESHOLD = max(0, int(float(cfg_get("SHORT_EMBED_CHAR_THRESHOLD", "50") or "50")))
except Exception:
    _SHORT_EMBED_CHAR_THRESHOLD = 50  # chars; below this we suspect truncated/partial content
try:
    _SHORT_EMBED_RETRY_DELAY = max(0.0, float(cfg_get("SHORT_EMBED_RETRY_DELAY_SECONDS", "5.0") or "5.0"))
except Exception:
    _SHORT_EMBED_RETRY_DELAY = 5.0  # seconds to wait before re-reading short embed payloads (embed hydration can be delayed)
try:
    _SHORT_EMBED_MAX_WAIT_SECONDS = max(0.0, float(cfg_get("SHORT_EMBED_MAX_WAIT_SECONDS", "35.0") or "35.0"))
except Exception:
    _SHORT_EMBED_MAX_WAIT_SECONDS = 35.0  # max time to wait for embed hydration via MESSAGE_UPDATE before forwarding
_MESSAGE_CACHE_TTL_SECONDS = 600  # keep cached MESSAGE_CREATE payloads for 10 minutes
_message_forward_queue: Dict[str, Dict[str, Any]] = {}  # Short embed retry queue keyed by message_id
_MESSAGE_PAYLOAD_CACHE: Dict[str, Dict[str, Any]] = {}  # message_id -> cached payload

# Track where each source message was forwarded so edits can PATCH instead of double-posting.
# source_message_id -> {"webhook": str, "dest_ids": [str], "last_signature": str, "updated_at": float}
_FORWARDED_MESSAGE_INDEX: Dict[str, Dict[str, Any]] = {}

_WEBHOOK_URL_RE = re.compile(r"/webhooks/(?P<id>\d+)/(?P<token>[^/?#]+)")

_SOURCE_CHANNELS_PATH = os.path.join(_CONFIG_DIR, "source_channels.json")
_DESTINATION_CHANNELS_PATH = os.path.join(_CONFIG_DIR, "destination_channels.json")
_SOURCE_CHANNEL_LOOKUP: Dict[int, Dict[str, Any]] = {}
_SOURCE_GUILD_LOOKUP: Dict[int, Dict[str, Any]] = {}
_SOURCE_CHANNEL_LAST_LOAD = 0.0
_SOURCE_CHANNEL_TTL_SECONDS = 300.0

# Role name cache for mention rewriting (guild_id -> role_id -> role_name)
_GUILD_ROLE_LOOKUP: Dict[int, Dict[int, str]] = {}
_GUILD_ROLE_LAST_LOAD: Dict[int, float] = {}
_GUILD_ROLE_TTL_SECONDS = 3600.0  # 1 hour

# After sending, do a delayed REST re-fetch and PATCH the mirrored message if the embed/title was hydrated late.
_HYDRATION_EDIT_DELAY_SECONDS = 4.0
_PENDING_HYDRATION_EDITS: Dict[str, float] = {}

# MESSAGE_UPDATE can arrive before we record the destination webhook message id.
# Queue the latest update payload keyed by source_message_id so we can PATCH once mapping exists.
_PENDING_EDIT_UPDATES: Dict[str, Dict[str, Any]] = {}


def _webhook_execute_url(webhook_url: str) -> str:
    """Ensure webhook execute URL includes wait=true (so Discord returns the created message id)."""
    url = str(webhook_url or "").strip()
    if not url:
        return url
    lower = url.lower()
    if "wait=true" in lower or "wait=1" in lower:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}wait=true"


def _prune_pending_edit_updates(now: Optional[float] = None) -> None:
    """Bound memory + drop stale queued edit payloads."""
    if not _PENDING_EDIT_UPDATES:
        return
    now = now or time.time()
    ttl = float(_MESSAGE_CACHE_TTL_SECONDS or 600)
    stale = [mid for mid, entry in _PENDING_EDIT_UPDATES.items() if (now - float(entry.get("updated_at", 0))) > ttl]
    for mid in stale:
        _PENDING_EDIT_UPDATES.pop(mid, None)
    # Hard bound to avoid unbounded growth on long runs.
    if len(_PENDING_EDIT_UPDATES) > 5000:
        items = sorted(_PENDING_EDIT_UPDATES.items(), key=lambda kv: float(kv[1].get("updated_at", 0)))
        for mid, _ in items[:1000]:
            _PENDING_EDIT_UPDATES.pop(mid, None)


def _queue_pending_edit_update(message_dict: Dict[str, Any], channel_id: int, guild_id: Any) -> None:
    """Store the latest MESSAGE_UPDATE payload until the forward mapping is available."""
    try:
        src_id = str(message_dict.get("id", "")).strip()
    except Exception:
        src_id = ""
    if not src_id:
        return
    try:
        channel_id_int = int(channel_id)
    except Exception:
        channel_id_int = channel_id
    try:
        guild_id_int = int(guild_id) if guild_id else guild_id
    except Exception:
        guild_id_int = guild_id

    try:
        frozen = json.loads(json.dumps(message_dict))
    except Exception:
        frozen = dict(message_dict or {})

    now = time.time()
    _PENDING_EDIT_UPDATES[src_id] = {
        "message": frozen,
        "channel_id": channel_id_int,
        "guild_id": guild_id_int,
        "updated_at": now,
    }
    _prune_pending_edit_updates(now=now)
    if VERBOSE:
        log_debug(f"Queued MESSAGE_UPDATE for {src_id} (waiting for destination mapping)")


def _flush_pending_edit_update(source_message_id: str) -> bool:
    """If an edit payload is queued for this message, apply it now (PATCH existing mirror post)."""
    sid = str(source_message_id or "").strip()
    if not sid:
        return False
    entry = _PENDING_EDIT_UPDATES.pop(sid, None)
    if not isinstance(entry, dict):
        return False
    try:
        msg = entry.get("message") or {}
        cid = entry.get("channel_id")
        gid = entry.get("guild_id")
        if isinstance(msg, dict) and cid is not None:
            _forward_to_webhook(msg, int(cid), gid, edit_existing=True)
            return True
    except Exception:
        return False
    return False

# Cache guild identity lookups so webhook username/avatar can always match the source server
# without relying on flaky gateway session metadata.
_GUILD_IDENTITY_CACHE: Dict[int, Dict[str, Any]] = {}  # guild_id -> {"name": str|None, "icon": str|None, "cached_at": float}
_GUILD_IDENTITY_TTL_SECONDS = 3600.0  # 1 hour

def _refresh_source_channel_lookup(force: bool = False) -> None:
    """Load source channel metadata for friendly logging."""
    global _SOURCE_CHANNEL_LOOKUP, _SOURCE_GUILD_LOOKUP, _SOURCE_CHANNEL_LAST_LOAD
    now = time.time()
    if not force and _SOURCE_CHANNEL_LOOKUP and (now - _SOURCE_CHANNEL_LAST_LOAD) < _SOURCE_CHANNEL_TTL_SECONDS:
        return
    try:
        with open(_SOURCE_CHANNELS_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        _SOURCE_CHANNEL_LOOKUP = {}
        _SOURCE_CHANNEL_LAST_LOAD = now
        return
    except Exception as exc:
        if VERBOSE:
            print(f"[WARN] Failed to load source_channels.json: {exc}")
        return

    new_lookup: Dict[int, Dict[str, Any]] = {}
    new_guild_lookup: Dict[int, Dict[str, Any]] = {}
    for guild in data.get("guilds", []) or []:
        try:
            guild_id = int(guild.get("guild_id"))
        except (TypeError, ValueError):
            continue
        guild_name = guild.get("guild_name") or f"Guild-{guild_id}"
        guild_icon = guild.get("guild_icon") or None
        new_guild_lookup[guild_id] = {"guild_name": guild_name, "guild_icon": guild_icon}
        for channel in guild.get("channels", []) or []:
            try:
                channel_id = int(channel.get("id"))
            except (TypeError, ValueError):
                continue
            channel_name = channel.get("name") or f"Channel-{channel_id}"
            new_lookup[channel_id] = {
                "channel_name": channel_name,
                "guild_name": guild_name,
                "guild_id": guild_id,
                "guild_icon": guild_icon,
            }
    _SOURCE_CHANNEL_LOOKUP = new_lookup
    _SOURCE_GUILD_LOOKUP = new_guild_lookup
    _SOURCE_CHANNEL_LAST_LOAD = now

def _get_source_channel_details(channel_id: int, guild_id: Optional[int] = None) -> Dict[str, Any]:
    """Return friendly channel + guild names for logging."""
    _refresh_source_channel_lookup()
    info = _SOURCE_CHANNEL_LOOKUP.get(channel_id, {})
    channel_name = info.get("channel_name") or f"Channel-{channel_id}"
    guild_name = info.get("guild_name")
    resolved_guild_id = info.get("guild_id")
    guild_icon = info.get("guild_icon")
    if not guild_name:
        if guild_id:
            guild_name = f"Guild-{guild_id}"
            resolved_guild_id = guild_id
        else:
            guild_name = "Unknown server"
    return {
        "channel_name": channel_name,
        "guild_name": guild_name,
        "guild_id": resolved_guild_id or guild_id,
        "guild_icon": guild_icon,
    }


_ROLE_MENTION_RE = re.compile(r"<@&(\d+)>")
_CHANNEL_MENTION_RE = re.compile(r"<#(\d+)>")
_USER_MENTION_RE = re.compile(r"<@!?(\d+)>")


def _refresh_guild_role_lookup(guild_id: int, *, force: bool = False) -> None:
    """Fetch and cache guild roles for mention rewriting."""
    if not guild_id:
        return
    now = time.time()
    try:
        last = float(_GUILD_ROLE_LAST_LOAD.get(int(guild_id), 0.0) or 0.0)
    except Exception:
        last = 0.0
    if not force and (now - last) < _GUILD_ROLE_TTL_SECONDS and _GUILD_ROLE_LOOKUP.get(int(guild_id)):
        return
    if not DISCUM_BOT:
        return
    try:
        headers = {
            "Authorization": DISCUM_BOT,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        url = f"https://discord.com/api/v9/guilds/{int(guild_id)}/roles"
        # One retry on 429
        for _ in range(2):
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    role_map: Dict[int, str] = {}
                    for r in data:
                        if not isinstance(r, dict):
                            continue
                        try:
                            rid = int(r.get("id"))
                        except Exception:
                            continue
                        name = str(r.get("name") or "").strip()
                        if name:
                            role_map[rid] = name
                    _GUILD_ROLE_LOOKUP[int(guild_id)] = role_map
                    _GUILD_ROLE_LAST_LOAD[int(guild_id)] = now
                return
            if resp.status_code == 429:
                try:
                    retry_after = float((resp.json() or {}).get("retry_after", 1.0) or 1.0)
                except Exception:
                    retry_after = 1.0
                time.sleep(min(retry_after, 2.0))
                continue
            # 403/404 or other errors → don't spam; just stop
            return
    except Exception:
        return


def _render_role_mention(guild_id: Optional[int], role_id: int) -> str:
    """Render a role mention as plain text (no ID) so the destination guild doesn't show @unknown-role."""
    try:
        gid = int(guild_id) if guild_id else 0
    except Exception:
        gid = 0
    name = None
    if gid:
        try:
            _refresh_guild_role_lookup(gid)
            name = (_GUILD_ROLE_LOOKUP.get(gid) or {}).get(int(role_id))
        except Exception:
            name = None
    if name:
        return f"@{name}"
    # Fallback: keep it readable but never as a real mention
    return f"@role-{str(role_id)[-6:]}"


def _render_channel_mention(channel_id: int) -> str:
    """Render a channel mention as plain text (no ID)."""
    try:
        _refresh_source_channel_lookup()
        info = _SOURCE_CHANNEL_LOOKUP.get(int(channel_id), {}) or {}
        name = str(info.get("channel_name") or "").strip()
        if name:
            return f"#{name}"
    except Exception:
        pass
    return f"#channel-{str(channel_id)[-6:]}"


def _sanitize_mentions_for_destination(text: str, *, guild_id: Optional[int]) -> str:
    """Rewrite mentions into plain text to avoid @unknown-role and prevent pings."""
    if not isinstance(text, str) or not text:
        return text
    s = text
    # Even with allowed_mentions disabled, add a harmless ZWSP to reduce accidental pings in clients.
    s = s.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")
    if REWRITE_ROLE_MENTIONS:
        s = _ROLE_MENTION_RE.sub(lambda m: _render_role_mention(guild_id, int(m.group(1))), s)
    if REWRITE_CHANNEL_MENTIONS:
        s = _CHANNEL_MENTION_RE.sub(lambda m: _render_channel_mention(int(m.group(1))), s)
    if REWRITE_USER_MENTIONS:
        # Avoid turning these into real mentions in the destination guild.
        s = _USER_MENTION_RE.sub(lambda m: f"@user-{str(m.group(1))[-6:]}", s)
    return s


def _sanitize_mentions_in_obj(obj: Any, *, guild_id: Optional[int]) -> Any:
    """Recursively sanitize mention tokens in dict/list payloads (embeds)."""
    if isinstance(obj, str):
        return _sanitize_mentions_for_destination(obj, guild_id=guild_id)
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = _sanitize_mentions_in_obj(obj[i], guild_id=guild_id)
        return obj
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            obj[k] = _sanitize_mentions_in_obj(v, guild_id=guild_id)
        return obj
    return obj


def _schedule_hydration_edit(*, source_message_id: str, channel_id: int, guild_id: int) -> None:
    """
    Schedule a best-effort post-send REST fetch and webhook PATCH.

    This fixes cases where the gateway delivers a partially-hydrated embed/title, but the REST message
    becomes complete a few seconds later. We PATCH instead of re-posting, so no duplicates.
    """
    sid = str(source_message_id or "").strip()
    if not sid:
        return
    now = time.time()
    last = float(_PENDING_HYDRATION_EDITS.get(sid, 0.0) or 0.0)
    # Avoid spamming hydration edits for the same message id.
    if last and (now - last) < 60.0:
        return
    _PENDING_HYDRATION_EDITS[sid] = now
    if len(_PENDING_HYDRATION_EDITS) > 5000:
        # bound size (drop oldest)
        try:
            items = sorted(_PENDING_HYDRATION_EDITS.items(), key=lambda kv: float(kv[1] or 0.0))
            for k, _ in items[:1000]:
                _PENDING_HYDRATION_EDITS.pop(k, None)
        except Exception:
            pass

    def _run():
        try:
            time.sleep(float(_HYDRATION_EDIT_DELAY_SECONDS or 4.0))
        except Exception:
            return
        try:
            full = _fetch_full_message(int(channel_id), sid)
            if not isinstance(full, dict):
                return
            # Apply authoritative payload and PATCH the existing mirrored message.
            try:
                full["id"] = sid
            except Exception:
                pass
            _forward_to_webhook(full, int(channel_id), int(guild_id), edit_existing=True)
        except Exception:
            return

    try:
        threading.Thread(target=_run, daemon=True).start()
    except Exception:
        return

def _build_guild_icon_url(guild_id: int, icon_hash: str) -> Optional[str]:
    if not guild_id or not icon_hash:
        return None
    try:
        ext = "gif" if str(icon_hash).startswith("a_") else "png"
        return f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}"
    except Exception:
        return None

def _get_guild_identity(guild_id: Optional[int], channel_id: Optional[int] = None) -> Dict[str, Optional[str]]:
    """Best-effort source guild identity (name + icon url) for webhook username/avatar."""
    if not guild_id:
        return {"guild_name": None, "guild_icon_url": None}

    gid = None
    try:
        gid = int(guild_id)
    except Exception:
        return {"guild_name": None, "guild_icon_url": None}

    # 1) Try cached channel/guild lookup from source_channels.json (fast, local)
    try:
        _refresh_source_channel_lookup()
        if channel_id is not None:
            ch_info = _SOURCE_CHANNEL_LOOKUP.get(int(channel_id), {})
            name = ch_info.get("guild_name")
            icon_hash = ch_info.get("guild_icon")
            icon_url = _build_guild_icon_url(gid, icon_hash) if icon_hash else None
            if name or icon_url:
                return {"guild_name": name, "guild_icon_url": icon_url}
        g_info = _SOURCE_GUILD_LOOKUP.get(gid, {})
        name = g_info.get("guild_name")
        icon_hash = g_info.get("guild_icon")
        icon_url = _build_guild_icon_url(gid, icon_hash) if icon_hash else None
        if name or icon_url:
            return {"guild_name": name, "guild_icon_url": icon_url}
    except Exception:
        pass

    now = time.time()
    cached = _GUILD_IDENTITY_CACHE.get(gid) or {}
    if cached and (now - float(cached.get("cached_at", 0.0))) < _GUILD_IDENTITY_TTL_SECONDS:
        cached_name = cached.get("name")
        cached_icon = cached.get("icon")
        return {
            "guild_name": cached_name,
            "guild_icon_url": _build_guild_icon_url(gid, cached_icon) if cached_icon else None,
        }

    # 2) Try gateway session (can be incomplete/unreliable; keep as best-effort)
    try:
        guild_data = bot.gateway.session.guild(gid)
        if isinstance(guild_data, dict):
            g_name = guild_data.get("name")
            g_icon = guild_data.get("icon")
            if g_name or g_icon:
                _GUILD_IDENTITY_CACHE[gid] = {"name": g_name, "icon": g_icon, "cached_at": now}
                return {"guild_name": g_name, "guild_icon_url": _build_guild_icon_url(gid, g_icon) if g_icon else None}
    except Exception:
        pass

    # 3) REST guild fetch (usually reliable for identity even when message REST is blocked)
    try:
        headers = {
            "Authorization": DISCUM_BOT,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        resp = requests.get(
            f"https://discord.com/api/v9/guilds/{gid}?with_counts=false",
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json() if hasattr(resp, "json") else None
            if isinstance(data, dict):
                g_name = data.get("name")
                g_icon = data.get("icon")
                _GUILD_IDENTITY_CACHE[gid] = {"name": g_name, "icon": g_icon, "cached_at": now}
                return {"guild_name": g_name, "guild_icon_url": _build_guild_icon_url(gid, g_icon) if g_icon else None}
    except Exception:
        pass

    # Final fallback
    return {"guild_name": None, "guild_icon_url": None}


#
# NOTE: Standalone forwarder mode does not auto-subscribe to new channels.
# The only source of truth is `config/channel_map.json`.
#


def _should_retry_short_embed(message_dict: Dict[str, Any]) -> bool:
    """Return True when an embed message likely has truncated content."""
    if not isinstance(message_dict, dict):
        return False
    embeds = message_dict.get("embeds") or []
    if not embeds:
        return False
    content = (message_dict.get("content") or "").strip()
    # If we already have meaningful content, don't delay forwarding just because embeds exist.
    if len(content) >= _SHORT_EMBED_CHAR_THRESHOLD:
        return False

    # Many Discord embeds are "hydrated" asynchronously via MESSAGE_UPDATE. A bare embed with only a
    # title/url often arrives first; then description/fields/images populate later. Treat "bare"
    # embeds as retry-worthy so we wait for the fuller payload.
    for raw in embeds:
        if not isinstance(raw, dict):
            continue
        desc = (raw.get("description") or "").strip()
        if desc:
            return False
        fields = raw.get("fields") or []
        if isinstance(fields, list):
            for f in fields:
                if not isinstance(f, dict):
                    continue
                if str(f.get("name", "")).strip() or str(f.get("value", "")).strip():
                    return False
        # Media blocks count as "hydrated" (often missing on initial thin payload).
        if raw.get("image") or raw.get("thumbnail") or raw.get("video"):
            return False
        author = raw.get("author") or {}
        if isinstance(author, dict) and (author.get("name") or author.get("url") or author.get("icon_url")):
            return False
        footer = raw.get("footer") or {}
        if isinstance(footer, dict) and str(footer.get("text", "")).strip():
            return False
        provider = raw.get("provider") or {}
        if isinstance(provider, dict) and provider.get("name"):
            return False

    # Content is short AND embeds are still bare -> retry for hydration.
    return True

def _prune_message_payload_cache(now: Optional[float] = None) -> None:
    """Remove cached payloads that have exceeded the TTL."""
    if not _MESSAGE_PAYLOAD_CACHE:
        return
    now = now or time.time()
    stale_ids = [
        msg_id for msg_id, payload in _MESSAGE_PAYLOAD_CACHE.items()
        if now - payload.get("_cached_at", 0) > _MESSAGE_CACHE_TTL_SECONDS
    ]
    for msg_id in stale_ids:
        _MESSAGE_PAYLOAD_CACHE.pop(msg_id, None)

def _cache_message_payload(message_dict: Dict[str, Any]) -> None:
    """Store the original payload from MESSAGE_CREATE so we can restore data on edits.
    
    IMPORTANT: Cache ALL messages (not just embeds) so we can detect when messages are
    edited and restore the original content if the edit truncates it.
    """
    msg_id = str(message_dict.get("id", "")).strip()
    if not msg_id:
        return
    
    content = message_dict.get("content", "") or ""
    embeds = message_dict.get("embeds") or []
    attachments = message_dict.get("attachments") or []
    
    # Skip if message has no content, embeds, or attachments
    if not content.strip() and not embeds and not attachments:
        return
    
    now = time.time()
    try:
        cached_payload = {
            "content": content,
            "embeds": json.loads(json.dumps(embeds)),
            "attachments": json.loads(json.dumps(attachments)),
            "_cached_at": now,
        }
        _MESSAGE_PAYLOAD_CACHE[msg_id] = cached_payload
        
        # Reduced logging - only log every 50th cache entry to prevent log bloat
        if VERBOSE and len(_MESSAGE_PAYLOAD_CACHE) % 50 == 0:
            content_len = len(content)
            embeds_count = len(embeds)
            attach_count = len(attachments)
            print(f"[CACHE] Cache stats: {len(_MESSAGE_PAYLOAD_CACHE)} entries | Latest: content={content_len}, embeds={embeds_count}")
        
        # Prune old entries periodically
        if len(_MESSAGE_PAYLOAD_CACHE) % 100 == 0:
            _prune_message_payload_cache(now=now)
    except Exception as cache_err:
        if VERBOSE:
            print(f"[CACHE] Failed to cache message {msg_id}: {cache_err}")

def _apply_cached_payload_if_richer(message_dict: Dict[str, Any], *, reason: str = "") -> bool:
    """If cached payload contains richer data than current message, merge it in."""
    msg_id = str(message_dict.get("id", "")).strip()
    if not msg_id:
        return False
    cached = _MESSAGE_PAYLOAD_CACHE.get(msg_id)
    if not cached:
        return False
    if time.time() - cached.get("_cached_at", 0) > _MESSAGE_CACHE_TTL_SECONDS:
        _MESSAGE_PAYLOAD_CACHE.pop(msg_id, None)
        return False
    changed = False
    current_content = message_dict.get("content") or ""
    cached_content = cached.get("content") or ""
    if len(cached_content) > len(current_content):
        message_dict["content"] = cached_content
        changed = True
    current_embeds = message_dict.get("embeds") or []
    cached_embeds = cached.get("embeds") or []
    if cached_embeds and (not current_embeds or len(str(current_embeds)) < len(str(cached_embeds))):
        message_dict["embeds"] = json.loads(json.dumps(cached_embeds))
        changed = True
    current_attachments = message_dict.get("attachments") or []
    cached_attachments = cached.get("attachments") or []
    if cached_attachments and not current_attachments:
        message_dict["attachments"] = json.loads(json.dumps(cached_attachments))
        changed = True
    if changed:
        prefix = "[INFO]" if not VERBOSE else '[DEBUG]'
        description = f" ({reason})" if reason else ""
        print(f"{prefix} Restored cached payload for message {msg_id}{description}")
    return changed
# ================= Single-instance Lock =================
# Standalone: local lock only (per-folder).
# This avoids Windows PID-reuse issues / false positives from token-global locks.
_LOCK_FILE_PATH = os.path.join(_project_root, ".d2d.lock")

def _cleanup_lock_file():
    try:
        if os.path.exists(_LOCK_FILE_PATH):
            os.remove(_LOCK_FILE_PATH)
    except Exception:
        pass

# REMOVED: _close_terminal_window() function
# Was causing PowerShell windows to pop up repeatedly.
# DiscumBot typically runs in the background (service/manager), so terminal closing is not needed.

def _pid_is_running(pid: int, *, expected_cmd_substring: Optional[str] = None) -> bool:
    """Best-effort PID existence check (cross-platform).

    If expected_cmd_substring is provided, only returns True when the process
    appears to be running with a command line containing that substring.
    This avoids Windows PID reuse false-positives for stale lock files.
    """
    try:
        pid = int(pid)
    except Exception:
        return False
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True

    expected = (expected_cmd_substring or "").strip().lower()

    # Prefer psutil if present (works well on Windows)
    try:
        import psutil  # type: ignore

        if not psutil.pid_exists(pid):
            return False
        if not expected:
            return True
        try:
            proc = psutil.Process(pid)
            cmdline = proc.cmdline()
            joined = " ".join(str(x) for x in (cmdline or []))
            return expected in joined.lower()
        except Exception:
            # If we can't inspect cmdline, fall back to PID existence.
            return True
    except Exception:
        pass

    # Windows fallback (no psutil): tasklist
    try:
        if platform.system().lower().startswith("win"):
            # First, confirm PID exists.
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=3,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            out = (result.stdout or "") + "\n" + (result.stderr or "")
            if "No tasks are running" in out or str(pid) not in out:
                return False
            if not expected:
                return True

            # Then, verify command line contains expected substring (wmic is deprecated but still common on Win10/11).
            try:
                wm = subprocess.run(
                    ["wmic", "process", "where", f"ProcessId={pid}", "get", "CommandLine", "/value"],
                    capture_output=True,
                    text=True,
                    timeout=3,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                wm_out = (wm.stdout or "") + "\n" + (wm.stderr or "")
                # Format: CommandLine=<...>
                return expected in wm_out.lower()
            except Exception:
                # If we can't inspect cmdline, fall back to PID existence.
                return True
    except Exception:
        pass

    # POSIX fallback: signal 0
    try:
        os.kill(pid, 0)
        if not expected:
            return True
        # Can't inspect cmdline without psutil; treat as running.
        return True
    except Exception:
        return False

def _acquire_single_instance_lock() -> None:
    """Prevent multiple concurrent d2d instances from running.

    Uses an atomic lock file create (O_CREAT|O_EXCL). If the file already exists,
    we assume another instance is running and exit quietly.
    """
    # Local directory lock (prevents multiple instances in same directory)
    try:
        fd = os.open(_LOCK_FILE_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(f"pid={os.getpid()}\nstart={int(time.time())}\n")
        atexit.register(_cleanup_lock_file)
    except FileExistsError:

        # Another instance detected - check if stale and handle gracefully
        print("[WARN] Lock file exists - checking if stale...")
        try:
            if os.path.exists(_LOCK_FILE_PATH):
                with open(_LOCK_FILE_PATH, 'r') as f:
                    lock_content = f.read()
                    if 'pid=' in lock_content:
                        import re
                        pid_match = re.search(r'pid=(\d+)', lock_content)
                        if pid_match:
                            pid = int(pid_match.group(1))
                            # Check if process is still running (and is THIS script).
                            # This avoids Windows PID-reuse false positives.
                            try:
                                _this_script_hint = os.path.abspath(__file__).lower()
                            except Exception:
                                _this_script_hint = "discumbot.py"
                            if _pid_is_running(pid, expected_cmd_substring=_this_script_hint):
                                # Process exists - real duplicate
                                print(f"[EXIT] Process {pid} is running - duplicate instance detected")
                                print("[EXIT] Exiting to avoid conflicts.")
                                time.sleep(0.5)
                                os._exit(0)

                            # Process doesn't exist - stale lock, try to remove with retry
                            print(f"[INFO] Process {pid} not found - stale lock detected")
                            print("[INFO] Attempting to remove stale lock file...")
                                
                            # Retry removing lock file with multiple strategies
                            removed = False
                                
                            # Strategy 1: Simple retry with delays
                            for attempt in range(5):
                                try:
                                    if attempt > 0:
                                        time.sleep(0.5)
                                    os.remove(_LOCK_FILE_PATH)
                                    removed = True
                                    break
                                except PermissionError:
                                    continue
                                except FileNotFoundError:
                                    removed = True
                                    break
                                except Exception:
                                    continue
                                
                            # Strategy 2: Try to rename first (sometimes works when delete doesn't)
                            if not removed:
                                try:
                                    temp_lock = _LOCK_FILE_PATH + ".old"
                                    if os.path.exists(temp_lock):
                                        try:
                                            os.remove(temp_lock)
                                        except Exception:
                                            pass
                                    os.rename(_LOCK_FILE_PATH, temp_lock)
                                    time.sleep(0.2)
                                    try:
                                        os.remove(temp_lock)
                                    except Exception:
                                        pass  # Ignore if temp file can't be removed
                                    removed = True
                                except Exception:
                                    pass
                                
                            # Strategy 3: Try opening file in write mode and truncating (forces unlock)
                            if not removed:
                                try:
                                    with open(_LOCK_FILE_PATH, 'w') as f:
                                        f.write("")  # Truncate file
                                    time.sleep(0.2)
                                    os.remove(_LOCK_FILE_PATH)
                                    removed = True
                                except Exception:
                                    pass
                                
                            # Strategy 4: Use Windows delete (if on Windows)
                            if not removed and platform.system().lower() == 'windows':
                                try:
                                    # This is a last resort that sometimes works when file is locked
                                    subprocess.run(
                                        ['cmd', '/c', f'del /F /Q "{_LOCK_FILE_PATH}"'],
                                        timeout=2,
                                        capture_output=True,
                                        creationflags=subprocess.CREATE_NO_WINDOW
                                    )
                                    if not os.path.exists(_LOCK_FILE_PATH):
                                        removed = True
                                except Exception:
                                    pass
                                
                            if removed:
                                # Wait a moment for filesystem to sync
                                time.sleep(0.3)
                                # Retry acquiring lock
                                try:
                                    fd = os.open(_LOCK_FILE_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                                    with os.fdopen(fd, "w") as f:
                                        f.write(f"pid={os.getpid()}\nstart={int(time.time())}\n")
                                    atexit.register(_cleanup_lock_file)
                                    print("[INFO] Lock file acquired - continuing startup")
                                    return  # Successfully acquired lock, continue
                                except FileExistsError:
                                    # Race condition - another process got it first
                                    print("[EXIT] Another instance started while removing lock")
                                    print("[EXIT] Exiting to avoid conflicts.")
                                    time.sleep(0.5)
                                    os._exit(0)
                            else:
                                # Could not remove lock file - try overwriting instead
                                print("[WARN] Could not remove lock file after all attempts")
                                print("[INFO] Attempting to overwrite lock file instead...")
                                try:
                                    # Try to overwrite the lock file (if we can write to it, it's safe)
                                    with open(_LOCK_FILE_PATH, 'w') as f:
                                        f.write(f"pid={os.getpid()}\nstart={int(time.time())}\n")
                                    # Successfully overwrote - register cleanup and continue
                                    atexit.register(_cleanup_lock_file)
                                    print("[INFO] Lock file overwritten - continuing startup")
                                    return  # Successfully acquired lock, continue
                                except Exception as overwrite_error:
                                    # Can't even overwrite - file is truly locked
                                    print("[WARN] Cannot overwrite lock file (may be locked by antivirus/explorer)")
                                    print(f"[INFO] Manual fix: Delete {_LOCK_FILE_PATH} and restart")
                                    # Log error to systemlogs.json
                                    try:
                                        write_error_log({
                                            "scope": "discumbot",
                                            "error": f"Lock file removal/overwrite failed: {_LOCK_FILE_PATH}",
                                            "context": {
                                                "lock_file": _LOCK_FILE_PATH,
                                                "pid": pid,
                                                "overwrite_error": str(overwrite_error),
                                                "note": "File may be locked by Windows Explorer or antivirus. Manual deletion required."
                                            }
                                        }, bot_type="discumbot")
                                    except Exception:
                                        pass
                                    print("[EXIT] Exiting for safety")
                                    time.sleep(0.5)
                                    os._exit(1)
        except Exception as e:
            # If something goes wrong while checking the existing lock, try to overwrite it
            # (safer than permanently preventing startup due to transient Windows FS errors).
            print(f"[WARN] Error checking lock file: {e}")
            try:
                with open(_LOCK_FILE_PATH, 'w') as f:
                    f.write(f"pid={os.getpid()}\nstart={int(time.time())}\n")
                atexit.register(_cleanup_lock_file)
                print("[INFO] Lock file overwritten after check error - continuing startup")
                return
            except Exception:
                pass

            # Log error to systemlogs.json
            try:
                write_error_log({
                    "scope": "discumbot",
                    "error": f"Lock file check failed: {str(e)}",
                    "context": {
                        "lock_file": _LOCK_FILE_PATH
                    }
                }, bot_type="discumbot")
            except Exception:
                pass
            print("[EXIT] Exiting to avoid duplicate instances")
            time.sleep(0.5)
            os._exit(0)  # Exit if we couldn't resolve the lock issue
    except Exception as e:
        # If lock cannot be established, proceed but warn

        print(f"[WARN] Unable to create lock file; continuing without single-instance guard: {e}")
        # Log error to systemlogs.json
        try:
            write_error_log({
                "scope": "discumbot",
                "error": f"Lock file creation failed: {str(e)}",
                "context": {
                    "lock_file": _LOCK_FILE_PATH
                }
            }, bot_type="discumbot")
        except Exception:
            pass

if not (DISCUM_BOT or "").strip():
    print("[ERROR] DISCUM_BOT is not set in environment/.env")

    # Log error to systemlogs.json
    try:
        write_error_log({
            "scope": "discumbot",
            "error": "DISCUM_BOT not set in environment/.env",
            "context": {}
        }, bot_type="discumbot")
    except Exception:
        pass
    sys.exit(1)

# ===== Console Style Setup (match bots) - MUST BE BEFORE ANY OUTPUT =====
if platform.system().lower().startswith("win"):
    try:
        os.system("color 0a")  # Green text on black background (0=black bg, a=green text)
        # Also try setting via ctypes for more reliable color setting
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            # 10 = green text, 0 = black background
            kernel32.SetConsoleTextAttribute(kernel32.GetStdHandle(-11), 10)
        except Exception:
            pass
    except Exception:
        pass  # Ignore if color command fails

# Acquire single-instance lock as early as possible (after console setup)
_acquire_single_instance_lock()

startup_banner(
    "Discord2Discord Bridge v3.4 (Pure Forwarder)",
    [
        "Initializing environment...",
        "Waiting for Discord connection...",
    ]
)

# Initialize discum client with USER ACCOUNT TOKEN (selfbot, not bot token)cx fv n 
# discum.Client is specifically designed for user account tokens from browser DevTools
# NOTE: This is different from datamanagerbot.py and pingbot.py which use bot tokens (discord.py)
bot = discum.Client(token=DISCUM_BOT, log=False)
ENABLE_CLASSIFIER = False  # hard-off: D2D runs as a pure forwarder

# Cache webhook destination metadata (channel/channel name) to avoid repeated lookups
_WEBHOOK_INFO_CACHE = {}
_WEBHOOK_INFO_TTL = 3600  # seconds
_DEST_CHANNEL_NAME_CACHE: Dict[int, Dict[str, object]] = {}
_DEST_CHANNEL_NAME_TTL = 300  # seconds
_DEST_CHANNEL_FILE_LOOKUP: Dict[int, str] = {}
_DEST_CHANNEL_FILE_LAST_LOAD = 0.0
_DEST_CHANNEL_FILE_TTL = 300.0  # seconds

def _refresh_destination_channel_file_lookup(force: bool = False) -> None:
    """Load destination channel id->name from destination_channels.json (preferred; avoids session 'guilds' errors)."""
    global _DEST_CHANNEL_FILE_LOOKUP, _DEST_CHANNEL_FILE_LAST_LOAD
    now = time.time()
    if not force and _DEST_CHANNEL_FILE_LOOKUP and (now - _DEST_CHANNEL_FILE_LAST_LOAD) < _DEST_CHANNEL_FILE_TTL:
        return
    try:
        with open(_DESTINATION_CHANNELS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        _DEST_CHANNEL_FILE_LOOKUP = {}
        _DEST_CHANNEL_FILE_LAST_LOAD = now
        return
    except Exception:
        return

    lookup: Dict[int, str] = {}
    try:
        for ch in (data.get("channels", []) if isinstance(data, dict) else []) or []:
            if not isinstance(ch, dict):
                continue
            ch_id = ch.get("id")
            name = ch.get("name")
            if not ch_id or not name:
                continue
            try:
                lookup[int(ch_id)] = str(name)
            except Exception:
                continue
    except Exception:
        lookup = {}

    _DEST_CHANNEL_FILE_LOOKUP = lookup
    _DEST_CHANNEL_FILE_LAST_LOAD = now

def _resolve_destination_channel_name(dest_channel_id: Optional[int]) -> str:
    """Resolve destination channel ID to a human-readable name."""
    if dest_channel_id is None:
        return "Unknown"
    try:
        dest_id_int = int(dest_channel_id)
    except Exception:
        return f"Channel {dest_channel_id}"
    
    now = time.time()
    cached = _DEST_CHANNEL_NAME_CACHE.get(dest_id_int)
    if cached and now - cached.get("cached_at", 0) < _DEST_CHANNEL_NAME_TTL:
        cached_name = cached.get("name")
        if cached_name:
            return cached_name
    
    # Prefer local destination cache file (stable)
    try:
        _refresh_destination_channel_file_lookup()
        file_name = _DEST_CHANNEL_FILE_LOOKUP.get(dest_id_int)
        if file_name:
            _DEST_CHANNEL_NAME_CACHE[dest_id_int] = {"name": file_name, "cached_at": now}
            return file_name
    except Exception:
        pass

    resolved_name = None
    try:
        if hasattr(bot.gateway, 'session') and bot.gateway.session:
            target_guild_id = MIRRORWORLD_SERVER or DISCORD_GUILD_ID
            guild_data = bot.gateway.session.guild(target_guild_id)
            channels = getattr(guild_data, "channels", None) if guild_data else None
            
            if isinstance(channels, dict):
                candidate = channels.get(dest_id_int) or channels.get(str(dest_id_int))
                if candidate:
                    resolved_name = getattr(candidate, "name", None)
                    if not resolved_name and isinstance(candidate, dict):
                        resolved_name = candidate.get("name")
            elif isinstance(channels, (list, tuple)):
                for channel_obj in channels:
                    ch_id = getattr(channel_obj, "id", None)
                    if ch_id is None and isinstance(channel_obj, dict):
                        ch_id = channel_obj.get("id")
                    try:
                        if ch_id is not None and int(ch_id) == dest_id_int:
                            resolved_name = getattr(channel_obj, "name", None)
                            if not resolved_name and isinstance(channel_obj, dict):
                                resolved_name = channel_obj.get("name")
                            if resolved_name:
                                break
                    except Exception:
                        continue
    except Exception as exc:
        if VERBOSE:
            print(f"[WARN] Failed to resolve channel name for {dest_channel_id}: {exc}")
    
    if not resolved_name:
        resolved_name = f"Channel {dest_id_int}"
    
    _DEST_CHANNEL_NAME_CACHE[dest_id_int] = {"name": resolved_name, "cached_at": now}
    return resolved_name

# Duplicate detection helpers (matches datamanagerbot logic)
def _hash_message_content_for_duplicate_check(message_dict: Dict[str, Any]) -> str:
    """Create a hash of message content for duplicate detection (matches datamanagerbot logic)."""
    content = (message_dict.get("content") or "")[:500]  # First 500 chars like datamanagerbot
    # Normalize URLs in content so tracking params don't defeat dedupe.
    try:
        content = re.sub(r"https?://\\S+", "<url>", str(content), flags=re.IGNORECASE)
    except Exception:
        content = str(content)
    
    embed_urls = []
    embeds = message_dict.get("embeds") or []
    if isinstance(embeds, list):
        for e in embeds:
            if not isinstance(e, dict):
                continue
            embed_url = e.get("url") or ""
            if embed_url:
                try:
                    embed_url = str(embed_url).split("?", 1)[0]
                except Exception:
                    embed_url = str(embed_url)
                embed_urls.append(embed_url)
            # Also include embed title for better duplicate detection
            embed_title = e.get("title") or ""
            if embed_title:
                embed_urls.append(embed_title[:100])  # First 100 chars of title
    
    attachment_urls = []
    attachments = message_dict.get("attachments") or []
    if isinstance(attachments, list):
        for a in attachments:
            if not isinstance(a, dict):
                continue
            attach_url = a.get("url") or a.get("proxy_url") or ""
            if attach_url:
                try:
                    attach_url = str(attach_url).split("?", 1)[0]
                except Exception:
                    attach_url = str(attach_url)
                attachment_urls.append(attach_url)
    
    # Combine all hashes - use ALL URLs, not just first (matches datamanagerbot)
    # Use deterministic MD5 hash instead of Python's hash() for consistency across runs
    all_text = content + "|".join(sorted(embed_urls)) + "|".join(sorted(attachment_urls))
    combined_hash = hashlib.md5(all_text.encode('utf-8')).hexdigest()
    return str(combined_hash)

def _fetch_full_message(channel_id: int, message_id: str) -> Optional[Dict[str, Any]]:
    """Fetch complete message data from REST API (content + embeds) to get full data when gateway truncates."""
    if not (channel_id and message_id and DISCUM_BOT):
        return None
    # Use v9 for user-token compatibility (v10 can return 403 for some user-token requests)
    url = f"https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}"
    headers = {
        "Authorization": DISCUM_BOT,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    for attempt in range(2):
        try:
            response = requests.get(url, headers=headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if VERBOSE:
                    full_content_len = len(data.get("content", ""))
                    embeds_count = len(data.get("embeds", []))
                    print(f"[DEBUG] Successfully fetched full message {message_id}: content={full_content_len} chars, embeds={embeds_count}")
                return data
            if response.status_code == 429:
                retry_after = response.json().get("retry_after", 1)
                time.sleep(min(float(retry_after), 2.0))
                continue
            if response.status_code in (403, 404):
                if response.status_code == 403:
                    # Actionable: this almost always means the user-token account lacks Read Message History
                    # in the source channel, which causes gateway-only (often truncated) forwards.
                    print(
                        f"[WARN] Cannot fetch full message {message_id} in channel {channel_id}: HTTP 403. "
                        "Grant the Discum user 'Read Message History' (and View Channel) in the monitored source channel "
                        "to eliminate truncation; forwarding will fall back to gateway payload."
                    )
                elif VERBOSE:
                    print(f"[DEBUG] Cannot fetch full message for {message_id}: HTTP 404 (deleted/not found)")
                return None
            # Log other HTTP errors
            if VERBOSE:
                print(f"[WARN] Failed to fetch full message for {message_id}: HTTP {response.status_code}")
        except Exception as e:
            if VERBOSE:
                print(f"[WARN] Exception fetching full message for {message_id}: {e}")
            time.sleep(0.3)
    return None

def _apply_full_message_authoritative(dst: Dict[str, Any], full_message: Dict[str, Any]) -> None:
    """Apply REST-fetched message as the source of truth (used for edits)."""
    if not isinstance(dst, dict) or not isinstance(full_message, dict):
        return
    for k in ("content", "embeds", "attachments", "mentions", "mention_roles", "edited_timestamp"):
        if k in full_message:
            dst[k] = full_message.get(k)

def _fetch_full_message_embeds(channel_id: int, message_id: str) -> Optional[List[Dict[str, Any]]]:
    """Legacy function - kept for compatibility. Use _fetch_full_message instead."""
    full_message = _fetch_full_message(channel_id, message_id)
    if full_message:
        return full_message.get("embeds") or []
    return None

def _maybe_enrich_message(message_dict: Dict[str, Any], channel_id: int, message_id: str, *, authoritative: bool = False) -> None:
    """Enrich message with full content and embeds from REST API when gateway data is incomplete.
    
    Always attempts to fetch full message to fix truncation issues, even if gateway data looks complete.
    """
    try:
        full_message = _fetch_full_message(channel_id, message_id)
        if full_message:
            if authoritative:
                _apply_full_message_authoritative(message_dict, full_message)
                return
            # Always update content if full message has more content (gateway sometimes truncates)
            full_content = full_message.get("content", "")
            current_content = message_dict.get("content", "")
            if len(full_content) > len(current_content):
                message_dict["content"] = full_content
                if VERBOSE:
                    print(f"[DEBUG] Enriched content: {len(current_content)} -> {len(full_content)} chars")
            elif VERBOSE and full_content != current_content:
                print(f"[DEBUG] Content lengths match but may differ: gateway={len(current_content)}, api={len(full_content)}")
            
            # Always update embeds if available (REST API has complete embed data)
            enriched_embeds = full_message.get("embeds") or []
            current_embeds = message_dict.get("embeds") or []
            if enriched_embeds:
                message_dict["embeds"] = enriched_embeds
                if VERBOSE:
                    print(f"[DEBUG] Enriched embeds: {len(current_embeds)} -> {len(enriched_embeds)} embed(s)")
            elif VERBOSE and current_embeds:
                print(f"[DEBUG] No embeds in API response, keeping gateway embeds")
        else:
            # Failed to fetch - log if this is an edited message (more likely to be truncated)
            if message_dict.get("edited_timestamp"):
                if VERBOSE:
                    print(f"[WARN] Could not fetch full content for edited message {message_id} - may be truncated")
    except Exception as e:
        # Re-raise to let caller handle (they'll log appropriately)
        raise

def _maybe_enrich_message_embeds(message_dict: Dict[str, Any], channel_id: int, message_id: str) -> None:
    """Legacy function - now calls _maybe_enrich_message for full enrichment."""
    _maybe_enrich_message(message_dict, channel_id, message_id)

def _should_skip_due_to_duplicate(message_dict: Dict[str, Any], channel_id: int, *, dedupe_scope: Optional[str] = None) -> bool:
    """
    Return True if an equivalent message was forwarded very recently.

    IMPORTANT: We dedupe by destination scope when possible (webhook URL / mapping),
    not only by source channel id, so multiple sources routing into the same destination
    don't double-post the same deal.
    """
    global _RECENT_MESSAGE_HASHES
    try:
        # Create hash key like datamanagerbot: "channel_id:hash"
        content_hash = _hash_message_content_for_duplicate_check(message_dict)
        scope = str(dedupe_scope).strip() if dedupe_scope else str(channel_id)
        if not scope:
            scope = str(channel_id)
        key = f"{scope}:{content_hash}"
        
        now = time.time()
        last = _RECENT_MESSAGE_HASHES.get(key, 0)
        
        if now - last < _DUPLICATE_TTL_SECONDS:
            # Log duplicate detection (matches datamanagerbot format)
            try:
                author = message_dict.get("author", {}) or {}
                if not isinstance(author, dict):
                    author = {}
                author_name = author.get("username") or author.get("name") or "Unknown"
                
                # Try to get channel name (fallback to ID)
                channel_name = f"Channel-{channel_id}"
                try:
                    if hasattr(bot, 'gateway') and bot.gateway and bot.gateway.session:
                        guild_id = message_dict.get("guild_id")
                        if guild_id:
                            guild_data = bot.gateway.session.guild(str(guild_id))
                            if guild_data:
                                channels = None
                                if isinstance(guild_data, dict):
                                    channels = guild_data.get("channels", {})
                                elif hasattr(guild_data, "channels"):
                                    channels = guild_data.channels
                                
                                if isinstance(channels, dict):
                                    channel_data = channels.get(channel_id) or channels.get(str(channel_id))
                                    if channel_data:
                                        if isinstance(channel_data, dict):
                                            channel_name = channel_data.get("name", channel_name)
                                        elif hasattr(channel_data, "name"):
                                            channel_name = channel_data.name
                except Exception:
                    pass
                
                time_since = round(now - last, 1)
                content_preview = (message_dict.get("content") or "")[:80]
                
                embed_urls = []
                embeds = message_dict.get("embeds") or []
                if isinstance(embeds, list):
                    for e in embeds:
                        if isinstance(e, dict):
                            url = e.get("url") or ""
                            if url:
                                embed_urls.append(url)
                
                log_forwarder(
                    f"Duplicate message detected (posted {time_since}s ago)",
                    channel_name=channel_name,
                    user_name=author_name
                )
                if VERBOSE:
                    if content_preview:
                        log_info(f"  Content preview: {content_preview}...")
                    if embed_urls:
                        log_info(f"  Embed URLs: {embed_urls[0][:80]}...")
                    if dedupe_scope:
                        log_info("  Note: Duplicate check prevents same content routed to the SAME destination mapping")
                    else:
                        log_info("  Note: Duplicate check prevents same content in SAME source channel")
            except Exception:
                time_since = round(now - last, 1)
                log_forwarder(f"Duplicate message detected (posted {time_since}s ago)")
            
            return True
        
        # Store this message hash
        _RECENT_MESSAGE_HASHES[key] = now
        
        # Clean old entries (keep only last 1000, like datamanagerbot)
        if len(_RECENT_MESSAGE_HASHES) > 1000:
            cutoff_time = now - _DUPLICATE_TTL_SECONDS
            _RECENT_MESSAGE_HASHES = {k: v for k, v in _RECENT_MESSAGE_HASHES.items() if v > cutoff_time}
        
        return False
    except Exception as e:
        if VERBOSE:
            log_warn(f"Duplicate check error: {e}")
        return False

# Track processed MESSAGE_CREATE IDs to avoid reconnect replays double-posting.
_processed_create_message_ids: set[str] = set()

try:
    write_discum_log({"event": "bridge_start", "bot_type": "discum"})
except Exception:
    pass

# ================= Graceful Exit =================
def sigint_handler(signum, frame):
    print("\n[STOP] Ctrl+C detected. Exiting cleanly.")
    try:
        # Stop heartbeat thread
        _heartbeat_stop_flag.set()
    except Exception:
        pass
    try:
        # Close gateway connection gracefully
        if hasattr(bot, 'gateway') and bot.gateway:
            try:
                bot.gateway.close()
            except Exception:
                pass
    except Exception:
        pass
    try:
        _cleanup_lock_file()
    except Exception:
        pass
    sys.exit(0)
signal.signal(signal.SIGINT, sigint_handler)

# ================= Channel Caching =================
def _fetch_and_cache_channels():
    """Fetch channels from all source guilds and destination guild, save to cache files."""
    import json
    from datetime import datetime, timezone
    
    try:
        # Wait a bit for gateway to be ready
        time.sleep(2)
        
        config_dir = _CONFIG_DIR
        source_cache_path = os.path.join(config_dir, "source_channels.json")
        dest_cache_path = os.path.join(config_dir, "destination_channels.json")
        
        # Fetch source guild channels
        source_guilds_data: List[Dict[str, Any]] = []
        source_guild_ids: List[str] = list(SOURCE_GUILD_IDS or [])
        if not source_guild_ids:
            fallback_gid = str(DISCORD_GUILD_ID or "").strip()
            if fallback_gid:
                source_guild_ids = [fallback_gid]
        
        # Debug: Log how many source guilds we're trying to fetch (destination will be cached after)
        if VERBOSE:
            dest_hint = f" + destination guild {MIRRORWORLD_SERVER}" if MIRRORWORLD_SERVER else ""
            print(f"[INFO] Fetching channels from {len(source_guild_ids)} source guild(s){dest_hint}: {source_guild_ids}")
        
        # Fetch channels from each source guild using REST API (discum session might not have channels loaded)
        for guild_id_str in source_guild_ids:
            try:
                guild_id = int(guild_id_str)
                if VERBOSE:
                    print(f"[INFO] Attempting to fetch channels from source guild {guild_id}")
                
                # Use REST API to fetch guild info and channels (more reliable than gateway session)
                headers = {
                    'Authorization': DISCUM_BOT,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # First get guild info
                guild_resp = requests.get(
                    f'https://discord.com/api/v9/guilds/{guild_id}?with_counts=false',
                    headers=headers,
                    timeout=10
                )
                
                guild_name = f"Guild {guild_id}"
                guild_icon = None
                if guild_resp.status_code == 200:
                    guild_info = guild_resp.json()
                    guild_name = guild_info.get("name", guild_name)
                    guild_icon = guild_info.get("icon")
                
                # Fetch channels via REST API
                channels_resp = requests.get(
                    f'https://discord.com/api/v9/guilds/{guild_id}/channels',
                    headers=headers,
                    timeout=10
                )
                
                if channels_resp.status_code != 200:
                    if VERBOSE:
                        print(f"[WARN] Failed to fetch channels from source guild {guild_id}: HTTP {channels_resp.status_code}")
                    continue
                
                channels_data = channels_resp.json()
                if not isinstance(channels_data, list):
                    if VERBOSE:
                        print(f"[WARN] Unexpected response format from guild {guild_id}")
                    continue
                
                channels = []
                categories = []
                
                # Parse channels from REST API response
                for ch_data in channels_data:
                    if not isinstance(ch_data, dict):
                        continue
                    
                    ch_id = str(ch_data.get("id", ""))
                    ch_type = ch_data.get("type", 0)
                    
                    if ch_type == 4:  # Category
                        categories.append({
                            "id": ch_id,
                            "name": ch_data.get("name", ""),
                            "type": 4,
                            "position": ch_data.get("position", 0)
                        })
                    else:  # Regular channel
                        channels.append({
                            "id": ch_id,
                            "name": ch_data.get("name", ""),
                            "type": ch_type,
                            "parent_id": str(ch_data.get("parent_id", "")) if ch_data.get("parent_id") else None,
                            "position": ch_data.get("position", 0)
                        })
                
                source_guilds_data.append({
                    "guild_id": str(guild_id),
                    "guild_name": guild_name,
                    "guild_icon": guild_icon,
                    "channels": channels,
                    "categories": categories
                })

                if VERBOSE:
                    print(f"[INFO] Cached {len(channels)} channels and {len(categories)} categories from source guild {guild_name}")
            except Exception as e:
                if VERBOSE:
                    print(f"[WARN] Failed to cache channels from source guild {guild_id_str}: {e}")
                try:
                    write_error_log({
                        "scope": "discumbot",
                        "error": f"Failed to cache source guild channels: {str(e)}",
                        "context": {"guild_id": guild_id_str}
                    }, bot_type="discumbot")
                except Exception:
                    pass

        # Save source channels cache
        source_cache = {
            "last_updated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "guilds": source_guilds_data
        }
        with open(source_cache_path, 'w', encoding='utf-8') as f:
            json.dump(source_cache, f, indent=2)
        if VERBOSE:
            try:
                print(f"[SAVE] Source cache written: {os.path.basename(source_cache_path)} (guilds={len(source_guilds_data)})")
            except Exception:
                pass
        
        # Fetch destination guild channels
        dest_channels = []
        dest_categories = []
        dest_guild_id = None
        dest_guild_name = None
        dest_guild_icon = None
        
        if MIRRORWORLD_SERVER:
            try:
                dest_guild_id_str = str(MIRRORWORLD_SERVER).strip()
                dest_guild_id = dest_guild_id_str
                
                # Use REST API to fetch destination guild channels (more reliable than gateway session)
                headers = {
                    'Authorization': DISCUM_BOT,
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                # First get guild info
                guild_resp = requests.get(
                    f'https://discord.com/api/v9/guilds/{dest_guild_id_str}?with_counts=false',
                    headers=headers,
                    timeout=10
                )
                
                dest_guild_name = f"Guild {dest_guild_id_str}"
                if guild_resp.status_code == 200:
                    guild_info = guild_resp.json()
                    dest_guild_name = guild_info.get("name", dest_guild_name)
                    dest_guild_icon = guild_info.get("icon")
                
                # Fetch channels via REST API
                channels_resp = requests.get(
                    f'https://discord.com/api/v9/guilds/{dest_guild_id_str}/channels',
                    headers=headers,
                    timeout=10
                )
                
                if channels_resp.status_code == 200:
                    channels_data = channels_resp.json()
                    if isinstance(channels_data, list):
                        for ch_data in channels_data:
                            if not isinstance(ch_data, dict):
                                continue
                            
                            ch_id = str(ch_data.get("id", ""))
                            ch_type = ch_data.get("type", 0)
                            
                            if ch_type == 4:  # Category
                                dest_categories.append({
                                    "id": ch_id,
                                    "name": ch_data.get("name", ""),
                                    "type": 4,
                                    "position": ch_data.get("position", 0)
                                })
                            else:  # Regular channel
                                dest_channels.append({
                                    "id": ch_id,
                                    "name": ch_data.get("name", ""),
                                    "type": ch_type,
                                    "parent_id": str(ch_data.get("parent_id", "")) if ch_data.get("parent_id") else None,
                                    "position": ch_data.get("position", 0)
                                                                })

                    if VERBOSE:
                        print(f"[INFO] Cached {len(dest_channels)} channels and {len(dest_categories)} categories from destination guild {dest_guild_name}")
                else:
                    if VERBOSE:
                        print(f"[WARN] Could not access destination guild {dest_guild_id_str} for channel caching")
            except Exception as e:
                if VERBOSE:
                    print(f"[WARN] Failed to cache destination guild channels: {e}")
                try:
                    write_error_log({
                        "scope": "discumbot",
                        "error": f"Failed to cache destination guild channels: {str(e)}",
                        "context": {"guild_id": str(MIRRORWORLD_SERVER)}
                    }, bot_type="discumbot")
                except Exception:
                    pass

        # Save destination channels cache
        dest_cache = {
            "last_updated": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "guild_id": dest_guild_id or "",
            "guild_name": dest_guild_name or "",
            "guild_icon": dest_guild_icon,
            "channels": dest_channels,
            "categories": dest_categories
        }
        with open(dest_cache_path, 'w', encoding='utf-8') as f:
            json.dump(dest_cache, f, indent=2)
        if VERBOSE:
            try:
                print(f"[SAVE] Destination cache written: {os.path.basename(dest_cache_path)} (channels={len(dest_channels)}, categories={len(dest_categories)})")
            except Exception:
                pass
        
        # Mark channels as updated in systemlogs.json (needs_sync flag)
        try:
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            write_system_log(
                {
                    "event": "channels_updated",
                    "timestamp": now_iso,
                    "needs_sync": True,
                    "source_guilds_count": len(source_guilds_data),
                    "destination_channels_count": len(dest_channels),
                }
            )
        except Exception:
            pass
        
        print(f"[SUCCESS] Channel cache updated: {len(source_guilds_data)} source guild(s) + 1 destination guild → {len(source_guilds_data)+ (1 if MIRRORWORLD_SERVER else 0)} total guild(s);")
        print(f"          Saved to: {os.path.basename(source_cache_path)} and {os.path.basename(dest_cache_path)}")
        
        try:
            write_discum_log({
                "event": "channels_cached",
                "source_guilds_count": len(source_guilds_data),
                "destination_channels_count": len(dest_channels),
                "bot_type": "discum"
            })
        except Exception:
            pass
            
    except Exception as e:
        print(f"[ERROR] Failed to fetch and cache channels: {e}")
        try:
            write_error_log({
                "scope": "discumbot",
                "error": f"Channel caching failed: {str(e)}",
                "context": {"error_type": type(e).__name__}
            }, bot_type="discumbot")
        except Exception:
            pass

# ================= Logging =================
# Centralized via log_utils.write_log

# ================= Main Event Handler =================
# Store user info globally so we can use it in ready_supplemental
_discum_user_info = {"username": None, "discriminator": None}
_startup_banner_emitted = False
_last_channel_cache_run_at = 0.0

@bot.gateway.command
def ready_handler(resp):
    """Handle READY event to capture user info early."""
    if resp.event.ready:
        try:
            # Try to get user from READY event
            parsed = resp.parsed.auto()
            if isinstance(parsed, dict):
                user_data = parsed.get('user') or parsed.get('d', {}).get('user')
                if user_data:
                    if isinstance(user_data, dict):
                        _discum_user_info["username"] = user_data.get('username')
                        _discum_user_info["discriminator"] = user_data.get('discriminator', '')
                    else:
                        _discum_user_info["username"] = getattr(user_data, 'username', None)
                        _discum_user_info["discriminator"] = getattr(user_data, 'discriminator', '')
        except Exception:
            pass

@bot.gateway.command
def bridge_listener(resp):
    if resp.event.ready_supplemental:
        # Safely get user data (may not be available immediately)
        user = None
        user_display = "Unknown"
        # Method 0: Use cached user info from READY event (most reliable)
        try:
            if _discum_user_info.get("username"):
                username = _discum_user_info["username"]
                discriminator = _discum_user_info.get("discriminator", "")
                user_display = f"{username}#{discriminator}" if discriminator else username
        except Exception:
            pass

        # Method 1: Direct session.user access (can raise KeyError('user') on some discum builds)
        if user_display == "Unknown" and hasattr(bot.gateway, "session") and bot.gateway.session:
            try:
                user = bot.gateway.session.user
                if user:
                    if isinstance(user, dict):
                        username = user.get("username", "Unknown")
                        discriminator = user.get("discriminator", "")
                    else:
                        username = getattr(user, "username", None) or getattr(user, "name", None) or "Unknown"
                        discriminator = getattr(user, "discriminator", "")
                    if username and username != "Unknown":
                        user_display = f"{username}#{discriminator}" if discriminator else username
                        _discum_user_info["username"] = username
                        _discum_user_info["discriminator"] = discriminator
            except Exception as e:
                if VERBOSE:
                    print(f"[DEBUG] Could not get user info from session: {e}")

        # Method 2: Try from parsed ready_supplemental event data
        if user_display == "Unknown":
            try:
                parsed = resp.parsed.auto()
                if isinstance(parsed, dict):
                    user_data = parsed.get("user") or parsed.get("d", {}).get("user")
                    if user_data:
                        if isinstance(user_data, dict):
                            username = user_data.get("username", "Unknown")
                            discriminator = user_data.get("discriminator", "")
                        else:
                            username = getattr(user_data, "username", "Unknown")
                            discriminator = getattr(user_data, "discriminator", "")
                        if username and username != "Unknown":
                            user_display = f"{username}#{discriminator}" if discriminator else username
                            _discum_user_info["username"] = username
                            _discum_user_info["discriminator"] = discriminator
            except Exception:
                pass

        # Method 3: brief wait then retry session.user
        if user_display == "Unknown":
            try:
                import time as _time
                _time.sleep(0.5)
                if hasattr(bot.gateway, "session") and bot.gateway.session:
                    user = bot.gateway.session.user
                    if user:
                        if isinstance(user, dict):
                            username = user.get("username", "Unknown")
                            discriminator = user.get("discriminator", "")
                        else:
                            username = getattr(user, "username", None) or getattr(user, "name", None) or "Unknown"
                            discriminator = getattr(user, "discriminator", "")
                        if username and username != "Unknown":
                            user_display = f"{username}#{discriminator}" if discriminator else username
                            _discum_user_info["username"] = username
                            _discum_user_info["discriminator"] = discriminator
            except Exception:
                pass

        # Method 4: REST fallback (/users/@me) using the user token
        if user_display == "Unknown":
            try:
                headers = {
                    "Authorization": DISCUM_BOT,
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                }
                me = requests.get("https://discord.com/api/v9/users/@me", headers=headers, timeout=10)
                if me.status_code == 200:
                    data = me.json()
                    if isinstance(data, dict):
                        username = data.get("username")
                        discriminator = data.get("discriminator", "")
                        if username:
                            user_display = f"{username}#{discriminator}" if discriminator else username
                            _discum_user_info["username"] = username
                            _discum_user_info["discriminator"] = discriminator
            except Exception as e:
                if VERBOSE:
                    print(f"[DEBUG] Could not get user info from REST /users/@me: {e}")
        
        try:
            write_discum_log({"event": "bot_ready", "user": user_display, "bot_type": "discum"})
        except Exception:
            pass

        # READY_SUPPLEMENTAL can fire again on reconnects. Avoid spamming the full startup banner
        # and avoid re-fetching all channels repeatedly unless a minimum interval has elapsed.
        global _startup_banner_emitted, _last_channel_cache_run_at
        now_ts = time.time()
        try:
            min_interval = float(cfg_get("CHANNEL_CACHE_MIN_INTERVAL_SECONDS", "600") or "600")
        except Exception:
            min_interval = 600.0
        if _startup_banner_emitted and (now_ts - _last_channel_cache_run_at) < min_interval:
            if VERBOSE:
                print("[INFO] Gateway reconnected (READY_SUPPLEMENTAL). Skipping startup banner + channel cache refresh.")
            return
        
        # Fetch and cache channels FIRST (before banner)
        log_info("Fetching and caching channels...")
        _last_channel_cache_run_at = now_ts
        _fetch_and_cache_channels()
        
        # Get monitored channel names for banner
        try:
            _refresh_source_channel_lookup(force=True)
        except Exception:
            pass
        
        # Build channel list for banner
        monitored_channels = []
        for cid in list(CHANNEL_MAP.keys())[:6]:
            try:
                info = _get_source_channel_details(int(cid))
                ch_name = info.get("channel_name", "unknown")
                monitored_channels.append(f"  • {_fmt_channel(ch_name)}")
            except Exception:
                monitored_channels.append(f"  • {_fmt_channel('unknown')}")
        
        # Get source guilds from channel map
        src_guild_ids = list(SOURCE_GUILD_IDS or [])
        if not src_guild_ids:
            fallback = str(DISCORD_GUILD_ID or "").strip()
            if fallback:
                src_guild_ids = _parse_guild_ids(fallback) or [fallback]
        src_guild_count = len(src_guild_ids)
        if src_guild_count == 0:
            src_guild_summary = "(unset)"
        else:
            preview = ", ".join(src_guild_ids[:3])
            if src_guild_count > 3:
                preview = f"{preview}, ... (+{src_guild_count - 3} more)"
            src_guild_summary = preview
        
        # Build unified startup banner lines
        banner_lines = [
            f"Logged in as {_fmt_user(user_display)}",
            "Mode: Pure Forwarder (Source → Webhook)",
            "",
            f"Source Guilds: {_fmt_server(str(src_guild_count))}",
            f"Source Guild IDs: {_fmt_server(str(src_guild_summary))}",
            f"Channel Mappings: {len(CHANNEL_MAP)}",
        ]
        
        if monitored_channels:
            banner_lines.append("")
            banner_lines.append("Monitored Channels:")
            banner_lines.extend(monitored_channels)
            if len(CHANNEL_MAP) > 6:
                banner_lines.append(f"  ... and {len(CHANNEL_MAP) - 6} more")
        elif len(CHANNEL_MAP) == 0:
            banner_lines.append("")
            banner_lines.append(f"{_F.YELLOW}⚠ No channel mappings configured!{_S.RESET_ALL}")
        
        banner_lines.append("")
        banner_lines.append("Status: Ready and listening for messages")
        
        startup_banner("Discord2Discord Bridge v3.4 (Pure Forwarder)", banner_lines)
        _startup_banner_emitted = True
        
        # Startup report dispatch removed.
        # This bot is a pure forwarder and does not depend on external "startup message" schemas.
        
        try:
            write_discum_log({"event": "bridge_listening", "channel_map_count": len(CHANNEL_MAP), "bot_type": "discum"})
        except Exception:
            pass
        
        # Start watchdog heartbeat thread AFTER channel cache is complete
        threading.Thread(target=heartbeat, daemon=True).start()

        # Note: Startup diagnostics are included in the startup payload above

# ================= Channel Event Handlers =================
@bot.gateway.command
def channel_event_handler(resp):
    """Handle Discord channel create/delete/update events and refresh cache."""
    # Early return if this is not a channel event - check resp.raw for event type
    event_type_str = None
    try:
        # In discum, Gateway events expose event type via resp.raw['t']
        if not hasattr(resp, 'raw'):
            return
        if not isinstance(resp.raw, dict):
            return
        event_type_str = resp.raw.get('t')
        if event_type_str not in ('GUILD_CHANNEL_CREATE', 'GUILD_CHANNEL_DELETE', 'GUILD_CHANNEL_UPDATE'):
            return
    except (AttributeError, TypeError, KeyError):
        # Not a channel event or can't determine event type - exit early
        return
    
    # Process channel event (event_type_str is guaranteed to be set here)
    try:
        # Parse the event data
        data = resp.parsed.auto()
        guild_id = data.get("guild_id")
        
        # Check if this guild is one we monitor (source or destination)
        is_monitored = False
        if DISCORD_GUILD_ID and str(guild_id) == str(DISCORD_GUILD_ID):
            is_monitored = True
        if MIRRORWORLD_SERVER and str(guild_id) == str(MIRRORWORLD_SERVER):
            is_monitored = True
        
        if is_monitored:
            if VERBOSE:
                event_type_map = {
                    'GUILD_CHANNEL_CREATE': 'create',
                    'GUILD_CHANNEL_DELETE': 'delete',
                    'GUILD_CHANNEL_UPDATE': 'update'
                }
                event_type = event_type_map.get(event_type_str, 'unknown')
                channel_name = data.get("name", "unknown")
                print(f"[CHANNEL_EVENT] Channel {event_type} detected: #{channel_name} in guild {guild_id}")
            
            # Refresh cache in background thread (non-blocking)
            threading.Thread(target=_fetch_and_cache_channels, daemon=True).start()
    except Exception as e:
        if VERBOSE:
            print(f"[WARN] Error handling channel event: {e}")

# Global flag to stop heartbeat thread on shutdown
_heartbeat_stop_flag = threading.Event()

def heartbeat():
    """Watchdog thread that logs status periodically and detects silent crashes."""
    last_heartbeat_time = time.time()
    consecutive_misses = 0
    while not _heartbeat_stop_flag.is_set():
        try:
            current_time = time.time()
            elapsed = current_time - last_heartbeat_time
            
            # Check if gateway connection is actually open before proceeding
            gateway_status = "unknown"
            connection_open = False
            try:
                if hasattr(bot, 'gateway') and bot.gateway:
                    # Check if WebSocket connection is open
                    if hasattr(bot.gateway, 'ws') and bot.gateway.ws:
                        connection_open = hasattr(bot.gateway.ws, 'sock') and bot.gateway.ws.sock and bot.gateway.ws.sock.connected
                    gateway_status = "connected" if (bot.gateway.session and connection_open) else 'disconnected'
                else:
                    gateway_status = "disconnected"
            except Exception:
                gateway_status = "error"
            
            # Skip logging if connection is closed (prevents WebSocket errors)
            if not connection_open and gateway_status == "disconnected":
                time.sleep(60)
                continue
            
            # Log heartbeat with more details
            try:
                
                heartbeat_data = {
                    "event": "heartbeat",
                    "bot_name": "discumbot",
                    "status": "listening",
                    "channels_monitored": len(CHANNEL_MAP),
                    "bot_type": "discum",
                    "gateway_status": gateway_status,
                    "uptime_seconds": int(elapsed),
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
                }
                write_discum_log(heartbeat_data)
                
                # Print heartbeat to console every 5 minutes
                if int(elapsed) % 300 == 0 or int(elapsed) < 60:
                    print(f"[HEARTBEAT] Listening... (channels: {len(CHANNEL_MAP)}, status: {gateway_status}, uptime: {int(elapsed//60)}m)")
                
                last_heartbeat_time = current_time
                consecutive_misses = 0
                
            except Exception as log_err:
                consecutive_misses += 1
                if VERBOSE or consecutive_misses > 5:
                    print(f"[WARN] Heartbeat log write failed ({consecutive_misses} misses): {log_err}")
            
            time.sleep(60)  # Heartbeat every 60 seconds
            
        except Exception as e:
            consecutive_misses += 1
            if VERBOSE or consecutive_misses > 3:
                print(f"[WARN] Heartbeat thread error ({consecutive_misses} misses): {e}")
                try:
                    write_error_log({
                        "scope": "discumbot",
                        "error": f"Heartbeat thread error: {str(e)}",
                        "context": {"consecutive_misses": consecutive_misses}
                    }, bot_type="discumbot")
                except Exception:
                    pass
            time.sleep(60)  # Continue even if there's an error

# ================= Message Event Handler =================
@bot.gateway.command
def message_handler(resp):
    """Handle MESSAGE_CREATE and MESSAGE_UPDATE events - forward messages from monitored channels.
    
    MESSAGE_UPDATE events occur when messages are edited. These often have truncated content
    in the gateway event, so we always try to fetch the full message via REST API.
    """
    # Check if this is a MESSAGE_CREATE or MESSAGE_UPDATE event
    try:
        # In discum, check event type via resp.raw['t'] or resp.event.message
        event_type = None
        if hasattr(resp, 'raw') and isinstance(resp.raw, dict):
            event_type = resp.raw.get('t')
        
        # Accept both MESSAGE_CREATE and MESSAGE_UPDATE (edited messages)
        if event_type:
            if event_type not in ('MESSAGE_CREATE', 'MESSAGE_UPDATE'):
                return
        elif hasattr(resp, 'event') and hasattr(resp.event, 'message'):
            # Alternative check - resp.event.message exists
            if not resp.event.message:
                return
        else:
            # No way to determine event type - skip
            return
    except Exception as check_err:
        # Silently ignore event type check errors
        return
    
    # Comprehensive error handling for message processing
    try:

        # Parse message with error handling
        try:
            # Check if resp.parsed exists and has auto method
            if not hasattr(resp, 'parsed') or not hasattr(resp.parsed, 'auto'):
                return
            m = resp.parsed.auto()
            # Validate that we got a valid message dict
            if not m or not isinstance(m, dict):
                if VERBOSE:
                    print(f"[WARN] Invalid message format: {type(m)}")
                return
        except AttributeError as attr_err:
            # resp.parsed or auto() doesn't exist - not a message event
            return
        except Exception as parse_err:
            # Only log parsing errors if they're not related to None values
            err_str = str(parse_err).lower()
            if "'nonetype' object has no attribute 'lower'" in err_str:
                # This is a known issue with malformed messages - silently ignore
                return
            if VERBOSE:
                print(f"[WARN] Failed to parse message: {parse_err}")
            try:
                write_error_log({
                    "scope": "discumbot",
                    "error": f"Message parse error: {str(parse_err)}",
                    "context": {"error_type": type(parse_err).__name__}
                }, bot_type="discumbot")
            except Exception:
                pass
            return

        # Apply forwarded-message snapshot payload early so downstream logic (dedupe, SOL, retry)
        # operates on the real content/embeds/attachments when gateway payloads are empty.
        try:
            _apply_forwarded_snapshot_if_present(m)
        except Exception:
            pass

        guildID = m.get("guild_id")
        chan_id = m.get("channel_id")
        message_id_str = str(m.get("id", "")).strip()
        
        # Validate channel_id exists and is valid
        if not chan_id:
            if VERBOSE:
                print(f"[WARN] Message has no channel_id")
            return
            
        try:
            channelID = int(chan_id)
        except (ValueError, TypeError):
            if VERBOSE:
                print(f"[WARN] Invalid channel_id format: {chan_id} (type: {type(chan_id)})")
            return

        # Reload CHANNEL_MAP dynamically (in case it was updated on disk)
        # This allows adding channels without restarting discumbot
        # Only reload every 10 seconds to avoid excessive file I/O
        try:
            import time
            if not hasattr(message_handler, '_last_reload_time'):
                message_handler._last_reload_time = 0
            current_time = time.time()
            if current_time - message_handler._last_reload_time > 10:  # Reload every 10 seconds max
                current_channel_map = load_channel_map(CHANNEL_MAP_PATH)
                if current_channel_map:
                    # Update the global CHANNEL_MAP reference
                    global CHANNEL_MAP
                    CHANNEL_MAP.update(current_channel_map)
                    message_handler._last_reload_time = current_time
        except Exception as reload_err:
            if VERBOSE:
                print(f"[WARN] Failed to reload CHANNEL_MAP: {reload_err}")
        
        # Debug: Check if channel is in CHANNEL_MAP (with type coercion)
        # CHANNEL_MAP keys should be integers, but check both string and int
        channel_in_map = channelID in CHANNEL_MAP or str(channelID) in CHANNEL_MAP
        friendly_channel_name: Optional[str] = None
        friendly_guild_name: Optional[str] = None
        friendly_guild_id: Optional[int] = None
        if channel_in_map:
            source_lookup = _get_source_channel_details(channelID, guildID)
            friendly_channel_name = source_lookup.get("channel_name")
            friendly_guild_name = source_lookup.get("guild_name")
            friendly_guild_id = source_lookup.get("guild_id")
            if VERBOSE:
                channel_segment = f"{friendly_channel_name or channelID}-({channelID})"
                guild_segment = f"{friendly_guild_name or 'Unknown'}-({friendly_guild_id or guildID or 'unknown'})"
                print(f"[INFO] Message detected in monitored {channel_segment} | {guild_segment}")

        # Get message details for backend logging (with error handling)
        username = "Unknown"
        is_webhook = False
        is_monitored = False
        try:
            author = m.get("author") or {}
            if not isinstance(author, dict):
                author = {}
            username = author.get("username") if author.get("username") else 'Unknown'
            # True webhook messages have webhook_id set. Bot-authored app posts should NOT count as webhooks.
            is_webhook = bool(m.get("webhook_id"))
            is_monitored = channel_in_map
        except Exception as e:
            if VERBOSE:
                print(f"[WARN] Failed to process message metadata: {e}")
            # Set defaults if processing fails
            is_monitored = channel_in_map
        
        # Early return if channel is not monitored (skip processing unmonitored channels)
        if not channel_in_map:
            return

        # Relay/noise filter: skip known webhook reposts (e.g. "Rerouter") so one deal forwards once.
        try:
            if is_webhook:
                uname_l = str(username or "").strip().lower()
                if SKIP_WEBHOOK_MESSAGES:
                    if VERBOSE:
                        log_debug(f"Skipping webhook relay message (all webhooks) from {username} in channel {channelID}")
                    return
                if uname_l and uname_l in SKIP_WEBHOOK_USERNAMES:
                    if VERBOSE:
                        log_debug(f"Skipping webhook relay message from {username} in channel {channelID}")
                    return
        except Exception:
            pass
        
        # Log message detection for monitored channels only
        try:
            write_discum_log({
                "event": "message_detected",
                "channel_id": channelID,
                "source_channel_id": channelID,
                "source_channel_name": friendly_channel_name,
                "source_guild_id": friendly_guild_id or guildID,
                "source_guild_name": friendly_guild_name,
                "user": username,
                "is_monitored": True,
                "is_webhook": is_webhook,
                "action": "detected",
                "bot_type": "discum"
            })
        except Exception:
            pass  # Don't crash if logging fails

        # MESSAGE_UPDATE:
        # - During "short embed hydration" window: update the retry queue snapshot only.
        # - Otherwise: sync edits by PATCHing the already-forwarded webhook message (no duplicates).
        if event_type == 'MESSAGE_UPDATE':
            msg_id = message_id_str
            try:
                if msg_id:
                    # Cache richer payload for potential restoration during retry.
                    _cache_message_payload(m)
            except Exception:
                pass
            try:
                if msg_id and msg_id in _message_forward_queue:
                    _queue_short_embed_retry(m, channelID, guildID, username, is_webhook)
                    if VERBOSE:
                        print(f"[DEBUG] MESSAGE_UPDATE received for {msg_id} (short-embed retry: updated queue snapshot)")
                    return
            except Exception:
                pass

            # Try to fetch the full current message by ID (authoritative) so we can rebuild the mirror post.
            try:
                if msg_id:
                    _maybe_enrich_message(m, channelID, msg_id, authoritative=True)
            except Exception:
                pass
            try:
                if msg_id:
                    _cache_message_payload(m)
            except Exception:
                pass

            try:
                _forward_to_webhook(m, channelID, guildID, edit_existing=True)
            except Exception as e:
                if VERBOSE:
                    print(f"[WARN] Edit sync failed for {msg_id}: {e}")
            return

        # CRITICAL: Check for duplicates BEFORE enrichment (on original message content)
        # This must happen before enrichment to catch duplicates reliably
        # Check ALL messages (including webhooks) to prevent echo loops
        # When discumbot forwards via webhook, that webhook message might be in a monitored channel
        # and get forwarded again, creating duplicates - so we MUST check webhooks too
        if channel_in_map:
            webhook_url = (
                CHANNEL_MAP.get(channelID)
                or CHANNEL_MAP.get(str(channelID))
                or ""
            )
            # Prefer destination-channel scope so multiple webhooks targeting the same channel
            # don't double-post the same content.
            dedupe_scope = webhook_url or f"src:{channelID}"
            try:
                meta = _resolve_webhook_destination_metadata(webhook_url) if webhook_url else {}
                dest_cid = meta.get("channel_id") if isinstance(meta, dict) else None
                if dest_cid:
                    dedupe_scope = f"dest:{int(dest_cid)}"
            except Exception:
                pass
            if _should_skip_due_to_duplicate(m, channelID, dedupe_scope=dedupe_scope):
                return  # Skip duplicate (logging already done in function)

        # Create-event replay guard (prevents replays on reconnect).
        if channelID in CHANNEL_MAP:
            msg_id = message_id_str
            if msg_id:
                if msg_id in _processed_create_message_ids:
                    if VERBOSE:
                        log_info(f"Message {msg_id} already processed (create) - skipping duplicate")
                    return
                _processed_create_message_ids.add(msg_id)
                # Keep bounded so long runs don't grow unbounded.
                if len(_processed_create_message_ids) > 50000:
                    try:
                        # Drop arbitrary older items (set has no order; this is fine for a guard)
                        for _ in range(10000):
                            _processed_create_message_ids.pop()
                    except Exception:
                        pass

                # Enrich from REST API (anti-cutoff). For create events, this represents the best
                # available "original" payload we can capture.
                try:
                    _maybe_enrich_message(m, channelID, msg_id)
                except Exception as enrich_err:
                    print(f"[WARN] Message enrichment failed for message {msg_id} in channel {channelID}: {enrich_err}")
                    if VERBOSE:
                        import traceback
                        print(f"[DEBUG] Message enrichment traceback: {traceback.format_exc()}")

                # Cache the canonical original (post-enrichment if available).
                try:
                    _cache_message_payload(m)
                except Exception:
                    pass

        # Check if message is in CHANNEL_MAP (webhook forwarding)
        # Check CHANNEL_MAP with both int and string key (for safety)
        channel_in_map = channelID in CHANNEL_MAP or str(channelID) in CHANNEL_MAP
        if channel_in_map:
            try:
                if VERBOSE:
                    log_d2d(f"Processing message", channel_name=friendly_channel_name or f"Channel-{str(channelID)[-6:]}", dest_channel_name=None)
                # Duplicate check already happened above before enrichment
                
                if _should_retry_short_embed(m):
                    restored = _apply_cached_payload_if_richer(m, reason="short embed detection")
                    if restored and not _should_retry_short_embed(m):
                        _forward_to_webhook(m, channelID, guildID)
                    else:
                        print(f"[INFO] Short embed content detected for message {m.get('id')} - retrying before forward")
                        _queue_short_embed_retry(m, channelID, guildID, username, is_webhook)
                else:
                    _forward_to_webhook(m, channelID, guildID)
            except Exception as e:
                if VERBOSE:
                    print(f"[ERROR] Failed to forward message to webhook: {e}")
                try:
                    write_error_log({
                        "scope": "discumbot",
                        "message_id": str(m.get("id", "unknown")),
                        "error": f"Webhook forward failed: {str(e)}",
                        "context": {
                            "channel_id": channelID,
                            "guild_id": str(guildID) if guildID else None
                        }
                    }, bot_type="discumbot")
                except Exception:
                    pass  # Don't crash if error logging fails
    
        # Classification is disabled for D2D in pure forwarder mode
        if ENABLE_CLASSIFIER and False:
            pass

    except Exception as bridge_err:
        # Catch ALL unhandled exceptions in message handler to prevent crashes
        error_msg = str(bridge_err)
        print(f"[CRITICAL] Unhandled error in message_handler: {error_msg}")
        try:
            write_error_log({
                "scope": "discumbot",
                "error": f"Unhandled message_handler error: {error_msg}",
                "context": {
                    "error_type": type(bridge_err).__name__,
                    "traceback": str(bridge_err)[:500]
                }
            }, bot_type="discumbot")
        except Exception:
            pass
        # Don't re-raise - continue listening for next message
        return

# ================= Logging & Summary Schema =================

def _resolve_webhook_destination_metadata(webhook_url):
    """Return cached webhook metadata (channel id, guild id, webhook name)."""
    if not webhook_url:
        return {}

    now = time.time()
    cached = _WEBHOOK_INFO_CACHE.get(webhook_url)
    if cached and (now - cached.get("cached_at", 0)) < _WEBHOOK_INFO_TTL:
        return cached

    match = re.search(r"/webhooks/(\d+)/([^/?]+)", str(webhook_url))
    if not match:
        return cached or {}

    wh_id, wh_token = match.group(1), match.group(2)
    info_url = f"https://discord.com/api/v10/webhooks/{wh_id}/{wh_token}"

    try:
        resp = requests.get(info_url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            metadata = {
                "channel_id": int(data["channel_id"]) if data.get("channel_id") else None,
                "guild_id": str(data.get("guild_id")) if data.get("guild_id") else None,
                "webhook_name": data.get("name"),
                "cached_at": now,
            }
            _WEBHOOK_INFO_CACHE[webhook_url] = metadata
            return metadata
        if VERBOSE:
            print(f"[WARN] Webhook info lookup returned HTTP {resp.status_code} for {wh_id}")
    except Exception as exc:
        if VERBOSE:
            print(f"[WARN] Webhook info lookup error for {wh_id}: {exc}")

    _WEBHOOK_INFO_CACHE[webhook_url] = {"cached_at": now}
    return _WEBHOOK_INFO_CACHE[webhook_url]

def _build_log_entry(m, channelID, channelName, dest_channel_id, dest_channel_name, username, guildID, content, success, error_msg, webhook_url=None):

    """Build a log entry for console output and operational logging.
    
    This function creates a summary entry for console display and discumlogs.json.
    It does NOT write to Mirror_World_SOL.json (handled by datamanagerbot.py).
    
    Returns a tuple of (log_entry dict, summary string) for console output.
    
    Structure:
    {
        "message_id": str,
        "source_channel_id": int,
        "source_channel_name": str,
        "dest_channel_id": int | None,
        "dest_channel_name": str,
        "user": str,
        "guild_id": str,
        "content": str,
        "link_type": str,
        "event": str,
        "success": bool,
        "summary": str,
        "error": str | None,
        "webhook_url": str | None
    }
    """
    try:
        msg_id_for_summary = str(m.get("id", "unknown"))
    except Exception:
        msg_id_for_summary = "unknown"
    
    status_text = "successfully posted" if success else (error_msg or "failed")
    
    # Ensure dest_channel_name is set properly
    if dest_channel_id is not None:
        resolved_name = _resolve_destination_channel_name(dest_channel_id)
        if not dest_channel_name or dest_channel_name == "Unknown" or dest_channel_name.startswith("Channel "):
            dest_channel_name = resolved_name
    
    source_label = channelName or f"Channel-{channelID}"
    dest_label = dest_channel_name or "Unknown"
    if source_label and not source_label.startswith("#"):
        source_label = f"#{source_label}"
    if dest_label and not dest_label.startswith("#"):
        dest_label = f"#{dest_label}"
    # Build unified summary line
    summary = f"Source {source_label} → {dest_label} | {status_text}"
    
    entry = {
        "message_id": msg_id_for_summary,
        "source_channel_id": channelID,
        "source_channel_name": channelName,
        "dest_channel_id": dest_channel_id,
        "dest_channel_name": dest_channel_name,
        "user": username,
        "guild_id": guildID,
        "content": content or "[embed/attachment]",
        "link_type": ("D2D" if success else 'ERROR'),
        "event": ("webhook_forward" if success else 'error'),
        "success": success,
        "summary": summary,
        "error": (error_msg if not success else None)
    }
    
    if webhook_url:
        entry["webhook_url"] = webhook_url
    
    return entry, summary

# ===== Normalization Helpers =====
def _normalize_tag_name(tag: str) -> str:
    try:
        cleaned = str(tag or "")
        # Strip any leading 'Filtered-' (any case) and convert to UI-friendly form
        if cleaned.lower().startswith("filtered-"):
            cleaned = cleaned[len("Filtered-"):] if cleaned.startswith("Filtered-") else cleaned[len("filtered-"):]
        return cleaned.replace("_", "-").lower()
    except Exception:
        return str(tag or "").lower()

def _apply_forwarded_snapshot_if_present(message_dict: Dict[str, Any]) -> bool:
    """Merge forwarded-message snapshot payload into canonical fields.

    Discord's newer "forward message" feature can produce gateway payloads where
    `content`/`embeds`/`attachments` are empty, but a `message_snapshots` field
    contains the forwarded message data.

    This function mutates `message_dict` in-place, filling missing canonical fields
    from the first snapshot message. Returns True if any field was populated.
    """
    try:
        if not isinstance(message_dict, dict):
            return False

        content = (message_dict.get("content") or "").strip()
        embeds = message_dict.get("embeds") or []
        attachments = message_dict.get("attachments") or []

        snapshots = message_dict.get("message_snapshots") or []
        if not isinstance(snapshots, list) or not snapshots:
            return False

        first = snapshots[0]
        snapshot_msg = None
        if isinstance(first, dict):
            snapshot_msg = first.get("message") if isinstance(first.get("message"), dict) else None
            if snapshot_msg is None and "content" in first:
                # Some payloads may store the message fields directly on the snapshot object.
                snapshot_msg = first

        if not isinstance(snapshot_msg, dict):
            return False

        changed = False

        # Fill ONLY missing canonical fields. Discord often includes link-preview embeds on the outer
        # message while leaving `content` empty; snapshots still hold the original text/links.
        snap_content = snapshot_msg.get("content") or ""
        if (not content) and isinstance(snap_content, str) and snap_content.strip():
            message_dict["content"] = snap_content
            changed = True

        snap_embeds = snapshot_msg.get("embeds") or []
        if (not embeds) and isinstance(snap_embeds, list) and snap_embeds:
            message_dict["embeds"] = snap_embeds
            changed = True

        snap_attachments = snapshot_msg.get("attachments") or []
        if (not attachments) and isinstance(snap_attachments, list) and snap_attachments:
            message_dict["attachments"] = snap_attachments
            changed = True

        return changed
    except Exception:
        return False

def _parse_webhook_url(webhook_url: str) -> Optional[Tuple[str, str]]:
    """Return (webhook_id, webhook_token) from a webhook URL."""
    try:
        m = _WEBHOOK_URL_RE.search(str(webhook_url or ""))
        if not m:
            return None
        return (m.group("id"), m.group("token"))
    except Exception:
        return None


def _webhook_message_endpoint(webhook_url: str, message_id: str) -> Optional[str]:
    parts = _parse_webhook_url(webhook_url)
    if not parts or not message_id:
        return None
    wh_id, wh_token = parts
    return f"https://discord.com/api/v10/webhooks/{wh_id}/{wh_token}/messages/{message_id}"


def _compute_forward_signature(*, msg_text: str, embeds: List[Dict[str, Any]], attachment_urls: List[str]) -> str:
    """Deterministic signature for 'exact same forwarded content' detection."""
    try:
        emb = json.dumps(embeds or [], ensure_ascii=False, default=str, sort_keys=True)
    except Exception:
        emb = str(embeds or [])
    parts = [
        (msg_text or "").strip(),
        emb,
        "|".join(sorted([str(u).strip() for u in (attachment_urls or []) if str(u).strip()])),
    ]
    return hashlib.md5("||".join(parts).encode("utf-8")).hexdigest()


def _webhook_patch_message(
    *,
    webhook_url: str,
    dest_message_id: str,
    content: str,
    embeds: List[Dict[str, Any]],
) -> bool:
    """Edit an existing webhook message (PATCH) so edits don't create duplicates."""
    url = _webhook_message_endpoint(webhook_url, str(dest_message_id or "").strip())
    if not url:
        return False
    payload: Dict[str, Any] = {"embeds": embeds or []}
    if isinstance(content, str) and content.strip():
        payload["content"] = content
    # Never allow mentions on patch edits
    payload["allowed_mentions"] = {"parse": []}
    if "content" not in payload and not payload.get("embeds"):
        payload["content"] = "[Message forwarded from source channel]"
    try:
        r = requests.patch(url, params={"wait": "true"}, json=payload, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False


def _webhook_delete_message(*, webhook_url: str, dest_message_id: str) -> bool:
    """Best-effort delete for replace-on-edit when content/attachments need re-send."""
    url = _webhook_message_endpoint(webhook_url, str(dest_message_id or "").strip())
    if not url:
        return False
    try:
        r = requests.delete(url, timeout=10)
        return r.status_code in (200, 204)
    except Exception:
        return False


def _record_forward_index(*, source_message_id: str, webhook_url: str, dest_ids: List[str], signature: str) -> None:
    try:
        sid = str(source_message_id or "").strip()
        if not sid:
            return
        _FORWARDED_MESSAGE_INDEX[sid] = {
            "webhook": str(webhook_url or ""),
            "dest_ids": [str(x) for x in (dest_ids or []) if str(x).strip()],
            "last_signature": str(signature or ""),
            "updated_at": time.time(),
        }
        # keep index bounded (LRU-ish by updated_at)
        if len(_FORWARDED_MESSAGE_INDEX) > 5000:
            items = sorted(_FORWARDED_MESSAGE_INDEX.items(), key=lambda kv: float(kv[1].get("updated_at", 0)))
            for k, _ in items[:1000]:
                _FORWARDED_MESSAGE_INDEX.pop(k, None)
    except Exception:
        return


def _send_chunked_message_with_embeds(
    webhook: str,
    username: str,
    avatar: Optional[str],
    msg_text: str,
    embeds: List[Dict[str, Any]],
    file_payloads: List[Tuple[str, bytes]],
) -> List[str]:
    """Send a long message by splitting into multiple webhook posts (first chunk keeps embeds).
    
    Splits content intelligently at word boundaries when possible to avoid cutting words in half.
    First chunk includes embeds and attachments, subsequent chunks are text-only.
    """
    if not msg_text:
        return []

    # Split into chunks, trying to break at word boundaries when possible
    chunks: List[str] = []
    max_chunk_size = 2000
    i = 0
    
    while i < len(msg_text):
        chunk_end = min(i + max_chunk_size, len(msg_text))
        
        # If not at the end, try to find a good break point (newline, space, or punctuation)
        if chunk_end < len(msg_text):
            # Look for newline first (best break point)
            newline_pos = msg_text.rfind('\n', i, chunk_end)
            if newline_pos != -1:
                chunk_end = newline_pos + 1
            else:
                # Look for space (word boundary)
                space_pos = msg_text.rfind(' ', i, chunk_end)
                if space_pos != -1 and space_pos > i + 100:  # Don't break too early
                    chunk_end = space_pos + 1
                else:
                    # Look for punctuation
                    for punct in ['. ', '! ', '? ', ', ']:
                        punct_pos = msg_text.rfind(punct, i, chunk_end)
                        if punct_pos != -1 and punct_pos > i + 100:
                            chunk_end = punct_pos + len(punct)
                            break
        
        chunk = msg_text[i:chunk_end].rstrip()
        if chunk:  # Only add non-empty chunks
            chunks.append(chunk)
        i = chunk_end
    
    if not chunks:
        return []

    first_payload: Dict[str, Any] = {
        "username": username,
        "content": chunks[0],
        "embeds": embeds,
    }
    # Never allow mentions on forwarded content
    first_payload["allowed_mentions"] = {"parse": []}
    if avatar and str(avatar).startswith("http"):
        first_payload["avatar_url"] = avatar

    created_ids: List[str] = []
    try:
        execute_url = _webhook_execute_url(webhook)
        request_kwargs: Dict[str, Any] = {}
        if file_payloads:
            request_kwargs["data"] = {"payload_json": json.dumps(first_payload)}
            request_kwargs["files"] = [
                (f"files[{idx}]", (filename, data)) for idx, (filename, data) in enumerate(file_payloads)
            ]
        else:
            request_kwargs["json"] = first_payload

        response = requests.post(execute_url, timeout=10, **request_kwargs)
        if response.status_code not in (200, 204):
            return []
        if response.status_code == 200:
            try:
                mid = str((response.json() or {}).get("id") or "").strip()
                if mid:
                    created_ids.append(mid)
            except Exception:
                pass
            if not created_ids:
                try:
                    m_id = re.search(r'"id"\s*:\s*"(?P<id>\d+)"', response.text or "")
                    if m_id:
                        created_ids.append(m_id.group("id"))
                except Exception:
                    pass

        for chunk in chunks[1:]:
            payload: Dict[str, Any] = {"username": username, "content": chunk, "allowed_mentions": {"parse": []}}
            if avatar and str(avatar).startswith("http"):
                payload["avatar_url"] = avatar
            follow_resp = requests.post(
                execute_url,
                json=payload,
                timeout=10,
            )
            if follow_resp.status_code not in (200, 204):
                return created_ids
            if follow_resp.status_code == 200:
                mid_val = ""
                try:
                    mid_val = str((follow_resp.json() or {}).get("id") or "").strip()
                except Exception:
                    mid_val = ""
                if mid_val:
                    created_ids.append(mid_val)
                else:
                    try:
                        m_id = re.search(r'"id"\s*:\s*"(?P<id>\d+)"', follow_resp.text or "")
                        if m_id:
                            created_ids.append(m_id.group("id"))
                    except Exception:
                        pass

    except Exception:
        return created_ids

    return created_ids

# ================= Embed URL extraction (no "full preview" appends) =================
def _extract_embed_urls(embeds: List[Dict[str, Any]]) -> List[str]:
    """Extract embed URLs for raw-link replacement (no extra preview text)."""
    urls: List[str] = []
    for raw in embeds or []:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "").strip()
        url = str(raw.get("url") or "").strip()

        # Some embed payloads omit `url` but have a provider or author url; prefer canonical `url` when present.
        if not url:
            try:
                author = raw.get("author") or {}
                if isinstance(author, dict):
                    url = str(author.get("url") or "").strip()
            except Exception:
                pass

        if url:
            urls.append(url)

        if not title and not url:
            continue
    return urls

_URL_FINDER = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)

def _maybe_replace_single_content_url_with_embed_url(msg_text: str, embed_urls: List[str]) -> Tuple[str, bool]:
    """
    If the message body contains a single URL and an embed URL exists (often the resolved/raw URL),
    replace the body URL with the embed URL (wrapped in <...> to avoid generating extra embeds).
    """
    if not isinstance(msg_text, str) or not msg_text.strip():
        return (msg_text, False)
    if not embed_urls:
        return (msg_text, False)

    target = None
    for u in embed_urls:
        if isinstance(u, str) and u.startswith("http"):
            target = u.strip()
            break
    if not target:
        return (msg_text, False)

    urls = _URL_FINDER.findall(msg_text)
    if len(urls) != 1:
        return (msg_text, False)

    source_url = urls[0].strip()
    if not source_url or source_url == target or target in msg_text:
        return (msg_text, False)

    replaced = msg_text.replace(source_url, f"<{target}>")
    return (replaced, replaced != msg_text)

# ================= Short Embed Retry Queue =================

def _queue_short_embed_retry(m: Dict[str, Any], channelID: int, guildID, username: str, is_webhook: bool) -> None:
    """Queue only short edited embed messages for a brief retry before forwarding."""
    global _message_forward_queue
    msg_id = str(m.get("id", ""))
    if not msg_id:
        # No message ID - forward immediately
        _forward_to_webhook(m, channelID, guildID)
        return
    
    now = time.time()
    
    # Check if message is already in queue (means it was edited)
    if msg_id in _message_forward_queue:
        # Update existing queue entry with latest message data
        queue_entry = _message_forward_queue[msg_id]
        old_content_len = len(queue_entry["message"].get("content", "") or "")
        new_content_len = len(m.get("content", "") or "")
        queue_entry["message"] = m
        queue_entry["updated_at"] = now
        if VERBOSE:
            print(f"[DEBUG] Updated queued message {msg_id} with edit: content {old_content_len} -> {new_content_len} chars (will retry in {_SHORT_EMBED_RETRY_DELAY}s)")
        else:
            # Always log edits even if not verbose (important for debugging truncation)
            if new_content_len != old_content_len:
                print(f"[INFO] Message {msg_id} edited: {old_content_len} -> {new_content_len} chars (short embed retry pending)")
    else:
        # New message - add to queue
        _message_forward_queue[msg_id] = {
            "message": m,
            "channel_id": channelID,
            "guild_id": guildID,
            "username": username,
            "is_webhook": is_webhook,
            "queued_at": now,
            "updated_at": now
        }
        if VERBOSE:
            print(f"[DEBUG] Short embed detected; retrying message {msg_id} in {_SHORT_EMBED_RETRY_DELAY}s")
        
        # Schedule forwarding after delay
        def _delayed_forward():
            start_ts = time.time()
            time.sleep(_SHORT_EMBED_RETRY_DELAY)
            try:
                if msg_id in _message_forward_queue:
                    # Wait a bit longer for embed hydration updates (MESSAGE_UPDATE) to arrive.
                    # We keep the entry in the queue during this window so updates can replace it.
                    while True:
                        queue_entry = _message_forward_queue.get(msg_id)
                        if not queue_entry:
                            return
                        latest_message = queue_entry.get("message") or {}
                        if not _should_retry_short_embed(latest_message):
                            break
                        if (time.time() - start_ts) >= _SHORT_EMBED_MAX_WAIT_SECONDS:
                            break
                        time.sleep(1.0)

                    # Done waiting; remove from queue and forward the latest snapshot.
                    queue_entry = _message_forward_queue.pop(msg_id, None)
                    if not queue_entry:
                        return
                    latest_message = queue_entry.get("message") or {}
                    
                    # Log content length before enrichment
                    content_before = latest_message.get("content", "")
                    content_len_before = len(content_before) if content_before else 0
                    embeds_before = latest_message.get("embeds", [])
                    embeds_count_before = len(embeds_before) if isinstance(embeds_before, list) else 0
                    
                    # Try restoring from cache before hitting REST API
                    cache_restored = _apply_cached_payload_if_richer(latest_message, reason="short embed retry")
                    if cache_restored:
                        content_before = latest_message.get("content", "")
                        content_len_before = len(content_before) if content_before else 0
                        embeds_before = latest_message.get("embeds", [])
                        embeds_count_before = len(embeds_before) if isinstance(embeds_before, list) else 0
                    
                    # Enrich with full content one more time before forwarding (critical for edited messages)
                    enrichment_success = False
                    try:
                        _maybe_enrich_message(latest_message, queue_entry["channel_id"], msg_id)
                        enrichment_success = True
                    except Exception as enrich_err:
                        # Log enrichment failure (especially important for edited messages)
                        error_str = str(enrich_err)
                        if "403" in error_str or "Forbidden" in error_str:
                            print(f"[WARN] Cannot fetch full content for message {msg_id} (HTTP 403 - no REST API permission)")
                            print(f"[WARN] Forwarding with gateway content only ({content_len_before} chars)")
                        elif VERBOSE:
                            print(f"[DEBUG] Enrichment failed for {msg_id}: {enrich_err}")
                    
                    # Log content length after enrichment
                    content_after = latest_message.get("content", "")
                    content_len_after = len(content_after) if content_after else 0
                    embeds_after = latest_message.get("embeds", [])
                    embeds_count_after = len(embeds_after) if isinstance(embeds_after, list) else 0
                    
                    # Log if content was enriched
                    if enrichment_success and content_len_after > content_len_before:
                        print(f"[INFO] Content enriched: {content_len_before} -> {content_len_after} chars (message {msg_id})")
                    elif VERBOSE:
                        print(f"[DEBUG] Forwarding message {msg_id}: {content_len_after} chars, {embeds_count_after} embed(s)")
                    if content_len_after < _SHORT_EMBED_CHAR_THRESHOLD:
                        print(f"[WARN] Short embed retry exhausted for message {msg_id} ({content_len_after} chars) - forwarding gateway content")
                    
                    # Forward the (possibly updated) message
                    _forward_to_webhook(
                        latest_message,
                        queue_entry["channel_id"],
                        queue_entry["guild_id"]
                    )
                    # Log forwarding
                    try:
                        write_discum_log({
                            "event": "message_detected",
                            "channel_id": queue_entry["channel_id"],
                            "source_channel_id": queue_entry["channel_id"],
                            "user": queue_entry["username"],
                            "is_monitored": True,
                            "is_webhook": queue_entry["is_webhook"],
                            "action": "forwarded",
                            "bot_type": "discum"
                        })
                    except Exception:
                        pass
            except Exception as e:
                if VERBOSE:
                    print(f"[ERROR] Delayed forward failed for {msg_id}: {e}")
                # Clean up queue entry on error
                _message_forward_queue.pop(msg_id, None)
        
        # Start delayed forward in background thread
        threading.Thread(target=_delayed_forward, daemon=True).start()

# ================= Discord2Discord (Discum) Bridge - Webhook Forwarding =================

def _forward_to_webhook(m, channelID, guildID, *, edit_existing: bool = False):
    """Forward message to webhook (original d2d functionality)."""

    try:
        # Safety check - ensure channelID is in CHANNEL_MAP before proceeding
        # Check both int and string keys (CHANNEL_MAP should have int keys, but be safe)
        webhook = CHANNEL_MAP.get(channelID) or CHANNEL_MAP.get(str(channelID))
        if not webhook:
            if VERBOSE:
                try:
                    source_lookup = _get_source_channel_details(channelID)
                    channel_name = source_lookup.get("channel_name") or f"Channel-{str(channelID)[-6:]}"
                except Exception:
                    channel_name = f"Channel-{str(channelID)[-6:]}"
                log_warn(f"Channel {_fmt_channel(channel_name)} not in CHANNEL_MAP, skipping webhook forward")
                log_debug(f"CHANNEL_MAP has {len(CHANNEL_MAP)} entries, keys: {list(CHANNEL_MAP.keys())[:5] if CHANNEL_MAP else 'empty'}")
            return
        
        author = m.get("author", {}) or {}
        if not isinstance(author, dict):
            author = {}
        author_username = author.get("username", "Unknown")
        author_avatar = (
            f"https://cdn.discordapp.com/avatars/{author.get('id')}/{author.get('avatar')}.png"
            if author.get("avatar") else None
        )
        
        # Get guild icon + name for source server identification.
        # IMPORTANT: do not rely only on gateway session metadata (can be missing); fall back to
        # local cache + REST guild fetch so webhook identity is consistently the source server.
        guild_name = None
        guild_icon_url = None
        try:
            ident = _get_guild_identity(guildID, channelID)
            if isinstance(ident, dict):
                guild_name = ident.get("guild_name") or None
                guild_icon_url = ident.get("guild_icon_url") or None
        except Exception:
            pass

        # Canonical forwarding identity: webhook username + avatar reflect the SOURCE SERVER.
        # Preserve original author name in message body/footer.
        username = str(guild_name or f"Server-{str(guildID) if guildID else 'unknown'}")
        avatar = guild_icon_url or None
        
        # Extract canonical fields (and repair forwarded-message payloads when needed)
        content = m.get("content", "")
        attachments = m.get("attachments", [])
        embeds = m.get("embeds", [])

        # If this is a Discord "forwarded message" gateway payload that looks empty,
        # merge snapshot message data into content/embeds/attachments before forwarding.
        try:
            if _apply_forwarded_snapshot_if_present(m):
                content = m.get("content", "") or content
                attachments = m.get("attachments", []) or attachments
                embeds = m.get("embeds", []) or embeds
        except Exception:
            pass
        
        # Log original content length from Discord gateway for debugging
        if VERBOSE and content:
            print(f"[DEBUG] Original content from gateway: {len(content)} chars, preview: '{content[:100] if len(content) > 100 else content}...'")

        try:
            guild_data = bot.gateway.session.guild(guildID)
            if guild_data and "channels" in guild_data and channelID in guild_data["channels"]:
                channel_data = guild_data["channels"][channelID]
                channelName = channel_data.get("name") if channel_data and isinstance(channel_data, dict) else str(channelID)
            else:
                channelName = str(channelID)
            # Try to get friendly channel name from source lookup if available
            try:
                source_lookup = _get_source_channel_details(channelID, guildID)
                friendly_name = source_lookup.get("channel_name")
                if friendly_name and (not channelName or channelName == str(channelID)):
                    channelName = friendly_name
            except Exception:
                pass
        except Exception:
            channelName = str(channelID)
    except Exception as e:
        if VERBOSE:
            log_error(f"Failed to initialize webhook forward: {e}")
        try:
            write_error_log({
                "scope": "discumbot",
                "message_id": str(m.get("id", "unknown")),
                "error": f"Webhook forward initialization failed: {str(e)}",
                "context": {
                    "channel_id": channelID,
                    "guild_id": str(guildID) if guildID else None
                }
            }, bot_type="discumbot")
        except Exception:
            pass
        return  # Exit early if initialization fails
    msg_text = content

    # Rewrite mentions into plain text so the destination guild doesn't show @unknown-role / #unknown-channel.
    try:
        gid_int = int(guildID) if guildID else None
    except Exception:
        gid_int = None
    try:
        if isinstance(msg_text, str) and msg_text:
            msg_text = _sanitize_mentions_for_destination(msg_text, guild_id=gid_int)
    except Exception:
        pass
    
    # Log content length for debugging truncation issues
    if VERBOSE and msg_text:
        content_len = len(msg_text)
        content_preview = msg_text[:100] if content_len > 100 else msg_text
        print(f"[DEBUG] Message content length: {content_len} chars, preview: '{content_preview}...'")

    webhook_meta = _resolve_webhook_destination_metadata(webhook) or {}

    embed_list = []
    for raw_embed in embeds or []:
        try:
            # Deep copy so nested payloads (e.g., forwarded message embeds) stay intact
            normalized = json.loads(json.dumps(raw_embed))
        except (TypeError, ValueError):
            normalized = raw_embed
        # Sanitize mention tokens inside embed text as well
        try:
            _sanitize_mentions_in_obj(normalized, guild_id=gid_int)
        except Exception:
            pass
        embed_list.append(normalized)
    
    # Log embed processing for debugging
    if embed_list and VERBOSE:
        print(f"[DEBUG] Processing {len(embed_list)} embed(s) for message from {author_username}")
        for idx, embed in enumerate(embed_list):
            embed_title = embed.get("title", "")
            embed_title_len = len(embed_title) if embed_title else 0
            embed_title_preview = embed_title[:50] if embed_title else 'No title'
            embed_desc = embed.get("description", "")
            embed_desc_len = len(embed_desc) if embed_desc else 0
            print(f"[DEBUG] Embed {idx+1}: Title length={embed_title_len}, Title='{embed_title_preview}...', Description length={embed_desc_len}")
            # Also log full title if it's longer than 50 chars to see if it's truncated
            if embed_title_len > 50:
                print(f"[DEBUG] Embed {idx+1} full title: {embed_title}")

    # Validate and sanitize payload before sending
    # Discord webhook limits:
    # - Content: 2000 characters max
    # - Username: 80 characters max
    # - Total payload: ~6000 characters
    # - Embed limit: 10 embeds, 6000 chars total
    if len(username) > 80:
        username = username[:77] + "..."
    
    # Validate embeds and add source server footer
    valid_embeds = []
    for embed in embed_list[:10]:
        try:
            # Check embed size (rough validation)
            embed_size = len(str(embed))
            if embed_size > 6000:
                if VERBOSE:
                    print(f"[WARN] Embed too large, skipping")
                continue
            
            # Add source server + author info to footer
            if guild_name or guild_icon_url:
                existing_footer = embed.get("footer", {}) or {}
                footer_text = existing_footer.get("text", "")
                
                # Only add if not already tagged with source server
                if guild_name and "From:" not in footer_text:
                    base = f"From: {guild_name}"
                    if author_username and author_username != "Unknown" and "By:" not in footer_text:
                        base = f"{base} | By: {author_username}"
                    new_footer_text = base + (f" | {footer_text}" if footer_text else "")
                    new_footer = {"text": new_footer_text}
                    if guild_icon_url:
                        new_footer["icon_url"] = guild_icon_url
                    embed["footer"] = new_footer
            
            valid_embeds.append(embed)
        except Exception:
            if VERBOSE:
                print(f"[WARN] Invalid embed format, skipping")
            continue
    
    # Process attachments for webhook payload
    attachment_urls: List[str] = []
    file_payloads: List[Tuple[str, bytes]] = []
    downloaded_urls = set()
    for att in attachments:
        try:
            att_url = att.get("url") or att.get("proxy_url")
            if att_url and att_url.startswith("http"):
                attachment_urls.append(att_url)
                if len(file_payloads) < 10:
                    filename = att.get("filename") or att.get("id")
                    if filename:
                        try:
                            resp = requests.get(att_url, timeout=10)
                            resp.raise_for_status()
                            file_payloads.append((filename, resp.content))
                            downloaded_urls.add(att_url)
                        except Exception as download_err:
                            if VERBOSE:
                                print(f"[WARN] Failed to download attachment {filename}: {download_err}")
        except Exception:
            pass
    
    # CRITICAL: Discord requires at least ONE of: content, embeds, or attachments
    has_content = bool(msg_text and msg_text.strip())
    has_embeds = bool(valid_embeds)
    has_attachments = bool(file_payloads or attachment_urls)
    
    if not has_content and not has_embeds and not has_attachments:
        if VERBOSE:
            log_info(f"Message {m.get('id', 'unknown')} appears empty, adding placeholder content for webhook")
        msg_text = "[Message forwarded from source channel]"
        has_content = True

    # If Discord visually truncates link-preview cards, we DO NOT append "full preview" plaintext.
    # We only do a safe raw-link replacement when the message body contains a single URL.
    try:
        embed_urls = _extract_embed_urls(valid_embeds)
        # Prefer rewriting the single short link in the message body to the embed URL (often the resolved/raw URL).
        # Wrap in <...> to avoid Discord generating a second embed preview.
        if isinstance(msg_text, str) and embed_urls:
            msg_text, changed = _maybe_replace_single_content_url_with_embed_url(msg_text, embed_urls)
            if changed:
                has_content = bool(msg_text and msg_text.strip())
    except Exception:
        pass

    # If we couldn't download some attachments, append URLs to the message body (single post, no spam).
    try:
        leftover_urls = [url for url in attachment_urls if url and url not in downloaded_urls]
        if leftover_urls:
            appendix = "\n".join(leftover_urls[:10])
            if appendix:
                # Keep under 2000 chars if possible (chunking will handle longer anyway)
                msg_text = (str(msg_text or "").rstrip() + "\n\n" + appendix).strip()
                has_content = bool(msg_text and str(msg_text).strip())
    except Exception:
        pass

    signature = ""
    try:
        signature = _compute_forward_signature(msg_text=msg_text, embeds=valid_embeds, attachment_urls=attachment_urls)
    except Exception:
        signature = ""

    # MESSAGE_UPDATE sync: prefer editing the existing forwarded webhook message (no duplicates).
    if edit_existing:
        src_id = str(m.get("id", "")).strip()
        idx = _FORWARDED_MESSAGE_INDEX.get(src_id) if src_id else None
        dest_ids = []
        try:
            dest_ids = [str(x) for x in (idx.get("dest_ids") if isinstance(idx, dict) else []) if str(x).strip()]
        except Exception:
            dest_ids = []
        # Use stored webhook URL if present (more reliable than current map after edits)
        try:
            if isinstance(idx, dict) and idx.get("webhook"):
                webhook = str(idx.get("webhook") or webhook)
        except Exception:
            pass

        if not webhook or not dest_ids:
            if VERBOSE:
                log_warn(f"Edit sync skipped for {src_id}: no destination mapping")
            # MESSAGE_UPDATE can arrive before we have recorded the destination mapping.
            # Queue this update and apply it once the create-forward records dest message id(s).
            try:
                _queue_pending_edit_update(m, channelID, guildID)
            except Exception:
                pass
            return

        try:
            last_sig = str(idx.get("last_signature") or "") if isinstance(idx, dict) else ""
            if signature and signature == last_sig:
                if VERBOSE:
                    log_debug(f"Edit sync no-op for {src_id}: signature unchanged")
                return
        except Exception:
            pass

        # If the new payload requires chunking or files, replace instead of PATCH (keeps content correct).
        needs_replace = bool(file_payloads) or (isinstance(msg_text, str) and len(msg_text) > 2000)
        if not needs_replace:
            ok = _webhook_patch_message(
                webhook_url=webhook,
                dest_message_id=dest_ids[0],
                content=str(msg_text or ""),
                embeds=valid_embeds,
            )
            if ok:
                _record_forward_index(
                    source_message_id=src_id,
                    webhook_url=webhook,
                    dest_ids=dest_ids,
                    signature=signature,
                )
                if VERBOSE:
                    log_d2d("Edited mirror message", channel_name=channelName, dest_channel_name=dest_channel_name)
                return
            # Patch failed → fall back to replace
            needs_replace = True

        if needs_replace:
            for did in dest_ids:
                _webhook_delete_message(webhook_url=webhook, dest_message_id=did)
            # fall through to POST flow to re-send fresh content
    
    # If content > 2000 chars, always chunk (no truncation).
    # First chunk keeps embeds + attachments; remaining chunks are text-only.
    if isinstance(msg_text, str) and len(msg_text) > 2000:
        if valid_embeds:
            log_forwarder(f"Content length {len(msg_text)} chars exceeds 2000 limit with embeds present")
            log_info("  Splitting content into chunks to preserve embeds fully")
        else:
            log_forwarder(f"Content length {len(msg_text)} chars exceeds 2000 limit (no embeds)")
            log_info("  Splitting content into chunks (no truncation)")
        if VERBOSE:
            log_info(f"  Content preview: {msg_text[:80]}...")

        created_ids = _send_chunked_message_with_embeds(
            webhook,
            username,
            avatar,
            msg_text,
            valid_embeds,
            file_payloads,
        )

        if not created_ids:
            log_error("Chunked webhook send failed; skipping send to avoid truncation")
            return
        _record_forward_index(
            source_message_id=str(m.get("id", "")),
            webhook_url=webhook,
            dest_ids=created_ids,
            signature=signature,
        )
        try:
            _flush_pending_edit_update(str(m.get("id", "")).strip())
        except Exception:
            pass

        # Log successful chunked send
        try:
            write_discum_log({
                "event": "webhook_forward_chunked",
                "channel_id": channelID,
                "content_length": len(msg_text),
                "embeds_count": len(valid_embeds),
                "chunks": (len(msg_text) // 2000) + 1,
                "dest_message_ids": created_ids[:10],
                "bot_type": "discum"
            })
        except Exception:
            pass

        # Build log entry for console
        try:
            dest_channel_id = webhook_meta.get("channel_id") if isinstance(webhook_meta, dict) else None
            if dest_channel_id is not None and not isinstance(dest_channel_id, int):
                try:
                    dest_channel_id = int(dest_channel_id)
                except Exception:
                    dest_channel_id = None
            dest_channel_name = None
            if isinstance(webhook_meta, dict) and webhook_meta.get("webhook_name"):
                dest_channel_name = webhook_meta["webhook_name"]
            if dest_channel_id is not None:
                resolved_name = _resolve_destination_channel_name(dest_channel_id)
                dest_channel_name = resolved_name if not dest_channel_name or dest_channel_name.startswith("Channel ") else dest_channel_name
            if not dest_channel_name:
                dest_channel_name = "Unknown"

            log_entry, summary = _build_log_entry(m, channelID, channelName, dest_channel_id, dest_channel_name, username, guildID, content, True, None, webhook)
            timestamp = time.strftime("%H:%M:%S")
            log_d2d(f"{summary} (chunked)", channel_name=channelName, dest_channel_name=dest_channel_name)
        except Exception:
            timestamp = time.strftime("%H:%M:%S")
            log_d2d(f"Message forwarded (chunked)", channel_name=f"Channel-{str(channelID)[-6:]}", dest_channel_name="Webhook")

        return  # Exit early after chunked send
    
    payload = {
        "username": username,
        "embeds": valid_embeds,
    }
    # Never allow mentions on forwarded content (prevents accidental pings)
    payload["allowed_mentions"] = {"parse": []}
    
    if has_content:
        payload["content"] = msg_text
    elif not has_embeds and attachment_urls:
        payload["content"] = "\n".join(attachment_urls[:5])
        if VERBOSE:
            log_info(f"Including {len(attachment_urls)} attachment URL(s) in content")
    
    if avatar and avatar.startswith("http"):
        payload["avatar_url"] = avatar
    
    if "content" not in payload and not payload.get("embeds"):
        if VERBOSE:
            log_warn("Payload validation failed - no content or embeds, skipping send")
        return
    
    multipart_files = []
    if file_payloads:
        for index, (filename, data) in enumerate(file_payloads):
            multipart_files.append((f"files[{index}]", (filename, data)))
    
    execute_url = _webhook_execute_url(webhook)
    request_kwargs: Dict[str, Any] = {}
    if multipart_files:
        request_kwargs["data"] = {"payload_json": json.dumps(payload)}
        request_kwargs["files"] = multipart_files
    else:
        request_kwargs["json"] = payload

    # In-process duplicate guard for direct webhook forwarding (message id based).
    # Helps avoid accidental double posts in the same process (e.g., reconnect quirks).
    # For edit-sync replace mode, we must bypass this or updates within 30s would be blocked.
    if not edit_existing:
        global _recent_forward_ids
        try:
            _recent_forward_ids
        except NameError:
            _recent_forward_ids = {}
        try:
            msg_id_key = str(m.get("id", ""))
            now_ts = time.time()
            # prune old entries (> 30s)
            for k, ts in list(_recent_forward_ids.items()):
                if now_ts - ts > 30:
                    _recent_forward_ids.pop(k, None)
            if msg_id_key:
                if msg_id_key in _recent_forward_ids:
                    if VERBOSE:
                        log_info(f"Duplicate message_id {msg_id_key} within 30s window - skipping webhook forward")
                    return
                _recent_forward_ids[msg_id_key] = now_ts
        except Exception:
            pass

    # Destination channel info (resolved from webhook response or metadata)
    dest_channel_id = webhook_meta.get("channel_id") if isinstance(webhook_meta, dict) else None
    if dest_channel_id is not None and not isinstance(dest_channel_id, int):
        try:
            dest_channel_id = int(dest_channel_id)
        except Exception:
            dest_channel_id = None
    dest_channel_name = None
    if isinstance(webhook_meta, dict) and webhook_meta.get("webhook_name"):
        dest_channel_name = webhook_meta["webhook_name"]
    if dest_channel_id is not None:
        resolved_name = _resolve_destination_channel_name(dest_channel_id)
        dest_channel_name = resolved_name if not dest_channel_name or dest_channel_name.startswith("Channel ") else dest_channel_name
    if not dest_channel_name:
        dest_channel_name = "Unknown"

    message_id = None
    post_status_code: Optional[int] = None
    success = False
    error_msg = None
    max_webhook_retries = 3
    webhook_retry_count = 0

    while webhook_retry_count < max_webhook_retries:
        try:
            r = requests.post(execute_url, timeout=10, **request_kwargs)
            post_status_code = r.status_code
            # Discord webhooks can return 200 (with message data) or 204 (no content) for success
            if r.status_code in [200, 204]:
                success = True
                # Extract message ID from webhook response (only if status is 200)
                if r.status_code == 200:
                    try:
                        response_data = r.json()
                        message_id = str(response_data.get("id") or "").strip() or None
                        # Capture destination channel from response when available
                        cid = response_data.get("channel_id")
                        if cid:
                            try:
                                dest_channel_id = int(cid)
                            except Exception:
                                dest_channel_id = None
                        if dest_channel_id is not None:
                            dest_channel_name = _resolve_destination_channel_name(dest_channel_id)
                    except:
                        pass
                    if not message_id:
                        try:
                            m_id = re.search(r'"id"\s*:\s*"(?P<id>\d+)"', r.text or "")
                            if m_id:
                                message_id = m_id.group("id")
                        except Exception:
                            pass
                if VERBOSE:
                    log_debug(f"Webhook POST status={r.status_code} message_id={'yes' if message_id else 'no'} for src={m.get('id')}")

                # Success - break out of retry loop
                break
            else:
                # Non-200/204 response - get detailed error info
                error_msg = f"HTTP {r.status_code}"

                try:
                    error_detail = r.text[:200] if r.text else 'No error details'
                    if VERBOSE:
                        log_webhook(f"Error: {error_detail}", channel_name=channelName, user_name=username, status_code=r.status_code)
                    
                    # Log HTTP 400 errors with detailed diagnostics
                    if r.status_code == 400:
                        log_error(f"Webhook Bad Request (400) for {_fmt_channel(channelName)}")
                        log_info(f"  Username: {_fmt_user(username[:50])} ({len(username)} chars)")
                        log_info(f"  Content length: {len(msg_text)} chars")
                        log_info(f"  Embeds: {len(valid_embeds)}")
                        log_info(f"  Error details: {error_detail}")
                        
                        # Check for common issues
                        if len(msg_text) > 2000:
                            log_warn("  ⚠️ Content exceeds 2000 character limit!")
                        if len(username) > 80:
                            log_warn("  ⚠️ Username exceeds 80 character limit!")
                        if len(valid_embeds) > 10:
                            log_warn("  ⚠️ Too many embeds (limit: 10)!")
                        
                        # Log to systemlogs.json
                        try:
                            write_error_log({
                                "scope": "discumbot",
                                "error": f"Webhook HTTP 400 Bad Request",
                                "context": {
                                    "channel_id": str(channelID),
                                    "channel_name": channelName,
                                    "username": username[:50],
                                    "content_length": len(msg_text),
                                    "embeds_count": len(valid_embeds),
                                    "error_detail": error_detail[:200]
                                }
                            }, bot_type="discumbot")
                        except Exception:
                            pass
                except Exception as e:
                    if VERBOSE:
                        log_webhook(f"Failed to parse error: {e}", channel_name=channelName, user_name=username, status_code=r.status_code)
                
                # Non-200/204 response - break and log as error
                break
        except (requests.Timeout, requests.ConnectionError, requests.RequestException) as req_exc:
            webhook_retry_count += 1
            if webhook_retry_count >= max_webhook_retries:
                error_msg = f"Webhook request failed after {max_webhook_retries} retries: {str(req_exc)}"
                print(f"[ERROR] Failed to send via webhook: {error_msg}")
                # Log final error
                try:
                    write_error_log({
                        "scope": "discumbot",
                        "message_id": str(m.get("id", "unknown")),
                        "error": error_msg,
                        "context": {
                            "channel_id": channelID,
                            "guild_id": str(guildID) if guildID else None,
                            "webhook_url": webhook[:50] + "..." if webhook and len(webhook) > 50 else webhook,
                            "retry_count": webhook_retry_count
                        }
                    }, bot_type="discumbot")
                except Exception:
                    pass
                break  # Give up after max retries
            else:
                # Retry with exponential backoff
                delay = min(2 ** webhook_retry_count, 10)  # Max 10 seconds
                if VERBOSE:
                    log_webhook(f"Retry attempt {webhook_retry_count}/{max_webhook_retries}, waiting {delay}s...", channel_name=channelName, user_name=username)
                time.sleep(delay)
                continue  # Retry the webhook request
        except Exception as e:
            error_msg = str(e)
            log_error(f"Failed to send via webhook: {e}")

            # Log error to systemlogs.json
            try:
                write_error_log({
                    "scope": "discumbot",
                    "message_id": str(m.get("id", "unknown")),
                    "error": f"Webhook send failed: {str(e)}",
                    "context": {
                        "channel_id": channelID,
                        "guild_id": str(guildID) if guildID else None,
                        "webhook_url": webhook[:50] + "..." if webhook and len(webhook) > 50 else webhook
                    }
                }, bot_type="discumbot")
            except Exception:
                pass
            # Break out of retry loop on unexpected exception
            break

    # Fallback: refresh webhook metadata if destination id was not resolved yet
    if dest_channel_id is None:
        refreshed_meta = _resolve_webhook_destination_metadata(webhook)
        if isinstance(refreshed_meta, dict):
            channel_candidate = refreshed_meta.get("channel_id")
            if channel_candidate:
                try:
                    dest_channel_id = int(channel_candidate)
                except Exception:
                    dest_channel_id = None
            if (not dest_channel_name or dest_channel_name == "Unknown") and refreshed_meta.get("webhook_name"):
                dest_channel_name = refreshed_meta["webhook_name"]

    if dest_channel_id is not None:
        dest_channel_name = _resolve_destination_channel_name(dest_channel_id)
    elif dest_channel_name == "Unknown":
        dest_channel_name = "Unknown"

    if dest_channel_id is not None:
        cache_entry = _WEBHOOK_INFO_CACHE.setdefault(webhook, {"cached_at": time.time()})
        cache_entry["channel_id"] = dest_channel_id
        cache_entry["cached_at"] = time.time()
        if dest_channel_name and dest_channel_name != "Unknown":
            cache_entry["webhook_name"] = dest_channel_name
    elif VERBOSE:
        wh_preview_match = re.search(r"/webhooks/(\d+)", webhook)
        wh_preview = wh_preview_match.group(1) if wh_preview_match else 'unknown'
        print(f"[WARN] Unable to resolve destination channel for webhook {wh_preview}; summary will use #Unknown")
    

    # Log webhook forwarding attempt to bot logs (with error handling)
    try:
        write_discum_log({
        "event": "webhook_forward",
        "channel_id": channelID,
        "webhook_url": webhook[:50] + "..." if len(webhook) > 50 else webhook,
        "success": success,
            "status_code": post_status_code,
            "dest_message_id": message_id,

            "error": error_msg,
            "bot_type": "discum"
    })

    except Exception as log_err:
        if VERBOSE:
            print(f"[WARN] Failed to write webhook forward log: {log_err}")

    # Build log entry for console output (no SOL write - datamanagerbot.py handles SOL archiving)
    try:
        log_entry, summary = _build_log_entry(m, channelID, channelName, dest_channel_id, dest_channel_name, username, guildID, content, success, error_msg, webhook)
        
        # Console output with timestamp
        log_d2d(summary, channel_name=channelName, dest_channel_name=dest_channel_name)
    except Exception as log_err:
        if VERBOSE:
            log_warn(f"Failed to build log entry: {log_err}")
        # Fallback simple output
        status_text = "successfully forwarded" if success else (error_msg or "failed")
        log_d2d(f"Message {status_text}", channel_name=f"Channel-{str(channelID)[-6:]}", dest_channel_name="Webhook")

    # Handle attachments that couldn't be downloaded (fallback to posting URLs)
    # Record forward index for edit-sync (only when we have a destination message id).
    try:
        src_id = str(m.get("id", "")).strip()
        if src_id and message_id:
            _record_forward_index(
                source_message_id=src_id,
                webhook_url=webhook,
                dest_ids=[str(message_id)],
                signature=signature,
            )
            _flush_pending_edit_update(src_id)
    except Exception:
        pass

    # Post-send hydration pass: if the embed/title was incomplete on first delivery, PATCH later.
    # Only schedule for initial posts (never for edit sync) and only when there are embeds/attachments.
    try:
        if success and (not edit_existing):
            src_id = str(m.get("id", "")).strip()
            has_embeds_or_attachments = bool((embeds or []) or (attachments or []))
            if src_id and has_embeds_or_attachments:
                try:
                    _schedule_hydration_edit(source_message_id=src_id, channel_id=int(channelID), guild_id=int(guildID))
                except Exception:
                    pass
    except Exception:
        pass

# Note: classified forwarding intentionally removed in pure forwarder mode.
# ================= Runtime Loop (Auto-restart on Socket Error) =================
if __name__ == "__main__":
    # Startup banners are printed above; avoid duplicate [START] lines here.
    try:
        write_discum_log({"event": "bridge_start", "bot_type": "discum"})
    except Exception:
        pass

    # Start slash command bot in background so /discum browse is registered (same process)
    _cmd_bot_started = False
    try:
        import discum_command_bot as _cmd_module
        _cmd_token = getattr(_cmd_module, "BOT_TOKEN", None) or ""
        if _cmd_token and not _cmd_bot_started:
            import threading
            import asyncio as _asyncio
            def _run_cmd_bot():
                try:
                    _asyncio.run(_cmd_module.bot.start(_cmd_token))
                except Exception as _e:
                    if VERBOSE:
                        print(f"[WARN] Slash command bot stopped: {_e}")
            _cmd_thread = threading.Thread(target=_run_cmd_bot, daemon=True)
            _cmd_thread.start()
            _cmd_bot_started = True
            print("[INFO] Slash command bot started (/discum browse will be available once synced).")
        elif not _cmd_token and VERBOSE:
            print("[INFO] No DISCORD_BOT_TOKEN/BOT_TOKEN in config - /discum browse not registered. Add a bot token to config/tokens.env to enable it.")
    except Exception as _e:
        if VERBOSE:
            print(f"[WARN] Could not start slash command bot: {_e}")

    max_restarts = 999999  # Essentially unlimited restarts
    restart_count = 0
    restart_delay = 5

    while restart_count < max_restarts:
        try:
            # discum gateway for USER ACCOUNT TOKEN (selfbot) - uses WebSocket similar to bots
            # auto_reconnect=True is built-in, but outer retry loop adds extra robustness
            print(f"[INFO] Starting gateway (attempt {restart_count + 1})...")
            bot.gateway.run(auto_reconnect=True)

            # If gateway.run() returns normally (uncommon), break
            print("[INFO] Gateway returned normally - restarting...")
            restart_count += 1
            time.sleep(5)  # Brief delay before restart
            continue
        except KeyboardInterrupt:
            print("[STOP] Manually terminated.")
            print("[SHUTDOWN] DiscumBot is shutting down...")
            
            # Stop heartbeat thread
            try:
                _heartbeat_stop_flag.set()
            except Exception:
                pass
            
            # Close gateway gracefully
            try:
                if hasattr(bot, 'gateway') and bot.gateway:
                    bot.gateway.close()
            except Exception:
                pass

            try:
                from datetime import datetime, timezone
                write_discum_log({
                    "event": "bot_shutdown",
                    "reason": "manual",
                    "bot_type": "discum",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message": "Bot is shutting down"
                })
            except Exception:
                pass
            sys.exit(0)
        except Exception as e:

            restart_count += 1
            err = str(e).lower()
            err_type = type(e).__name__

            print(f"[ERROR] Error (restart {restart_count}): {e}")
            try:
                write_error_log({
                    "scope": "discumbot",
                    "error": f"Gateway error: {str(e)}",
                    "context": {
                        "restart_count": restart_count,
                        "error_type": err_type
                    }
                }, bot_type="discumbot")
            except Exception:
                pass
            
            # Check for WebSocket connection closed errors (typically means token is in use elsewhere)
            if ("connection is already closed" in err or 
                "websocketconnectionclosed" in err_type.lower() or
                "connection already closed" in err):
                print("[ERROR] WebSocket connection closed - another instance may be running, or Discord dropped the socket.")
                print("[ERROR] If you just restarted, use run_discumbot_restart.bat to kill stale instances, then start again.")
                
                # If not confirmed in use, wait longer before retry (may be temporary Discord issue)
                delay = min(30 * restart_count, 120)  # Longer delay for connection closed
                print(f"[RETRY] Waiting {delay:.1f} seconds before retry...")
                time.sleep(delay)
                continue
            
            if "socket is already opened" in err:
                print("[WARN] Socket already opened, retrying in 5 seconds...")

                try:
                    write_discum_log({"event": "socket_restart", "error": err, "bot_type": "discum"})
                except Exception:
                    pass
                time.sleep(5)
                continue

            elif "connection" in err or "timeout" in err or "network" in err:
                # Network-related errors - shorter delay
                delay = min(restart_delay * (1.5 ** min(restart_count, 5)), 60)
                print(f"[RETRY] Network error, retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                continue
            else:

                # Other errors - longer delay
                delay = min(10 * restart_count, 120)  # Max 2 minutes
                print(f"[RETRY] Unexpected error, retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                continue
