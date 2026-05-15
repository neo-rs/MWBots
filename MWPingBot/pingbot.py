"""
MWPingBot (Standalone Pinger)
-----------------------------
Single responsibility:
  - Listen in the MirrorWorld destination server
  - If a message is posted in configured ping channels, send @everyone
  - Enforce per-channel cooldown + content dedupe to prevent spam
  - Register /ping slash command (same process) to manage settings in Discord

Config (standalone, local-only):
  - config/tokens.env     (secrets only)
  - config/settings.json  (non-secret)

Outputs (standalone, local-only):
  - logs/Botlogs/pingbotlogs.json   (JSONL)
  - config/systemlogs.json          (JSON array)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import platform
import re as _re
import sys
import threading
import time
import warnings
import builtins as _builtins
import importlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import discord
from discord.ext import commands

try:
    from dm_notify_embeds import send_ping_dm_notifications
except ImportError:
    send_ping_dm_notifications = None  # type: ignore[misc, assignment]


# ---------------- Console / runtime helpers ----------------

warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*GIL.*")

if platform.system().lower().startswith("win"):
    try:
        if sys.stdout.encoding != "utf-8":
            import codecs

            sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
            sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")
    except Exception:
        pass

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_LOGS_DIR = _BOT_DIR / "logs"

_TOKENS_ENV_PATH = _CONFIG_DIR / "tokens.env"
_SETTINGS_JSON_PATH = _CONFIG_DIR / "settings.json"

_PINGBOT_LOGS_PATH = _LOGS_DIR / "Botlogs" / "pingbotlogs.json"
_SYSTEM_LOGS_PATH = _CONFIG_DIR / "systemlogs.json"
_CONSOLE_LOCK = threading.RLock()

# ---------------- Discum-style console colors ----------------
try:
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
        def __getattr__(self, k):  # noqa: D401
            return ""
    _F = _S = _Dummy()

_ANSI_ESC = "\x1b["


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
    s = _re.sub(r"^\[PING\]", f"{_F.MAGENTA}[PING]{_S.RESET_ALL}", s)
    # Channel-like tokens (#channel-name) to blue
    s = _re.sub(
        r"(?P<prefix>\s|^)#([a-z0-9\-_]+)",
        lambda m: f"{m.group('prefix')}{_F.BLUE}#{m.group(2)}{_S.RESET_ALL}",
        s,
        flags=_re.IGNORECASE,
    )
    # User-like tokens (@name) to magenta (avoid emails)
    s = _re.sub(
        r"(?P<prefix>\s|^)@([A-Za-z0-9_][A-Za-z0-9_\.\-]{1,30})",
        lambda m: f"{m.group('prefix')}{_F.MAGENTA}@{m.group(2)}{_S.RESET_ALL}",
        s,
    )
    return s


def _print_colorized(*args, **kwargs):
    try:
        with _CONSOLE_LOCK:
            if not args:
                return _builtins.print(*args, **kwargs)
            text = " ".join(str(a) for a in args)
            # ASCII-safe replacements for Windows consoles
            text = (
                text.replace("→", "->")
                .replace("←", "<-")
                .replace("↔", "<->")
                .replace("•", "*")
                .replace("✓", "[OK]")
                .replace("✗", "[X]")
            )
            try:
                _builtins.print(_colorize_line(text), **kwargs)
            except UnicodeEncodeError:
                safe_text = text.encode("ascii", errors="replace").decode("ascii")
                _builtins.print(_colorize_line(safe_text), **kwargs)
    except Exception:
        _builtins.print(*args, **kwargs)


# Intercept raw prints in this module to apply colors to legacy lines
print = _print_colorized  # type: ignore


def _ensure_parent_dir(p: Path) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _append_json_line(path: Path, entry: Dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    try:
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass

try:
    _PINGBOT_LOG_MAX_LINES = max(1, int(float(os.getenv("PINGBOT_LOG_MAX_LINES", "1000") or "1000")))
except Exception:
    _PINGBOT_LOG_MAX_LINES = 1000

try:
    _PINGBOT_LOG_TRIM_CHECK_EVERY_WRITES = max(1, int(float(os.getenv("PINGBOT_LOG_TRIM_CHECK_EVERY_WRITES", "50") or "50")))
except Exception:
    _PINGBOT_LOG_TRIM_CHECK_EVERY_WRITES = 50

_pingbot_log_write_count = 0

def _tail_jsonl_lines_bytes(path: Path, max_lines: int) -> tuple[List[str], bool]:
    """Efficiently grab the last `max_lines` JSONL lines by reading from EOF in chunks.

    Returns (lines, truncated) where `truncated` means the file had more than `max_lines` lines.
    """
    max_lines = max(1, int(max_lines or 1))
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size <= 0:
                return ([], False)

            chunk_size = 64 * 1024
            pos = file_size
            data = b""
            nl_count = 0

            while pos > 0 and nl_count <= max_lines:
                read_size = min(chunk_size, pos)
                pos -= read_size
                f.seek(pos)
                chunk = f.read(read_size)
                data = chunk + data
                nl_count = data.count(b"\n")
                if pos <= 0:
                    break

            lines = data.splitlines()
            truncated = nl_count > max_lines
            if len(lines) > max_lines:
                lines = lines[-max_lines:]
            return ([ln.decode("utf-8", errors="ignore") for ln in lines], truncated)
    except Exception:
        return ([], False)

def _trim_jsonl_file_to_last_lines(path: Path, max_lines: int) -> None:
    """Hard cap a JSONL file to its last `max_lines` entries."""
    try:
        lines, truncated = _tail_jsonl_lines_bytes(path, max_lines)
        if not truncated or not lines:
            return
        tmp = Path(str(path) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for ln in lines:
                f.write(ln)
                f.write("\n")
        os.replace(str(tmp), str(path))
    except Exception:
        pass


def write_pingbot_log(entry: Dict[str, Any]) -> None:
    global _pingbot_log_write_count
    _append_json_line(_PINGBOT_LOGS_PATH, entry)
    _pingbot_log_write_count += 1
    if _pingbot_log_write_count % _PINGBOT_LOG_TRIM_CHECK_EVERY_WRITES != 0:
        return
    try:
        if _PINGBOT_LOGS_PATH.exists():
            _trim_jsonl_file_to_last_lines(_PINGBOT_LOGS_PATH, _PINGBOT_LOG_MAX_LINES)
    except Exception:
        pass


def write_system_log(entry: Dict[str, Any]) -> None:
    _ensure_parent_dir(_SYSTEM_LOGS_PATH)
    try:
        entry = dict(entry)
        if "timestamp" not in entry:
            entry["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        logs: List[Dict[str, Any]] = []
        try:
            if _SYSTEM_LOGS_PATH.exists():
                with open(_SYSTEM_LOGS_PATH, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    if isinstance(loaded, list):
                        logs = loaded
        except Exception:
            logs = []
        logs.append(entry)
        logs = logs[-500:]
        tmp = Path(str(_SYSTEM_LOGS_PATH) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(logs, f, indent=2, ensure_ascii=False)
        try:
            os.replace(str(tmp), str(_SYSTEM_LOGS_PATH))
        except Exception:
            with open(_SYSTEM_LOGS_PATH, "w", encoding="utf-8") as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


# ---------------- Config loading (standalone) ----------------

def _load_env_file(path: Path) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or line.startswith("-") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key:
                    out[key] = val
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
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


_TOKENS = _load_env_file(_TOKENS_ENV_PATH)
_SETTINGS = _load_settings_json(_SETTINGS_JSON_PATH)


def _get_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


VERBOSE: bool = _get_bool(_SETTINGS.get("verbose"), True)


def _setup_console_logging() -> None:
    """
    Keep discord.py internal logs quiet so our Discum-style console output is consistent.
    """
    try:
        logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler(sys.stdout)], force=True)
    except Exception:
        try:
            logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler(sys.stdout)])
        except Exception:
            pass
    try:
        logging.getLogger("discord").setLevel(logging.WARNING)
    except Exception:
        pass


_setup_console_logging()

def _fmt_channel(bot_obj: commands.Bot, channel_id: int) -> str:
    """Journal-friendly: use <#id> so Discord renders a clickable channel link."""
    return f"<#{int(channel_id)}>"


def log_debug(msg: str) -> None:
    if not VERBOSE:
        return
    print(f"{_F.WHITE}[DEBUG]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    try:
        write_pingbot_log({"event": "debug", "message": msg, "bot_type": "pingbot"})
    except Exception:
        pass


def log_info(msg: str) -> None:
    print(f"{_F.GREEN}[INFO]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    try:
        write_pingbot_log({"event": "info", "message": msg, "bot_type": "pingbot"})
    except Exception:
        pass


def log_warn(msg: str) -> None:
    print(f"{_F.YELLOW}[WARN]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    try:
        write_pingbot_log({"event": "warn", "message": msg, "bot_type": "pingbot"})
    except Exception:
        pass


def log_error(msg: str, error: Optional[BaseException] = None) -> None:
    print(f"{_F.RED}[ERROR]{_S.RESET_ALL} {_F.WHITE}{msg}{_S.RESET_ALL}", flush=True)
    try:
        write_system_log(
            {
                "event": "error",
                "scope": "pingbot",
                "message": msg,
                "error_type": type(error).__name__ if error else None,
                "error": str(error) if error else None,
            }
        )
    except Exception:
        pass


def log_ping(msg: str, *, channel_label: Optional[str] = None) -> None:
    """Discum-style tag log for ping actions."""
    try:
        context = f" ({channel_label})" if channel_label else ""
        print(f"{_F.MAGENTA}[PING]{_S.RESET_ALL} {_F.WHITE}{msg}{context}{_S.RESET_ALL}", flush=True)
    except Exception:
        try:
            print(f"[PING] {msg}", flush=True)
        except Exception:
            pass


def startup_banner(lines: List[str]) -> None:
    bar = "=" * 55
    with _CONSOLE_LOCK:
        print(_F.WHITE + bar + _S.RESET_ALL, flush=True)
        print(f"{_F.GREEN}[START]{_S.RESET_ALL} {_F.WHITE}MWPingBot (Standalone Pinger){_S.RESET_ALL}", flush=True)
        for line in lines:
            print(f"{_F.WHITE}{line}{_S.RESET_ALL}", flush=True)
        print(_F.WHITE + bar + _S.RESET_ALL, flush=True)


MIRRORWORLD_SERVER_ID: str = str(_SETTINGS.get("mirrorworld_server_id") or "").strip()

try:
    COOLDOWN_SECONDS = int(float(_SETTINGS.get("cooldown_seconds", 30)))
except Exception:
    COOLDOWN_SECONDS = 30
if COOLDOWN_SECONDS < 0:
    COOLDOWN_SECONDS = 0

try:
    DEDUPE_TTL_SECONDS = int(float(_SETTINGS.get("dedupe_ttl_seconds", COOLDOWN_SECONDS)))
except Exception:
    DEDUPE_TTL_SECONDS = COOLDOWN_SECONDS
if DEDUPE_TTL_SECONDS < 0:
    DEDUPE_TTL_SECONDS = 0

PING_CHANNEL_IDS: List[int] = []
try:
    raw = _SETTINGS.get("ping_channel_ids") or []
    if isinstance(raw, str):
        raw_list = [x.strip() for x in raw.split(",") if x.strip()]
    elif isinstance(raw, list):
        raw_list = raw
    else:
        raw_list = []
    for x in raw_list:
        try:
            PING_CHANNEL_IDS.append(int(str(x).strip()))
        except Exception:
            continue
except Exception:
    PING_CHANNEL_IDS = []

PING_BOT_TOKEN: str = str(_TOKENS.get("PING_BOT") or "").strip()

DM_NOTIFY_USER_IDS: List[int] = []
try:
    raw_dm = _SETTINGS.get("dm_notify_user_ids") or []
    if isinstance(raw_dm, str):
        raw_dm_list = [x.strip() for x in raw_dm.split(",") if x.strip()]
    elif isinstance(raw_dm, list):
        raw_dm_list = raw_dm
    else:
        raw_dm_list = []
    for x in raw_dm_list:
        try:
            DM_NOTIFY_USER_IDS.append(int(str(x).strip()))
        except Exception:
            continue
except Exception:
    DM_NOTIFY_USER_IDS = []


# ---------------- Canonical ping state ----------------

_PING_CHANNEL_SET = set(PING_CHANNEL_IDS)
_channel_cooldowns: Dict[int, float] = {}  # channel_id -> last_ping_ts
_channel_locks: Dict[int, asyncio.Lock] = {}
_recent_ping_hashes: Dict[str, float] = {}  # key: "channel_id:md5", value: ts


_last_ping_channels_mtime: float = 0.0


def _reload_ping_channels_from_file() -> None:
    """Re-read settings.json and update ping channels + DM notify user ids without restart."""
    global PING_CHANNEL_IDS, _PING_CHANNEL_SET, DM_NOTIFY_USER_IDS, _last_ping_channels_mtime
    try:
        mtime = _SETTINGS_JSON_PATH.stat().st_mtime if _SETTINGS_JSON_PATH.exists() else 0.0
        if mtime <= _last_ping_channels_mtime:
            return
        _last_ping_channels_mtime = mtime
        data = _load_settings_json(_SETTINGS_JSON_PATH)
        raw = data.get("ping_channel_ids") or []
        if isinstance(raw, str):
            raw_list = [x.strip() for x in raw.split(",") if x.strip()]
        elif isinstance(raw, list):
            raw_list = raw
        else:
            raw_list = []
        new_ids: List[int] = []
        for x in raw_list:
            try:
                new_ids.append(int(str(x).strip()))
            except Exception:
                continue
        PING_CHANNEL_IDS = new_ids
        _PING_CHANNEL_SET = set(PING_CHANNEL_IDS)

        raw_dm = data.get("dm_notify_user_ids") or []
        if isinstance(raw_dm, str):
            raw_dm_list = [x.strip() for x in raw_dm.split(",") if x.strip()]
        elif isinstance(raw_dm, list):
            raw_dm_list = raw_dm
        else:
            raw_dm_list = []
        new_dm: List[int] = []
        for x in raw_dm_list:
            try:
                new_dm.append(int(str(x).strip()))
            except Exception:
                continue
        DM_NOTIFY_USER_IDS = new_dm
    except Exception:
        pass


def _hash_message_for_ping(message: discord.Message) -> str:
    """Deterministic content hash for ping dedupe."""
    content = (getattr(message, "content", "") or "")[:500]

    embed_urls: List[str] = []
    for embed in getattr(message, "embeds", []) or []:
        url = getattr(embed, "url", None) or ""
        if url:
            embed_urls.append(str(url))
        title = getattr(embed, "title", None) or ""
        if title:
            embed_urls.append(str(title)[:100])

    attachment_urls: List[str] = []
    for a in getattr(message, "attachments", []) or []:
        url = getattr(a, "url", None) or ""
        if url:
            attachment_urls.append(str(url))

    all_text = content + "|" + "|".join(sorted(embed_urls)) + "|" + "|".join(sorted(attachment_urls))
    return hashlib.md5(all_text.encode("utf-8", errors="ignore")).hexdigest()


def _should_skip_ping_due_to_duplicate(message: discord.Message) -> bool:
    """Return True if same content was pinged very recently in the same channel."""
    ttl = int(DEDUPE_TTL_SECONDS) if int(DEDUPE_TTL_SECONDS) > 0 else 0
    if ttl <= 0:
        return False
    try:
        now = time.time()
        content_hash = _hash_message_for_ping(message)
        key = f"{int(message.channel.id)}:{content_hash}"
        last = _recent_ping_hashes.get(key, 0)
        if now - last < float(ttl):
            return True
        _recent_ping_hashes[key] = now

        if len(_recent_ping_hashes) > 2000:
            cutoff = now - float(ttl)
            for k, ts in list(_recent_ping_hashes.items()):
                if ts < cutoff:
                    _recent_ping_hashes.pop(k, None)
    except Exception:
        return False
    return False


# ---------------- Discord client ----------------

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)


@bot.event
async def on_ready() -> None:
    try:
        # Build friendly monitored-channel preview (names if cached)
        preview_lines: List[str] = []
        for cid in PING_CHANNEL_IDS[:6]:
            try:
                preview_lines.append(f"  * {_fmt_channel(bot, cid)}")
            except Exception:
                preview_lines.append(f"  * Channel-{cid}")

        startup_banner(
            [
                f"cwd: {os.getcwd()}",
                f"script: {str(Path(__file__).resolve())}",
                f"python: {sys.executable}",
                f"platform: {platform.platform()}",
                f"config_dir: {str(_CONFIG_DIR)}",
                f"settings: {str(_SETTINGS_JSON_PATH)}",
                f"token_env: {str(_TOKENS_ENV_PATH)}",
                "mode: ping-only",
                f"mirrorworld_server_id: {MIRRORWORLD_SERVER_ID or '(unset)'}",
                f"ping_channels: {len(PING_CHANNEL_IDS)}",
                f"cooldown_seconds: {COOLDOWN_SECONDS}",
                f"dedupe_ttl_seconds: {DEDUPE_TTL_SECONDS}",
                f"dm_notify_user_ids: {len(DM_NOTIFY_USER_IDS)}",
                "",
                "Monitored Channels (preview):",
                *preview_lines,
                *(
                    [f"  ... and {len(PING_CHANNEL_IDS) - 6} more"]
                    if len(PING_CHANNEL_IDS) > 6
                    else []
                ),
            ]
        )
        log_info(f"Logged in as {bot.user} (id={getattr(bot.user, 'id', 'unknown')})")
        if VERBOSE and PING_CHANNEL_IDS:
            preview = ", ".join(f"<#{cid}>" for cid in PING_CHANNEL_IDS[:12])
            log_info(f"PING_CHANNEL_IDS preview: {preview}" + (" ..." if len(PING_CHANNEL_IDS) > 12 else ""))
        log_info("Status: Ready and listening for messages")
    except Exception as e:
        log_error("PingBot on_ready failed", error=e)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author == bot.user:
        return
    if not message.guild:
        return

    if MIRRORWORLD_SERVER_ID and str(message.guild.id) != str(MIRRORWORLD_SERVER_ID):
        return

    _reload_ping_channels_from_file()

    try:
        channel_id = int(message.channel.id)
    except Exception:
        return

    if channel_id not in _PING_CHANNEL_SET:
        return

    if not (getattr(message, "content", None) or getattr(message, "embeds", None) or getattr(message, "attachments", None)):
        return

    if _should_skip_ping_due_to_duplicate(message):
        log_debug(f"Skip ping: duplicate content in {_fmt_channel(bot, channel_id)}")
        return

    ttl = int(COOLDOWN_SECONDS) if int(COOLDOWN_SECONDS) > 0 else 0
    lock = _channel_locks.setdefault(channel_id, asyncio.Lock())
    async with lock:
        now = time.time()
        last = _channel_cooldowns.get(channel_id, 0)
        if ttl > 0 and (now - last) < float(ttl):
            remaining = int(float(ttl) - (now - last))
            log_debug(f"Skip ping: cooldown active ({remaining}s) in {_fmt_channel(bot, channel_id)}")
            return

        try:
            _channel_cooldowns[channel_id] = now  # cooldown BEFORE send
            allowed = discord.AllowedMentions(everyone=True)
            await message.channel.send("@everyone", allowed_mentions=allowed)
            write_pingbot_log(
                {
                    "event": "ping_sent",
                    "channel_id": channel_id,
                    "message_id": getattr(message, "id", None),
                    "bot_type": "pingbot",
                }
            )
            log_ping("Sent @everyone", channel_label=_fmt_channel(bot, channel_id))
            if DM_NOTIFY_USER_IDS and send_ping_dm_notifications is not None:
                asyncio.create_task(
                    send_ping_dm_notifications(
                        bot,
                        message,
                        DM_NOTIFY_USER_IDS,
                        log_info=log_info,
                        log_warn=log_warn,
                        log_error=log_error,
                        write_log=write_pingbot_log,
                    )
                )
            elif DM_NOTIFY_USER_IDS and send_ping_dm_notifications is None:
                log_warn(
                    "dm_notify_user_ids configured but dm_notify_embeds.py is missing; "
                    "deploy MWPingBot/dm_notify_embeds.py and restart"
                )
        except Exception as e:
            log_error(f"Failed to send @everyone in channel {channel_id}", error=e)

    await bot.process_commands(message)


def _main() -> None:
    if not PING_BOT_TOKEN:
        msg = f"PING_BOT is not set in {str(_TOKENS_ENV_PATH)}"
        log_error(msg)
        raise SystemExit(1)
    if not MIRRORWORLD_SERVER_ID:
        log_warn(f"mirrorworld_server_id is not set in {str(_SETTINGS_JSON_PATH)} (bot will run but ignore all guilds)")
    if not PING_CHANNEL_IDS:
        log_warn(f"ping_channel_ids is empty in {str(_SETTINGS_JSON_PATH)} (bot will run but never ping)")
    if not DM_NOTIFY_USER_IDS:
        log_warn(
            f"dm_notify_user_ids is empty in {str(_SETTINGS_JSON_PATH)} "
            "(no DM alerts after @everyone; add user IDs to enable)"
        )

    # Register /ping slash command (same bot, same token) and sync to guild
    try:
        import ping_command_bot as _ping_cmd
        log_info("Registering slash commands with tree...")
        write_pingbot_log({"event": "command_register_start", "bot_type": "pingbot", "message": "registering /ping with bot tree"})
        _ping_cmd.register_ping_commands_to_bot(bot)
        _registered = [getattr(c, "name", str(c)) for c in bot.tree.walk_commands()]
        log_info(f"Slash commands on tree: {_registered}")
        write_pingbot_log({"event": "command_register_done", "bot_type": "pingbot", "commands": _registered})
        _gid = int(MIRRORWORLD_SERVER_ID or "0") or 0
        _orig_setup = getattr(bot, "setup_hook", None)

        async def _setup_hook():
            if _orig_setup and asyncio.iscoroutinefunction(_orig_setup):
                await _orig_setup()
            if _gid:
                write_pingbot_log({"event": "command_sync_start", "bot_type": "pingbot", "guild_id": _gid, "guild_id_str": str(_gid)})
                log_info(f"Syncing slash commands to guild (mirror-world server) id={_gid}")
                try:
                    await _ping_cmd.sync_ping_commands(bot, _gid)
                    log_info("Slash /ping: synced to guild (use /ping settings in Discord)")
                    write_pingbot_log({"event": "command_sync_ok", "bot_type": "pingbot", "guild_id": _gid})
                except Exception as e:
                    log_error("Slash /ping sync failed", error=e)
                    write_pingbot_log({"event": "command_sync_failed", "bot_type": "pingbot", "guild_id": _gid, "error": str(e), "error_type": type(e).__name__})
            else:
                log_warn("mirrorworld_server_id not set; slash commands not synced to guild (global sync only)")
                write_pingbot_log({"event": "command_sync_skipped", "bot_type": "pingbot", "reason": "guild_id_empty"})

        bot.setup_hook = _setup_hook
    except Exception as e:
        log_warn(f"Could not register /ping slash command: {e}")
        write_pingbot_log({"event": "command_register_failed", "bot_type": "pingbot", "error": str(e), "error_type": type(e).__name__})

    bot.run(PING_BOT_TOKEN)


if __name__ == "__main__":
    _main()
