"""
MWDataManagerBot (Standalone Data Manager / Forwarder)
------------------------------------------------------
This is a local-only standalone refactor target for the former `neonxt/bots/datamanagerbot.py`.

Responsibilities (kept, but modularized for easier debugging):
  - Live forwarder (discord.py bot token)
    - gate by destination guild(s) + source channel lists
    - classify messages (local routing) + detect global triggers
    - dedupe and forward to destination channels/webhooks
    - handle edits (on_raw_message_edit) without spam
  - Commands (kept). Fetch-all/fetchsync run in MWDiscumBot only (single user-token consumer).

Config (standalone, local-only):
  - config/tokens.env     (secrets only)
  - config/settings.json  (non-secret)

Outputs (standalone, local-only):
  - logs/Botlogs/datamanagerbotlogs.json  (JSONL)
  - config/systemlogs.json               (JSON array)
"""

from __future__ import annotations

import atexit
import os
import platform
import sys
import warnings
from pathlib import Path

# Suppress harmless runtime warnings seen on Windows setups
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*GIL.*")

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"
_LOCK_PATH = _BOT_DIR / "logs" / "datamanagerbot.pid"


def _acquire_single_instance_lock() -> tuple[bool, int]:
    """
    Best-effort single-instance guard for Windows:
    - if pid file exists and process is alive -> reject
    - else write our current pid
    Returns (ok, existing_pid_if_any)
    """
    try:
        _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        if _LOCK_PATH.exists():
            raw = str(_LOCK_PATH.read_text(encoding="utf-8", errors="ignore") or "").strip()
            old_pid = int(raw) if raw.isdigit() else 0
            if old_pid > 0:
                try:
                    # os.kill(pid, 0) is a cross-platform "is process alive" probe.
                    os.kill(old_pid, 0)
                    return False, old_pid
                except Exception:
                    pass
        _LOCK_PATH.write_text(str(os.getpid()), encoding="utf-8")
        return True, 0
    except Exception:
        # If lock cannot be created, do not block startup.
        return True, 0


def _release_single_instance_lock() -> None:
    try:
        if _LOCK_PATH.exists():
            raw = str(_LOCK_PATH.read_text(encoding="utf-8", errors="ignore") or "").strip()
            if raw == str(os.getpid()):
                _LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def main() -> int:
    # Import locally so startup banner can render even if deps are missing.
    from runtime_proof import build_runtime_proof_lines
    from logging_utils import (
        log_error,
        log_info,
        log_warn,
        setup_console_logging,
        startup_banner,
    )
    from config import load_settings_and_tokens

    settings, tokens = load_settings_and_tokens(_CONFIG_DIR)
    verbose = bool(settings.get("verbose", True))
    setup_console_logging(verbose=verbose)

    # Runtime proof banner (Discum/Ping style)
    proof_lines = build_runtime_proof_lines(
        bot_name="MWDataManagerBot",
        script_path=Path(__file__).resolve(),
        config_dir=_CONFIG_DIR,
        settings_path=_CONFIG_DIR / "settings.json",
        tokens_path=_CONFIG_DIR / "tokens.env",
        extra={
            "platform": platform.platform(),
            "python": sys.executable,
        },
    )
    startup_banner(proof_lines, bot_name="MWDataManagerBot (Standalone)")

    # Prevent accidental multi-run (common cause of duplicate forwards).
    ok, existing_pid = _acquire_single_instance_lock()
    if not ok:
        log_error(
            f"Another MWDataManagerBot instance appears to be running (pid={existing_pid}). "
            "Stop the existing process before starting a new one."
        )
        return 2
    atexit.register(_release_single_instance_lock)

    # ---------------- Canonical tokens (standalone) ----------------
    # DATAMANAGER_BOT only: no user token. Fetchall/fetchsync run in MWDiscumBot only (single user-token consumer).
    bot_token = str(tokens.get("DATAMANAGER_BOT") or "").strip()
    if not bot_token:
        legacy = str(tokens.get("DISCORD_BOT_DATAMANAGER") or "").strip()
        if legacy:
            log_warn("Using legacy token key DISCORD_BOT_DATAMANAGER; rename it to DATAMANAGER_BOT (canonical).")
            bot_token = legacy
    if not bot_token or bot_token.upper() == "YOUR_TOKEN_HERE":
        log_error("Missing bot token. Set DATAMANAGER_BOT in MWDataManagerBot/config/tokens.env")
        return 2

    try:
        from live_forwarder import run_bot
    except Exception as e:
        log_error("Failed to import live_forwarder/run_bot (incomplete skeleton?)", error=e)
        return 2

    log_info("Starting MWDataManagerBot...")
    settings["_tokens"] = {"DATAMANAGER_BOT": bot_token}
    return int(run_bot(settings=settings, token=bot_token) or 0)


if __name__ == "__main__":
    raise SystemExit(main())

