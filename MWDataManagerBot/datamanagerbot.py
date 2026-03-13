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

import platform
import sys
import warnings
from pathlib import Path

# Suppress harmless runtime warnings seen on Windows setups
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*GIL.*")

_BOT_DIR = Path(__file__).resolve().parent
_CONFIG_DIR = _BOT_DIR / "config"


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

