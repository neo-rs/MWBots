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
  - Fetch-all + scheduler (kept)
  - Commands (kept)

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
    # One source of truth:
    # - DATAMANAGER_BOT: discord.py bot token (runs MWDataManagerBot in Mirror World)
    # - FETCHALL_USER_TOKEN: Discum/user token used ONLY for fetchall source discovery
    bot_token = str(tokens.get("DATAMANAGER_BOT") or "").strip()
    if not bot_token:
        legacy = str(tokens.get("DISCORD_BOT_DATAMANAGER") or "").strip()
        if legacy:
            log_warn("Using legacy token key DISCORD_BOT_DATAMANAGER; rename it to DATAMANAGER_BOT (canonical).")
            bot_token = legacy
    if not bot_token or bot_token.upper() == "YOUR_TOKEN_HERE":
        log_error("Missing bot token. Set DATAMANAGER_BOT in MWDataManagerBot/config/tokens.env")
        return 2

    # Discum/user token for fetchall against source servers the bot is not in.
    fetchall_source_token = str(tokens.get("FETCHALL_USER_TOKEN") or "").strip()
    if not fetchall_source_token:
        legacy = (
            str(tokens.get("DISCUM_USER_DISCUMBOT") or "").strip()
            or str(tokens.get("DISCUM_BOT") or "").strip()
            or str(tokens.get("DISCUM_USER_NEO") or "").strip()
            or str(tokens.get("NEOBOT_DISCORD_USER_TOKEN") or "").strip()
        )
        if legacy:
            log_warn(
                "Using legacy Discum token key (DISCUM_USER_DISCUMBOT / DISCUM_BOT / DISCUM_USER_NEO / NEOBOT_DISCORD_USER_TOKEN). "
                "Rename it to FETCHALL_USER_TOKEN (canonical)."
            )
            fetchall_source_token = legacy
    if not fetchall_source_token:
        log_warn(
            "Fetchall source token not found. "
            "Set FETCHALL_USER_TOKEN in MWDataManagerBot/config/tokens.env "
            "(fetchall will only work for guilds the bot is in)."
        )

    try:
        from live_forwarder import run_bot
    except Exception as e:
        log_error("Failed to import live_forwarder/run_bot (incomplete skeleton?)", error=e)
        return 2

    log_info("Starting MWDataManagerBot...")
    # Pass canonical tokens through in-memory only (not written to disk).
    settings["_tokens"] = {
        "DATAMANAGER_BOT": bot_token,
        "FETCHALL_USER_TOKEN": fetchall_source_token,
    }
    return int(run_bot(settings=settings, token=bot_token) or 0)


if __name__ == "__main__":
    raise SystemExit(main())

