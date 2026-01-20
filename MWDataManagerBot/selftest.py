"""
Local-only selftest for MWDataManagerBot core logic.

Runs without Discord connectivity:
  - loads config/settings.json
  - initializes settings_store
  - exercises classifier + global triggers + signature generation
"""

from __future__ import annotations

from pathlib import Path

from config import load_settings_and_tokens
from global_triggers import detect_global_triggers
from classifier import detect_all_link_types, select_target_channel_id
import settings_store as cfg
from utils import generate_content_signature


def main() -> int:
    bot_dir = Path(__file__).resolve().parent
    settings, _tokens = load_settings_and_tokens(bot_dir / "config")
    cfg.init(settings)

    samples = [
        "Amazon deal https://amzn.to/abc123 B012345678 retail: $10 resell: $50",
        "where: walmart retail: $20 resell: $100 confirmed in-store stock 300% ROI",
        "drops on <t:1893456000:R> pre-order goes live tomorrow",
        "random link https://walmart.com/ip/123",
        "price error glitched wrong price checkout working",
        "clearance markdown 80% off where: ross retail: $5 resell: $40",
    ]

    print("MWDataManagerBot selftest")
    print(f"destination_guild_ids={sorted(cfg.DESTINATION_GUILD_IDS)}")
    print(f"monitored_channels={len(cfg.SMART_SOURCE_CHANNELS)}")
    print()

    for i, text in enumerate(samples, 1):
        attachments = []
        embeds = []
        local = detect_all_link_types(text, attachments, keywords_list=[], embeds=embeds, source_channel_id=0)
        fallback = select_target_channel_id(text, attachments, keywords_list=[], source_channel_id=0)
        global_tr = detect_global_triggers(text, source_channel_id=0, link_tracking_cache={}, embeds=embeds, attachments=attachments)
        sig = generate_content_signature(text, embeds, attachments)
        print(f"[{i}] text={text[:60]}...")
        print(f"    local={local}")
        print(f"    fallback={fallback}")
        print(f"    global={global_tr}")
        print(f"    sig={sig}")
        print()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())

