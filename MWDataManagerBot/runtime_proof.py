from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_git_head(project_root: Path) -> str:
    try:
        head = project_root / ".git" / "HEAD"
        if not head.exists():
            return "no_git"
        ref = head.read_text(encoding="utf-8", errors="ignore").strip()
        if ref.startswith("ref:"):
            ref_path = project_root / ".git" / ref.split(":", 1)[1].strip()
            if ref_path.exists():
                return ref_path.read_text(encoding="utf-8", errors="ignore").strip()[:12] or "unknown"
        return ref[:12] or "unknown"
    except Exception:
        return "unknown"


def build_runtime_proof_lines(
    *,
    bot_name: str,
    script_path: Path,
    config_dir: Path,
    settings_path: Path,
    tokens_path: Path,
    extra: Optional[Dict[str, Any]] = None,
) -> List[str]:
    extra = dict(extra or {})
    # Standalone rule: treat MWDataManagerBot folder as the runtime root.
    bot_root = script_path.resolve().parent
    lines: List[str] = []
    lines.append(f"cwd: {os.getcwd()}")
    lines.append(f"script: {str(script_path)}")
    lines.append(f"python: {sys.executable}")
    lines.append(f"python_version: {platform.python_version()}")
    lines.append(f"platform: {platform.platform()}")
    lines.append(f"bot_root: {str(bot_root)}")
    # MWDataManagerBot is local-only; do not read repo-root .git for runtime proof.
    lines.append("git_head: no_git")
    lines.append(f"config_dir: {str(config_dir)}")
    lines.append(f"settings: {str(settings_path)}")
    lines.append(f"token_env: {str(tokens_path)}")
    for k, v in extra.items():
        if v is None:
            continue
        lines.append(f"{k}: {v}")
    return lines

