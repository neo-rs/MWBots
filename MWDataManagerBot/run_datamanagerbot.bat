@echo off
setlocal

REM Run MWDataManagerBot from this folder (standalone, local-only).
REM Prefers repo .venv if present; otherwise uses python on PATH.

cd /d "%~dp0"

set PYTHONUNBUFFERED=1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

set "ROOT=%~dp0.."
if exist "%ROOT%\.venv\Scripts\python.exe" (
  "%ROOT%\.venv\Scripts\python.exe" "%~dp0datamanagerbot.py"
) else (
  python "%~dp0datamanagerbot.py"
)

endlocal

