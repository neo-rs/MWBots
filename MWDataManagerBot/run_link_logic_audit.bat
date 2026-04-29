@echo off
setlocal EnableDelayedExpansion

REM Local link-logic audit (no Discord API; no sends)
REM - If you pass arguments, they are forwarded to link_logic_audit.py unchanged.
REM - If you run the batch with NO arguments, you get a paste prompt + a sample.

cd /d "%~dp0"

set PYTHONUNBUFFERED=1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

REM mirror-world repo root (MWBots\MWDataManagerBot -> ..\..)
set "REPOROOT=%~dp0..\.."
set "MWBOTSROOT=%~dp0.."
set "VENV_PY="
if exist "%REPOROOT%\.venv\Scripts\python.exe" set "VENV_PY=%REPOROOT%\.venv\Scripts\python.exe"
if not defined VENV_PY if exist "%MWBOTSROOT%\.venv\Scripts\python.exe" set "VENV_PY=%MWBOTSROOT%\.venv\Scripts\python.exe"

if not "%~1"=="" (
  echo [link_logic_audit] Forwarding args: %*
  if defined VENV_PY (
    "%VENV_PY%" "%~dp0link_logic_audit.py" %*
  ) else (
    py -3 "%~dp0link_logic_audit.py" %*
  )
  goto :eof
)

set "SAMPLE=https://pricedoffers.com/y8ndc"
set "TEXT="
set "SRC="
set "JSON="

echo.
echo ==============================================================================
echo  MWDataManagerBot - Link logic audit (interactive^)
echo ==============================================================================
echo  Paste a URL or message text and it will replay:
echo   - enable_raw_link_unwrap augmentation (classification only^)
echo   - detect_all_link_types / global_triggers / fallback selection
echo  No Discord calls. No sends.
echo.
echo Sample (Enter to use^): !SAMPLE!
set /p TEXT="Input text or URL: "
if "!TEXT!"=="" set "TEXT=!SAMPLE!"

echo.
set /p SRC="Replay source_channel_id (blank = auto: first online source in settings^): "

echo.
echo JSON: optional output filename (e.g. link_audit.json^) - written next to this .bat if relative.
set /p JSON="JSON output file (blank = skip^): "

set "ARGS=--text \"!TEXT!\""
if not "!SRC!"=="" set "ARGS=!ARGS! --source-channel-id !SRC!"
if not "!JSON!"=="" set "ARGS=!ARGS! --json-out \"!JSON!\""

echo.
if defined VENV_PY (
  echo Running: "%VENV_PY%" link_logic_audit.py !ARGS!
) else (
  echo Running: py -3 link_logic_audit.py !ARGS!
)
echo.

if defined VENV_PY (
  "%VENV_PY%" "%~dp0link_logic_audit.py" !ARGS!
) else (
  py -3 "%~dp0link_logic_audit.py" !ARGS!
)

echo.
set "AUDIT_EXIT=!ERRORLEVEL!"
echo Exit code: !AUDIT_EXIT!
echo.
pause
exit /b !AUDIT_EXIT!

