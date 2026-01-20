@echo off
setlocal

REM Restart MWPingBot (standalone):
REM - Kill any running python processes executing THIS folder's pingbot.py
REM - Then launch pingbot.py normally
REM
REM Usage:
REM   run_pingbot_restart.bat           -> restart and keep logs in this window
REM   run_pingbot_restart.bat --detach  -> restart in a NEW window and return immediately

cd /d "%~dp0"

set PYTHONUNBUFFERED=1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [INFO] Restarting MWPingBot...

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='SilentlyContinue';" ^
  "$root = (Resolve-Path '.').Path;" ^
  "$script = (Resolve-Path '.\\pingbot.py').Path.ToLower();" ^
  "$procs = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine.ToLower().Contains($script) };" ^
  "foreach ($p in $procs) { try { Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } catch {} }"

REM Run normally (same window) OR detach
if /I "%~1"=="--detach" (
  echo [INFO] Starting MWPingBot in a new window...
  start "MWPingBot" /D "%~dp0" cmd /c "\"%~dp0run_pingbot.bat\""
) else (
  call "%~dp0run_pingbot.bat"
)

endlocal

