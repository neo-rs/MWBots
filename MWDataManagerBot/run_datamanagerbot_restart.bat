@echo off
setlocal

REM Restart MWDataManagerBot (standalone):
REM - Kill any running python processes executing THIS folder's datamanagerbot.py
REM - Then launch datamanagerbot.py normally
REM
REM Usage:
REM   run_datamanagerbot_restart.bat           -> restart and keep logs in this window
REM   run_datamanagerbot_restart.bat --detach  -> restart in a NEW window and return immediately

cd /d "%~dp0"

set PYTHONUNBUFFERED=1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [INFO] Restarting MWDataManagerBot...

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $root=(Resolve-Path '.').Path.ToLower(); $scriptPathObj=(Resolve-Path '.\datamanagerbot.py' -ErrorAction SilentlyContinue); $script=if($scriptPathObj){$scriptPathObj.Path.ToLower()}else{''}; $needleRel1='mwdatamanagerbot\\datamanagerbot.py'; $needleRel2='\\mwdatamanagerbot\\datamanagerbot.py'; $procs=Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and ($_.Name -match '^(python|py)\.exe$') } | Where-Object { $cl=$_.CommandLine.ToLower(); ($script -and $cl.Contains($script)) -or $cl.Contains($needleRel1) -or $cl.Contains($needleRel2) -or ($cl.Contains($root) -and $cl.Contains('datamanagerbot.py')) }; $ids=@($procs | Select-Object -ExpandProperty ProcessId); foreach($id in $ids){ try { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue } catch {} }; if($ids.Count -gt 0){ Write-Output ('[INFO] Killed MWDataManagerBot PIDs: ' + ($ids -join ', ')) } else { Write-Output '[INFO] No MWDataManagerBot processes found to kill' }"

REM Run normally (same window) OR detach
if /I "%~1"=="--detach" (
  echo [INFO] Starting MWDataManagerBot in a new window...
  start "MWDataManagerBot" /D "%~dp0" cmd /c "\"%~dp0run_datamanagerbot.bat\""
) else (
  call "%~dp0run_datamanagerbot.bat"
)

endlocal

