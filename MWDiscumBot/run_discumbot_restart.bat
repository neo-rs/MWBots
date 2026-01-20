@echo off
setlocal

REM Restart MWDiscumBot (standalone):
REM - Kill any running python processes executing THIS folder's discumbot.py
REM - Then launch discumbot.py normally
REM
REM Usage:
REM   run_discumbot_restart.bat           -> restart and keep logs in this window
REM   run_discumbot_restart.bat --detach  -> restart in a NEW window and return immediately

cd /d "%~dp0"

set PYTHONUNBUFFERED=1
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo [INFO] Restarting MWDiscumBot...

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; $root=(Resolve-Path '.').Path.ToLower(); $scriptPathObj=(Resolve-Path '.\discumbot.py' -ErrorAction SilentlyContinue); $script=if($scriptPathObj){$scriptPathObj.Path.ToLower()}else{''}; $needleRel1='mwdiscumbot\\discumbot.py'; $needleRel2='\\mwdiscumbot\\discumbot.py'; $lock=(Join-Path $root '.d2d.lock'); if(Test-Path $lock){ try { $txt = Get-Content -Raw -Path $lock; if($txt -match 'pid=(\d+)'){ $pid=[int]$Matches[1]; Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue } } catch {}; try { Remove-Item -Force -Path $lock -ErrorAction SilentlyContinue } catch {} }; $procs=Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and ($_.Name -match '^(python|py)\.exe$') } | Where-Object { $cl=$_.CommandLine.ToLower(); ($script -and $cl.Contains($script)) -or $cl.Contains($needleRel1) -or $cl.Contains($needleRel2) -or ($cl.Contains($root) -and $cl.Contains('discumbot.py')) }; $ids=@($procs | Select-Object -ExpandProperty ProcessId); foreach($id in $ids){ try { Stop-Process -Id $id -Force -ErrorAction SilentlyContinue } catch {} }; if($ids.Count -gt 0){ Write-Output ('[INFO] Killed MWDiscumBot PIDs: ' + ($ids -join ', ')) } else { Write-Output '[INFO] No MWDiscumBot processes found to kill' }"

REM Run normally (same window) OR detach
if /I "%~1"=="--detach" (
  echo [INFO] Starting MWDiscumBot in a new window...
  start "MWDiscumBot" /D "%~dp0" cmd /c "\"%~dp0run_discumbot.bat\""
) else (
  call "%~dp0run_discumbot.bat"
)

endlocal
