@echo off
setlocal EnableDelayedExpansion

REM Replay classifier against Discord channel history (read-only).
REM - If you pass arguments, they are forwarded to channel_route_audit.py unchanged.
REM - If you run the batch with NO arguments, you get prompts (channel preset + limit + options).

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

REM Do NOT quote "py -3" as one path - use venv python.exe when present, else: py -3 script ...
if not "%~1"=="" (
  echo [channel_route_audit] Forwarding args: %*
  if defined VENV_PY (
    "%VENV_PY%" "%~dp0channel_route_audit.py" %*
  ) else (
    py -3 "%~dp0channel_route_audit.py" %*
  )
  goto :eof
)

:MENU_LOOP
set "CHID="
set "CHLBL="
set "MSGLINK="
set "LIM="
set "SRC="
set "JSON="
set "MIRCAT="

echo.
echo ==============================================================================
echo  MWDataManagerBot - Channel route audit (interactive^)
echo ==============================================================================
echo  Reads messages from a channel and replays classification (no sends^).
echo  Pass args on the command line to skip this menu, e.g.:
echo    run_channel_route_audit.bat --channel-id 1435066421133443174 --limit 40
echo  Replay source auto: if scan channel is in source_channel_ids_online OR source_channel_ids_instore,
echo  leaving replay blank uses that same id ^(matches production online vs instore routing^).
echo.

set /p PICK="Pick preset [1=CONVERSATIONAL_DEALS  2=AMAZON  3=custom channel id  4=message link] (1): "
if "!PICK!"=="" set PICK=1

if "!PICK!"=="1" set "CHID=1438970053352751215" & set "CHLBL=CONVERSATIONAL_DEALS (conversational-deals)"
if "!PICK!"=="2" set "CHID=1435066421133443174" & set "CHLBL=AMAZON"
if "!PICK!"=="3" (
  set /p CHID="Enter channel ID (numeric): "
  set "CHLBL=custom"
)
if "!PICK!"=="4" (
  set /p MSGLINK="Paste Discord message link (ptb/discord.com): "
  set "CHLBL=message_link"
  REM channel id is derived from the link; keep a dummy value for display/compat
  if not defined CHID set "CHID=0"
)
if not defined CHID (
  echo ERROR: No channel id.
  goto :eof
)

set /p LIM="Message limit, newest first [30]: "
if "!LIM!"=="" set LIM=30

set /p SRC="Replay source channel id [Enter=auto: scan id if online/instore source, else first online in settings^]: "

echo.
echo JSON: type a NEW output filename ^(e.g. audit.json^) - it does NOT need to exist; results are written next to this .bat
echo      Leave blank to skip JSON. Existing file with the same name is overwritten.
set /p JSON="JSON output file (blank = skip^): "

echo.
REM Interactive default mirror category (edit if yours differs). CLI mode: pass --mirror-category-id or omit.
set "MIRROR_CATEGORY_DEFAULT=1492550056409174076"
echo Mirror: preview posts go under audit-tag channels in a Discord CATEGORY ^(not live destinations^).
echo        Existing audit-* text channels in that category are DELETED first ^(fresh run^).
echo        To keep old staging channels, add: --no-mirror-delete-staging to the forwarded args ^(CLI mode^).
echo        Press Enter to use default category !MIRROR_CATEGORY_DEFAULT!
echo        Or type another category id. Type 0 to skip mirroring ^(no preview posts^).
set /p MIRCAT="Mirror category id [Enter=default  0=skip]: "
if "!MIRCAT!"=="" set "MIRCAT=!MIRROR_CATEGORY_DEFAULT!"
if /i "!MIRCAT!"=="0" set "MIRCAT="
if /i "!MIRCAT!"=="skip" set "MIRCAT="

set "ARGS=--channel-id !CHID! --limit !LIM!"
if not "!MSGLINK!"=="" set "ARGS=--message-link !MSGLINK!"
if not "!SRC!"=="" set "ARGS=!ARGS! --source-channel-id !SRC!"
if not "!JSON!"=="" set "ARGS=!ARGS! --json-out !JSON!"
if not "!MIRCAT!"=="" set "ARGS=!ARGS! --mirror-category-id !MIRCAT!"

if not "!MIRCAT!"=="" (
  echo Using mirror category id: !MIRCAT!
) else (
  echo Mirror preview: skipped ^(you typed 0/skip^).
)
echo.
if defined VENV_PY (
  echo Running: "%VENV_PY%" channel_route_audit.py !ARGS!
) else (
  echo Running: py -3 channel_route_audit.py !ARGS!
)
echo Channel: !CHLBL! ^(!CHID!^)
echo.

if defined VENV_PY (
  "%VENV_PY%" "%~dp0channel_route_audit.py" !ARGS!
) else (
  py -3 "%~dp0channel_route_audit.py" !ARGS!
)
set "AUDIT_EXIT=!ERRORLEVEL!"

echo.
if not "%AUDIT_EXIT%"=="0" (
  echo channel_route_audit finished with exit code %AUDIT_EXIT% ^(see messages above^).
) else (
  echo channel_route_audit finished OK.
)
echo.
set /p AGAIN="Run another audit? [Enter=Y, N=exit] (Y): "
if "!AGAIN!"=="" set "AGAIN=Y"
if /i "!AGAIN!"=="Y" goto :MENU_LOOP
if /i "!AGAIN!"=="YES" goto :MENU_LOOP

endlocal
