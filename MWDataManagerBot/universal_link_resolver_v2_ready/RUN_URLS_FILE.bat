@echo off
title Universal Link Resolver V2 - URLs File
cd /d "%~dp0"
py -X utf8 universal_link_resolver.py --urls-file urls.txt --use-playwright --profile-dir ".\pw_profile" --settle-ms 8000 --json-out last_result.json
echo.
pause
