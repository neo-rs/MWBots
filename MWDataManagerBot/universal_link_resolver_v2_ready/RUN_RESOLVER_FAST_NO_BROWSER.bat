@echo off
title Universal Link Resolver V2 - Fast No Browser
cd /d "%~dp0"
set /p URL=Paste URL to resolve: 
py -X utf8 universal_link_resolver.py --url "%URL%" --json-out last_result.json
echo.
pause
