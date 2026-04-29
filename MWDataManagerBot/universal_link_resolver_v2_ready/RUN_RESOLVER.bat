@echo off
title Universal Link Resolver V2
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_resolver.ps1"
echo.
pause
