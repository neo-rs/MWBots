@echo off
title Install Universal Link Resolver Requirements
cd /d "%~dp0"
echo Installing Python packages...
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
echo.
echo Installing Playwright Chromium browser...
py -m playwright install chromium
echo.
echo Done.
pause
