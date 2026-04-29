Universal Link Resolver V2

Why this version:
- pricedoffers.com is now treated as a redirect/intermediate domain, not a final merchant.
- The main launcher uses PowerShell Read-Host so paste should work better than plain set /p batch input.
- The main launcher enables Playwright fallback by default, because some shortlinks return HTTP 200 and only reveal the destination through browser/JS behavior.

First time setup:
1. Double-click install_requirements.bat
2. Wait for packages and Chromium install to finish.

Normal use:
1. Double-click RUN_RESOLVER.bat
2. Paste the URL
3. Press Enter

Fast mode:
- RUN_RESOLVER_FAST_NO_BROWSER.bat skips Playwright.
- Use this only for simple links. It may fail on JS/browser links like pricedoffers.

Batch file mode:
1. Put URLs in urls.txt, one per line.
2. Double-click RUN_URLS_FILE.bat

Output:
- Console report
- last_result.json
