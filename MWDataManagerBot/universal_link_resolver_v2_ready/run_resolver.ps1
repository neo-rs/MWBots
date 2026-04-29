$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

Write-Host ""
Write-Host "Universal Link Resolver V2"
Write-Host "Paste works here with Ctrl+V or right-click."
Write-Host ""

$url = Read-Host "Paste URL to resolve"
if ([string]::IsNullOrWhiteSpace($url)) {
    Write-Host "No URL entered."
    exit 1
}

Write-Host ""
Write-Host "Resolving..."
Write-Host ""

py -X utf8 ".\universal_link_resolver.py" --url "$url" --use-playwright --profile-dir ".\pw_profile" --settle-ms 8000 --json-out "last_result.json"

Write-Host ""
Write-Host "Saved JSON report to last_result.json"
