# Used by ..\push_mwbots_py_only.bat — stage safe paths for commit.
# - Updates to already-tracked files (same filters as legacy script).
# - NEW: untracked safe files only under prefix folders below ^(avoids staging random untracked trees^).
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Only auto-stage untracked files whose path starts with one of these prefixes (forward slashes).
$UntrackedAutoStagePrefixes = @(
    "Instorebotforwarder/"
)

function Test-AllowListedPath {
    param([string]$p)
    if (($p -match '\.(py|md|json|txt|ps1)$') -or ($p -eq "requirements.txt")) {
        return $true
    }
    return $false
}

function Test-SafeContentPath {
    param([string]$p)
    if ($p -match '(^|/)config\.secrets\.json$') { return $false }
    if ($p -match '(^|/)tokens\.env$') { return $false }
    if ($p -match '\.env$') { return $false }
    if ($p -match 'member_history\.json$') { return $false }
    if ($p -match 'playwright_profile/') { return $false }
    if ($p -match '(^|/)\.playwright/') { return $false }
    if ($p -match '(^|/)api-token\.env$') { return $false }
    if ($p -match 'mavely_(cookies|refresh_token|auth_token|id_token)\.txt$') { return $false }
    return $true
}

function Test-UntrackedUnderAllowedPrefix {
    param([string]$p)
    foreach ($prefix in $UntrackedAutoStagePrefixes) {
        if ($p.StartsWith($prefix)) {
            return $true
        }
    }
    return $false
}

$tracked = @(git ls-files | Where-Object { Test-AllowListedPath $_ -and (Test-SafeContentPath $_) })

$allUntracked = @(git ls-files --others --exclude-standard | Where-Object { Test-AllowListedPath $_ -and (Test-SafeContentPath $_) })
$untracked = @($allUntracked | Where-Object { Test-UntrackedUnderAllowedPrefix $_ })

git reset 2>$null | Out-Null

if ($tracked.Count -gt 0) {
    git add -u -- $tracked
}

if ($untracked.Count -gt 0) {
    git add -- $untracked
}

# Always allow staging this script itself when newly added ^(lives at MWBots root^).
if (Test-Path -LiteralPath "stage_mwbots_push.ps1") {
    $us = git status --porcelain -- "stage_mwbots_push.ps1"
    if ($us -match '^\?\?') {
        git add -- "stage_mwbots_push.ps1"
    }
}

$runtime = @(
    "MWDiscumBot/config/fetchall_mappings.runtime.json",
    "MWDiscumBot/config/settings.runtime.json"
)
foreach ($rel in $runtime) {
    if (Test-Path -LiteralPath $rel) {
        git add -- $rel
    }
}
