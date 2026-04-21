# Used by ..\push_mwbots_py_only.bat — stage safe paths for commit.
# - Updates already-tracked allowlisted paths (each path staged separately so one bad pathspec cannot abort the batch).
# - Adds untracked allowlisted paths only under Instorebotforwarder/ (+ this script when new).
$ErrorActionPreference = "Continue"

Set-Location $PSScriptRoot

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

# IMPORTANT: `git add -u -- path1 path2 ...` can fail the whole command if any pathspec is not "known to git"
# for -u (e.g. stray untracked path). Stage updates one file at a time.
foreach ($p in $tracked) {
    $rel = [string]$p
    if (-not $rel) { continue }
    # Only run -u for paths that are actually in the index (tracked).
    git ls-files --error-unmatch -- $rel 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        continue
    }
    git add -u -- $rel 2>$null
}

foreach ($p in $untracked) {
    $rel = [string]$p
    if (-not $rel) { continue }
    if (-not (Test-Path -LiteralPath $rel)) {
        continue
    }
    git add -- $rel
}

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
