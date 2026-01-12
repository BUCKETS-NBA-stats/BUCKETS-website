# scripts/local/update.ps1
# Runs: pull -> ingest -> stage -> validate -> commit+push (if changed)
# Writes logs to: logs\update.log

$ErrorActionPreference = "Stop"

# ===== CONFIG =====
$Season = "2025-26"
$SeasonType = "Regular Season"

$RepoRoot = (Get-Location).Path

$LockDir = Join-Path $RepoRoot "scripts\local\.lock"
$LockFile = Join-Path $LockDir "update.lock"
$LogDir  = Join-Path $RepoRoot "logs"
$LogFile = Join-Path $LogDir "update.log"

function Write-Log($msg) {
  $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
  "$ts  $msg" | Tee-Object -FilePath $LogFile -Append
}

function Run-Step($label, $command) {
  Write-Log $label
  & $command
  if ($LASTEXITCODE -ne 0) {
    throw "Step failed (exit code $LASTEXITCODE): $label"
  }
}

New-Item -ItemType Directory -Force -Path $LockDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if (Test-Path $LockFile) {
  Write-Log "Another update appears to be running (lock exists). Exiting."
  exit 0
}

try {
  New-Item -ItemType File -Path $LockFile -Force | Out-Null

  Write-Log "=== Starting update ==="
  Write-Log "RepoRoot: $RepoRoot"
  Write-Log "Season: $Season | SeasonType: $SeasonType"

  $VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
  if (!(Test-Path $VenvPython)) {
    throw "Missing venv python at $VenvPython. Create it with: python -m venv .venv"
  }

  Write-Log "Checking git status..."
  $status = git status --porcelain
  if ($status) {
    throw "Working tree not clean. Please commit/stash changes before scheduled runs."
  }

  Write-Log "Pulling latest main..."
  git checkout main | Out-Null
  git pull | Out-Null

  # ---- Ingest / Stage / Validate (stop immediately if any fails) ----
  Write-Log "Running ingest..."
  & $VenvPython scripts/ingest/run_ingest.py --season $Season --season-type $SeasonType
  if ($LASTEXITCODE -ne 0) { throw "Ingest failed (exit code $LASTEXITCODE)." }

  Write-Log "Building staging..."
  & $VenvPython scripts/stage/build_stage_season.py --season $Season --season-type $SeasonType
  if ($LASTEXITCODE -ne 0) { throw "Stage build failed (exit code $LASTEXITCODE)." }

  Write-Log "Validating staging..."
  & $VenvPython scripts/qa/validate_stage_season.py --season $Season --season-type $SeasonType
  if ($LASTEXITCODE -ne 0) { throw "Stage validation failed (exit code $LASTEXITCODE)." }

  # ---- Commit only if changed ----
  Write-Log "Checking for changes to commit..."
  git add assets/data/staging reports

  $diff = git diff --cached --name-only
  if (-not $diff) {
    Write-Log "No changes detected. Done."
    exit 0
  }

  Write-Log "Changes detected:"
  $diff | ForEach-Object { Write-Log "  $_" }

  $commitMsg = "Daily refresh: $Season ($SeasonType) - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') CT"
  Write-Log "Committing..."
  git commit -m "$commitMsg" | Out-Null

  Write-Log "Pushing..."
  git push | Out-Null

  Write-Log "Push complete. Done."
}
catch {
  Write-Log "ERROR: $($_.Exception.Message)"
  exit 1
}
finally {
  if (Test-Path $LockFile) { Remove-Item $LockFile -Force }
  Write-Log "=== Update finished ==="
}
