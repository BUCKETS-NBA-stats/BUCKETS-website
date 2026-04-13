# BUCKETS Project Plan & Progress

## Overview
BUCKETS is a custom NBA stats website hosted on GitHub Pages. The goal is to fully automate the data pipeline from raw source data → calculated metrics → published website, replacing manual Google Sheets workflows.

## Architecture
- **Website**: Static HTML (HTML5 UP Editorial theme), DataTables, jQuery, Papa Parse, Select2, Highcharts
- **Data pipeline**: Python scripts running on a local Windows machine, pushing to GitHub where Actions rebuild the combined CSV and deploy via GitHub Pages
- **Repo structure**: Two local clones — `C:\BUCKETS\dev` (development/testing) and `C:\BUCKETS\prod` (daily scheduled runs). Both track the same GitHub repo.
- **Daily automation**: Windows Task Scheduler runs at 4 AM Central from `prod`, executing `scripts/local/update.ps1`

## Data Flow
NBA.com API (traditional + passing + 11 play types)
PBPStats API (scoring + turnovers totals)
Cleaning the Glass (4 league averages, scraped)
↓
scripts/ingest/.py → assets/data/raw/{season}/{type}/.parquet
↓
scripts/stage/build_stage_season.py → assets/data/staging/{season}__{type}.parquet (438 cols)
↓
scripts/qa/validate_stage_season.py (QA gate)
↓
scripts/calculate/build_season.py → assets/data/season/league-table-{year}.csv (151 cols)
↓
scripts/build_master.py → assets/data/league-table-combined.csv
↓
GitHub Pages serves the website

## Key Metrics / Columns
- PRF = Points Responsible For
- PC = Points Created
- rORTG = points per 100 plays above baseline
- cUSG% = total creation usage
- On-ball / Off-ball / Transition splits
- Components of Total PC: Score / Pass / Floor

## Multi-Phase Plan

### Phase 0 — Publishing automation + global canonical naming ✅ COMPLETE
- Manual refresh in Google Sheets → export CSVs → push → GitHub Actions rebuilds combined master + deploys
- Player name canonicalization via mappings/player_aliases.csv

### Phase 1 — Canonical player identity (crosswalk) ✅ COMPLETE
- Global mapping across all seasons
- ASCII display names (e.g., "Nikola Jokic")
- Built from per-season Name Fixer CSVs in mappings/name_fixer_inputs/

### Phase 2 — Source adapters / automated ingest ✅ COMPLETE (as of 2026-03-29)
All data sources automated:
- **NBA.com traditional + passing** (existed before Phase 2): `scripts/ingest/run_ingest.py`
- **NBA.com play types ×11** (ISO, PnR BH, Roll/Pop, Post-Up, Spot-Up, Handoff, Cut, Off-Screen, Putbacks, Misc, Transition): `scripts/ingest/nba_playtypes.py`
- **PBPStats** (scoring + turnovers, single API call): `scripts/ingest/nba_pbpstats.py`
- **Cleaning the Glass** (4 league averages, HTML scrape with session auth): `scripts/ingest/ctg_league_avgs.py`
- **bball-index**: SKIPPED — planned changes will eliminate this dependency

Staging parquet: 438 columns covering all raw source data.
Daily Task Scheduler job runs the full pipeline from `C:\BUCKETS\prod` at 4 AM Central.

### Phase 3 — Port calculations from Sheets to code ✅ COMPLETE
- All calculated columns ported: PRF, PC, rORTG, cUSG%, on-ball/off-ball/transition splits, floor raising, scoring/playmaking decomposition
- scripts/calculate/build_season.py replaces the Google Sheets step entirely
- All 13 seasons (2013-14 through 2025-26) rebuilt and validated
- Daily pipeline now runs fully end-to-end: ingest → stage → calculate → master → deploy
- Test suite: tests/test_end_to_end.py — 1368 checks across structural, identity, rate consistency, data gating, range sanity, and master CSV (all pass)

### Phase 4 — Website improvements ⏳ NEXT
- Sortable/filterable stats tables
- Player pages, team pages
- Conditional formatting / gradients

## Important Scripts
| Script | Purpose |
|--------|---------|
| scripts/ingest/run_ingest.py | NBA.com traditional + passing totals |
| scripts/ingest/nba_playtypes.py | NBA.com play types (11 categories) |
| scripts/ingest/nba_pbpstats.py | PBPStats scoring + turnovers |
| scripts/ingest/ctg_league_avgs.py | Cleaning the Glass league averages |
| scripts/ingest/nba_tracking_shots.py | NBA.com tracking shot data |
| scripts/stage/build_stage_season.py | Merges all sources into staging parquet |
| scripts/qa/validate_stage_season.py | Validates staged data |
| scripts/calculate/build_season.py | Calculates all metrics → season CSV |
| scripts/calculate/config.py | Loads per-season config (CTG averages, constants) |
| scripts/calculate/compute_pct_ast_pts.py | Computes pct_ast_pts_in_pa constant |
| scripts/build_master.py | Combines all season CSVs into master |
| scripts/build_player_aliases.py | Builds canonical name mappings |
| scripts/local/update.ps1 | Daily pipeline orchestrator (Task Scheduler) |
| scripts/local/create_scheduled_task.ps1 | One-time Task Scheduler setup |
| tests/test_end_to_end.py | End-to-end validation suite (1368 checks) |

## Name Resolution
- NBA.com data joins on PLAYER_ID (numeric)
- PBPStats data joins via name → player_aliases.csv (source="pbp") → player_key → PLAYER_ID
- Unmatched names are logged to reports/carry_forward_report.json
- To fix: update Name Fixer CSV in mappings/name_fixer_inputs/, run build_player_aliases.py

## Environment
- Python venv at .venv/ in each clone
- requirements-ingest.txt: pandas, pyarrow, nba_api, beautifulsoup4, requests, pyyaml
- CTG_EMAIL / CTG_PASSWORD env vars optional (league averages are public)
- Windows, PowerShell, VS Code, Git

## Common Issues
- Git push/pull confusion: prod auto-commits daily; always pull before working in dev
- GitHub Actions "canceled" runs: normal when multiple commits push quickly
- Player name diacritics: handled by ASCII canonicalization in alias pipeline
- Multi-team players: aggregated by PLAYER_ID during staging
