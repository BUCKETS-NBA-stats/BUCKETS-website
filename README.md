# Phase 0 Starter Kit (NBA stats website)

This helps you stop manually combining seasons into one master CSV.

## Folder expectations

Put your per-season CSV exports here:

- `site/data/season/2013-14.csv`
- `site/data/season/2014-15.csv`
- ...
- `site/data/season/2024-25.csv`

Then the script will generate:

- `site/data/master.csv`

Your GitHub Pages website should read `site/data/master.csv`.

## Run locally

1. Install Python 3 (3.10+ is fine)
2. In your repo folder:

```bash
pip install -r requirements.txt
python scripts/build_master.py
```

## What it checks
- Every season file has the exact same columns (and order) as the contract in `contracts/league_table_contract.json`
- Warns if there are duplicate `(Player, Year, Season type, Tm)` rows

## GitHub Actions
A workflow is included that can rebuild `master.csv` automatically on push and commit it back to the repo.
