"""
batch_build_historical.py

Runs the full ingest + staging + compute pipeline for a list of seasons.
Logs errors per step and continues on failure. Prints a summary table at the end.

Usage:
    python scripts/local/batch_build_historical.py
    python scripts/local/batch_build_historical.py --seasons 2013-14 2017-18 2021-22
    python scripts/local/batch_build_historical.py --delay 5
"""

import json
import os
import subprocess
import sys
import time
from datetime import datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PYTHON = os.path.join(REPO_ROOT, ".venv", "Scripts", "python.exe")
SEASON_TYPE = "Regular Season"
SEASON_TYPE_SLUG = "regular"

SEASONS = [
    "2013-14", "2014-15", "2015-16", "2016-17", "2017-18",
    "2018-19", "2019-20", "2020-21", "2021-22", "2022-23",
    "2023-24", "2024-25", "2025-26",
]

STEPS = [
    ("run_ingest",        ["scripts/ingest/run_ingest.py"]),
    ("nba_playtypes",     ["scripts/ingest/nba_playtypes.py"]),
    ("nba_pbpstats",      ["scripts/ingest/nba_pbpstats.py"]),
    ("ctg_league_avgs",   ["scripts/ingest/ctg_league_avgs.py"]),
    ("nba_tracking_shots",["scripts/ingest/nba_tracking_shots.py"]),
    ("build_stage",       ["scripts/stage/build_stage_season.py"]),
    ("validate_stage",    ["scripts/qa/validate_stage_season.py"]),
    ("compute_pct_ast",   ["scripts/calculate/compute_pct_ast_pts.py"]),
]

LOG_PATH = os.path.join(REPO_ROOT, "logs", "batch_build_historical.log")


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}  {msg}"
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def run_step(step_name: str, script_args: list[str], season: str) -> tuple[bool, str]:
    cmd = [PYTHON] + script_args + ["--season", season, "--season-type", SEASON_TYPE]
    log(f"  [{step_name}] Running: {' '.join(cmd[1:])}")
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=300,  # 5 min per step
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip().splitlines()
            short_err = err[-1] if err else f"exit code {result.returncode}"
            log(f"  [{step_name}] FAILED: {short_err}")
            return False, short_err
        log(f"  [{step_name}] OK")
        return True, ""
    except subprocess.TimeoutExpired:
        msg = "Timed out after 5 minutes"
        log(f"  [{step_name}] FAILED: {msg}")
        return False, msg
    except Exception as e:
        log(f"  [{step_name}] FAILED: {e}")
        return False, str(e)


def read_staging_info(season: str) -> tuple[int, int]:
    """Return (rows, cols) from staging parquet, or (-1, -1) on error."""
    try:
        import pandas as pd
        path = os.path.join(REPO_ROOT, "assets", "data", "staging",
                            f"{season}__{SEASON_TYPE_SLUG}.parquet")
        if not os.path.exists(path):
            return -1, -1
        df = pd.read_parquet(path)
        return int(df.shape[0]), int(df.shape[1])
    except Exception:
        return -1, -1


def read_pct_ast(season: str) -> str:
    try:
        path = os.path.join(REPO_ROOT, "assets", "data", "raw", season,
                            SEASON_TYPE_SLUG, "pct_ast_pts_in_pa.json")
        if not os.path.exists(path):
            return "n/a"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return f"{data['values']['PCT_AST_PTS_IN_PA']:.4f}"
    except Exception:
        return "error"


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", nargs="+", default=None,
                        help="Subset of seasons to run (default: all)")
    parser.add_argument("--delay", type=int, default=2,
                        help="Seconds to sleep between seasons (default: 2)")
    args = parser.parse_args()

    seasons = args.seasons if args.seasons else SEASONS
    inter_delay = args.delay

    # Validate any explicitly requested seasons
    for s in seasons:
        if s not in SEASONS:
            print(f"[ERROR] Unknown season: {s}. Valid: {SEASONS}")
            return

    os.makedirs(os.path.join(REPO_ROOT, "logs"), exist_ok=True)
    log("=" * 70)
    log(f"batch_build_historical.py  --  {len(seasons)} seasons  (delay={inter_delay}s)")
    log("=" * 70)

    results: dict[str, dict] = {}

    for i, season in enumerate(seasons):
        log(f"\n{'='*70}")
        log(f"Season {i+1}/{len(seasons)}: {season}")
        log(f"{'='*70}")

        season_result = {
            "steps": {},
            "failed_step": None,
        }

        for step_name, script_args in STEPS:
            ok, err = run_step(step_name, script_args, season)
            season_result["steps"][step_name] = {"ok": ok, "err": err}
            if not ok:
                if season_result["failed_step"] is None:
                    season_result["failed_step"] = step_name
                # Continue running remaining steps even if one fails,
                # unless it's a dependency (staging needs ingest, compute needs staging)
                # Stop season on: build_stage or validate_stage failure (downstream will be wrong)
                if step_name in ("build_stage", "validate_stage"):
                    log(f"  Stopping season {season} -- staging step failed.")
                    break

        rows, cols = read_staging_info(season)
        pct_ast    = read_pct_ast(season)
        season_result["rows"]    = rows
        season_result["cols"]    = cols
        season_result["pct_ast"] = pct_ast

        all_ok = all(v["ok"] for v in season_result["steps"].values())
        season_result["overall_ok"] = all_ok

        results[season] = season_result

        if i < len(seasons) - 1:
            log(f"\n  Sleeping {inter_delay}s before next season...")
            time.sleep(inter_delay)

    # ── Summary table (all 13 seasons, merging prior results for skipped ones) ─
    # Load prior results for seasons not in this run
    prior_summary_path = os.path.join(REPO_ROOT, "reports", "batch_build_historical.json")
    prior_results: dict[str, dict] = {}
    if os.path.exists(prior_summary_path) and seasons != SEASONS:
        try:
            with open(prior_summary_path, encoding="utf-8") as f:
                prior_data = json.load(f)
            prior_results = prior_data.get("seasons", {})
        except Exception:
            pass

    all_results = {}
    for s in SEASONS:
        if s in results:
            all_results[s] = results[s]
        elif s in prior_results:
            all_results[s] = prior_results[s]

    log("\n")
    log("=" * 70)
    log("SUMMARY (all seasons)")
    log("=" * 70)
    header = f"{'Season':<10}  {'Status':<6}  {'Rows':>5}  {'Cols':>4}  {'PCT_AST_PTS':>12}  Failed step"
    log(header)
    log("-" * 70)
    for season, r in all_results.items():
        status    = "PASS" if r["overall_ok"] else "FAIL"
        rows      = str(r["rows"]) if r["rows"] >= 0 else "n/a"
        cols      = str(r["cols"]) if r["cols"] >= 0 else "n/a"
        pct       = r["pct_ast"]
        failed_at = r["failed_step"] or ""
        log(f"{season:<10}  {status:<6}  {rows:>5}  {cols:>4}  {pct:>12}  {failed_at}")

    log("=" * 70)
    n_pass = sum(1 for r in all_results.values() if r.get("overall_ok"))
    log(f"Result: {n_pass}/{len(all_results)} seasons fully passed.")

    # Write machine-readable summary (full 13-season view)
    summary_path = os.path.join(REPO_ROOT, "reports", "batch_build_historical.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "seasons": all_results,
        }, f, indent=2)
    log(f"Summary JSON: {summary_path}")


if __name__ == "__main__":
    main()
