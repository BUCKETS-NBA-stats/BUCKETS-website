import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashptstats

T = TypeVar("T")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_parquet(df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)


def with_retries(fn: Callable[[], T], label: str, attempts: int = 5) -> T:
    last_err: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i == attempts:
                break
            wait = 2 ** i
            print(f"[WARN] {label} failed (attempt {i}/{attempts}): {e}")
            print(f"[WARN] Waiting {wait}s then retrying...")
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def fetch_traditional_totals(season: str, season_type: str) -> pd.DataFrame:
    def _call() -> pd.DataFrame:
        resp = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="Totals",
        )
        return resp.get_data_frames()[0]

    return with_retries(_call, "NBA traditional totals", attempts=5)


def fetch_passing_totals(season: str, season_type: str) -> pd.DataFrame:
    """
    This corresponds to the NBA.com player tracking passing dashboard.
    nba_api endpoint: LeagueDashPtStats
    We request player-tracking 'Passing' measure type.
    """
    def _call() -> pd.DataFrame:
        resp = leaguedashptstats.LeagueDashPtStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
            pt_measure_type="Passing",
        )
        return resp.get_data_frames()[0]

    return with_retries(_call, "NBA passing totals", attempts=5)


def fetch_touches_totals(season: str, season_type: str) -> pd.DataFrame:
    """
    NBA.com player tracking touches dashboard.
    """
    def _call() -> pd.DataFrame:
        resp = leaguedashptstats.LeagueDashPtStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
            pt_measure_type="Touches",
        )
        return resp.get_data_frames()[0]

    return with_retries(_call, "NBA touches totals", attempts=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help='e.g. "2025-26"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    out_dir = os.path.join("assets", "data", "raw", season, season_type_slug)
    ensure_dir(out_dir)

    manifest_files = []

    # 1) Traditional totals
    df_trad = fetch_traditional_totals(season, season_type)
    path_trad = os.path.join(out_dir, "nba_traditional_totals.parquet")
    atomic_write_parquet(df_trad, path_trad)
    manifest_files.append(("nba_traditional_totals", path_trad, df_trad))

    # 2) Passing totals
    df_pass = fetch_passing_totals(season, season_type)
    path_pass = os.path.join(out_dir, "nba_passing_totals.parquet")
    atomic_write_parquet(df_pass, path_pass)
    manifest_files.append(("nba_passing_totals", path_pass, df_pass))

    # 3) Touches totals
    df_touch = fetch_touches_totals(season, season_type)
    path_touch = os.path.join(out_dir, "nba_touches_totals.parquet")
    atomic_write_parquet(df_touch, path_touch)
    manifest_files.append(("nba_touches_totals", path_touch, df_touch))

    ensure_dir("reports")
    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "raw_files": [
            {
                "id": fid,
                "path": fpath.replace("\\", "/"),
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
            }
            for (fid, fpath, df) in manifest_files
        ],
    }
    with open(os.path.join("reports", "ingest_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print("Wrote raw NBA files:")
    for (fid, fpath, df) in manifest_files:
        print(f" - {fid}: {fpath} ({df.shape[0]} rows, {df.shape[1]} cols)")


if __name__ == "__main__":
    main()
