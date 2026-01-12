import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats, leaguedashptstats

from scripts.ingest.nba_policy import validate_player_totals_df

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
    """
    NBA traditional dashboard (player-level by nature), Totals, unfiltered.
    """
    def _call() -> pd.DataFrame:
        resp = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="Totals",  # <-- totals
            # We rely on defaults for "no filters" (blank/0 values),
            # and we do NOT set any filters like TeamID, LastNGames, Month, etc.
        )
        return resp.get_data_frames()[0]

    df = with_retries(_call, "NBA traditional totals", attempts=5)
    validate_player_totals_df(df, "nba_traditional_totals", min_rows=150)
    return df


def fetch_passing_totals(season: str, season_type: str) -> pd.DataFrame:
    """
    NBA player tracking -> Passing, Totals, PLAYER-level.
    Key requirement: player_or_team="Player"
    """
    def _call() -> pd.DataFrame:
        resp = leaguedashptstats.LeagueDashPtStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",     # <-- totals
            pt_measure_type="Passing",
            player_or_team="Player",      # <-- player-level
            # No filters
        )
        return resp.get_data_frames()[0]

    df = with_retries(_call, "NBA passing totals", attempts=5)
    validate_player_totals_df(df, "nba_passing_totals", min_rows=150)
    return df


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help='e.g. "2025-26"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    out_dir = os.path.join("assets", "data", "raw", season, season_type_slug)
    ensure_dir(out_dir)
    ensure_dir("reports")

    sources = [
        {
            "id": "nba_traditional_totals",
            "filename": "nba_traditional_totals.parquet",
            "fetch": lambda: fetch_traditional_totals(season, season_type),
        },
        {
            "id": "nba_passing_totals",
            "filename": "nba_passing_totals.parquet",
            "fetch": lambda: fetch_passing_totals(season, season_type),
        },
    ]

    manifest_files = []

    for src in sources:
        fid = src["id"]
        fpath = os.path.join(out_dir, src["filename"])
        df = src["fetch"]()
        atomic_write_parquet(df, fpath)
        manifest_files.append(
            {
                "id": fid,
                "path": fpath.replace("\\", "/"),
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
                "ok": True,
                "error": None,
            }
        )
        print(f"[OK] Wrote {fid}: {fpath} ({df.shape[0]} rows, {df.shape[1]} cols)")

    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "raw_dir": out_dir.replace("\\", "/"),
        "raw_files": manifest_files,
        "ok": True,
        "policy": {
            "level": "player",
            "mode": "totals",
            "filters": "none",
        },
    }

    with open(os.path.join("reports", "ingest_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
