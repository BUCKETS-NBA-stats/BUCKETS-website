import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats

T = TypeVar("T")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_parquet(df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)  # atomic replace


def with_retries(fn: Callable[[], T], label: str, attempts: int = 5) -> T:
    """
    Simple exponential backoff retry helper.
    Attempts: 1..attempts
    Sleep: 2s, 4s, 8s, 16s (etc)
    """
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


def fetch_nba_traditional_totals(season: str, season_type: str) -> pd.DataFrame:
    def _call() -> pd.DataFrame:
        resp = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="Totals",
        )
        return resp.get_data_frames()[0]

    return with_retries(_call, label="NBA leaguedashplayerstats (Totals)", attempts=5)


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

    df = fetch_nba_traditional_totals(season=season, season_type=season_type)

    out_path = os.path.join(out_dir, "nba_traditional_total
