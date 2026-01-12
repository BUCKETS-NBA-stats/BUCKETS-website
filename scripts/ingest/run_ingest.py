import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_parquet(df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)  # atomic replace


def fetch_nba_traditional_totals(season: str, season_type: str) -> pd.DataFrame:
    resp = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star=season_type,
        per_mode_detailed="Totals",
    )
    return resp.get_data_frames()[0]


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

    out_path = os.path.join(out_dir, "nba_traditional_totals.parquet")
    atomic_write_parquet(df, out_path)

    ensure_dir("reports")
    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "raw_files": [
            {
                "id": "nba_traditional_totals",
                "path": out_path.replace("\\", "/"),
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
            }
        ],
    }
    with open(os.path.join("reports", "ingest_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"Wrote raw NBA traditional totals: {out_path} ({df.shape[0]} rows, {df.shape[1]} cols)")


if __name__ == "__main__":
    main()
