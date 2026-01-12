import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_parquet(df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--season-type", required=True)
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    raw_path = os.path.join(
        "assets", "data", "raw", season, season_type_slug, "nba_traditional_totals.parquet"
    )
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Missing raw file: {raw_path}. Run ingest first.")

    df = pd.read_parquet(raw_path)

    # Minimal staging additions
    df.insert(0, "Season", season)
    df.insert(1, "SeasonType", season_type)

    # Use NBA's display name for now. We'll swap to your canonical mapping later.
    if "PLAYER_NAME" in df.columns:
        df.insert(2, "Player", df["PLAYER_NAME"])
    else:
        df.insert(2, "Player", "")

    ensure_dir(os.path.join("assets", "data", "staging"))
    out_path = os.path.join("assets", "data", "staging", f"{season}__{season_type_slug}.parquet")
    atomic_write_parquet(df, out_path)

    ensure_dir("reports")
    status = {
        "last_refresh_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "staging_path": out_path.replace("\\", "/"),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "ok": True,
    }
    with open(os.path.join("reports", "refresh_status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"Wrote staging: {out_path} ({df.shape[0]} rows, {df.shape[1]} cols)")


if __name__ == "__main__":
    main()
