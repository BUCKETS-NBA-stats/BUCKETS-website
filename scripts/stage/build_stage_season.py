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


def prefix_except(df: pd.DataFrame, prefix: str, keep: set[str]) -> pd.DataFrame:
    rename = {}
    for c in df.columns:
        if c in keep:
            continue
        rename[c] = f"{prefix}{c}"
    return df.rename(columns=rename)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--season-type", required=True)
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    raw_dir = os.path.join("assets", "data", "raw", season, season_type_slug)

    paths = {
        "trad": os.path.join(raw_dir, "nba_traditional_totals.parquet"),
        "pass": os.path.join(raw_dir, "nba_passing_totals.parquet"),
        "touch": os.path.join(raw_dir, "nba_touches_totals.parquet"),
    }

    for k, p in paths.items():
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing raw file for {k}: {p}. Run ingest first.")

    df_trad = pd.read_parquet(paths["trad"])
    df_pass = pd.read_parquet(paths["pass"])
    df_touch = pd.read_parquet(paths["touch"])

    # Common join key we expect from NBA endpoints
    key = "PLAYER_ID"
    if key not in df_trad.columns:
        raise RuntimeError("Traditional file missing PLAYER_ID; cannot join.")

    keep_cols = {key}

    # Use NBA name from traditional as display name for now
    player_name_col = "PLAYER_NAME" if "PLAYER_NAME" in df_trad.columns else None

    # Prefix all non-key columns to avoid collisions
    df_trad_p = prefix_except(df_trad, "nba_trad__", keep_cols)
    df_pass_p = prefix_except(df_pass, "nba_pass__", keep_cols)
    df_touch_p = prefix_except(df_touch, "nba_touch__", keep_cols)

    # Merge
    df = df_trad_p.merge(df_pass_p, on=key, how="outer").merge(df_touch_p, on=key, how="outer")

    # Add metadata columns at front
    df.insert(0, "Season", season)
    df.insert(1, "SeasonType", season_type)

    # Add Player display name at front
    if player_name_col and player_name_col in df_trad.columns:
        # df_trad_p has prefixed column name for PLAYER_NAME
        # It would now be nba_trad__PLAYER_NAME
        name_col = "nba_trad__PLAYER_NAME"
        if name_col in df.columns:
            df.insert(2, "Player", df[name_col])
        else:
            df.insert(2, "Player", "")
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
