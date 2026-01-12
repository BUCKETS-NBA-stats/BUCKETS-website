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

    path_trad = os.path.join(raw_dir, "nba_traditional_totals.parquet")
    path_pass = os.path.join(raw_dir, "nba_passing_totals.parquet")

    if not os.path.exists(path_trad):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_trad}. Run ingest first.")
    if not os.path.exists(path_pass):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_pass}. Run ingest first.")

    df_trad = pd.read_parquet(path_trad)
    df_pass = pd.read_parquet(path_pass)

    key = "PLAYER_ID"
    if key not in df_trad.columns:
        raise RuntimeError("Traditional file missing PLAYER_ID; cannot join.")

    keep_cols = {key}

    df_trad_p = prefix_except(df_trad, "nba_trad__", keep_cols)
    df_pass_p = prefix_except(df_pass, "nba_pass__", keep_cols)

    df = df_trad_p.merge(df_pass_p, on=key, how="outer")

    df.insert(0, "Season", season)
    df.insert(1, "SeasonType", season_type)

    name_col = "nba_trad__PLAYER_NAME"
    df.insert(2, "Player", df[name_col] if name_col in df.columns else "")

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
