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


def load_prev_staging(season: str, season_type_slug: str) -> pd.DataFrame | None:
    prev_path = os.path.join("assets", "data", "staging", f"{season}__{season_type_slug}.parquet")
    if not os.path.exists(prev_path):
        return None
    try:
        return pd.read_parquet(prev_path)
    except Exception:
        return None


def carry_forward_columns_from_prev(
    *,
    current: pd.DataFrame,
    prev: pd.DataFrame | None,
    key: str,
    source_prefix: str,
    base_player_name_col: str = "Player",
) -> dict:
    """
    For a given source_prefix (e.g. 'nba_pass__'):
      - Find rows where current is missing ALL source columns for that player
      - If prev has values for those columns, fill them in (last observation carried forward)
      - Return a report dict
    """
    report = {
        "source_prefix": source_prefix,
        "missing_player_ids": [],
        "carried_forward_player_ids": [],
        "not_found_in_prev_player_ids": [],
        "carried_forward_count": 0,
        "missing_count": 0,
    }

    # Identify the source columns in current
    source_cols = [c for c in current.columns if c.startswith(source_prefix)]
    if not source_cols:
        # No columns to carry forward for this source (source not merged yet)
        return report

    # Ensure a carried-forward flag exists
    flag_col = f"{source_prefix}carried_forward"
    if flag_col not in current.columns:
        current[flag_col] = False

    # Missing in source == all source cols are NA for that row
    # (We only consider rows that have a PLAYER_ID)
    mask_has_id = current[key].notna()
    mask_missing_source = mask_has_id & current[source_cols].isna().all(axis=1)

    missing_ids = current.loc[mask_missing_source, key].astype(int).tolist()
    report["missing_player_ids"] = missing_ids
    report["missing_count"] = len(missing_ids)

    if not missing_ids or prev is None:
        # Nothing to do, or no previous snapshot available
        return report

    if key not in prev.columns:
        return report

    prev_indexed = prev.set_index(key, drop=False)

    carried = []
    not_found = []

    for pid in missing_ids:
        if pid not in prev_indexed.index:
            not_found.append(pid)
            continue

        prev_row = prev_indexed.loc[pid]

        # Fill each source column if it exists in prev
        any_filled = False
        for c in source_cols:
            if c in prev_indexed.columns:
                val = prev_row[c]
                if pd.notna(val):
                    current.loc[current[key] == pid, c] = val
                    any_filled = True

        if any_filled:
            current.loc[current[key] == pid, flag_col] = True
            carried.append(pid)
        else:
            not_found.append(pid)

    report["carried_forward_player_ids"] = carried
    report["not_found_in_prev_player_ids"] = not_found
    report["carried_forward_count"] = len(carried)

    return report


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
    if key not in df_pass.columns:
        raise RuntimeError(
            "Passing file missing PLAYER_ID; this usually means you pulled team-level passing data. "
            "Fix: ensure player_or_team='Player' in the passing ingestion."
        )

    keep_cols = {key}

    # Prefix non-key columns to avoid collisions
    df_trad_p = prefix_except(df_trad, "nba_trad__", keep_cols)
    df_pass_p = prefix_except(df_pass, "nba_pass__", keep_cols)

    # Merge (outer keeps anyone that appears in either table)
    df = df_trad_p.merge(df_pass_p, on=key, how="outer")

    # Add metadata columns at front
    df.insert(0, "Season", season)
    df.insert(1, "SeasonType", season_type)

    # Add Player display name (from traditional)
    name_col = "nba_trad__PLAYER_NAME"
    df.insert(2, "Player", df[name_col] if name_col in df.columns else "")

    # ---- Carry-forward logic for missing players in Passing ----
    prev_stage = load_prev_staging(season, season_type_slug)
    carry_reports = []

    pass_report = carry_forward_columns_from_prev(
        current=df,
        prev=prev_stage,
        key=key,
        source_prefix="nba_pass__",
        base_player_name_col="Player",
    )
    carry_reports.append(pass_report)

    # ---- Write staging ----
    ensure_dir(os.path.join("assets", "data", "staging"))
    out_path = os.path.join("assets", "data", "staging", f"{season}__{season_type_slug}.parquet")
    atomic_write_parquet(df, out_path)

    # ---- Reports ----
    ensure_dir("reports")

    # Include readable player names for impacted IDs
    impacted = []
    if pass_report["missing_player_ids"]:
        tmp = df[df[key].isin(pass_report["missing_player_ids"])][[key, "Player"]].drop_duplicates()
        impacted = [
            {"PLAYER_ID": int(r[key]), "Player": str(r["Player"])}
            for _, r in tmp.iterrows()
        ]

    carry_forward_report = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "staging_path": out_path.replace("\\", "/"),
        "sources": carry_reports,
        "impacted_players": impacted,
        "notes": "If a player is missing from a secondary table today, we carry forward their last available values from the previous staging snapshot.",
    }
    with open(os.path.join("reports", "carry_forward_report.json"), "w", encoding="utf-8") as f:
        json.dump(carry_forward_report, f, indent=2)

    # refresh_status.json (include carry-forward summary)
    status = {
        "last_refresh_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "staging_path": out_path.replace("\\", "/"),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "ok": True,
        "carry_forward": {
            "nba_pass__missing": pass_report["missing_count"],
            "nba_pass__carried_forward": pass_report["carried_forward_count"],
        },
    }
    with open(os.path.join("reports", "refresh_status.json"), "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)

    print(f"Wrote staging: {out_path} ({df.shape[0]} rows, {df.shape[1]} cols)")
    if pass_report["missing_count"] > 0:
        print(
            f"[INFO] Passing carry-forward: missing={pass_report['missing_count']} "
            f"carried_forward={pass_report['carried_forward_count']}"
        )


if __name__ == "__main__":
    main()
