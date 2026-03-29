import argparse
import json
import os
import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd

ALIASES_PATH = os.path.join("mappings", "player_aliases.csv")

PLAY_TYPE_PREFIX_MAP = {
    "Isolation":    "iso",
    "PRBallHandler": "prballhandler",
    "PRRollman":    "prrollman",
    "Postup":       "postup",
    "Spotup":       "spotup",
    "Handoff":      "handoff",
    "Cut":          "cut",
    "OffScreen":    "offscreen",
    "OffRebound":   "offrebound",
    "Misc":         "misc",
    "Transition":   "transition",
}

# Columns to select per play type from the raw long-format parquet.
# The API may return these as-is or with slight name differences (e.g. TOV_POSS_PCT vs TO).
# Only columns that actually exist in the DataFrame will be selected.
PT_STAT_COLS = ["GP", "POSS", "PPP", "PTS", "FGA", "FGM", "TOV_POSS_PCT", "SCORE_POSS_PCT", "PERCENTILE"]


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


_PT_COUNT_COLS = {"POSS", "PTS", "FGM", "FGA", "GP"}


def _aggregate_playtypes(df_pt: pd.DataFrame, key: str, available_stat_cols: list[str]) -> pd.DataFrame:
    """
    Traded players appear once per team per play type. Consolidate to one row
    per PLAYER_ID × PLAY_TYPE by summing counting stats and taking the dominant
    team's rates (highest POSS row wins for PPP, TOV_POSS_PCT, SCORE_POSS_PCT,
    PERCENTILE). PPP is then recalculated from the summed PTS/POSS totals.
    """
    count_cols = [c for c in available_stat_cols if c in _PT_COUNT_COLS]
    rate_cols  = [c for c in available_stat_cols if c not in _PT_COUNT_COLS]

    # Sort so dominant team (most POSS) is first — used for rate 'first' aggregation
    sort_col = "POSS" if "POSS" in df_pt.columns else key
    df_sorted = df_pt.sort_values(sort_col, ascending=False)

    agg: dict = {c: "sum" for c in count_cols}
    agg.update({c: "first" for c in rate_cols})

    df_agg = df_sorted.groupby([key, "PLAY_TYPE"], as_index=False).agg(agg)

    # Recalculate PPP from season totals if both columns are available
    if "PPP" in rate_cols and "PTS" in count_cols and "POSS" in count_cols:
        df_agg["PPP"] = (df_agg["PTS"] / df_agg["POSS"].replace(0, float("nan"))).round(3)

    multi_team_count = int((df_pt.groupby([key, "PLAY_TYPE"]).size() > 1).sum())
    if multi_team_count > 0:
        print(f"[INFO] Play-type: aggregated {multi_team_count} multi-team player×play_type rows into season totals")

    return df_agg


def pivot_playtypes_to_wide(df_pt: pd.DataFrame, key: str = "PLAYER_ID") -> pd.DataFrame:
    """
    Convert long-format play-type DataFrame (one row per player × play_type)
    to wide format (one row per player, columns like nba_pt_iso__POSS).
    Multi-team players are aggregated to season totals before pivoting.
    Only columns in PT_STAT_COLS that exist in df_pt are included.
    """
    available_stat_cols = [c for c in PT_STAT_COLS if c in df_pt.columns]
    missing_stat_cols = [c for c in PT_STAT_COLS if c not in df_pt.columns]
    if missing_stat_cols:
        print(f"[WARN] play-type stat columns not found in raw data (may use different names): {missing_stat_cols}")

    df_pt = _aggregate_playtypes(df_pt, key, available_stat_cols)

    frames = []
    for api_name, slug in PLAY_TYPE_PREFIX_MAP.items():
        sub = df_pt[df_pt["PLAY_TYPE"] == api_name][[key] + available_stat_cols].copy()
        sub = sub.rename(columns={c: f"nba_pt_{slug}__{c}" for c in available_stat_cols})
        frames.append(sub)

    df_wide = frames[0]
    for f in frames[1:]:
        df_wide = df_wide.merge(f, on=key, how="outer")

    return df_wide


def _make_player_key(s: str) -> str:
    """Normalize a player name to a lookup key: accent-strip, lowercase, alphanumeric only.
    Mirrors make_key() in scripts/build_player_aliases.py."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    for bad, good in [("Ł", "L"), ("ł", "l"), ("Đ", "D"), ("đ", "d"),
                      ("Ø", "O"), ("ø", "o"), ("ß", "ss")]:
        s = s.replace(bad, good)
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def load_pbp_alias_lookup(aliases_path: str) -> dict[str, str]:
    """Returns {source_key: player_key} for source='pbp' rows in player_aliases.csv.
    source_key is the pre-normalized PBP name; player_key is the canonical player key."""
    aliases = pd.read_csv(aliases_path)
    pbp = aliases[aliases["source"] == "pbp"]
    return dict(zip(pbp["source_key"].astype(str), pbp["player_key"].astype(str)))


def resolve_pbp_names(
    df_pbp: pd.DataFrame,
    staging_df: pd.DataFrame,
    pbp_lookup: dict[str, str],
) -> tuple[pd.DataFrame, list[str]]:
    """
    Add PLAYER_ID to a PBPStats DataFrame by resolving player names:
      1. Normalize PBP 'Name' → pbp_key via _make_player_key
      2. Look up pbp_key in pbp_lookup → player_key (fallback: use pbp_key directly)
      3. Build {player_key: PLAYER_ID} from the current staging df
      4. Map player_key → PLAYER_ID
    Returns (df_with_player_id, list_of_unmatched_names).
    """
    df_pbp = df_pbp.copy()
    df_pbp["_pbp_key"] = df_pbp["Name"].apply(
        lambda n: _make_player_key(str(n)) if pd.notna(n) else ""
    )
    df_pbp["_player_key"] = df_pbp["_pbp_key"].map(pbp_lookup)
    # Fallback: if no alias entry, use the normalized name directly as player_key
    df_pbp["_player_key"] = df_pbp["_player_key"].fillna(df_pbp["_pbp_key"])

    # Build player_key → PLAYER_ID map from staging (Player column is display name)
    staging_df = staging_df.copy()
    staging_df["_player_key"] = staging_df["Player"].apply(
        lambda n: _make_player_key(str(n)) if pd.notna(n) else ""
    )
    key_to_pid = (
        staging_df.dropna(subset=["PLAYER_ID"])
        .set_index("_player_key")["PLAYER_ID"]
        .to_dict()
    )

    df_pbp["PLAYER_ID"] = df_pbp["_player_key"].map(key_to_pid)

    unmatched = (
        df_pbp[df_pbp["PLAYER_ID"].isna()]["Name"]
        .dropna()
        .unique()
        .tolist()
    )
    if unmatched:
        safe = [n.encode("ascii", errors="replace").decode("ascii") for n in unmatched[:10]]
        print(f"[WARN] PBPStats: {len(unmatched)} names could not be resolved to a PLAYER_ID: {safe}")

    df_pbp = df_pbp.drop(columns=["_pbp_key", "_player_key"])
    return df_pbp, [str(n) for n in unmatched]


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
    path_pt   = os.path.join(raw_dir, "nba_playtypes.parquet")
    path_pbp  = os.path.join(raw_dir, "nba_pbpstats.parquet")
    path_ctg  = os.path.join(raw_dir, "ctg_league_averages.json")

    if not os.path.exists(path_trad):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_trad}. Run ingest first.")
    if not os.path.exists(path_pass):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_pass}. Run ingest first.")
    if not os.path.exists(path_pt):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_pt}. Run playtypes ingest first.")
    if not os.path.exists(path_pbp):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_pbp}. Run pbpstats ingest first.")
    if not os.path.exists(path_ctg):
        raise FileNotFoundError(f"Missing REQUIRED raw file: {path_ctg}. Run ctg_league_avgs ingest first.")

    df_trad = pd.read_parquet(path_trad)
    df_pass = pd.read_parquet(path_pass)
    df_pt   = pd.read_parquet(path_pt)
    df_pbp  = pd.read_parquet(path_pbp)
    with open(path_ctg, encoding="utf-8") as f:
        ctg_data = json.load(f)
    ctg_values = ctg_data["values"]

    key = "PLAYER_ID"
    if key not in df_trad.columns:
        raise RuntimeError("Traditional file missing PLAYER_ID; cannot join.")
    if key not in df_pass.columns:
        raise RuntimeError(
            "Passing file missing PLAYER_ID; this usually means you pulled team-level passing data. "
            "Fix: ensure player_or_team='Player' in the passing ingestion."
        )
    if key not in df_pt.columns:
        raise RuntimeError(
            "Play-type file missing PLAYER_ID; this usually means player_or_team_abbreviation was not 'P'."
        )
    if "PLAY_TYPE" not in df_pt.columns:
        raise RuntimeError("Play-type file missing PLAY_TYPE column; was it assembled correctly?")
    if "Name" not in df_pbp.columns:
        raise RuntimeError("PBPStats file missing Name column; cannot perform name matching.")

    keep_cols = {key}

    # Prefix non-key columns to avoid collisions
    df_trad_p = prefix_except(df_trad, "nba_trad__", keep_cols)
    df_pass_p = prefix_except(df_pass, "nba_pass__", keep_cols)

    # Merge trad + pass (outer keeps anyone that appears in either table)
    df = df_trad_p.merge(df_pass_p, on=key, how="outer")

    # Pivot play-type data from long to wide and merge
    df_pt_wide = pivot_playtypes_to_wide(df_pt, key=key)
    df = df.merge(df_pt_wide, on=key, how="outer")

    # Add metadata columns at front
    df.insert(0, "Season", season)
    df.insert(1, "SeasonType", season_type)

    # Add Player display name (from traditional)
    name_col = "nba_trad__PLAYER_NAME"
    df.insert(2, "Player", df[name_col] if name_col in df.columns else "")

    # ---- PBPStats name resolution + merge ----
    # Must happen after Player column is set (used for player_key → PLAYER_ID lookup)
    pbp_lookup = load_pbp_alias_lookup(ALIASES_PATH)
    df_pbp, unmatched_pbp = resolve_pbp_names(df_pbp, df, pbp_lookup)
    # Drop PBP rows that couldn't be resolved to a PLAYER_ID — they can't be joined
    df_pbp = df_pbp[df_pbp[key].notna()].copy()
    df_pbp_p = prefix_except(df_pbp, "pbp__", keep_cols)
    df = df.merge(df_pbp_p, on=key, how="outer")

    # ---- CTG league-average constants (same value for every player row) ----
    df["ctg__tov_pct"]       = ctg_values["tov_pct"]
    df["ctg__hc_pts_per_play"] = ctg_values["hc_pts_per_play"]
    df["ctg__hc_oreb_pct"]   = ctg_values["hc_oreb_pct"]
    df["ctg__pb_pts_per_play"] = ctg_values["pb_pts_per_play"]

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

    pt_report = carry_forward_columns_from_prev(
        current=df,
        prev=prev_stage,
        key=key,
        source_prefix="nba_pt_",
        base_player_name_col="Player",
    )
    carry_reports.append(pt_report)

    pbp_report = carry_forward_columns_from_prev(
        current=df,
        prev=prev_stage,
        key=key,
        source_prefix="pbp__",
        base_player_name_col="Player",
    )
    carry_reports.append(pbp_report)

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
        "pbp_unmatched_names": unmatched_pbp,
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
            "nba_pt__missing": pt_report["missing_count"],
            "nba_pt__carried_forward": pt_report["carried_forward_count"],
            "pbp__missing": pbp_report["missing_count"],
            "pbp__carried_forward": pbp_report["carried_forward_count"],
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
    if pt_report["missing_count"] > 0:
        print(
            f"[INFO] Play-type carry-forward: missing={pt_report['missing_count']} "
            f"carried_forward={pt_report['carried_forward_count']}"
        )
    if pbp_report["missing_count"] > 0:
        print(
            f"[INFO] PBPStats carry-forward: missing={pbp_report['missing_count']} "
            f"carried_forward={pbp_report['carried_forward_count']}"
        )
    if unmatched_pbp:
        print(f"[INFO] PBPStats unmatched names ({len(unmatched_pbp)}): see carry_forward_report.json")


if __name__ == "__main__":
    main()
