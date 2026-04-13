"""
Phase 4 diagnostic: Transition Playmaking — checksum + CSV divergence report.

Pass/fail checks (internal integrity only):
    scoring_points_created + playmaking_points_created ≈ points_created  (checksum)

Diagnostic only (not pass/fail):
    points_created / GP  vs  CSV "Transition PC/g"

WHY the CSV comparison is diagnostic only:
    Phase 4 uses est_tr_pm_plays_final and est_tr_pm_pts from Phase 3b, which
    come from the HC/TR regression split.  That split intentionally differs from
    the original Google Sheets methodology.  The mismatch flows through to
    Transition PC/g, so systematic divergences from the CSV are expected and are
    not bugs to fix.

Known divergence pattern (2024-25):
    - Most players are computed higher than the CSV (computed > csv by ~0.5–1.5/g).
    - High-PPPA passers (Jokic, Giannis) show the largest positive deltas.
    - Many CSV values are negative (player below transition baseline); our values
      rarely go negative, reflecting the different baseline methodology.
"""

import os
import re
import sys
import unicodedata

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.calculate.build_season import (
    PLAYTYPES,
    compute_scoring_share_of_tov,
    phase1a_playtype,
    phase2_transition,
    phase3a_passing,
    phase3b_passing,
    phase4_transition,
)
from scripts.calculate.config import build_config

SEASON           = "2024-25"
SEASON_TYPE_SLUG = "regular"

STAGING_PATH = os.path.join(REPO_ROOT, "assets", "data", "staging",
                             f"{SEASON}__{SEASON_TYPE_SLUG}.parquet")
CSV_PATH     = os.path.join(REPO_ROOT, "assets", "data", "season", "league-table-2025.csv")
CTG_PATH     = os.path.join(REPO_ROOT, "assets", "data", "raw",
                             SEASON, SEASON_TYPE_SLUG, "ctg_league_averages.json")

TOLERANCE_PPG   = 0.15
MIN_GP_FRACTION = 0.80
PRIMARY_SLUG    = "transition"


def normalize_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def main() -> int:
    print(f"Staging : {STAGING_PATH}")
    print(f"CSV     : {CSV_PATH}")

    df_stg = pd.read_parquet(STAGING_PATH)
    df_csv = pd.read_csv(CSV_PATH)
    cfg    = build_config(CTG_PATH)

    df_stg = compute_scoring_share_of_tov(df_stg)

    # Phase 1a (needed by Phase 3a)
    pt1a: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        pt1a[pt["slug"]] = phase1a_playtype(df_stg, pt["slug"])

    # Phase 2
    pt2, tr_agg = phase2_transition(df_stg, cfg)

    # Phase 3a
    pt3a, agg3a = phase3a_passing(df_stg, pt1a, pt2, cfg)

    # Phase 3b
    pt3b = phase3b_passing(pt3a, agg3a, tr_agg["TR_PPP_RATIO"], cfg)

    # Phase 4
    pt4 = phase4_transition(pt2, pt3b, tr_agg, cfg)

    # ── Integrity: decomposition checksum ────────────────────────────────────
    checksum = (
        pt4["scoring_points_created"] + pt4["playmaking_points_created"]
        - pt4["points_created"]
    ).abs()
    bad_checksum = pt4[checksum > 0.01]
    if not bad_checksum.empty:
        print(f"[FAIL] Checksum broken for {len(bad_checksum)} players  "
              f"(max error={checksum.max():.4f})")
    else:
        print(f"[OK  ] Scoring + Playmaking PC == Total PC for all {len(pt4)} players")

    # ── Name / GP lookups ─────────────────────────────────────────────────────
    df_stg["_key"] = df_stg["Player"].apply(normalize_name)
    df_csv["_key"] = df_csv["Player"].apply(normalize_name)

    pid_to_key: dict[int, str] = (
        df_stg.dropna(subset=["PLAYER_ID"])
        .set_index("PLAYER_ID")["_key"]
        .to_dict()
    )
    trad_gp: dict[int, float] = (
        df_stg.dropna(subset=["PLAYER_ID", "nba_trad__GP"])
        .set_index("PLAYER_ID")["nba_trad__GP"]
        .to_dict()
    )
    primary_gp_col = f"nba_pt_{PRIMARY_SLUG}__GP"
    primary_gp: dict[int, float] = {
        int(r["PLAYER_ID"]): float(r[primary_gp_col])
        for _, r in df_stg[df_stg[primary_gp_col].notna()][
            ["PLAYER_ID", primary_gp_col]
        ].iterrows()
    }

    # ── Join against CSV ──────────────────────────────────────────────────────
    pt4["_key"]    = pt4["PLAYER_ID"].map(pid_to_key)
    pt4["trad_gp"] = pt4["PLAYER_ID"].map(trad_gp)
    pt4["pt_gp"]   = pt4["PLAYER_ID"].map(
        lambda pid: primary_gp.get(int(pid))
    )

    csv_tr = (
        df_csv[df_csv["Has data? Transition"] == True][["_key", "Transition PC/g"]]
        .copy()
        .rename(columns={"Transition PC/g": "csv_pcpg"})
        .dropna(subset=["csv_pcpg"])
    )

    joined = pt4.merge(csv_tr, on="_key", how="inner")
    joined = joined[joined["trad_gp"].notna() & joined["pt_gp"].notna()]

    joined["computed_pcpg"] = joined["points_created"] / joined["trad_gp"]
    joined["gp_fraction"]   = joined["pt_gp"] / joined["trad_gp"].clip(lower=1)

    complete   = joined[joined["gp_fraction"] >= MIN_GP_FRACTION].copy()
    incomplete = joined[joined["gp_fraction"] <  MIN_GP_FRACTION]

    complete["diff"] = (complete["computed_pcpg"] - complete["csv_pcpg"]).abs()

    matched    = int((complete["diff"] <= TOLERANCE_PPG).sum())
    tested     = len(complete)
    mismatches = complete[complete["diff"] > TOLERANCE_PPG].copy()

    print(f"\n[INFO] Transition PC/g divergence report (methodology differs from CSV — expected)")
    print(f"       Players with data : {tested}")
    print(f"       Players skipped   : {len(incomplete)}  (incomplete multi-team data)")
    print(f"       Within ±{TOLERANCE_PPG}        : {matched}")
    print(f"       Outside ±{TOLERANCE_PPG}       : {len(mismatches)}")
    if not mismatches.empty:
        print(f"\n  Top divergences (computed - csv):")
        for _, row in mismatches.sort_values("diff", ascending=False).head(15).iterrows():
            sign = "+" if row["computed_pcpg"] > row["csv_pcpg"] else ""
            delta = row["computed_pcpg"] - row["csv_pcpg"]
            print(f"    {row['_key']}: computed={row['computed_pcpg']:.3f}  "
                  f"csv={row['csv_pcpg']:.2f}  delta={sign}{delta:.3f}  "
                  f"pc={row['points_created']:.1f}  gp={row['trad_gp']:.0f}")

    # ── Scoring/Playmaking split vs CSV (diagnostic, not pass/fail) ───────────
    for split_col, csv_col in [
        ("scoring_points_created",    "Transition Scoring PC/g"),
        ("playmaking_points_created", "Transition Playmaking PC/g"),
    ]:
        if csv_col not in df_csv.columns:
            continue
        csv_split = (
            df_csv[df_csv["Has data? Transition"] == True][["_key", csv_col]]
            .copy()
            .rename(columns={csv_col: "csv_split"})
            .dropna(subset=["csv_split"])
        )
        j2 = complete.merge(csv_split, on="_key", how="inner")
        if j2.empty:
            continue
        j2["computed_split"] = j2[split_col] / j2["trad_gp"]
        j2["split_diff"] = (j2["computed_split"] - j2["csv_split"]).abs()
        split_matched = int((j2["split_diff"] <= TOLERANCE_PPG).sum())
        print(f"\n[INFO] {csv_col}: {split_matched}/{len(j2)} within ±{TOLERANCE_PPG}  "
              f"(diagnostic only — split methodology may differ)")

    print(f"\n{'='*52}")
    if bad_checksum.empty:
        print("PASS — Phase 4 decomposition checksum holds for all players")
        return 0
    else:
        print(f"FAIL — Checksum broken for {len(bad_checksum)} players")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
