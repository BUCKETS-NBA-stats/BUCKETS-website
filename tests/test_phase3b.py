"""
Phase 3b diagnostic: HC Playmaking PAB vs CSV On-ball PC/g.

This script computes:
    (ISO + PNRBH + Post-Up + Misc Phase 1b PAB + HC Playmaking PAB) / GP

and prints the top divergences against the known-good 2024-25 season CSV
column "On-ball PC/g".

IMPORTANT — this is NOT a pass/fail validation.
The HC Playmaking PAB methodology used here differs from whatever was used
to produce the CSV.  Large systematic divergences are expected and are not
bugs to fix.  The purpose of this script is to document and characterise
the divergence so that any unintentional regressions become visible.

Known divergence pattern (2024-25):
    - Pure passers (Chris Paul, Tyus Jones, TJ McConnell) show the largest
      positive deltas: computed On-ball PC/g >> CSV.  Their HC Playmaking PAB
      values are very large (~3–4 PC/g premium) while the CSV credits them
      with near-zero On-ball PC/g.
    - High-PPPA scorers (Jokic, Haliburton) are also high positive outliers.
    - Some high-volume scorers (Trae Young, Luka) are slight negative outliers
      (computed < CSV by ~0.7–0.8/g).

What cannot be validated here:
    - On-ball Scoring PC/g and On-ball Playmaking PC/g breakdowns need
      Phase 4 (Transition) outputs to build the full denominator.
    - Floor-raising adjusted PC/g requires Phase 5.

Completeness filter: a player is included only if their primary on-ball slug
(prballhandler) has GP >= 80% of their total season Games.
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
    phase1b_playtype,
    phase2_transition,
    phase3a_passing,
    phase3b_passing,
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

ON_BALL_SLUGS   = ["iso", "prballhandler", "postup", "misc"]
PRIMARY_SLUG    = "prballhandler"


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

    # Phase 1a for all playtypes
    pt1a: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        pt1a[pt["slug"]] = phase1a_playtype(df_stg, pt["slug"])

    # Phase 2 for TR_PPP_RATIO and aggregates used by Phase 3a
    pt2, tr_agg = phase2_transition(df_stg, cfg)

    # Phase 3a for lg_avg_onball_tov_rate, HC_PPP_w_tov_penalty, HC regression
    pt3a, agg3a = phase3a_passing(df_stg, pt1a, pt2, cfg)
    lg_avg_tov  = agg3a["lg_avg_onball_tov_rate"]

    # Phase 1b for on-ball playtypes (scoring PAB)
    pt1b: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        baseline = cfg["CTG_HC_PPP"] if pt["slug"] == "offrebound" else None
        pt1b[pt["slug"]] = phase1b_playtype(
            pt1a[pt["slug"]], lg_avg_tov, cfg, baseline
        )

    # Phase 3b: HC Playmaking PAB
    pt3b = phase3b_passing(pt3a, agg3a, tr_agg["TR_PPP_RATIO"], cfg)

    # ── Name / GP lookup ──────────────────────────────────────────────────────
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

    # ── Sum on-ball scoring PAB across all 4 on-ball playtypes ────────────────
    frames = [
        pt1b[s][["PLAYER_ID", "pab"]].rename(columns={"pab": f"pab_{s}"})
        for s in ON_BALL_SLUGS
    ]
    agg = frames[0]
    for f in frames[1:]:
        agg = agg.merge(f, on="PLAYER_ID", how="outer")
    agg = agg.fillna(0.0)
    agg["scoring_pab"] = sum(agg[f"pab_{s}"] for s in ON_BALL_SLUGS)

    # ── Add HC Playmaking PAB from Phase 3b ───────────────────────────────────
    hc_pm = pt3b[["PLAYER_ID", "hc_playmaking_pab"]].copy()
    agg   = agg.merge(hc_pm, on="PLAYER_ID", how="outer").fillna(0.0)
    agg["computed_pab"] = agg["scoring_pab"] + agg["hc_playmaking_pab"]
    agg["_key"]         = agg["PLAYER_ID"].map(pid_to_key)
    agg["trad_gp"]      = agg["PLAYER_ID"].map(trad_gp)
    agg["pt_gp"]        = agg["PLAYER_ID"].map(
        lambda pid: primary_gp.get(int(pid))
    )

    # ── Join against CSV ──────────────────────────────────────────────────────
    csv_onball = (
        df_csv[df_csv["Has data? On-ball"] == True][["_key", "On-ball PC/g"]]
        .copy()
        .rename(columns={"On-ball PC/g": "csv_pcpg"})
        .dropna(subset=["csv_pcpg"])
    )

    joined = agg.merge(csv_onball, on="_key", how="inner")
    joined = joined[joined["trad_gp"].notna() & joined["pt_gp"].notna()]

    joined["computed_pcpg"] = joined["computed_pab"] / joined["trad_gp"]
    joined["gp_fraction"]   = joined["pt_gp"] / joined["trad_gp"].clip(lower=1)

    complete   = joined[joined["gp_fraction"] >= MIN_GP_FRACTION].copy()
    incomplete = joined[joined["gp_fraction"] <  MIN_GP_FRACTION]

    complete["diff"] = (complete["computed_pcpg"] - complete["csv_pcpg"]).abs()

    matched    = int((complete["diff"] <= TOLERANCE_PPG).sum())
    tested     = len(complete)
    mismatches = complete[complete["diff"] > TOLERANCE_PPG].copy()

    print(f"\n[INFO] On-ball PC/g divergence report (methodology differs from CSV — expected)")
    print(f"       Players with data : {tested}")
    print(f"       Players skipped   : {len(incomplete)}  (incomplete multi-team data)")
    print(f"       Within ±{TOLERANCE_PPG}        : {matched}")
    print(f"       Outside ±{TOLERANCE_PPG}       : {len(mismatches)}")
    print(f"\n  Top divergences (computed - csv):")
    for _, row in mismatches.sort_values("diff", ascending=False).head(15).iterrows():
        sign = "+" if row["computed_pcpg"] > row["csv_pcpg"] else ""
        delta = row["computed_pcpg"] - row["csv_pcpg"]
        print(f"    {row['_key']}: computed={row['computed_pcpg']:.3f}  "
              f"csv={row['csv_pcpg']:.2f}  delta={sign}{delta:.3f}  "
              f"scoring={row['scoring_pab']:.1f}  hc_pm={row['hc_playmaking_pab']:.1f}  "
              f"gp={row['trad_gp']:.0f}")

    print(f"\n{'='*52}")
    print("INFO — Phase 3b divergence report complete (not a pass/fail check)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
