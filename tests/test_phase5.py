"""
Phase 5 regression test: Floor Raising PC/g.

Validates that floor_raising_pc_per_g matches the known-good 2024-25 season
CSV column "Floor raising PC/g" within ±0.15.  290/298 players pass.

The 8 known failures are expected and are not bugs:

    floor_raising_pc = raw_fr(on_ball_plays_per_g) - baseline
    on_ball_plays_total = on_ball_scoring_plays + est_hc_pm_plays_final

    The baseline is the mean raw_fr for players with >= FR_MIN_POSS possessions.
    Any difference in est_hc_pm_plays_final (from the Phase 3b HC/TR regression
    split that intentionally diverges from the CSV) shifts a player's
    on_ball_plays_per_g AND the shared baseline, affecting all players.

    Known failures (2024-25):
      - Pure passers (Chris Paul, Tyus Jones, Mike Conley, Sabonis, Josh Hart):
        est_hc_pm_plays_final is larger than CSV -> ob_plays/g overstated ->
        their raw_fr shifts the baseline -> computed FR < csv FR (diff ~0.15-0.19)
      - Low-sample players (AJ Lawson 26 GP, Jaylen Martin 16 GP, Killian Hayes
        6 GP): quadratic regression is sensitive to small input differences at
        low samples; small ob_plays deviations produce diffs of 0.17-0.23

Completeness filter: a player is tested only if their primary on-ball slug
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
    phase5_floor_raising,
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

    pt1a: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        pt1a[pt["slug"]] = phase1a_playtype(df_stg, pt["slug"])

    pt2, tr_agg = phase2_transition(df_stg, cfg)
    pt3a, agg3a = phase3a_passing(df_stg, pt1a, pt2, cfg)
    lg_avg_tov  = agg3a["lg_avg_onball_tov_rate"]

    pt1b: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        baseline = cfg["CTG_HC_PPP"] if pt["slug"] == "offrebound" else None
        pt1b[pt["slug"]] = phase1b_playtype(pt1a[pt["slug"]], lg_avg_tov, cfg, baseline)

    pt3b = phase3b_passing(pt3a, agg3a, tr_agg["TR_PPP_RATIO"], cfg)

    pt5 = phase5_floor_raising(pt1b, pt3b, df_stg, cfg)

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
    pt5["_key"]    = pt5["PLAYER_ID"].map(pid_to_key)
    pt5["trad_gp"] = pt5["PLAYER_ID"].map(trad_gp)
    pt5["pt_gp"]   = pt5["PLAYER_ID"].map(
        lambda pid: primary_gp.get(int(pid))
    )

    csv_fr = (
        df_csv[["_key", "Floor raising PC/g"]]
        .dropna(subset=["Floor raising PC/g"])
        .rename(columns={"Floor raising PC/g": "csv_fr_pcpg"})
    )

    joined = pt5.merge(csv_fr, on="_key", how="inner")
    joined = joined[joined["trad_gp"].notna() & joined["pt_gp"].notna()]
    joined["gp_fraction"] = joined["pt_gp"] / joined["trad_gp"].clip(lower=1)

    complete   = joined[joined["gp_fraction"] >= MIN_GP_FRACTION].copy()
    incomplete = joined[joined["gp_fraction"] <  MIN_GP_FRACTION]

    complete["diff"] = (complete["floor_raising_pc_per_g"] - complete["csv_fr_pcpg"]).abs()

    matched    = int((complete["diff"] <= TOLERANCE_PPG).sum())
    tested     = len(complete)
    mismatches = complete[complete["diff"] > TOLERANCE_PPG].copy()

    status = "OK  " if matched == tested else "FAIL"
    print(f"\n[{status}] Floor raising PC/g")
    print(f"       Players tested  : {tested}")
    print(f"       Players skipped : {len(incomplete)}  (incomplete multi-team data)")
    print(f"       Matched (±{TOLERANCE_PPG})   : {matched}")
    print(f"       Mismatches      : {len(mismatches)}")
    print(f"       FR baseline/g   : {pt5['fr_baseline_per_g'].iloc[0]:.4f}")
    if not mismatches.empty:
        for _, row in mismatches.sort_values("diff", ascending=False).head(15).iterrows():
            print(f"         {row['_key']}: computed={row['floor_raising_pc_per_g']:.3f}  "
                  f"csv={row['csv_fr_pcpg']:.3f}  diff={row['diff']:.3f}  "
                  f"ob_plays/g={row['on_ball_plays_per_g']:.1f}  "
                  f"trad_gp={row['trad_gp']:.0f}")

    print(f"\n{'='*52}")
    if matched == tested:
        print(f"PASS — Phase 5 Floor raising PC/g matches known-good CSV within ±{TOLERANCE_PPG}")
        return 0
    else:
        print(f"FAIL — {len(mismatches)} mismatches out of {tested} tested")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
