"""
Phase 1b regression test: off-ball scoring PAB (Points Created).

Validates that computed PAB/g for the three pure off-ball categories
(Partner, Space, Crash) match the known-good 2024-25 season CSV within
±0.15 PC/game.

Off-ball categories are the only ones validatable at Phase 1b because:
    Off-ball: Partner = Roll and Pop + Handoff PAB (no playmaking component)
    Off-ball: Space   = Spot-Up + Off-Screen PAB   (no playmaking component)
    Off-ball: Crash   = Open Rim + Putbacks PAB    (no playmaking component)

On-ball categories (ISO, PNRBH, Post-Up, Misc) cannot be validated here
because their CSV "PC/g" column includes HC Playmaking PAB added in Phase 3b.

Same completeness filter as Phase 1a: skip players whose primary-slug GP
is less than 80% of their total season Games (multi-team data gaps).
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

CATEGORY_TESTS = [
    {
        "name":         "Off-ball: Partner",
        "slugs":        ["prrollman", "handoff"],
        "primary_slug": "prrollman",
        "csv_pc_col":   "Off-ball: Partner PC/g",
        "csv_has_col":  "Has data? Off-ball: Partner",
    },
    {
        "name":         "Off-ball: Space",
        "slugs":        ["spotup", "offscreen"],
        "primary_slug": "spotup",
        "csv_pc_col":   "Off-ball: Space PC/g",
        "csv_has_col":  "Has data? Off-ball: Space",
    },
    {
        "name":         "Off-ball: Crash",
        "slugs":        ["cut", "offrebound"],
        "primary_slug": "cut",
        "csv_pc_col":   "Off-ball: Crash PC/g",
        "csv_has_col":  "Has data? Off-ball: Crash",
    },
]


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

    # Phase 2 and 3a to get lg_avg_onball_tov_rate
    pt2, _     = phase2_transition(df_stg, cfg)
    _, agg3a   = phase3a_passing(df_stg, pt1a, pt2, cfg)
    lg_avg_tov = agg3a["lg_avg_onball_tov_rate"]

    # Phase 1b for all playtypes
    # Putbacks (offrebound) use CTG_HC_PPP as baseline directly.
    pt1b: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        baseline = cfg["CTG_HC_PPP"] if pt["slug"] == "offrebound" else None
        pt1b[pt["slug"]] = phase1b_playtype(pt1a[pt["slug"]], lg_avg_tov, cfg, baseline)

    # Name / GP lookup maps
    df_stg["_key"] = df_stg["Player"].apply(normalize_name)
    df_csv["_key"] = df_csv["Player"].apply(normalize_name)

    trad_gp: dict[int, float] = (
        df_stg.dropna(subset=["PLAYER_ID", "nba_trad__GP"])
        .set_index("PLAYER_ID")["nba_trad__GP"]
        .to_dict()
    )
    pt_gp_lookup: dict[int, dict[str, float]] = {}
    for pt in PLAYTYPES:
        slug   = pt["slug"]
        gp_col = f"nba_pt_{slug}__GP"
        if gp_col not in df_stg.columns:
            continue
        for _, row in df_stg[df_stg[gp_col].notna()][["PLAYER_ID", gp_col]].iterrows():
            pt_gp_lookup.setdefault(int(row["PLAYER_ID"]), {})[slug] = float(row[gp_col])

    pid_to_key: dict[int, str] = (
        df_stg.dropna(subset=["PLAYER_ID"])
        .set_index("PLAYER_ID")["_key"]
        .to_dict()
    )

    total_tested  = 0
    total_matched = 0
    total_skipped = 0
    failures      = []

    for cat in CATEGORY_TESTS:
        # Sum PAB across slugs in the category
        frames = [
            pt1b[s][["PLAYER_ID", "pab"]].rename(columns={"pab": f"pab_{s}"})
            for s in cat["slugs"]
        ]
        agg = frames[0]
        for f in frames[1:]:
            agg = agg.merge(f, on="PLAYER_ID", how="outer")
        agg = agg.fillna(0.0)
        agg["computed_pab"] = sum(agg[f"pab_{s}"] for s in cat["slugs"])
        agg["_key"]         = agg["PLAYER_ID"].map(pid_to_key)

        primary = cat["primary_slug"]
        agg["pt_gp"]   = agg["PLAYER_ID"].map(
            lambda pid: pt_gp_lookup.get(int(pid), {}).get(primary)
        )
        agg["trad_gp"] = agg["PLAYER_ID"].map(lambda pid: trad_gp.get(pid))

        csv_cat = df_csv[df_csv[cat["csv_has_col"]] == True][
            ["_key", cat["csv_pc_col"]]
        ].copy().rename(columns={cat["csv_pc_col"]: "csv_pcpg"}).dropna(subset=["csv_pcpg"])

        joined = agg.merge(csv_cat, on="_key", how="inner")
        joined = joined[joined["trad_gp"].notna() & joined["pt_gp"].notna()]

        joined["computed_pcpg"] = joined["computed_pab"] / joined["trad_gp"]
        joined["gp_fraction"]   = joined["pt_gp"] / joined["trad_gp"].clip(lower=1)

        complete   = joined[joined["gp_fraction"] >= MIN_GP_FRACTION].copy()
        incomplete = joined[joined["gp_fraction"] <  MIN_GP_FRACTION]

        complete["diff"] = (complete["computed_pcpg"] - complete["csv_pcpg"]).abs()

        matched    = int((complete["diff"] <= TOLERANCE_PPG).sum())
        tested     = len(complete)
        mismatches = complete[complete["diff"] > TOLERANCE_PPG].copy()

        total_tested  += tested
        total_matched += matched
        total_skipped += len(incomplete)

        status = "OK  " if matched == tested else "FAIL"
        print(f"\n[{status}] {cat['name']}")
        print(f"       Players tested  : {tested}")
        print(f"       Players skipped : {len(incomplete)}  (incomplete multi-team data)")
        print(f"       Matched (±{TOLERANCE_PPG})  : {matched}")
        print(f"       Mismatches      : {len(mismatches)}")
        if not mismatches.empty:
            for _, row in mismatches.head(10).iterrows():
                print(f"         {row['_key']}: computed={row['computed_pcpg']:.3f}  "
                      f"csv={row['csv_pcpg']:.2f}  diff={row['diff']:.3f}  "
                      f"trad_gp={row['trad_gp']:.0f}  pt_gp={row['pt_gp']:.0f}")
            failures.append(f"{cat['name']}: {len(mismatches)} mismatches")

    print(f"\n{'='*52}")
    print(f"TOTAL players tested  : {total_tested}")
    print(f"TOTAL players skipped : {total_skipped}  (incomplete multi-team data)")
    print(f"TOTAL matched (±{TOLERANCE_PPG})  : {total_matched}")
    print(f"TOTAL mismatches      : {total_tested - total_matched}")

    if not failures:
        print("\nPASS — Phase 1b off-ball PAB/g match known-good CSV within ±0.15")
        return 0
    else:
        for f in failures:
            print(f"FAIL — {f}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
