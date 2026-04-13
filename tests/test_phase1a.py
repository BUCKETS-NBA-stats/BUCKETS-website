"""
Phase 1a regression test.

Validates that computed Scoring Plays/g for the three pure off-ball categories
(Partner, Space, Crash) match the known-good 2024-25 season CSV within ±0.15
plays/game.

Comparison is per-game (not total plays) to avoid false failures from the
round-trip rounding that occurs when recovering total plays from a CSV column
stored at 1 decimal precision.  Uses the primary play-type GP from staging as
the denominator (first slug in each category group).

Multi-team completeness filter: players are skipped when their primary-slug GP
is less than 80% of their total season Games per the CSV.  This excludes traded
players whose second-team play-type rows were absent from the NBA API response
(a data-availability issue, not a formula error).

These three categories are exact validation targets at Phase 1a because:
    Off-ball: Partner = Roll and Pop + Handoff scoring plays (no playmaking)
    Off-ball: Space   = Spot-Up + Off-Screen scoring plays   (no playmaking)
    Off-ball: Crash   = Open Rim + Putbacks scoring plays    (no playmaking)

On-ball playtypes (ISO, PNRBH, Post-Up, Misc) cannot be validated here because
their category column in the CSV includes HC Playmaking plays added in Phase 3b.
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
)
from scripts.calculate.config import build_config

SEASON           = "2024-25"
SEASON_TYPE_SLUG = "regular"

STAGING_PATH = os.path.join(REPO_ROOT, "assets", "data", "staging",
                             f"{SEASON}__{SEASON_TYPE_SLUG}.parquet")
CSV_PATH     = os.path.join(REPO_ROOT, "assets", "data", "season", "league-table-2025.csv")
CTG_PATH     = os.path.join(REPO_ROOT, "assets", "data", "raw",
                             SEASON, SEASON_TYPE_SLUG, "ctg_league_averages.json")

# ±0.15 plays/game — one unit of CSV precision is 0.1; ±0.15 accommodates the
# half-unit rounding band without masking real formula errors.
TOLERANCE_PPG = 0.15

# Players whose primary play-type GP is below this fraction of their total
# season Games are skipped — their NBA API data is incomplete for that category.
MIN_GP_FRACTION = 0.80

# Off-ball category validation targets.
# primary_slug: used for the GP denominator in plays/game calculation.
# csv_has_col: filters to players who actually have data in that category.
CATEGORY_TESTS = [
    {
        "name":          "Off-ball: Partner",
        "slugs":         ["prrollman", "handoff"],
        "primary_slug":  "prrollman",
        "csv_plays_col": "Off-ball: Partner Plays/g",
        "csv_has_col":   "Has data? Off-ball: Partner",
    },
    {
        "name":          "Off-ball: Space",
        "slugs":         ["spotup", "offscreen"],
        "primary_slug":  "spotup",
        "csv_plays_col": "Off-ball: Space Plays/g",
        "csv_has_col":   "Has data? Off-ball: Space",
    },
    {
        "name":          "Off-ball: Crash",
        "slugs":         ["cut", "offrebound"],
        "primary_slug":  "cut",
        "csv_plays_col": "Off-ball: Crash Plays/g",
        "csv_has_col":   "Has data? Off-ball: Crash",
    },
]


def normalize_name(s: str) -> str:
    """Accent-strip + lowercase + alphanumeric only. Mirrors _make_player_key."""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def main() -> int:
    print(f"Staging : {STAGING_PATH}")
    print(f"CSV     : {CSV_PATH}")

    df_stg = pd.read_parquet(STAGING_PATH)
    df_csv = pd.read_csv(CSV_PATH)
    cfg    = build_config(CTG_PATH)  # noqa: F841

    # Add ScoringShareOfTOV
    df_stg = compute_scoring_share_of_tov(df_stg)

    # Run Phase 1a for all 10 playtypes
    pt1a: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        pt1a[pt["slug"]] = phase1a_playtype(df_stg, pt["slug"])

    # Normalised name key for matching staging <-> CSV
    df_stg["_key"] = df_stg["Player"].apply(normalize_name)
    df_csv["_key"] = df_csv["Player"].apply(normalize_name)
    pid_to_key = (
        df_stg.dropna(subset=["PLAYER_ID"])
        .set_index("PLAYER_ID")["_key"]
        .to_dict()
    )

    # Traditional GP (authoritative games played) and play-type GP per slug.
    # Per-game rates always use nba_trad__GP as denominator.
    # Play-type GP is used only for the completeness filter.
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
        sub = df_stg[df_stg[gp_col].notna()][["PLAYER_ID", gp_col]]
        for _, row in sub.iterrows():
            pid = int(row["PLAYER_ID"])
            pt_gp_lookup.setdefault(pid, {})[slug] = float(row[gp_col])

    total_tested  = 0
    total_matched = 0
    total_skipped = 0

    for cat in CATEGORY_TESTS:
        # Aggregate scoring_plays from each slug in the category
        frames = [
            pt1a[s][["PLAYER_ID", "scoring_plays"]].rename(
                columns={"scoring_plays": f"sp_{s}"}
            )
            for s in cat["slugs"]
        ]
        agg = frames[0]
        for f in frames[1:]:
            agg = agg.merge(f, on="PLAYER_ID", how="outer")
        agg = agg.fillna(0.0)
        agg["computed_plays"] = sum(agg[f"sp_{s}"] for s in cat["slugs"])
        agg["_key"]           = agg["PLAYER_ID"].map(pid_to_key)

        # Add primary-slug play-type GP (for completeness filter only)
        primary = cat["primary_slug"]
        agg["pt_gp"] = agg["PLAYER_ID"].map(
            lambda pid: pt_gp_lookup.get(int(pid), {}).get(primary, None)
        )

        # Add traditional GP (authoritative denominator for per-game rate)
        agg["trad_gp"] = agg["PLAYER_ID"].map(
            lambda pid: trad_gp.get(pid, None)
        )

        # Expected plays/g from CSV: only rows where the Has data? flag is True
        csv_cat = df_csv[df_csv[cat["csv_has_col"]] == True][
            ["_key", cat["csv_plays_col"], "Games"]
        ].copy()
        csv_cat = csv_cat.rename(columns={cat["csv_plays_col"]: "csv_ppg"})
        csv_cat = csv_cat.dropna(subset=["csv_ppg"])

        joined = agg.merge(csv_cat, on="_key", how="inner")
        joined = joined[joined["trad_gp"].notna() & joined["pt_gp"].notna()]

        # Per-game computed rate: always use traditional GP as denominator
        joined["computed_ppg"] = joined["computed_plays"] / joined["trad_gp"]

        # Completeness filter: skip players whose play-type GP is << traditional GP
        # (indicates missing second-team data in the NBA API response)
        joined["gp_fraction"] = joined["pt_gp"] / joined["trad_gp"].clip(lower=1)
        complete   = joined[joined["gp_fraction"] >= MIN_GP_FRACTION].copy()
        incomplete = joined[joined["gp_fraction"] <  MIN_GP_FRACTION]
        skipped    = len(incomplete)

        complete["diff"] = (complete["computed_ppg"] - complete["csv_ppg"]).abs()

        matched    = int((complete["diff"] <= TOLERANCE_PPG).sum())
        tested     = len(complete)
        mismatches = complete[complete["diff"] > TOLERANCE_PPG].copy()

        total_tested  += tested
        total_matched += matched
        total_skipped += skipped

        status = "OK  " if matched == tested else "FAIL"
        print(f"\n[{status}] {cat['name']}")
        print(f"       Players tested  : {tested}")
        print(f"       Players skipped : {skipped}  (incomplete multi-team data)")
        print(f"       Matched (±{TOLERANCE_PPG})  : {matched}")
        print(f"       Mismatches      : {len(mismatches)}")
        if not mismatches.empty:
            for _, row in mismatches.head(10).iterrows():
                print(f"         {row['_key']}: computed={row['computed_ppg']:.3f}  "
                      f"csv={row['csv_ppg']:.1f}  diff={row['diff']:.3f}  "
                      f"trad_gp={row['trad_gp']:.0f}  pt_gp={row['pt_gp']:.0f}")

    print(f"\n{'='*52}")
    print(f"TOTAL players tested  : {total_tested}")
    print(f"TOTAL players skipped : {total_skipped}  (incomplete multi-team data)")
    print(f"TOTAL matched (±{TOLERANCE_PPG})  : {total_matched}")
    print(f"TOTAL mismatches      : {total_tested - total_matched}")

    if total_matched == total_tested:
        print("\nPASS — Phase 1a off-ball scoring plays/g match known-good CSV within ±0.15")
        return 0
    else:
        print(f"\nFAIL — {total_tested - total_matched} mismatches found")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
