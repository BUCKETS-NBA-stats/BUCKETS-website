"""
Phase 2 regression test.

Transition Scoring + tab-level aggregates.

Full validation against the known-good CSV is not possible at Phase 2 because
the CSV "Transition Plays/g" column = Scoring Plays + Playmaking Plays, and
Playmaking Plays are not computed until Phase 4.

What IS validated here:
  1. Tab aggregates are within plausible league-wide bounds.
  2. Per-player scoring_plays are non-negative.
  3. Per-player scoring_plays/trad_gp <= CSV "Transition Plays/g" for all
     players with transition data — scoring plays can't exceed total plays.
  4. tov_total = round(TOV_POSS_PCT * POSS, 0) spot-check on sampled players.
  5. transition_playmaking_tovs = tov_total - scoring_tovs for all rows.
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
    compute_scoring_share_of_tov,
    phase2_transition,
)
from scripts.calculate.config import build_config

SEASON           = "2024-25"
SEASON_TYPE_SLUG = "regular"

STAGING_PATH = os.path.join(REPO_ROOT, "assets", "data", "staging",
                             f"{SEASON}__{SEASON_TYPE_SLUG}.parquet")
CSV_PATH     = os.path.join(REPO_ROOT, "assets", "data", "season", "league-table-2025.csv")
CTG_PATH     = os.path.join(REPO_ROOT, "assets", "data", "raw",
                             SEASON, SEASON_TYPE_SLUG, "ctg_league_averages.json")

# Expected bounds for tab aggregates
TR_AVG_PPP_MIN,          TR_AVG_PPP_MAX          = 1.00, 1.30
TR_OVERALL_TOV_RATE_MIN, TR_OVERALL_TOV_RATE_MAX = 0.08, 0.20
TR_PPP_RATIO_MIN,        TR_PPP_RATIO_MAX         = 0.90, 1.40


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
    pt2, agg = phase2_transition(df_stg, cfg)

    failures = []

    # ── 1. Tab aggregate bounds ───────────────────────────────────────────────
    print(f"\n[INFO] Tab aggregates:")
    print(f"       TR_AVG_PPP          = {agg['TR_AVG_PPP']:.4f}  "
          f"(expected {TR_AVG_PPP_MIN}–{TR_AVG_PPP_MAX})")
    print(f"       TR_OVERALL_TOV_RATE = {agg['TR_OVERALL_TOV_RATE']:.4f}  "
          f"(expected {TR_OVERALL_TOV_RATE_MIN}–{TR_OVERALL_TOV_RATE_MAX})")
    print(f"       TR_AVG_PLAYS        = {agg['TR_AVG_PLAYS']:.2f}")
    print(f"       TR_PPP_RATIO        = {agg['TR_PPP_RATIO']:.4f}  "
          f"(expected {TR_PPP_RATIO_MIN}–{TR_PPP_RATIO_MAX})")

    for name, val, lo, hi in [
        ("TR_AVG_PPP",          agg["TR_AVG_PPP"],          TR_AVG_PPP_MIN,          TR_AVG_PPP_MAX),
        ("TR_OVERALL_TOV_RATE", agg["TR_OVERALL_TOV_RATE"], TR_OVERALL_TOV_RATE_MIN, TR_OVERALL_TOV_RATE_MAX),
        ("TR_PPP_RATIO",        agg["TR_PPP_RATIO"],        TR_PPP_RATIO_MIN,        TR_PPP_RATIO_MAX),
    ]:
        if not (lo <= val <= hi):
            failures.append(f"Tab aggregate {name}={val:.4f} outside [{lo}, {hi}]")

    # ── 2. No negative scoring_plays ─────────────────────────────────────────
    neg = pt2[pt2["scoring_plays"] < 0]
    if not neg.empty:
        failures.append(f"{len(neg)} players with negative scoring_plays")
        print(f"\n[FAIL] Negative scoring_plays:")
        for _, r in neg.iterrows():
            print(f"       {r['Player']}: scoring_plays={r['scoring_plays']}")
    else:
        print(f"\n[OK  ] No negative scoring_plays ({len(pt2)} players)")

    # ── 3. scoring_plays <= CSV Transition Plays/g * trad_gp ─────────────────
    df_stg["_key"] = df_stg["Player"].apply(normalize_name)
    df_csv["_key"] = df_csv["Player"].apply(normalize_name)

    trad_gp_map = (
        df_stg.dropna(subset=["PLAYER_ID", "nba_trad__GP"])
        .set_index("PLAYER_ID")["nba_trad__GP"]
        .to_dict()
    )
    pid_to_key = (
        df_stg.dropna(subset=["PLAYER_ID"])
        .set_index("PLAYER_ID")["_key"]
        .to_dict()
    )

    csv_tr = df_csv[df_csv["Has data? Transition"] == True][
        ["_key", "Transition Plays/g", "Games"]
    ].copy()

    pt2["_key"]     = pt2["PLAYER_ID"].map(pid_to_key)
    pt2["trad_gp"]  = pt2["PLAYER_ID"].map(trad_gp_map)
    joined = pt2.merge(csv_tr, on="_key", how="inner")
    joined = joined[joined["trad_gp"].notna()]

    # scoring_plays/trad_gp should not exceed CSV total Plays/g
    joined["scoring_ppg"] = joined["scoring_plays"] / joined["trad_gp"]
    exceeded = joined[joined["scoring_ppg"] > joined["Transition Plays/g"] + 0.05]

    if not exceeded.empty:
        failures.append(
            f"{len(exceeded)} players where scoring_plays/g exceeds CSV Transition Plays/g"
        )
        print(f"\n[FAIL] scoring_plays/g > CSV Transition Plays/g:")
        for _, r in exceeded.head(10).iterrows():
            print(f"       {r['_key']}: scoring={r['scoring_ppg']:.2f}  "
                  f"csv_total={r['Transition Plays/g']:.1f}")
    else:
        print(f"[OK  ] scoring_plays/g <= CSV Transition Plays/g for all "
              f"{len(joined)} matched players")

    # ── 4. tov_total spot-check ───────────────────────────────────────────────
    slug    = "transition"
    poss_col = f"nba_pt_{slug}__POSS"
    tov_col  = f"nba_pt_{slug}__TOV_POSS_PCT"
    mask    = df_stg[poss_col].notna()
    raw_sub = df_stg.loc[mask, ["PLAYER_ID", poss_col, tov_col]].copy()
    raw_sub["expected_tov_total"] = (raw_sub[tov_col] * raw_sub[poss_col]).round(0)

    check = pt2.merge(
        raw_sub[["PLAYER_ID", "expected_tov_total"]], on="PLAYER_ID", how="inner"
    )
    tov_mismatch = check[check["tov_total"] != check["expected_tov_total"]]
    if not tov_mismatch.empty:
        failures.append(f"{len(tov_mismatch)} players with wrong tov_total")
        print(f"\n[FAIL] tov_total mismatches:")
        for _, r in tov_mismatch.head(5).iterrows():
            print(f"       pid={r['PLAYER_ID']}: got {r['tov_total']}  "
                  f"expected {r['expected_tov_total']}")
    else:
        print(f"[OK  ] tov_total correct for all {len(check)} players")

    # ── 5. transition_playmaking_tovs = tov_total - scoring_tovs ─────────────
    bad_pmtov = pt2[pt2["transition_playmaking_tovs"] != pt2["tov_total"] - pt2["scoring_tovs"]]
    if not bad_pmtov.empty:
        failures.append(f"{len(bad_pmtov)} players with wrong transition_playmaking_tovs")
    else:
        print(f"[OK  ] transition_playmaking_tovs identity holds for all {len(pt2)} players")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*52}")
    if not failures:
        print("PASS — Phase 2 Transition scoring checks all passed")
        return 0
    else:
        for f in failures:
            print(f"FAIL: {f}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
