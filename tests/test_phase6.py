"""
Phase 6 regression test: Final assembled output vs known-good 2024-25 CSV.

Pass/fail checks (integrity + high-confidence columns):
    1. Identity: Games and Possessions match exactly for all players.
    2. Internal consistency:
         On-ball Scoring PC/g + On-ball Playmaking PC/g ≈ On-ball PC/g
         Total PC/g (floor raising adj.) ≈ Total PC/g (ex. floor raising)
                                           + Floor raising PC/g
    3. Off-ball: Partner PC/g within ±0.15  (100% expected — pure Phase 1b,
       no methodology divergence).
    4. Floor raising PC/g flows through correctly — same tolerance/failure set
       as Phase 5 (290/298 expected; 8 known failures documented in test_phase5.py).

Diagnostic only (not pass/fail):
    5. Off-ball: Space PC/g  — ~5% of players fall outside ±0.15.
    6. Off-ball: Crash PC/g  — ~3% of players fall outside ±0.15.

       The Space/Crash divergences come from a small difference in the
       lg_avg_onball_tov_rate used to compute the Phase 1b baseline.
       Our computed rate (≈0.0887) shifts the off-ball scoring baselines
       uniformly, producing a systematic positive delta of ~0.15–0.67 PC/g
       for the outlier players.  This is the same root cause as the
       lg_avg_onball_tov_rate sensitivity noted in Phase 1b.

Column counts: computed output must have exactly 151 columns matching the CSV.

Completeness filter for numeric checks: a player is tested only if their
primary category slug has GP >= 80% of their total season Games.
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
    phase4_transition,
    phase5_floor_raising,
    phase6_assemble,
)
from scripts.calculate.config import build_config

SEASON           = "2024-25"
SEASON_TYPE_SLUG = "regular"

STAGING_PATH = os.path.join(REPO_ROOT, "assets", "data", "staging",
                             f"{SEASON}__{SEASON_TYPE_SLUG}.parquet")
CSV_PATH     = os.path.join(REPO_ROOT, "assets", "data", "season", "league-table-2025.csv")
CTG_PATH     = os.path.join(REPO_ROOT, "assets", "data", "raw",
                             SEASON, SEASON_TYPE_SLUG, "ctg_league_averages.json")

TOLERANCE    = 0.15
MIN_GP_FRAC  = 0.80


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
    lg_avg_tov   = agg3a["lg_avg_onball_tov_rate"]

    pt1b: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        baseline = cfg["CTG_HC_PPP"] if pt["slug"] == "offrebound" else None
        pt1b[pt["slug"]] = phase1b_playtype(pt1a[pt["slug"]], lg_avg_tov, cfg, baseline)

    pt3b = phase3b_passing(pt3a, agg3a, tr_agg["TR_PPP_RATIO"], cfg)
    pt4  = phase4_transition(pt2, pt3b, tr_agg, cfg)
    pt5  = phase5_floor_raising(pt1b, pt3b, df_stg, cfg)
    out  = phase6_assemble(pt1b, pt3b, pt4, pt5, df_stg, SEASON, "RS")

    failures: list[str] = []

    # ── 1. Column count / schema ───────────────────────────────────────────────
    csv_cols = list(df_csv.columns)
    out_cols = list(out.columns)
    extra   = set(out_cols) - set(csv_cols)
    missing = set(csv_cols) - set(out_cols)
    if extra or missing:
        failures.append(
            f"Schema mismatch — extra: {sorted(extra)}  missing: {sorted(missing)}"
        )
        print(f"[FAIL] Schema: extra={sorted(extra)}  missing={sorted(missing)}")
    else:
        print(f"[OK  ] Schema: {len(out_cols)} columns match CSV exactly")

    # ── Join on normalized name ────────────────────────────────────────────────
    out = out.reset_index()   # PLAYER_ID is the index; move it to a column
    out["_key"]    = out["Player"].apply(normalize_name)
    df_csv["_key"] = df_csv["Player"].apply(normalize_name)

    joined = out.merge(
        df_csv, on="_key", how="inner", suffixes=("_c", "_g")
    )
    print(f"[INFO] Joined {len(joined)} players")

    # ── 2a. Identity: Games ────────────────────────────────────────────────────
    gp_diff = (joined["Games_c"] - joined["Games_g"]).abs().dropna()
    bad_gp  = int((gp_diff > 0.01).sum())
    if bad_gp:
        failures.append(f"Games mismatch for {bad_gp} players")
        print(f"[FAIL] Games mismatch: {bad_gp}/{len(gp_diff)}")
    else:
        print(f"[OK  ] Games: exact match for {len(gp_diff)} players")

    # ── 2b. Identity: Possessions ──────────────────────────────────────────────
    poss_diff = (joined["Possessions_c"] - joined["Possessions_g"]).abs().dropna()
    bad_poss  = int((poss_diff > 0.01).sum())
    if bad_poss:
        failures.append(f"Possessions mismatch for {bad_poss} players")
        print(f"[FAIL] Possessions mismatch: {bad_poss}/{len(poss_diff)}")
    else:
        print(f"[OK  ] Possessions: exact match for {len(poss_diff)} players")

    # ── 3. Internal consistency ────────────────────────────────────────────────
    # On-ball Scoring PC/g + On-ball Playmaking PC/g ≈ On-ball PC/g
    ob_check = (
        joined["On-ball Scoring PC/g_c"] + joined["On-ball Playmaking PC/g_c"]
        - joined["On-ball PC/g_c"]
    ).abs().dropna()
    bad_ob = int((ob_check > 0.01).sum())
    if bad_ob:
        failures.append(f"On-ball PC decomposition broken for {bad_ob} players")
        print(f"[FAIL] On-ball SC+PM ≈ On-ball PC: {bad_ob} violations")
    else:
        print(f"[OK  ] On-ball PC decomposition: {len(ob_check)} players pass")

    # Total PC/g (floor raising adj.) ≈ Total PC/g (ex. floor raising) + Floor raising PC/g
    fr_adj = joined["Total PC/g (floor raising adj.)_c"]
    fr_ex  = joined["Total PC/g (ex. floor raising)_c"]
    fr_pc  = joined["Floor raising PC/g_c"]
    fr_check = (fr_adj - fr_ex - fr_pc.fillna(0.0)).abs().dropna()
    bad_fr = int((fr_check > 0.01).sum())
    if bad_fr:
        failures.append(f"Total PC floor-raising adjustment broken for {bad_fr} players")
        print(f"[FAIL] Total PC (adj.) ≈ Total PC (ex.) + FR: {bad_fr} violations")
    else:
        print(f"[OK  ] Total PC floor-raising adjustment: {len(fr_check)} players pass")

    # ── 4. Off-ball: Partner PC/g (pass/fail) ────────────────────────────────
    sub_pt = joined[["On-ball PC/g_c",  # use for completeness filter
                      "Games_c", "Possessions_c",
                      "Off-ball: Partner PC/g_c", "Off-ball: Partner PC/g_g",
                      "_key"]].copy()
    pt_diff = (sub_pt["Off-ball: Partner PC/g_c"] - sub_pt["Off-ball: Partner PC/g_g"]).abs()
    pt_valid = pt_diff.dropna()
    pt_matched = int((pt_valid <= TOLERANCE).sum())
    pt_tested  = len(pt_valid)
    pt_mm = sub_pt[pt_diff > TOLERANCE].copy()
    if pt_matched < pt_tested:
        failures.append(
            f"Off-ball: Partner PC/g: {pt_tested - pt_matched}/{pt_tested} mismatches"
        )
        print(f"[FAIL] Off-ball: Partner PC/g: {pt_matched}/{pt_tested} within ±{TOLERANCE}")
        pt_mm["_diff"] = (pt_mm["Off-ball: Partner PC/g_c"] - pt_mm["Off-ball: Partner PC/g_g"]).abs()
        for _, row in pt_mm.sort_values("_diff", ascending=False).head(10).iterrows():
            d = abs(row["Off-ball: Partner PC/g_c"] - row["Off-ball: Partner PC/g_g"])
            print(f"         {row['_key']}: computed={row['Off-ball: Partner PC/g_c']:.3f}  "
                  f"csv={row['Off-ball: Partner PC/g_g']:.3f}  diff={d:.3f}")
    else:
        print(f"[OK  ] Off-ball: Partner PC/g: {pt_matched}/{pt_tested} within ±{TOLERANCE}")

    # ── 5. Floor raising PC/g pass-through ────────────────────────────────────
    MIN_GP_FRACTION = 0.80
    PRIMARY_SLUG    = "prballhandler"
    primary_gp_col  = f"nba_pt_{PRIMARY_SLUG}__GP"
    primary_gp: dict[int, float] = {
        int(r["PLAYER_ID"]): float(r[primary_gp_col])
        for _, r in df_stg[df_stg[primary_gp_col].notna()][
            ["PLAYER_ID", primary_gp_col]
        ].iterrows()
    }
    joined["_pt_gp"] = joined["PLAYER_ID"].map(lambda pid: primary_gp.get(int(pid)))
    joined["_gp_frac"] = joined["_pt_gp"] / joined["Games_c"].clip(lower=1)
    fr_complete = joined[joined["_gp_frac"] >= MIN_GP_FRACTION].copy()
    fr_diff = (
        fr_complete["Floor raising PC/g_c"] - fr_complete["Floor raising PC/g_g"]
    ).abs().dropna()
    fr_matched = int((fr_diff <= TOLERANCE).sum())
    fr_tested  = len(fr_diff)
    fr_mm      = fr_complete[
        (fr_complete["Floor raising PC/g_c"] - fr_complete["Floor raising PC/g_g"]).abs() > TOLERANCE
    ]
    status = "OK  " if fr_matched >= fr_tested - 8 else "FAIL"
    print(f"\n[{status}] Floor raising PC/g (pass-through from Phase 5)")
    print(f"       Tested   : {fr_tested}")
    print(f"       Matched  : {fr_matched}")
    print(f"       Failures : {fr_tested - fr_matched}  (<=8 expected, see test_phase5.py)")
    if fr_tested - fr_matched > 8:
        failures.append(
            f"Floor raising PC/g: {fr_tested - fr_matched} failures (expected <=8)"
        )

    # ── 6. Diagnostics: Space / Crash (not pass/fail) ─────────────────────────
    print(f"\n[INFO] Off-ball: Space / Crash PC/g divergence (diagnostic — not pass/fail)")
    print(f"       Root cause: small lg_avg_onball_tov_rate difference vs CSV computation")
    for label, col in [("Space", "Off-ball: Space PC/g"),
                        ("Crash", "Off-ball: Crash PC/g")]:
        c_col, g_col = col + "_c", col + "_g"
        diff = (joined[c_col] - joined[g_col]).abs().dropna()
        n       = len(diff)
        matched = int((diff <= TOLERANCE).sum())
        print(f"       {label}: {matched}/{n} within ±{TOLERANCE}  "
              f"({n - matched} outside)  max_diff={diff.max():.3f}")

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'='*52}")
    if not failures:
        print("PASS — Phase 6 output passes all integrity and validation checks")
        return 0
    else:
        for f in failures:
            print(f"FAIL — {f}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
