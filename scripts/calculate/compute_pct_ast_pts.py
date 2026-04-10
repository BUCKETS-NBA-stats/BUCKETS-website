"""
compute_pct_ast_pts.py

Computes PCT_AST_PTS_IN_PA for a given season following the spec at
docs/pct_ast_pts_in_pa_spec.md.

Runs once per season before build_season.py.
Output: assets/data/raw/{season}/{slug}/pct_ast_pts_in_pa.json
"""

import argparse
import json
import os
import sys

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# ---------------------------------------------------------------------------
# Play type distributions: (CS, PU1, PU2, LT10_01, LT10_2)
# See spec section "Play Type -> Tracking Column Distribution"
# ---------------------------------------------------------------------------
FULLY_ASSISTED_DIST = {
    "Spotup":    (0.80, 0.10, 0.00, 0.02, 0.08),
    "OffScreen": (0.70, 0.15, 0.02, 0.08, 0.05),
    "PRRollman": (0.25, 0.02, 0.00, 0.65, 0.08),
    "Handoff":   (0.05, 0.20, 0.25, 0.10, 0.40),
    "Cut":       (0.00, 0.00, 0.00, 1.00, 0.00),
}

UNASSISTED_DIST = {
    "OffRebound":    (0.00, 0.00, 0.00, 0.97, 0.03),
    "PRBallHandler": (0.00, 0.10, 0.30, 0.05, 0.55),
    "Isolation":     (0.00, 0.02, 0.40, 0.03, 0.55),
}

# Partial assist rates per tracking column: (CS, PU1, PU2, LT10_01, LT10_2)
PARTIAL_ASSIST_RATES = {
    "Transition": (1.00, 0.95, 0.20, 1.00, 0.15),
    "Postup":     (1.00, 0.70, 0.20, 0.70, 0.25),
    "Misc":       (0.80, 0.50, 0.20, 0.70, 0.30),
}

PARTIAL_FIXED_DIST = {
    # Transition CS is derived from gap analysis; remaining non-CS split is fixed
    # non-CS fractions: PU1=0.05, PU2=0.15, LT10_01=0.40, LT10_2=0.40
    "Postup": (0.02, 0.03, 0.05, 0.30, 0.60),
    "Misc":   (0.20, 0.20, 0.20, 0.20, 0.20),
}

# PPM for residual assisted FGM (blended)
RESIDUAL_LEQ1_RATE = 0.55
RESIDUAL_PPM = 2.2


def load_inputs(season: str, season_type_slug: str) -> dict:
    raw_dir     = os.path.join(REPO_ROOT, "assets", "data", "raw", season, season_type_slug)
    staging_path = os.path.join(REPO_ROOT, "assets", "data", "staging", f"{season}__{season_type_slug}.parquet")

    # Tracking shots JSON
    tracking_path = os.path.join(raw_dir, "nba_tracking_shots.json")
    if not os.path.exists(tracking_path):
        raise FileNotFoundError(f"Missing tracking shots: {tracking_path}\n"
                                f"Run: python scripts/ingest/nba_tracking_shots.py --season {season} --season-type ...")
    with open(tracking_path, encoding="utf-8") as f:
        tracking = json.load(f)["values"]

    # Play type FGMs from raw parquet
    pt_path = os.path.join(raw_dir, "nba_playtypes.parquet")
    if not os.path.exists(pt_path):
        raise FileNotFoundError(f"Missing play types: {pt_path}")
    df_pt = pd.read_parquet(pt_path)
    pt_fgm = df_pt.groupby("PLAY_TYPE")["FGM"].sum().to_dict()

    # Passing totals from staging parquet
    if not os.path.exists(staging_path):
        raise FileNotFoundError(f"Missing staging parquet: {staging_path}\n"
                                f"Run: python scripts/stage/build_stage_season.py --season {season} --season-type ...")
    df_stage = pd.read_parquet(staging_path, columns=["nba_pass__AST", "nba_pass__AST_PTS_CREATED"])
    total_ast_fgm = int(df_stage["nba_pass__AST"].sum())
    total_ast_pts = int(df_stage["nba_pass__AST_PTS_CREATED"].sum())

    return {
        "tracking":      tracking,
        "pt_fgm":        pt_fgm,
        "total_ast_fgm": total_ast_fgm,
        "total_ast_pts": total_ast_pts,
    }


def compute(inputs: dict, verbose: bool = True) -> dict:
    trk          = inputs["tracking"]
    pt_fgm       = inputs["pt_fgm"]
    TOTAL_AST_FGM = inputs["total_ast_fgm"]
    TOTAL_AST_PTS = inputs["total_ast_pts"]

    CS_FGM  = trk["CS_FGM"];   CS_FG3M  = trk["CS_FG3M"]
    PU_FGM  = trk["PU_FGM"];   PU_FG3M  = trk["PU_FG3M"]
    TOTAL_FGM = trk["TOTAL_FGM"]

    CS_PPM   = (CS_FGM * 2 + CS_FG3M) / CS_FGM
    PU_PPM   = (PU_FGM * 2 + PU_FG3M) / PU_FGM
    LT10_PPM = 2.0

    def p(s=""): print(s)

    SEP = "=" * 65

    if verbose:
        p(SEP)
        p("STEP 1: Derive Transition CS fraction from gap analysis")
        p(SEP)

    # Sum CS from all non-Transition play types
    est_cs_non_tr = 0.0
    for pt, dist in FULLY_ASSISTED_DIST.items():
        fgm = pt_fgm.get(pt, 0)
        est_cs_non_tr += fgm * dist[0]
    for pt, dist in UNASSISTED_DIST.items():
        est_cs_non_tr += pt_fgm.get(pt, 0) * dist[0]   # all 0, included for clarity
    for pt, dist in PARTIAL_FIXED_DIST.items():
        est_cs_non_tr += pt_fgm.get(pt, 0) * dist[0]

    cs_gap = CS_FGM - est_cs_non_tr
    tr_fgm = pt_fgm.get("Transition", 0)
    transition_cs_fgm      = max(cs_gap, 0.0)
    transition_cs_fraction = transition_cs_fgm / tr_fgm if tr_fgm > 0 else 0.0
    transition_non_cs      = 1.0 - transition_cs_fraction
    TRANSITION_DIST = (
        transition_cs_fraction,
        transition_non_cs * 0.05,   # PU1
        transition_non_cs * 0.15,   # PU2
        transition_non_cs * 0.40,   # LT10_01
        transition_non_cs * 0.40,   # LT10_2
    )

    tr_sanity_ok = 0.05 <= transition_cs_fraction <= 0.40

    if verbose:
        p(f"  est_cs_non_transition: {est_cs_non_tr:>8,.1f}")
        p(f"  actual CS_FGM:         {CS_FGM:>8,}")
        p(f"  gap -> Transition C&S: {cs_gap:>8,.1f}")
        p()
        p(f"  transition_cs_fraction: {transition_cs_fraction:.4f}  ({transition_cs_fraction:.1%})")
        sanity_label = "PASS" if tr_sanity_ok else "WARN -- outside expected 0.05-0.40 range"
        p(f"  Sanity check (0.05-0.40): {sanity_label}")
        p(f"  Transition dist: CS={TRANSITION_DIST[0]:.4f}  PU1={TRANSITION_DIST[1]:.4f}  "
          f"PU2={TRANSITION_DIST[2]:.4f}  LT10_01={TRANSITION_DIST[3]:.4f}  LT10_2={TRANSITION_DIST[4]:.4f}")

    # -------------------------------------------------------------------------
    if verbose:
        p()
        p(SEP)
        p("STEP 2: Fully-assisted play types")
        p(SEP)

    ast_fgm_leq1  = 0.0
    ast_fgm_2plus = 0.0
    ast_pts_leq1  = 0.0

    for pt, dist in FULLY_ASSISTED_DIST.items():
        fgm = pt_fgm.get(pt, 0)
        cs, pu1, pu2, lt10_01, lt10_2 = dist
        fl  = fgm * (cs + pu1 + lt10_01)
        f2p = fgm * (pu2 + lt10_2)
        pl  = fgm * (cs * CS_PPM + pu1 * PU_PPM + lt10_01 * LT10_PPM)
        ast_fgm_leq1  += fl
        ast_fgm_2plus += f2p
        ast_pts_leq1  += pl
        if verbose:
            p(f"  {pt:<12}  FGM={fgm:>6,}  leq1={cs+pu1+lt10_01:.2f}  "
              f"FGM_leq1={fl:>8,.1f}  PTS_leq1={pl:>9,.1f}")

    if verbose:
        p(f"\n  Subtotal: FGM_leq1={ast_fgm_leq1:,.1f}  FGM_2plus={ast_fgm_2plus:,.1f}  "
          f"PTS_leq1={ast_pts_leq1:,.1f}")

    # -------------------------------------------------------------------------
    if verbose:
        p()
        p(SEP)
        p("STEP 3: Partial-assist play types")
        p(SEP)

    partial_cfg = {
        "Transition": {"dist": TRANSITION_DIST, "ar": PARTIAL_ASSIST_RATES["Transition"]},
        "Postup":     {"dist": PARTIAL_FIXED_DIST["Postup"], "ar": PARTIAL_ASSIST_RATES["Postup"]},
        "Misc":       {"dist": PARTIAL_FIXED_DIST["Misc"],   "ar": PARTIAL_ASSIST_RATES["Misc"]},
    }

    for pt, cfg in partial_cfg.items():
        fgm = pt_fgm.get(pt, 0)
        cs, pu1, pu2, lt10_01, lt10_2         = cfg["dist"]
        ar_cs, ar_pu1, ar_pu2, ar_lt10_01, ar_lt10_2 = cfg["ar"]

        fl  = fgm * (cs * ar_cs + pu1 * ar_pu1 + lt10_01 * ar_lt10_01)
        f2p = fgm * (pu2 * ar_pu2 + lt10_2 * ar_lt10_2)
        pl  = fgm * (cs * ar_cs * CS_PPM + pu1 * ar_pu1 * PU_PPM + lt10_01 * ar_lt10_01 * LT10_PPM)

        ast_fgm_leq1  += fl
        ast_fgm_2plus += f2p
        ast_pts_leq1  += pl

        if verbose:
            eff = cs * ar_cs + pu1 * ar_pu1 + lt10_01 * ar_lt10_01
            p(f"  {pt:<12}  FGM={fgm:>6,}  eff_leq1={eff:.4f}  "
              f"FGM_leq1={fl:>7,.1f}  FGM_2plus={f2p:>6,.1f}  PTS_leq1={pl:>8,.1f}")

    if verbose:
        p(f"\n  classified_ast_fgm = {ast_fgm_leq1 + ast_fgm_2plus:,.1f}")

    # -------------------------------------------------------------------------
    if verbose:
        p()
        p(SEP)
        p("STEP 4: Residual")
        p(SEP)

    classified_ast   = ast_fgm_leq1 + ast_fgm_2plus
    residual_ast_fgm = TOTAL_AST_FGM - classified_ast
    residual_pct     = residual_ast_fgm / TOTAL_AST_FGM

    classified_pt_fgm = sum(pt_fgm.values())
    unclassified_fgm  = TOTAL_FGM - classified_pt_fgm
    residual_size_ok  = residual_pct < 0.20

    resid_fl = residual_ast_fgm * RESIDUAL_LEQ1_RATE
    resid_pl = resid_fl * RESIDUAL_PPM
    ast_fgm_leq1  += resid_fl
    ast_pts_leq1  += resid_pl

    if verbose:
        p(f"  Synergy classified FGM:         {classified_pt_fgm:>7,}")
        p(f"  Tracking TOTAL_FGM:             {TOTAL_FGM:>7,}")
        p(f"  Unclassified FGM gap:           {unclassified_fgm:>7,}  ({unclassified_fgm/TOTAL_FGM:.1%})")
        p(f"  classified_ast_fgm:             {classified_ast:>9,.1f}")
        p(f"  TOTAL_AST_FGM:                  {TOTAL_AST_FGM:>9,}")
        p(f"  residual_ast_fgm:               {residual_ast_fgm:>9,.1f}  ({residual_pct:.1%} of total)")
        p(f"  Residual size: {'PASS (<20%)' if residual_size_ok else 'WARN (>=20%)'}")

    # -------------------------------------------------------------------------
    if verbose:
        p()
        p(SEP)
        p("STEP 5: Final ratios")
        p(SEP)

    PCT_AST_FGM_IN_PA = ast_fgm_leq1 / TOTAL_AST_FGM
    PCT_AST_PTS_IN_PA = ast_pts_leq1 / TOTAL_AST_PTS
    ratio_sanity_ok   = 0.65 <= PCT_AST_PTS_IN_PA <= 0.90

    if verbose:
        p(f"  total_ast_fgm_leq1: {ast_fgm_leq1:>8,.1f}")
        p(f"  total_ast_pts_leq1: {ast_pts_leq1:>8,.1f}")
        p(f"  PCT_AST_FGM_IN_PA:  {PCT_AST_FGM_IN_PA:.4f}  ({PCT_AST_FGM_IN_PA:.1%})")
        p(f"  PCT_AST_PTS_IN_PA:  {PCT_AST_PTS_IN_PA:.4f}  ({PCT_AST_PTS_IN_PA:.1%})")
        p(f"  Sanity (0.65-0.90): {'PASS' if ratio_sanity_ok else 'WARN -- outside expected range'}")

    # -------------------------------------------------------------------------
    if verbose:
        p()
        p(SEP)
        p("VALIDATION: Tracking column reconciliation")
        p(SEP)

    all_dist_map = {}
    for pt, dist in FULLY_ASSISTED_DIST.items():  all_dist_map[pt] = dist
    for pt, dist in UNASSISTED_DIST.items():       all_dist_map[pt] = dist
    all_dist_map["Transition"] = TRANSITION_DIST
    all_dist_map["Postup"]     = PARTIAL_FIXED_DIST["Postup"]
    all_dist_map["Misc"]       = PARTIAL_FIXED_DIST["Misc"]

    est_CS   = sum(pt_fgm.get(pt, 0) * all_dist_map[pt][0] for pt in all_dist_map)
    est_PU   = sum(pt_fgm.get(pt, 0) * (all_dist_map[pt][1] + all_dist_map[pt][2]) for pt in all_dist_map)
    est_LT10 = sum(pt_fgm.get(pt, 0) * (all_dist_map[pt][3] + all_dist_map[pt][4]) for pt in all_dist_map)
    est_tot  = est_CS + est_PU + est_LT10

    trk_PU_FGM   = PU_FGM
    trk_LT10_FGM = TOTAL_FGM - CS_FGM - PU_FGM - trk.get("OTHER_FGM", TOTAL_FGM - CS_FGM - PU_FGM - 472)
    trk_ex_other = TOTAL_FGM - trk.get("OTHER_FGM", 472)

    if verbose:
        p(f"  {'Column':<22} {'Estimated':>10}  {'Actual':>8}  {'Delta':>8}  {'Delta%':>7}")
        p(f"  {'-'*60}")
        for col, est, actual in [
            ("CS",               est_CS,   CS_FGM),
            ("PU",               est_PU,   trk_PU_FGM),
            ("LT10",             est_LT10, 54770),   # from tracking shots spec
            ("Total (ex Other)", est_tot,  trk_ex_other),
        ]:
            d  = est - actual
            dp = d / actual * 100
            p(f"  {col:<22} {est:>10,.0f}  {actual:>8,}  {d:>+8,.0f}  {dp:>+6.1f}%")

    return {
        "transition_cs_fraction": round(transition_cs_fraction, 6),
        "residual_pct":           round(residual_pct, 6),
        "PCT_AST_FGM_IN_PA":      round(PCT_AST_FGM_IN_PA, 6),
        "PCT_AST_PTS_IN_PA":      round(PCT_AST_PTS_IN_PA, 6),
        "validation": {
            "transition_cs_sanity_ok": tr_sanity_ok,
            "residual_size_ok":        residual_size_ok,
            "ratio_sanity_ok":         ratio_sanity_ok,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compute PCT_AST_PTS_IN_PA for a given season"
    )
    parser.add_argument("--season",      required=True, help='e.g. "2024-25"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season           = args.season
    season_type      = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    print(f"[INFO] Computing PCT_AST_PTS_IN_PA for {season} ({season_type})")
    print()

    inputs = load_inputs(season, season_type_slug)
    result = compute(inputs, verbose=True)

    pct = result["PCT_AST_PTS_IN_PA"]
    all_ok = all(result["validation"].values())

    print()
    print("=" * 65)
    print("RESULT")
    print("=" * 65)
    print(f"  PCT_AST_PTS_IN_PA = {pct:.4f}  ({pct:.1%})")
    print(f"  All validation checks: {'PASS' if all_ok else 'FAIL'}")

    out_dir  = os.path.join(REPO_ROOT, "assets", "data", "raw", season, season_type_slug)
    out_path = os.path.join(out_dir, "pct_ast_pts_in_pa.json")
    tmp_path = out_path + ".tmp"

    output = {
        "generated_at_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "season":      season,
        "season_type": season_type,
        "source":      "computed by scripts/calculate/compute_pct_ast_pts.py",
        "spec":        "docs/pct_ast_pts_in_pa_spec.md",
        "inputs": {
            "tracking_shots":  f"assets/data/raw/{season}/{season_type_slug}/nba_tracking_shots.json",
            "play_types":      f"assets/data/raw/{season}/{season_type_slug}/nba_playtypes.parquet",
            "staging_parquet": f"assets/data/staging/{season}__{season_type_slug}.parquet",
        },
        "intermediate": {
            "transition_cs_fraction": result["transition_cs_fraction"],
            "residual_pct":           result["residual_pct"],
            "PCT_AST_FGM_IN_PA":      result["PCT_AST_FGM_IN_PA"],
        },
        "values": {
            "PCT_AST_PTS_IN_PA": pct,
        },
        "validation": result["validation"],
    }

    os.makedirs(out_dir, exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    os.replace(tmp_path, out_path)

    print(f"\n[OK] Wrote {out_path}")

    if not all_ok:
        print("[WARN] One or more validation checks outside expected bounds -- inspect output above.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
