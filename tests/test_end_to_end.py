#!/usr/bin/env python
"""
tests/test_end_to_end.py — Comprehensive pipeline validation suite.

Run with:
    .venv/Scripts/python.exe tests/test_end_to_end.py
"""

import sys
import io
import os
import json
import unicodedata
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────────────────────
REPO_ROOT  = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SEASON_DIR = os.path.join(REPO_ROOT, "assets", "data", "season")
MASTER_CSV = os.path.join(REPO_ROOT, "assets", "data", "league-table-combined.csv")

SEASONS = [
    ("2013-14", "2014"), ("2014-15", "2015"), ("2015-16", "2016"),
    ("2016-17", "2017"), ("2017-18", "2018"), ("2018-19", "2019"),
    ("2019-20", "2020"), ("2020-21", "2021"), ("2021-22", "2022"),
    ("2022-23", "2023"), ("2023-24", "2024"), ("2024-25", "2025"),
    ("2025-26", "2026"),
]
PRIMARY_SEASON = "2024-25"
PRIMARY_YR     = "2025"
POSS_MIN       = 600

HAS_DATA_COLS = [
    "Has data? All", "Has data? Any category",
    "Has data? ISO", "Has data? PNRBH", "Has data? Post-Up", "Has data? Misc",
    "Has data? On-ball", "Has data? Passing",
    "Has data? Roll and Pop", "Has data? Handoff", "Has data? Off-ball: Partner",
    "Has data? Spot-Up", "Has data? Off Screen", "Has data? Off-ball: Space",
    "Has data? Open Rim", "Has data? Putbacks", "Has data? Off-ball: Crash",
    "Has data? Transition",
]

# Category metric cols that should be blank when "Has data?" is False
CATEGORY_GATE = {
    "Has data? On-ball":          ["On-ball PRF/g", "On-ball Plays/g", "On-ball PC/g", "On-ball rORTG"],
    "Has data? Off-ball: Partner":["Off-ball: Partner PRF/g", "Off-ball: Partner Plays/g",
                                   "Off-ball: Partner PC/g", "Off-ball: Partner rORTG"],
    "Has data? Off-ball: Space":  ["Off-ball: Space PRF/g", "Off-ball: Space Plays/g",
                                   "Off-ball: Space PC/g", "Off-ball: Space rORTG"],
    "Has data? Off-ball: Crash":  ["Off-ball: Crash PRF/g", "Off-ball: Crash Plays/g",
                                   "Off-ball: Crash PC/g", "Off-ball: Crash rORTG"],
    "Has data? Transition":       ["Transition PRF/g", "Transition Plays/g",
                                   "Transition PC/g", "Transition rORTG"],
}

ANY_CAT_METRIC_COLS = [
    "Total PRF/g", "Total Plays/g", "Total PC/g (ex. floor raising)",
    "Total PC/g (floor raising adj.)", "Scoring PC/g", "Playmaking PC/g",
    "Floor raising PC/g",
]


# ── Helpers ────────────────────────────────────────────────────────────────────
def load_csv(yr: str) -> pd.DataFrame:
    path = os.path.join(SEASON_DIR, f"league-table-{yr}.csv")
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)

def f(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce")

def fmt_failures(rows: list, cols: list, limit: int = 10) -> str:
    lines = []
    for row in rows[:limit]:
        lines.append("    " + "  ".join(f"{c}={row.get(c, '?')!r}" for c in cols))
    if len(rows) > limit:
        lines.append(f"    ... and {len(rows) - limit} more")
    return "\n".join(lines) if lines else ""

def identity_check(df: pd.DataFrame, lhs: str, rhs_cols: list,
                   tol: float = 0.01, fillna_zero: bool = False) -> tuple:
    """
    Check lhs == sum(rhs_cols) within tolerance.
    If fillna_zero, treat blank rhs components as 0 (category may not have data).
    Returns (max_err, n_failures, failure_rows).
    """
    lhs_v = f(df, lhs)
    rhs_v = sum(
        f(df, c).fillna(0.0) if fillna_zero else f(df, c)
        for c in rhs_cols
    )
    err = (lhs_v - rhs_v).abs()
    both = lhs_v.notna() & rhs_v.notna()
    err_masked = err[both]
    max_err = float(err_masked.max()) if len(err_masked) > 0 else 0.0
    fails = df[both & (err > tol)].copy()
    fails["_err"] = err[both & (err > tol)]
    rows = [
        {**{c: r[c] for c in ["Player"] + [lhs] + rhs_cols if c in df.columns},
         "_err": f"{r['_err']:.4f}"}
        for _, r in fails.head(10).iterrows()
    ]
    return max_err, len(fails), rows


# ── Results tracker ────────────────────────────────────────────────────────────
_results: list = []

def record(section: str, check_id: str, label: str, passed: bool, details: str = ""):
    _results.append((section, check_id, label, passed, details))

def run_check(section: str, check_id: str, label: str,
              passed: bool, details: str = "") -> bool:
    record(section, check_id, label, passed, details)
    return passed


# ══════════════════════════════════════════════════════════════════════════════
# Load all season CSVs once
# ══════════════════════════════════════════════════════════════════════════════
print("Loading season CSVs...", end=" ", flush=True)
season_dfs: dict[str, pd.DataFrame] = {s: load_csv(yr) for s, yr in SEASONS}
print("done.")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — STRUCTURAL CHECKS
# ══════════════════════════════════════════════════════════════════════════════
print("\n[Section 1] Structural checks...", flush=True)

for season, yr in SEASONS:
    df = season_dfs[season]

    # 1a. Column count = 151
    ok = len(df.columns) == 151
    run_check("1", "1a", f"{season} column count=151", ok,
              f"got {len(df.columns)}" if not ok else "")

    # 1b. No duplicate players (known exception: 2013-14 has two real players named Tony Mitchell)
    KNOWN_DUPES = {"2013-14": {"Tony Mitchell"}}
    dupes_mask = df["Player"].duplicated(keep=False)
    dupe_names = set(df.loc[dupes_mask, "Player"].tolist())
    unexpected_dupes = dupe_names - KNOWN_DUPES.get(season, set())
    ok = len(unexpected_dupes) == 0
    run_check("1", "1b", f"{season} no duplicate players", ok,
              f"unexpected dupes: {sorted(unexpected_dupes)[:5]}" if not ok else "")

    # 1c. No literal "nan" / "NaN" strings
    nan_mask = (df == "nan") | (df == "NaN")
    ok = not nan_mask.any().any()
    details = ""
    if not ok:
        bad_cols = [c for c in df.columns if nan_mask[c].any()]
        details = f"cols with 'nan' strings: {bad_cols[:5]}"
    run_check("1", "1c", f"{season} no literal 'nan' strings", ok, details)

    # 1d. Year column matches season
    bad_year = df[df["Year"] != season]
    ok = len(bad_year) == 0
    run_check("1", "1d", f"{season} Year column = '{season}'", ok,
              f"{len(bad_year)} rows with wrong Year: {bad_year['Year'].unique().tolist()}"
              if not ok else "")

    # 1e. Season type = "RS" for all rows
    bad_st = df[df["Season type"] != "RS"]
    ok = len(bad_st) == 0
    run_check("1", "1e", f"{season} Season type = 'RS'", ok,
              f"{len(bad_st)} rows with wrong type: {bad_st['Season type'].unique().tolist()}"
              if not ok else "")

    # 1f. Games and Minutes must be positive; Possessions may be blank (players with no PBPStats data)
    for col in ["Games", "Minutes"]:
        vals = f(df, col)
        bad = df[(vals.isna()) | (vals <= 0)]
        ok = len(bad) == 0
        run_check("1", "1f", f"{season} {col} > 0 for all rows", ok,
                  f"{len(bad)} rows with {col} <= 0 or missing" if not ok else "")
    # Possessions: only fail if present (non-blank) but <= 0
    poss_vals = f(df, "Possessions")
    poss_blank = df["Possessions"] == ""
    bad_poss = df[~poss_blank & (poss_vals.isna() | (poss_vals <= 0))]
    ok = len(bad_poss) == 0
    run_check("1", "1f", f"{season} Possessions > 0 when present", ok,
              f"{len(bad_poss)} rows with non-blank Possessions <= 0" if not ok else "")

    # 1g. Has data? columns contain only "True", "False", or blank
    valid = {"True", "False", ""}
    for col in HAS_DATA_COLS:
        if col not in df.columns:
            run_check("1", "1g", f"{season} {col} valid values", False,
                      "column missing")
            continue
        bad_vals = df[~df[col].isin(valid)][col].unique()
        ok = len(bad_vals) == 0
        run_check("1", "1g", f"{season} '{col}' valid values", ok,
                  f"unexpected values: {bad_vals.tolist()[:5]}" if not ok else "")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — ACCOUNTING IDENTITIES
# ══════════════════════════════════════════════════════════════════════════════
print("[Section 2] Accounting identities...", flush=True)

TOL = 0.01

# For identities involving categories that may be blank for a given player,
# use fillna_zero=True so missing categories count as 0.

for season, yr in SEASONS:
    df = season_dfs[season]

    # For 2a-2c: only check rows where all 5 category has_data flags are True.
    # Players with partial data (e.g. passing but no on-ball) have PM/FR in Total
    # but blank in individual columns; restricting to fully-gated rows avoids false failures.
    has_all_cats = (
        (df.get("Has data? On-ball",           pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Partner", pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Space",   pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Crash",   pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Transition",        pd.Series("", index=df.index)) == "True")
    )
    df_all_cats = df[has_all_cats]

    # 2a. Total PRF/g = sum of category PRF/g
    max_e, n, rows = identity_check(df_all_cats, "Total PRF/g",
        ["On-ball PRF/g", "Off-ball: Partner PRF/g", "Off-ball: Space PRF/g",
         "Off-ball: Crash PRF/g", "Transition PRF/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2a", f"{season} Total PRF/g = Σ category PRF/g",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "Total PRF/g"]) if n else ""))

    # 2b. Total Plays/g = sum of category Plays/g
    max_e, n, rows = identity_check(df_all_cats, "Total Plays/g",
        ["On-ball Plays/g", "Off-ball: Partner Plays/g", "Off-ball: Space Plays/g",
         "Off-ball: Crash Plays/g", "Transition Plays/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2b", f"{season} Total Plays/g = Σ category Plays/g",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "Total Plays/g"]) if n else ""))

    # 2c. Total PC/g (ex floor raising) = sum of category PC/g
    max_e, n, rows = identity_check(df_all_cats, "Total PC/g (ex. floor raising)",
        ["On-ball PC/g", "Off-ball: Partner PC/g", "Off-ball: Space PC/g",
         "Off-ball: Crash PC/g", "Transition PC/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2c", f"{season} Total PC/g (ex FR) = Σ category PC/g",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "Total PC/g (ex. floor raising)"]) if n else ""))

    # 2d. Total PC/g (floor raising adj.) = Total PC/g (ex FR) + Floor raising PC/g
    max_e, n, rows = identity_check(df, "Total PC/g (floor raising adj.)",
        ["Total PC/g (ex. floor raising)", "Floor raising PC/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2d", f"{season} Total PC/g (adj.) = (ex FR) + FR",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "Total PC/g (floor raising adj.)", "Total PC/g (ex. floor raising)", "Floor raising PC/g"]) if n else ""))

    # 2e. On-ball PC/g (floor raising adj.) = On-ball PC/g + Floor raising PC/g
    max_e, n, rows = identity_check(df, "On-ball PC/g (floor raising adj.)",
        ["On-ball PC/g", "Floor raising PC/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2e", f"{season} On-ball PC/g (adj.) = On-ball PC/g + FR",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player"]) if n else ""))

    # 2f. Total PC/g (floor raising adj.) = Scoring + Playmaking + Floor raising
    # Only check rows where has_ob=True (PM and FR columns are gated on has_ob)
    df_ob = df[df.get("Has data? On-ball", pd.Series("", index=df.index)) == "True"]
    max_e, n, rows = identity_check(df_ob, "Total PC/g (floor raising adj.)",
        ["Scoring PC/g", "Playmaking PC/g", "Floor raising PC/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2f", f"{season} Total PC/g (adj.) = SC + PM + FR",
              n == 0, f"max_err={max_e:.6f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "Scoring PC/g", "Playmaking PC/g", "Floor raising PC/g", "Total PC/g (floor raising adj.)"]) if n else ""))

    # 2g–2i. Half court = On-ball + Partner + Space + Crash
    # Only check rows where all 4 HC category flags are True
    has_all_hc = (
        (df.get("Has data? On-ball",           pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Partner", pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Space",   pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Crash",   pd.Series("", index=df.index)) == "True")
    )
    df_all_hc = df[has_all_hc]
    for metric, label in [("PRF/g", "2g"), ("Plays/g", "2h"), ("PC/g", "2i")]:
        max_e, n, rows = identity_check(df_all_hc, f"Half court {metric}",
            [f"On-ball {metric}", f"Off-ball: Partner {metric}",
             f"Off-ball: Space {metric}", f"Off-ball: Crash {metric}"],
            tol=TOL, fillna_zero=True)
        run_check("2", label, f"{season} Half court {metric} = Σ HC categories",
                  n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                      "\n" + fmt_failures(rows, ["Player"]) if n else ""))

    # 2j–2l. Off-ball = Partner + Space + Crash
    # Only check rows where all 3 off-ball category flags are True
    has_all_ob = (
        (df.get("Has data? Off-ball: Partner", pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Space",   pd.Series("", index=df.index)) == "True") &
        (df.get("Has data? Off-ball: Crash",   pd.Series("", index=df.index)) == "True")
    )
    df_all_ob = df[has_all_ob]
    for metric, label in [("PRF/g", "2j"), ("Plays/g", "2k"), ("PC/g", "2l")]:
        max_e, n, rows = identity_check(df_all_ob, f"Off-ball {metric}",
            [f"Off-ball: Partner {metric}", f"Off-ball: Space {metric}",
             f"Off-ball: Crash {metric}"],
            tol=TOL, fillna_zero=True)
        run_check("2", label, f"{season} Off-ball {metric} = Σ off-ball categories",
                  n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                      "\n" + fmt_failures(rows, ["Player"]) if n else ""))

    # 2m. On-ball PC/g = On-ball Scoring PC/g + On-ball Playmaking PC/g
    # Only check rows where has_ob = True
    max_e, n, rows = identity_check(df_ob, "On-ball PC/g",
        ["On-ball Scoring PC/g", "On-ball Playmaking PC/g"],
        tol=TOL, fillna_zero=True)
    run_check("2", "2m", f"{season} On-ball PC/g = Scoring + Playmaking",
              n == 0, f"max_err={max_e:.4f}, n_fail={n}" + (
                  "\n" + fmt_failures(rows, ["Player", "On-ball PC/g", "On-ball Scoring PC/g", "On-ball Playmaking PC/g"]) if n else ""))

# 2n–2q: 2024-25 only, require pipeline intermediates
print("  [2n-2q] Loading pipeline intermediates for 2024-25...", end=" ", flush=True)

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
import scripts.calculate.build_season as bs
from scripts.calculate.config import build_config

_stg = pd.read_parquet(os.path.join(REPO_ROOT, "assets", "data", "staging",
                                     f"{PRIMARY_SEASON}__regular.parquet"))
_cfg = build_config(os.path.join(REPO_ROOT, "assets", "data", "raw",
                                  PRIMARY_SEASON, "regular", "ctg_league_averages.json"))
_cfg["PCT_AST_PTS_IN_PA"] = json.load(open(os.path.join(
    REPO_ROOT, "assets", "data", "raw", PRIMARY_SEASON, "regular",
    "pct_ast_pts_in_pa.json")))["values"]["PCT_AST_PTS_IN_PA"]
_stg = bs.compute_scoring_share_of_tov(_stg)

_pt1a = {pt["slug"]: bs.phase1a_playtype(_stg, pt["slug"]) for pt in bs.PLAYTYPES}
_pt2, _tr_agg = bs.phase2_transition(_stg, _cfg)
_pt3a, _agg3a = bs.phase3a_passing(_stg, _pt1a, _pt2, _cfg)
_pt3b = bs.phase3b_passing(_pt3a, _agg3a, _tr_agg["TR_PPP_RATIO"], _cfg)
_pt4  = bs.phase4_transition(_pt2, _pt3b, _tr_agg, _cfg)
_gp   = _stg.groupby("PLAYER_ID")["nba_trad__GP"].first()
_csv  = season_dfs[PRIMARY_SEASON].copy()
_csv["_pid"] = _csv["Player"].map(
    _pt3a.set_index("Player")["PLAYER_ID"].to_dict()
)
print("done.")

# ── Helper: compare CSV column/g * Games to expected per-player totals ─────────
_pid_to_name = _pt3a.set_index("PLAYER_ID")["Player"].to_dict()

def intermediate_identity_check(check_id, label, csv_col, expected_by_pid,
                                 has_flag, tol_per_game=0.01):
    """
    For each CSV row where has_flag='True' and PLAYER_ID is known:
        compare  csv_col/g * Games  vs  expected_by_pid[PLAYER_ID]
    Tolerance: tol_per_game * Games (i.e. 0.01 pts per game played).
    """
    failures = []
    n_checked = 0
    for _, row in _csv[_csv[has_flag] == "True"].iterrows():
        pid = row.get("_pid", np.nan)
        if pd.isna(pid) or pid not in expected_by_pid.index:
            continue
        gp_v  = pd.to_numeric(row["Games"], errors="coerce")
        csv_v = pd.to_numeric(row[csv_col],  errors="coerce")
        if pd.isna(gp_v) or gp_v == 0 or pd.isna(csv_v):
            continue
        exp   = expected_by_pid[pid]
        if pd.isna(exp):
            continue
        csv_total = csv_v * gp_v
        err       = abs(csv_total - exp)
        tol       = tol_per_game * gp_v
        n_checked += 1
        if err > tol:
            failures.append({
                "Player":   row["Player"],
                "csv_tot":  f"{csv_total:.3f}",
                "expected": f"{exp:.3f}",
                "err":      f"{err:.4f}",
                "tol":      f"{tol:.3f}",
            })
    details = (fmt_failures(failures, ["Player", "csv_tot", "expected", "err", "tol"])
               if failures else f"n_checked={n_checked}")
    run_check("2", check_id, f"{PRIMARY_SEASON} {label}",
              len(failures) == 0, details)

# ── Build intermediate per-player totals ──────────────────────────────────────
OB_SLUGS = ["iso", "prballhandler", "postup", "misc"]

# On-ball scoring PTS and scoring plays (sum across 4 HC slugs)
_ob_sc_pts   = pd.Series(dtype=float)
_ob_sc_plays = pd.Series(dtype=float)
for _s in OB_SLUGS:
    _sub = _pt1a[_s].set_index("PLAYER_ID")
    _ob_sc_pts   = _ob_sc_pts.add(  _sub["pts"],          fill_value=0.0)
    _ob_sc_plays = _ob_sc_plays.add(_sub["scoring_plays"], fill_value=0.0)

# HC playmaking points and plays
_hc_pm_pts   = _pt3b.set_index("PLAYER_ID")["est_hc_pm_pts"]
_hc_pm_plays = _pt3b.set_index("PLAYER_ID")["est_hc_pm_plays_final"]

# Transition scoring PTS, scoring plays, playmaking PTS, playmaking plays
_tr_sc_pts   = _pt2.set_index("PLAYER_ID")["pts"]
_tr_sc_plays = _pt2.set_index("PLAYER_ID")["scoring_plays"]
_tr_pm_pts   = _pt4.set_index("PLAYER_ID")["playmaking_pts"]
_tr_pm_plays = _pt4.set_index("PLAYER_ID")["playmaking_plays"]

# 2n. On-ball PRF/g * Games = OB scoring PTS + HC playmaking PTS
intermediate_identity_check(
    "2n",
    "On-ball PRF/g * Games = OB scoring PTS + HC playmaking PTS",
    "On-ball PRF/g",
    _ob_sc_pts.add(_hc_pm_pts, fill_value=0.0),
    "Has data? On-ball",
)

# 2o. On-ball Plays/g * Games = OB scoring plays + HC playmaking plays
intermediate_identity_check(
    "2o",
    "On-ball Plays/g * Games = OB scoring plays + HC playmaking plays",
    "On-ball Plays/g",
    _ob_sc_plays.add(_hc_pm_plays, fill_value=0.0),
    "Has data? On-ball",
)

# 2p. Transition PRF/g * Games = TR scoring PTS + TR playmaking PTS
intermediate_identity_check(
    "2p",
    "Transition PRF/g * Games = TR scoring PTS + TR playmaking PTS",
    "Transition PRF/g",
    _tr_sc_pts.add(_tr_pm_pts, fill_value=0.0),
    "Has data? Transition",
)

# 2q. Transition Plays/g * Games = TR scoring plays + TR playmaking plays
intermediate_identity_check(
    "2q",
    "Transition Plays/g * Games = TR scoring plays + TR playmaking plays",
    "Transition Plays/g",
    _tr_sc_plays.add(_tr_pm_plays, fill_value=0.0),
    "Has data? Transition",
)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — RATE CONSISTENCY
# ══════════════════════════════════════════════════════════════════════════════
print("[Section 3] Rate consistency...", flush=True)

RATE_TOL   = 0.02
RORTG_TOL  = 0.1

RATE_GROUPS = [
    # (prefix, has_data_col)
    ("On-ball",             "Has data? On-ball"),
    ("Off-ball: Partner",   "Has data? Off-ball: Partner"),
    ("Off-ball: Space",     "Has data? Off-ball: Space"),
    ("Off-ball: Crash",     "Has data? Off-ball: Crash"),
    ("Transition",          "Has data? Transition"),
    ("Total",               "Has data? Any category"),
    ("Half court",          "Has data? On-ball"),        # present when on-ball present
    ("Off-ball",            "Has data? Off-ball: Partner"),
    ("Scoring",             "Has data? Any category"),
    ("Playmaking",          "Has data? Passing"),
]

for season, yr in SEASONS:
    df = season_dfs[season]
    dff = df[pd.to_numeric(df["Possessions"], errors="coerce") >= POSS_MIN].copy()
    gp   = f(dff, "Games")
    mins = f(dff, "Minutes")
    poss = f(dff, "Possessions")

    for prefix, has_col in RATE_GROUPS:
        if has_col not in dff.columns:
            continue
        gate = dff[has_col] == "True"

        # Build column names: some prefixes have special PC column names
        if prefix == "Total":
            pg_col  = "Total PC/g (ex. floor raising)"
            p36_col = "Total PC/36 (ex. floor raising)"
            p75_col = "Total PC/75 (ex. floor raising)"
        else:
            pg_col  = f"{prefix} PC/g"
            p36_col = f"{prefix} PC/36"
            p75_col = f"{prefix} PC/75"

        plays_g   = f"{prefix} Plays/g"
        rortg_col = f"{prefix} rORTG"

        # 3a. /36 and /75 consistency
        for rate, rate_col, divisor in [
            ("/36", p36_col, mins / 36),
            ("/75", p75_col, poss / 75),
        ]:
            if rate_col not in dff.columns:
                continue
            pg_v   = f(dff, pg_col)
            rate_v = f(dff, rate_col)
            div_v  = divisor.clip(lower=1e-9)
            computed = pg_v * gp / div_v
            err      = (computed - rate_v).abs()
            mask     = gate & pg_v.notna() & rate_v.notna()
            max_e    = float(err[mask].max()) if mask.sum() > 0 else 0.0
            n_fail   = int((err[mask] > RATE_TOL).sum())
            run_check("3", "3a", f"{season} {prefix} {rate} consistency",
                      n_fail == 0, f"max_err={max_e:.4f}, n_fail={n_fail}" if n_fail else "")

        # 3b. rORTG = 100 * PC/g / Plays/g
        if rortg_col in dff.columns and plays_g in dff.columns:
            pg_v    = f(dff, pg_col)
            plays_v = f(dff, plays_g)
            rortg_v = f(dff, rortg_col)
            computed = 100.0 * pg_v / plays_v.replace(0, np.nan)
            err      = (computed - rortg_v).abs()
            mask     = gate & pg_v.notna() & plays_v.notna() & rortg_v.notna() & (plays_v > 0)
            max_e    = float(err[mask].max()) if mask.sum() > 0 else 0.0
            n_fail   = int((err[mask] > RORTG_TOL).sum())
            run_check("3", "3b", f"{season} {prefix} rORTG = 100*PC/Plays",
                      n_fail == 0, f"max_err={max_e:.4f}, n_fail={n_fail}" if n_fail else "")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DATA GATING
# ══════════════════════════════════════════════════════════════════════════════
print("[Section 4] Data gating...", flush=True)

for season, yr in SEASONS:
    df = season_dfs[season]

    # 4a. If "Has data?" = False, metric cols should be blank ("")
    for has_col, metric_cols in CATEGORY_GATE.items():
        if has_col not in df.columns:
            continue
        no_data = df[df[has_col] == "False"]
        for mc in metric_cols:
            if mc not in df.columns:
                continue
            non_blank = no_data[no_data[mc] != ""]
            ok = len(non_blank) == 0
            run_check("4", "4a", f"{season} {has_col}=False → {mc} blank",
                      ok, f"{len(non_blank)} rows non-blank: "
                      + non_blank["Player"].head(3).tolist().__repr__()
                      if not ok else "")

    # 4b. If "Has data? Any category" = False, all metric cols blank
    no_any = df[df["Has data? Any category"] == "False"]
    for mc in ANY_CAT_METRIC_COLS:
        if mc not in df.columns:
            continue
        non_blank = no_any[no_any[mc] != ""]
        ok = len(non_blank) == 0
        run_check("4", "4b", f"{season} Any=False → {mc} blank",
                  ok, f"{len(non_blank)} rows non-blank" if not ok else "")

    # 4c. If "Has data? All" = True, every category flag should also be True
    all_true = df[df["Has data? All"] == "True"]
    sub_flags = [
        "Has data? On-ball", "Has data? Off-ball: Partner",
        "Has data? Off-ball: Space", "Has data? Off-ball: Crash",
        "Has data? Transition", "Has data? Passing",
    ]
    for flag in sub_flags:
        if flag not in df.columns:
            continue
        not_true = all_true[all_true[flag] != "True"]
        ok = len(not_true) == 0
        run_check("4", "4c", f"{season} Has data? All=True → {flag}=True",
                  ok, f"{len(not_true)} rows where All=True but {flag}!=True"
                  if not ok else "")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — RANGE SANITY (2024-25, Poss >= 600)
# ══════════════════════════════════════════════════════════════════════════════
print("[Section 5] Range sanity...", flush=True)

df25 = season_dfs[PRIMARY_SEASON].copy()
df25["_poss"] = f(df25, "Possessions")
dff25 = df25[df25["_poss"] >= POSS_MIN].copy()

def range_check(col: str, lo: float, hi: float, label: str) -> None:
    v = f(dff25, col).dropna()
    if len(v) == 0:
        run_check("5", "5_range", label, False, "no data")
        return
    bad_lo = v[v < lo]
    bad_hi = v[v > hi]
    ok = len(bad_lo) == 0 and len(bad_hi) == 0
    details = ""
    if not ok:
        parts = []
        if len(bad_lo):
            parts.append(f"{len(bad_lo)} values < {lo} (min={v.min():.3f})")
        if len(bad_hi):
            parts.append(f"{len(bad_hi)} values > {hi} (max={v.max():.3f})")
        details = "; ".join(parts)
    run_check("5", "5_range", label, ok, details)

range_check("Total PC/g (floor raising adj.)",  -5,  10,  "5a Total PC/g (adj.) in [-5, 10]")
range_check("Scoring PC/g",                     -5,   6,  "5b Scoring PC/g in [-5, 6]")
range_check("Playmaking PC/g",                  -3,   4,  "5c Playmaking PC/g in [-3, 4]")
range_check("Floor raising PC/g",             -1.5,   3,  "5d Floor raising PC/g in [-1.5, 3]")
range_check("Total creation usage",              0,  0.65, "5f Total creation usage in [0, 0.65]")
range_check("% Playmaking",                      0,  1.0, "5g % Playmaking in [0, 1.0]")

# 5e. rORTG columns in [-40, 40].
# For category rORTGs, only check players with >= 1 play/g in that category —
# players with < 1 play/g produce extreme but mathematically expected values.
for cat in ["On-ball", "Off-ball: Partner", "Off-ball: Space", "Off-ball: Crash",
            "Transition", "Total", "Scoring", "Playmaking"]:
    col = f"{cat} rORTG"
    plays_col = f"{cat} Plays/g"
    if col not in dff25.columns:
        continue
    if plays_col in dff25.columns:
        sub = dff25[pd.to_numeric(dff25[plays_col], errors="coerce") >= 1.0]
    else:
        sub = dff25
    v = pd.to_numeric(sub[col], errors="coerce").dropna()
    bad_lo = v[v < -50]
    bad_hi = v[v > 50]
    ok = len(bad_lo) == 0 and len(bad_hi) == 0
    details = ""
    if not ok:
        parts = []
        if len(bad_lo):
            parts.append(f"{len(bad_lo)} values < -50 (min={v.min():.3f})")
        if len(bad_hi):
            parts.append(f"{len(bad_hi)} values > 50 (max={v.max():.3f})")
        details = "; ".join(parts)
    run_check("5", "5e", f"5e {col} in [-50, 50] (plays/g >= 1)", ok, details)

# 5h. No PRF/g values exceeding 60
for cat in ["On-ball", "Off-ball: Partner", "Off-ball: Space", "Off-ball: Crash",
            "Transition", "Total", "Half court", "Off-ball", "Scoring", "Playmaking"]:
    col = f"{cat} PRF/g"
    if col in dff25.columns:
        v = f(dff25, col).dropna()
        bad = v[v.abs() > 60]
        run_check("5", "5h", f"5h {col} |value| <= 60",
                  len(bad) == 0,
                  f"{len(bad)} values > 60 (max={v.abs().max():.2f})" if len(bad) else "")

# 5i. No Plays/g values exceeding 50
for cat in ["On-ball", "Off-ball: Partner", "Off-ball: Space", "Off-ball: Crash",
            "Transition", "Total", "Half court", "Off-ball", "Scoring", "Playmaking"]:
    col = f"{cat} Plays/g"
    if col in dff25.columns:
        v = f(dff25, col).dropna()
        bad = v[v > 50]
        run_check("5", "5i", f"5i {col} <= 50",
                  len(bad) == 0,
                  f"{len(bad)} values > 50 (max={v.max():.2f})" if len(bad) else "")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — COMBINED MASTER CSV
# ══════════════════════════════════════════════════════════════════════════════
print("[Section 6] Master CSV...", flush=True)

# 6a. Master CSV exists
ok = os.path.exists(MASTER_CSV)
run_check("6", "6a", "Master CSV exists", ok, MASTER_CSV if not ok else "")

if ok:
    master = pd.read_csv(MASTER_CSV, dtype=str, keep_default_na=False, na_filter=False)

    # 6b. Row count = sum of individual season row counts
    expected_rows = sum(len(season_dfs[s]) for s, _ in SEASONS)
    run_check("6", "6b", f"Master row count = {expected_rows}",
              len(master) == expected_rows,
              f"got {len(master)}, expected {expected_rows}" if len(master) != expected_rows else "")

    # 6c. All seasons present in Year column
    master_years = set(master["Year"].unique())
    expected_years = {s for s, _ in SEASONS}
    missing = expected_years - master_years
    run_check("6", "6c", "All 13 seasons present in master Year column",
              len(missing) == 0,
              f"missing: {sorted(missing)}" if missing else "")

    # 6d. Column count matches individual season CSVs
    run_check("6", "6d", "Master column count = 151",
              len(master.columns) == 151,
              f"got {len(master.columns)}" if len(master.columns) != 151 else "")


# ══════════════════════════════════════════════════════════════════════════════
# RESULTS SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 90)
print("RESULTS SUMMARY")
print("=" * 90)

passed = [r for r in _results if r[3]]
failed = [r for r in _results if not r[3]]
total  = len(_results)

# Print failures first
if failed:
    print(f"\nFAILURES ({len(failed)}):")
    print("-" * 90)
    for sec, cid, label, _, details in failed:
        print(f"  [S{sec} {cid}] FAIL  {label}")
        if details:
            print(f"         {details}")
    print()

# Summary table by section
print("Section breakdown:")
print(f"  {'Section':<12}  {'Pass':>5}  {'Fail':>5}  {'Total':>6}")
print(f"  {'-'*36}")
for sec_num in ["1", "2", "3", "4", "5", "6"]:
    sec_results = [r for r in _results if r[0] == sec_num]
    sp = sum(1 for r in sec_results if r[3])
    sf = sum(1 for r in sec_results if not r[3])
    label = {
        "1": "Structural",
        "2": "Identities",
        "3": "Rate consistency",
        "4": "Data gating",
        "5": "Range sanity",
        "6": "Master CSV",
    }.get(sec_num, sec_num)
    print(f"  {label:<22}  {sp:>5}  {sf:>5}  {len(sec_results):>6}")

print(f"  {'-'*36}")
print(f"  {'TOTAL':<22}  {len(passed):>5}  {len(failed):>5}  {total:>6}")
print()
print(f"Result: {len(passed)}/{total} checks passed.")

sys.exit(0 if len(failed) == 0 else 1)
