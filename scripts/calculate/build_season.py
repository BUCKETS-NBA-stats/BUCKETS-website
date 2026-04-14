import argparse
import os
import sys

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.calculate.config import build_config

# ── Playtype definitions ──────────────────────────────────────────────────────
# 10 non-Transition playtypes processed in Phase 1.
# slug:     matches the nba_pt_{slug}__ column prefix in the staging parquet.
# name:     human-readable label from the spec and final CSV output.
# category: grouping used in Phase 6 assembly.

PLAYTYPES = [
    {"slug": "iso",           "name": "ISO",         "category": "on_ball"},
    {"slug": "prballhandler", "name": "PNRBH",       "category": "on_ball"},
    {"slug": "postup",        "name": "Post-Up",      "category": "on_ball"},
    {"slug": "misc",          "name": "Misc",         "category": "on_ball"},
    {"slug": "prrollman",     "name": "Roll and Pop", "category": "partner"},
    {"slug": "handoff",       "name": "Handoff",      "category": "partner"},
    {"slug": "spotup",        "name": "Spot-Up",      "category": "space"},
    {"slug": "offscreen",     "name": "Off-Screen",   "category": "space"},
    {"slug": "cut",           "name": "Open Rim",     "category": "crash"},
    {"slug": "offrebound",    "name": "Putbacks",     "category": "crash"},
]

TRANSITION_SLUG = "transition"


# ── ScoringShareOfTOV ─────────────────────────────────────────────────────────

def compute_scoring_share_of_tov(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ScoringShareOfTOV column to the staging DataFrame.

    Formula:
        ScoringShareOfTOV = (Turnovers - BadPassTurnovers - BadPassOutOfBoundsTurnovers)
                            / Turnovers

    Bad-pass turnovers are attributed to playmaking (the player was trying to pass).
    The remainder are attributed to scoring plays (charges, travels on drives, etc.).

    Edge cases:
        Turnovers == 0 → 0.0  (no turnovers means no scoring turnovers)
        Player not in PBPStats (NaN) → 0.0  (warned, then defaulted)
    """
    df = df.copy()

    tov      = df["pbp__Turnovers"]
    # Fill NaN with 0 for granular sub-columns: PBPStats omits the column when
    # the count is zero rather than returning 0, so NaN means 0 here.
    bad_pass = df["pbp__BadPassTurnovers"].fillna(0.0)
    bad_oob  = df["pbp__BadPassOutOfBoundsTurnovers"].fillna(0.0)

    # Warn only for players fully absent from PBPStats (tov itself is null).
    n_missing = int(tov.isna().sum())
    if n_missing > 0:
        print(f"[WARN] ScoringShareOfTOV: {n_missing} players missing PBPStats data — defaulting to 0.0")

    ratio = (tov - bad_pass - bad_oob) / tov
    df["ScoringShareOfTOV"] = ratio.where(tov > 0, 0.0).fillna(0.0)

    return df


# ── Phase 1a ─────────────────────────────────────────────────────────────────

def phase1a_playtype(df: pd.DataFrame, slug: str) -> pd.DataFrame:
    """
    Phase 1a for one non-Transition playtype.

    Computes per-player, for players who have data for this playtype:
        tov_total     = round(TOV_POSS_PCT * POSS, 0)
        scoring_tovs  = round(tov_total * ScoringShareOfTOV, 0)
        scoring_plays = POSS - tov_total + scoring_tovs

    Note: TOV_POSS_PCT is stored as a decimal (0.102 = 10.2%); no /100 needed.

    This is Phase 1a only — xTOVs, TOV penalty, and PAB are deferred to Phase 1b,
    which runs after Lg Avg On-ball TOV rate is available from Phase 3a.

    Args:
        df:   Staging DataFrame with ScoringShareOfTOV already added.
        slug: Playtype slug, e.g. "iso" or "prballhandler".

    Returns:
        DataFrame with columns:
            PLAYER_ID, Player, poss, pts, tov_total, scoring_tovs, scoring_plays
        Only rows where POSS is not null (player appeared in this playtype).
    """
    poss_col = f"nba_pt_{slug}__POSS"
    pts_col  = f"nba_pt_{slug}__PTS"
    tov_col  = f"nba_pt_{slug}__TOV_POSS_PCT"

    mask = df[poss_col].notna()
    sub  = df.loc[mask, ["PLAYER_ID", "Player", poss_col, pts_col, tov_col, "ScoringShareOfTOV"]].copy()

    sub["tov_total"]    = (sub[tov_col] * sub[poss_col]).round(0)
    sub["scoring_tovs"] = (sub["tov_total"] * sub["ScoringShareOfTOV"]).round(0)
    sub["scoring_plays"] = sub[poss_col] - sub["tov_total"] + sub["scoring_tovs"]

    return (
        sub
        .rename(columns={poss_col: "poss", pts_col: "pts"})
        .drop(columns=[tov_col, "ScoringShareOfTOV"])
        .reset_index(drop=True)
    )


# ── Phase 1b ─────────────────────────────────────────────────────────────────

def phase1b_playtype(
    pt1a: pd.DataFrame,
    lg_avg_onball_tov_rate: float,
    cfg: dict,
    baseline_override: float | None = None,
) -> pd.DataFrame:
    """
    Phase 1b for one non-Transition playtype.

    Completes per-player scoring columns that depend on Lg Avg On-ball TOV rate
    (available only after Phase 3a).

    Per-playtype tab aggregate:
        tab_avg_scoring_ppp = SUM(pts) / SUM(scoring_plays)

    Per-player:
        scoring_ppp_baseline = baseline_override if provided,
                               else max(CTG_HC_PPP, tab_avg_scoring_ppp)
        xTOVs                = poss * lg_avg_onball_tov_rate
        tov_penalty          = (scoring_tovs - xTOVs) * CTG_TOV_PENALTY
        pab                  = pts + tov_penalty - (scoring_ppp_baseline * scoring_plays)

    Args:
        pt1a:                   Phase 1a output for this playtype.
        lg_avg_onball_tov_rate: League average on-ball TOV rate (from Phase 3a).
        cfg:                    Config dict (needs CTG_HC_PPP, CTG_TOV_PENALTY).
        baseline_override:      If provided, use this value as the scoring PPP
                                baseline instead of max(CTG_HC_PPP, tab_avg).
                                Used for Putbacks (offrebound), which uses
                                CTG_HC_PPP directly.

    Returns:
        pt1a DataFrame with added columns:
            tab_avg_scoring_ppp, scoring_ppp_baseline, xTOVs, tov_penalty, pab
    """
    df = pt1a.copy()

    tab_avg_scoring_ppp  = df["pts"].sum() / df["scoring_plays"].sum()
    scoring_ppp_baseline = (
        baseline_override
        if baseline_override is not None
        else max(cfg["CTG_HC_PPP"], tab_avg_scoring_ppp)
    )

    df["tab_avg_scoring_ppp"]  = tab_avg_scoring_ppp
    df["scoring_ppp_baseline"] = scoring_ppp_baseline
    df["xTOVs"]                = df["poss"] * lg_avg_onball_tov_rate
    df["tov_penalty"]          = (df["scoring_tovs"] - df["xTOVs"]) * cfg["CTG_TOV_PENALTY"]
    df["pab"]                  = df["pts"] + df["tov_penalty"] - (scoring_ppp_baseline * df["scoring_plays"])

    return df


# ── Phase 2 ───────────────────────────────────────────────────────────────────

def phase2_transition(df: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, dict]:
    """
    Phase 2: Transition Scoring + tab-level aggregates.

    Per-player (same TOV structure as Phase 1a):
        tov_total                  = round(TOV_POSS_PCT * POSS, 0)
        scoring_tovs               = round(tov_total * ScoringShareOfTOV, 0)
        transition_playmaking_tovs = tov_total - scoring_tovs
        poss_played                = pbp__OffPoss  (total offensive possessions)
        scoring_plays              = POSS - tov_total + scoring_tovs

    Tab-level aggregates (returned separately for use in Phase 3a):
        TR_AVG_PPP          = SUM(pts) / SUM(poss)
        TR_OVERALL_TOV_RATE = SUM(tov_total) / SUM(poss)
        TR_AVG_PLAYS        = mean(scoring_plays)
        TR_PPP_RATIO        = TR_AVG_PPP / CTG_HC_PPP

    Args:
        df:  Staging DataFrame with ScoringShareOfTOV already added.
        cfg: Config dict (needs CTG_HC_PPP).

    Returns:
        (player_df, aggregates_dict)
        player_df has columns:
            PLAYER_ID, Player, poss, pts, poss_played,
            tov_total, scoring_tovs, transition_playmaking_tovs, scoring_plays
        aggregates_dict keys: TR_AVG_PPP, TR_OVERALL_TOV_RATE, TR_AVG_PLAYS, TR_PPP_RATIO
    """
    slug     = TRANSITION_SLUG
    poss_col = f"nba_pt_{slug}__POSS"
    pts_col  = f"nba_pt_{slug}__PTS"
    tov_col  = f"nba_pt_{slug}__TOV_POSS_PCT"

    mask = df[poss_col].notna()
    sub  = df.loc[mask, [
        "PLAYER_ID", "Player", poss_col, pts_col, tov_col,
        "ScoringShareOfTOV", "pbp__OffPoss",
    ]].copy()

    sub["tov_total"]    = (sub[tov_col] * sub[poss_col]).round(0)
    sub["scoring_tovs"] = (sub["tov_total"] * sub["ScoringShareOfTOV"]).round(0)
    sub["transition_playmaking_tovs"] = sub["tov_total"] - sub["scoring_tovs"]
    sub["scoring_plays"] = sub[poss_col] - sub["tov_total"] + sub["scoring_tovs"]

    player_df = (
        sub
        .rename(columns={poss_col: "poss", pts_col: "pts", "pbp__OffPoss": "poss_played"})
        .drop(columns=[tov_col, "ScoringShareOfTOV"])
        .reset_index(drop=True)
    )

    tr_avg_ppp          = player_df["pts"].sum()       / player_df["poss"].sum()
    tr_overall_tov_rate = player_df["tov_total"].sum() / player_df["poss"].sum()
    tr_avg_plays        = (100 * player_df["scoring_plays"] / player_df["poss_played"]).mean()
    tr_ppp_ratio        = tr_avg_ppp / cfg["CTG_HC_PPP"]

    aggregates = {
        "TR_AVG_PPP":          tr_avg_ppp,
        "TR_OVERALL_TOV_RATE": tr_overall_tov_rate,
        "TR_AVG_PLAYS":        tr_avg_plays,
        "TR_PPP_RATIO":        tr_ppp_ratio,
    }

    return player_df, aggregates


# ── Phase 3a ─────────────────────────────────────────────────────────────────

# Slugs that contribute to OBS (on-ball scoring) and HC scoring plays
_OBS_SLUGS = ["iso", "prballhandler", "postup"]
_HC_EXTRA_SLUGS = ["spotup", "handoff", "cut", "offscreen", "offrebound", "misc"]
# Note: "prrollman" (Roll & Pop) is excluded from HC scoring plays per spec.


def _sum_scoring_plays(pt1a: dict[str, pd.DataFrame], slugs: list[str]) -> pd.Series:
    """
    Return a Series indexed by PLAYER_ID with the sum of scoring_plays across
    the given slugs. Players absent from a slug are treated as 0.
    """
    frames = [
        pt1a[s].set_index("PLAYER_ID")["scoring_plays"].rename(s)
        for s in slugs
        if s in pt1a
    ]
    if not frames:
        return pd.Series(dtype=float)
    combined = pd.concat(frames, axis=1).fillna(0.0)
    return combined.sum(axis=1)


def _sum_scoring_tovs(pt1a: dict[str, pd.DataFrame], slugs: list[str]) -> pd.Series:
    """Sum of scoring_tovs across the given slugs, indexed by PLAYER_ID."""
    frames = [
        pt1a[s].set_index("PLAYER_ID")["scoring_tovs"].rename(s)
        for s in slugs
        if s in pt1a
    ]
    if not frames:
        return pd.Series(dtype=float)
    combined = pd.concat(frames, axis=1).fillna(0.0)
    return combined.sum(axis=1)


def phase3a_passing(
    df_stg: pd.DataFrame,
    pt1a: dict[str, pd.DataFrame],
    pt2: pd.DataFrame,
    cfg: dict,
) -> tuple[pd.DataFrame, dict]:
    """
    Phase 3a: Passing & Playmaking — through clamped regression and On-ball
    Passing TOVs.  Also computes LG_AVG_PPPA, Lg Avg On-ball TOV rate, and
    HC PPP w/ TOV penalty (step 4–6 in the computation order).

    Inputs:
        df_stg: Staging DataFrame (with ScoringShareOfTOV already added).
        pt1a:   Dict of Phase 1a DataFrames keyed by playtype slug.
        pt2:    Phase 2 Transition DataFrame (per player).
        cfg:    Config dict.

    Returns:
        (player_df, aggregates)

        player_df columns (one row per player with any staging data):
            PLAYER_ID, Player,
            playmaking_tovs, hc_playmaking_tovs,
            ft_assist, playmaking_plays_ex_tov, playmaking_pts,
            actual_pppa, pppa_padded,
            obs_plays, hc_scoring_plays, tr_scoring_plays,
            total_scoring_plays, total_playmaking_plays,
            hc_scoring_pct, hc_scoring_pct_clamped,
            est_hc_pm_plays, est_tr_pm_plays,
            est_hc_pm_plays_clamped, est_tr_pm_plays_clamped,
            on_ball_passing_tovs

        aggregates keys:
            LG_AVG_PPPA, lg_avg_onball_tov_rate, HC_PPP_w_tov_penalty
    """
    # ── Base player list from staging ─────────────────────────────────────────
    base = df_stg[["PLAYER_ID", "Player"]].dropna(subset=["PLAYER_ID"]).copy()
    base = base.drop_duplicates(subset=["PLAYER_ID"]).set_index("PLAYER_ID")

    # ── Playmaking TOVs from PBP ──────────────────────────────────────────────
    pbp = df_stg.set_index("PLAYER_ID")[
        ["pbp__BadPassTurnovers", "pbp__BadPassOutOfBoundsTurnovers"]
    ].fillna(0.0)
    base["playmaking_tovs"] = (
        pbp["pbp__BadPassTurnovers"] + pbp["pbp__BadPassOutOfBoundsTurnovers"]
    )

    # ── Passing columns from NBA.com passing stats ────────────────────────────
    pass_cols = ["nba_pass__AST_ADJ", "nba_pass__AST",
                 "nba_pass__SECONDARY_AST", "nba_pass__POTENTIAL_AST",
                 "nba_pass__AST_PTS_CREATED"]
    passing = df_stg.set_index("PLAYER_ID")[pass_cols].fillna(0.0)

    base["ft_assist"] = (
        passing["nba_pass__AST_ADJ"]
        - passing["nba_pass__AST"]
        - passing["nba_pass__SECONDARY_AST"]
    ).clip(lower=0.0)

    base["playmaking_plays_ex_tov"] = (
        passing["nba_pass__POTENTIAL_AST"]
        + base["ft_assist"] * cfg["PCT_FT_AST_NO_SHOT"]
    )

    base["playmaking_pts"] = (
        passing["nba_pass__AST_PTS_CREATED"] * cfg["PCT_AST_PTS_IN_PA"]
        + base["ft_assist"] * cfg["LG_AVG_PP_FT_AST"]
    )

    # ── Transition values from Phase 2 ────────────────────────────────────────
    tr = pt2.set_index("PLAYER_ID")[
        ["scoring_plays", "tov_total", "transition_playmaking_tovs"]
    ].rename(columns={
        "scoring_plays":               "tr_scoring_plays",
        "tov_total":                   "tr_tov_total",
        "transition_playmaking_tovs":  "tr_playmaking_tovs",
    })
    base = base.join(tr, how="left")
    base[["tr_scoring_plays", "tr_tov_total", "tr_playmaking_tovs"]] = (
        base[["tr_scoring_plays", "tr_tov_total", "tr_playmaking_tovs"]].fillna(0.0)
    )

    # Clip tr_playmaking_tovs so it never exceeds total (data source mismatch can
    # cause NBA.com transition TOVs > PBPStats bad-pass TOVs for a small number
    # of low-volume players).
    base["tr_playmaking_tovs"] = base["tr_playmaking_tovs"].clip(
        upper=base["playmaking_tovs"]
    )
    base["hc_playmaking_tovs"] = base["playmaking_tovs"] - base["tr_playmaking_tovs"]

    # ── OBS and HC scoring plays (from Phase 1a) ──────────────────────────────
    obs_sp  = _sum_scoring_plays(pt1a, _OBS_SLUGS)
    hc_extra_sp = _sum_scoring_plays(pt1a, _HC_EXTRA_SLUGS)

    base["obs_plays"]        = obs_sp.reindex(base.index).fillna(0.0)
    base["hc_scoring_plays"] = base["obs_plays"] + hc_extra_sp.reindex(base.index).fillna(0.0)
    base["tr_scoring_plays"] = base["tr_scoring_plays"]  # already set above
    base["total_scoring_plays"] = base["hc_scoring_plays"] + base["tr_scoring_plays"]

    # ── Total playmaking plays ────────────────────────────────────────────────
    base["total_playmaking_plays"] = (
        base["playmaking_plays_ex_tov"] + base["playmaking_tovs"]
    )

    # ── HC scoring % (clamped) ────────────────────────────────────────────────
    base["hc_scoring_pct"] = (
        base["hc_scoring_plays"] / base["total_scoring_plays"]
    ).where(base["total_scoring_plays"] > 0, cfg["HC_CLAMP_MIN"])

    base["hc_scoring_pct_clamped"] = base["hc_scoring_pct"].clip(
        lower=cfg["HC_CLAMP_MIN"], upper=cfg["HC_CLAMP_MAX"]
    )

    # ── Regression: est HC and Transition playmaking plays ────────────────────
    hc_pct = base["hc_scoring_pct_clamped"]
    tpm    = base["total_playmaking_plays"]
    obs    = base["obs_plays"]
    tr_sp  = base["tr_scoring_plays"]

    base["est_hc_pm_plays"] = (
        cfg["HC_REG_INTERCEPT"]
        + tpm * cfg["HC_REG_PLAYMAKING"]
        + obs * cfg["HC_REG_OBS"]
        + hc_pct * cfg["HC_REG_HCPCT"]
    )
    base["est_tr_pm_plays"] = (
        cfg["TR_REG_INTERCEPT"]
        + tr_sp * cfg["TR_REG_TRANSITION"]
        + tpm   * cfg["TR_REG_PLAYMAKING"]
        + hc_pct * cfg["TR_REG_HCPCT"]
    )

    base["est_hc_pm_plays_clamped"] = base["est_hc_pm_plays"].clip(lower=0.0, upper=tpm)
    base["est_tr_pm_plays_clamped"] = base["est_tr_pm_plays"].clip(lower=0.0, upper=tpm)

    # ── On-ball Passing TOVs ──────────────────────────────────────────────────
    base["on_ball_passing_tovs"] = (
        base["playmaking_tovs"] - base["tr_tov_total"]
    ).clip(lower=0.0)

    # ── LG_AVG_PPPA (league average points per playmaking action) ─────────────
    lg_avg_pppa = (
        base["playmaking_pts"].sum()
        / base["playmaking_plays_ex_tov"].sum()
    )

    # ── PPPA (padded) — needs LG_AVG_PPPA ────────────────────────────────────
    base["actual_pppa"] = (
        base["playmaking_pts"] / base["playmaking_plays_ex_tov"]
    ).where(base["playmaking_plays_ex_tov"] > 0, 0.0)

    base["pppa_padded"] = (
        (lg_avg_pppa * cfg["PADDING_VOLUME"] + base["playmaking_pts"])
        / (base["playmaking_plays_ex_tov"] + cfg["PADDING_VOLUME"])
    ).where(
        base["playmaking_plays_ex_tov"] > 0,
        0.0,
    )

    # ── Lg Avg On-ball TOV rate ───────────────────────────────────────────────
    obs_scoring_tovs = _sum_scoring_tovs(pt1a, _OBS_SLUGS)
    obs_scoring_plays = _sum_scoring_plays(pt1a, _OBS_SLUGS)

    num = (
        obs_scoring_tovs.reindex(base.index).fillna(0.0).sum()
        + base["on_ball_passing_tovs"].sum()
    )
    den = (
        obs_scoring_plays.reindex(base.index).fillna(0.0).sum()
        + base["est_hc_pm_plays_clamped"].sum()
    )
    lg_avg_onball_tov_rate = num / den if den > 0 else 0.0

    hc_ppp_w_tov_penalty = cfg["CTG_HC_PPP"] + lg_avg_onball_tov_rate * cfg["CTG_TOV_PENALTY"]

    aggregates = {
        "LG_AVG_PPPA":             lg_avg_pppa,
        "lg_avg_onball_tov_rate":  lg_avg_onball_tov_rate,
        "HC_PPP_w_tov_penalty":    hc_ppp_w_tov_penalty,
    }

    player_df = base.reset_index()
    return player_df, aggregates


# ── Phase 3b ─────────────────────────────────────────────────────────────────

def phase3b_passing(
    pt3a: pd.DataFrame,
    agg3a: dict,
    tr_ppp_ratio: float,
    cfg: dict,
) -> pd.DataFrame:
    """
    Phase 3b: Passing & Playmaking — normalized HC/TR split, PPPA point
    estimates, and HC Playmaking PAB.

    Runs after Phase 1b because it depends on Lg Avg On-ball TOV rate (from
    Phase 3a) and the per-playtype scoring results from Phase 1b are not needed
    here.

    Inputs (from pt3a columns):
        est_hc_pm_plays_clamped, est_tr_pm_plays_clamped,
        total_playmaking_plays, pppa_padded, playmaking_plays_ex_tov,
        playmaking_pts, hc_playmaking_tovs

    Args:
        pt3a:              Phase 3a player DataFrame.
        agg3a:             Phase 3a aggregates dict
                           (keys: lg_avg_onball_tov_rate, HC_PPP_w_tov_penalty).
        tr_ppp_ratio:      TR_PPP_RATIO from Phase 2 aggregates.
        cfg:               Config dict (needs CTG_TOV_PENALTY).

    Returns:
        pt3a DataFrame with added columns:
            est_hc_pm_plays_final, est_tr_pm_plays_final,
            xtov,
            est_actual_hc_pppa, est_actual_tr_pppa,
            est_tr_pm_pts, est_hc_pm_pts,
            hc_padded_pppa, tr_padded_pppa,
            hc_playmaking_pab
    """
    df = pt3a.copy()

    lg_avg_tov    = agg3a["lg_avg_onball_tov_rate"]
    hc_ppp_w_tov  = agg3a["HC_PPP_w_tov_penalty"]
    tov_penalty   = cfg["CTG_TOV_PENALTY"]

    # ── Normalize HC / TR estimated plays so they sum to total ────────────────
    denom_norm = df["est_hc_pm_plays_clamped"] + df["est_tr_pm_plays_clamped"]
    factor = (df["total_playmaking_plays"] / denom_norm).where(denom_norm > 0, 0.0)

    df["est_hc_pm_plays_final"] = df["est_hc_pm_plays_clamped"] * factor
    df["est_tr_pm_plays_final"] = df["est_tr_pm_plays_clamped"] * factor

    # Zero out when total_playmaking_plays == 0
    no_plays = df["total_playmaking_plays"] == 0
    df.loc[no_plays, "est_hc_pm_plays_final"] = 0.0
    df.loc[no_plays, "est_tr_pm_plays_final"] = 0.0

    # ── xTOV ─────────────────────────────────────────────────────────────────
    df["xtov"] = df["est_hc_pm_plays_final"] * lg_avg_tov

    # ── Actual PPPA estimates (HC and TR) ─────────────────────────────────────
    # Denominator: TR plays converted to HC-equivalent using TR_PPP_RATIO
    pppa_denom = (
        df["est_tr_pm_plays_final"] * tr_ppp_ratio + df["est_hc_pm_plays_final"]
    )
    df["est_actual_hc_pppa"] = (
        df["playmaking_pts"] / pppa_denom
    ).where(pppa_denom > 0, 0.0)
    df["est_actual_tr_pppa"] = df["est_actual_hc_pppa"] * tr_ppp_ratio

    # ── Estimated playmaking point breakdowns ─────────────────────────────────
    df["est_tr_pm_pts"] = df["est_tr_pm_plays_final"] * df["est_actual_tr_pppa"]
    df["est_hc_pm_pts"] = df["est_hc_pm_plays_final"] * df["est_actual_hc_pppa"]

    # ── Padded PPPA (stabilized HC and TR) ───────────────────────────────────
    df["hc_padded_pppa"] = (
        df["pppa_padded"] * df["playmaking_plays_ex_tov"] / pppa_denom
    ).where(pppa_denom > 0, 0.0)
    df["tr_padded_pppa"] = df["hc_padded_pppa"] * tr_ppp_ratio

    # ── HC Playmaking PAB ─────────────────────────────────────────────────────
    hc_f   = df["est_hc_pm_plays_final"]
    hc_tov = df["hc_playmaking_tovs"]

    df["hc_playmaking_pab"] = (
        df["hc_padded_pppa"] * (hc_f - hc_tov)
        + hc_tov * tov_penalty
        - hc_f * hc_ppp_w_tov
    )

    return df


# ── Phase 4 ──────────────────────────────────────────────────────────────────

def phase4_transition(
    pt2: pd.DataFrame,
    pt3b: pd.DataFrame,
    tr_agg: dict,
    cfg: dict,
) -> pd.DataFrame:
    """
    Phase 4: Transition Playmaking — second pass of Transition.

    Adds playmaking columns to the Phase 2 Transition DataFrame using the
    HC/TR split from Phase 3b, then computes Points Created with a
    scoring/playmaking decomposition.

    Args:
        pt2:    Phase 2 per-player Transition DataFrame.
                Required columns: PLAYER_ID, poss, pts, poss_played,
                tov_total, scoring_tovs, transition_playmaking_tovs, scoring_plays.
        pt3b:   Phase 3b output DataFrame.
                Required columns: PLAYER_ID, est_tr_pm_plays_final, est_tr_pm_pts.
        tr_agg: Phase 2 aggregates dict.
                Keys: TR_AVG_PPP, TR_OVERALL_TOV_RATE, TR_AVG_PLAYS.
        cfg:    Config dict (needs CTG_HC_PPP, CTG_TOV_PENALTY).

    Returns:
        pt2 DataFrame augmented with:
            playmaking_plays, playmaking_pts, playmaking_tovs,
            xtov, x_playmaking_tovs, transition_prf, total_plays,
            transition_poss_per_100, tov_penalty_val, prf_w_tov,
            plays_up_to_la, plays_above_la, baseline_ppp,
            prf_baseline, baseline_xtov_penalty, prf_baseline_w_xtov,
            points_created,
            scoring_prf_w_tov, x_scoring_tovs, scoring_prf_baseline_w_xtov,
            scoring_points_created,
            playmaking_prf_w_tov, playmaking_prf_baseline_w_xtov,
            playmaking_points_created
    """
    df = pt2.copy()

    # ── Join Phase 3b playmaking inputs ──────────────────────────────────────
    pm = pt3b[["PLAYER_ID", "est_tr_pm_plays_final", "est_tr_pm_pts"]].copy()
    df = df.merge(pm, on="PLAYER_ID", how="left")
    df["est_tr_pm_plays_final"] = df["est_tr_pm_plays_final"].fillna(0.0)
    df["est_tr_pm_pts"]         = df["est_tr_pm_pts"].fillna(0.0)

    df["playmaking_plays"] = df["est_tr_pm_plays_final"]
    df["playmaking_pts"]   = df["est_tr_pm_pts"]
    df["playmaking_tovs"]  = df["transition_playmaking_tovs"]

    # ── xTOV ─────────────────────────────────────────────────────────────────
    tov_rate = tr_agg["TR_OVERALL_TOV_RATE"]
    df["xtov"]             = df["poss"] * tov_rate
    df["x_playmaking_tovs"] = df["playmaking_plays"] * tov_rate

    # ── PRF and Total Plays ───────────────────────────────────────────────────
    df["transition_prf"] = df["playmaking_pts"] + df["pts"]
    df["total_plays"]    = df["scoring_plays"] + df["playmaking_plays"]

    # ── Transition Poss/100 ───────────────────────────────────────────────────
    df["transition_poss_per_100"] = (
        100 * df["total_plays"] / df["poss_played"]
    ).where(df["poss_played"] > 0, 0.0)

    # ── TOV penalty and PRF w/ TOV ────────────────────────────────────────────
    df["tov_penalty_val"] = df["tov_total"] * cfg["CTG_TOV_PENALTY"]
    df["prf_w_tov"]       = df["transition_prf"] + df["tov_penalty_val"]

    # ── Baseline PPP (blended at TR_AVG_PLAYS threshold) ─────────────────────
    tr_avg_ppp   = tr_agg["TR_AVG_PPP"]
    tr_avg_plays = tr_agg["TR_AVG_PLAYS"]
    hc_ppp       = cfg["CTG_HC_PPP"]

    df["plays_up_to_la"] = df["transition_poss_per_100"].clip(upper=tr_avg_plays)
    df["plays_above_la"] = df["transition_poss_per_100"] - df["plays_up_to_la"]
    df["baseline_ppp"] = (
        (df["plays_up_to_la"] * tr_avg_ppp + df["plays_above_la"] * hc_ppp)
        / df["transition_poss_per_100"]
    ).where(df["transition_poss_per_100"] > 0, tr_avg_ppp)

    # ── PRF baseline and Points Created ──────────────────────────────────────
    df["prf_baseline"]          = df["baseline_ppp"] * df["total_plays"]
    df["baseline_xtov_penalty"] = df["xtov"] * cfg["CTG_TOV_PENALTY"]
    df["prf_baseline_w_xtov"]   = df["prf_baseline"] + df["baseline_xtov_penalty"]
    df["points_created"]        = df["prf_w_tov"] - df["prf_baseline_w_xtov"]

    # ── Scoring decomposition ─────────────────────────────────────────────────
    df["scoring_prf_w_tov"]          = df["pts"] + cfg["CTG_TOV_PENALTY"] * df["scoring_tovs"]
    df["x_scoring_tovs"]             = df["xtov"] - df["x_playmaking_tovs"]
    df["scoring_prf_baseline_w_xtov"] = (
        df["baseline_ppp"] * df["scoring_plays"]
        + cfg["CTG_TOV_PENALTY"] * df["x_scoring_tovs"]
    )
    df["scoring_points_created"] = df["scoring_prf_w_tov"] - df["scoring_prf_baseline_w_xtov"]

    # ── Playmaking decomposition ──────────────────────────────────────────────
    df["playmaking_prf_w_tov"]          = df["playmaking_pts"] + cfg["CTG_TOV_PENALTY"] * df["playmaking_tovs"]
    df["playmaking_prf_baseline_w_xtov"] = (
        df["baseline_ppp"] * df["playmaking_plays"]
        + cfg["CTG_TOV_PENALTY"] * df["x_playmaking_tovs"]
    )
    df["playmaking_points_created"] = df["playmaking_prf_w_tov"] - df["playmaking_prf_baseline_w_xtov"]

    return df


# ── Playtype slug groups (used by Phase 5 and Phase 6) ───────────────────────

_ON_BALL_SLUGS    = ["iso", "prballhandler", "postup", "misc"]
_PARTNER_SLUGS    = ["prrollman", "handoff"]
_SPACE_SLUGS      = ["spotup", "offscreen"]
_CRASH_SLUGS      = ["cut", "offrebound"]
_ALL_SCORING_SLUGS = _ON_BALL_SLUGS + _PARTNER_SLUGS + _SPACE_SLUGS + _CRASH_SLUGS


# ── Phase 6 ──────────────────────────────────────────────────────────────────


def _sum_pt1b(pt1b: dict[str, pd.DataFrame], slugs: list[str], col: str) -> pd.Series:
    """Sum a column from pt1b across slugs, indexed by PLAYER_ID."""
    frames = [
        pt1b[s].set_index("PLAYER_ID")[col].rename(s)
        for s in slugs
        if s in pt1b
    ]
    if not frames:
        return pd.Series(dtype=float)
    return pd.concat(frames, axis=1).fillna(0.0).sum(axis=1)


def _rates(base: pd.DataFrame, raw_col: str, gp: pd.Series, mins: pd.Series,
           poss: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return (per_g, per_36, per_75) Series for a raw total column."""
    per_g   = base[raw_col] / gp
    per_36  = base[raw_col] / mins.clip(lower=1e-9) * 36
    per_75  = base[raw_col] / poss.clip(lower=1e-9) * 75
    return per_g, per_36, per_75


def phase6_assemble(
    pt1b: dict[str, pd.DataFrame],
    pt3b: pd.DataFrame,
    pt4: pd.DataFrame,
    pt5: pd.DataFrame,
    df_stg: pd.DataFrame,
    season: str,
    season_type: str = "RS",
) -> pd.DataFrame:
    """
    Phase 6: Assemble the final season CSV.

    Joins all phase outputs into one row per player with PRF/Plays/PC
    for every category in /g, /36, /75 formats, plus aggregates,
    floor-raising adjustments, creation usage, and Has data? flags.

    Args:
        pt1b:        Phase 1b DataFrames keyed by slug.
        pt3b:        Phase 3b DataFrame (est_hc_pm_plays_final, est_hc_pm_pts,
                     hc_playmaking_pab, total_playmaking_plays, est_tr_pm_pts).
        pt4:         Phase 4 DataFrame (scoring_points_created, transition_prf,
                     total_plays, playmaking_points_created, scoring_plays).
        pt5:         Phase 5 DataFrame (floor_raising_pc_per_g/36/75).
        df_stg:      Staging DataFrame (Player, PLAYER_ID, nba_trad__GP,
                     nba_trad__MIN, pbp__OffPoss, nba_trad__TEAM_ABBREVIATION).
        season:      Season string, e.g. "2024-25".
        season_type: "RS" or "Playoffs".

    Returns:
        DataFrame with all final output columns, one row per player.
        Players are included if they appear in at least one data source.
    """
    # ── Player base: unique PLAYER_ID → Player, Team, GP, Min, Poss ──────────
    base = (
        df_stg[["PLAYER_ID", "Player",
                "nba_trad__GP", "nba_trad__MIN", "pbp__OffPoss",
                "nba_trad__TEAM_ABBREVIATION"]]
        .dropna(subset=["PLAYER_ID"])
        .drop_duplicates(subset=["PLAYER_ID"])
        .set_index("PLAYER_ID")
        .copy()
    )
    base.rename(columns={
        "nba_trad__GP":               "gp",
        "nba_trad__MIN":              "min",
        "pbp__OffPoss":               "poss",
        "nba_trad__TEAM_ABBREVIATION": "team",
    }, inplace=True)

    gp   = base["gp"]
    mins = base["min"]
    poss = base["poss"]

    # ── Per-category raw totals ───────────────────────────────────────────────
    # On-ball scoring component (Phase 1b)
    ob_pts    = _sum_pt1b(pt1b, _ON_BALL_SLUGS, "pts")
    ob_plays  = _sum_pt1b(pt1b, _ON_BALL_SLUGS, "scoring_plays")
    ob_pab    = _sum_pt1b(pt1b, _ON_BALL_SLUGS, "pab")

    # On-ball: add HC playmaking from Phase 3b
    pm3 = pt3b.set_index("PLAYER_ID")[
        ["est_hc_pm_plays_final", "est_hc_pm_pts", "hc_playmaking_pab",
         "total_playmaking_plays", "est_tr_pm_pts"]
    ]
    base = base.join(pm3, how="left")
    base[pm3.columns] = base[pm3.columns].fillna(0.0)

    base["ob_prf"]   = ob_pts.reindex(base.index).fillna(0.0) + base["est_hc_pm_pts"]
    base["ob_plays"] = ob_plays.reindex(base.index).fillna(0.0) + base["est_hc_pm_plays_final"]
    base["ob_pc"]    = ob_pab.reindex(base.index).fillna(0.0) + base["hc_playmaking_pab"]

    # Off-ball: Partner
    base["pt_prf"]   = _sum_pt1b(pt1b, _PARTNER_SLUGS, "pts").reindex(base.index).fillna(0.0)
    base["pt_plays"] = _sum_pt1b(pt1b, _PARTNER_SLUGS, "scoring_plays").reindex(base.index).fillna(0.0)
    base["pt_pc"]    = _sum_pt1b(pt1b, _PARTNER_SLUGS, "pab").reindex(base.index).fillna(0.0)

    # Off-ball: Space
    base["sp_prf"]   = _sum_pt1b(pt1b, _SPACE_SLUGS, "pts").reindex(base.index).fillna(0.0)
    base["sp_plays"] = _sum_pt1b(pt1b, _SPACE_SLUGS, "scoring_plays").reindex(base.index).fillna(0.0)
    base["sp_pc"]    = _sum_pt1b(pt1b, _SPACE_SLUGS, "pab").reindex(base.index).fillna(0.0)

    # Off-ball: Crash
    base["cr_prf"]   = _sum_pt1b(pt1b, _CRASH_SLUGS, "pts").reindex(base.index).fillna(0.0)
    base["cr_plays"] = _sum_pt1b(pt1b, _CRASH_SLUGS, "scoring_plays").reindex(base.index).fillna(0.0)
    base["cr_pc"]    = _sum_pt1b(pt1b, _CRASH_SLUGS, "pab").reindex(base.index).fillna(0.0)

    # Transition (Phase 4)
    tr4 = pt4.set_index("PLAYER_ID")[
        ["transition_prf", "total_plays", "scoring_points_created",
         "playmaking_points_created", "scoring_plays",
         "scoring_prf_w_tov", "pts"]
    ].rename(columns={
        "scoring_points_created":    "tr_sc_pc",
        "playmaking_points_created": "tr_pm_pc",
        "scoring_plays":             "tr_scoring_plays",
        "transition_prf":            "tr_prf",
        "total_plays":               "tr_plays",
        "scoring_prf_w_tov":         "tr_scoring_prf",
        "pts":                       "tr_pts",
    })
    base = base.join(tr4, how="left")
    for c in tr4.columns:
        base[c] = base[c].fillna(0.0)

    # ── Aggregates ────────────────────────────────────────────────────────────
    # Total (all categories)
    base["tot_prf"]   = base["ob_prf"]  + base["pt_prf"]  + base["sp_prf"]  + base["cr_prf"]  + base["tr_prf"]
    base["tot_plays"] = base["ob_plays"] + base["pt_plays"] + base["sp_plays"] + base["cr_plays"] + base["tr_plays"]
    base["tr_pc"]  = base["tr_sc_pc"] + base["tr_pm_pc"]
    base["tot_pc"] = base["ob_pc"] + base["pt_pc"] + base["sp_pc"] + base["cr_pc"] + base["tr_pc"]

    # Half court (On-ball + Partner + Space + Crash)
    base["hc_prf"]   = base["ob_prf"]  + base["pt_prf"]  + base["sp_prf"]  + base["cr_prf"]
    base["hc_plays"] = base["ob_plays"] + base["pt_plays"] + base["sp_plays"] + base["cr_plays"]
    base["hc_pc"]    = base["ob_pc"]   + base["pt_pc"]   + base["sp_pc"]   + base["cr_pc"]

    # Off-ball
    base["offb_prf"]   = base["pt_prf"]  + base["sp_prf"]  + base["cr_prf"]
    base["offb_plays"] = base["pt_plays"] + base["sp_plays"] + base["cr_plays"]
    base["offb_pc"]    = base["pt_pc"]   + base["sp_pc"]   + base["cr_pc"]

    # Scoring (all 10 non-TR pt1b + transition scoring)
    all_pts    = _sum_pt1b(pt1b, _ALL_SCORING_SLUGS, "pts")
    all_splays = _sum_pt1b(pt1b, _ALL_SCORING_SLUGS, "scoring_plays")
    all_pab    = _sum_pt1b(pt1b, _ALL_SCORING_SLUGS, "pab")
    base["sc_prf"]   = all_pts.reindex(base.index).fillna(0.0) + base["tr_pts"]
    base["sc_plays"] = all_splays.reindex(base.index).fillna(0.0) + base["tr_scoring_plays"]
    base["sc_pc"]    = all_pab.reindex(base.index).fillna(0.0) + base["tr_sc_pc"]

    # Playmaking
    base["pm_prf"]   = base["est_hc_pm_pts"] + base["est_tr_pm_pts"]
    base["pm_plays"] = base["total_playmaking_plays"]
    base["pm_pc"]    = base["hc_playmaking_pab"] + base["tr_pm_pc"]

    # Floor raising (Phase 5)
    fr5 = pt5.set_index("PLAYER_ID")[
        ["floor_raising_pc_per_g", "floor_raising_pc_per_36", "floor_raising_pc_per_75",
         "on_ball_plays_per_g", "on_ball_plays_per_36", "on_ball_plays_per_75",
         "on_ball_plays_total"]
    ]
    base = base.join(fr5, how="left")

    # ── Compute /g, /36, /75 for each category ────────────────────────────────
    def cat_cols(prefix: str, prf_raw: str, plays_raw: str, pc_raw: str,
                 has_mask: pd.Series) -> dict:
        """Build {col_name: Series} for one category's rate columns.

        Column order matches the known-good CSV:
            PRF/g, PRF/36, PRF/75, Plays/g, Plays/36, Plays/75,
            rORTG, PC/g, PC/36, PC/75
        """
        rates = [("g", gp), ("36", mins.clip(lower=1e-9) / 36),
                 ("75", poss.clip(lower=1e-9) / 75)]
        out = {}
        for rate, divisor in rates:
            out[f"{prefix} PRF/{rate}"] = (base[prf_raw] / divisor).where(has_mask)
        for rate, divisor in rates:
            out[f"{prefix} Plays/{rate}"] = (base[plays_raw] / divisor).where(has_mask)
        out[f"{prefix} rORTG"] = (
            (100 * base[pc_raw] / base[plays_raw])
            .replace([float("inf"), float("-inf")], float("nan"))
            .where(base[plays_raw] > 0)
            .where(has_mask)
        )
        for rate, divisor in rates:
            out[f"{prefix} PC/{rate}"] = (base[pc_raw] / divisor).where(has_mask)
        return out

    # Has data? flags (player appears in at least one slug of the category)
    def has_any(slugs: list[str]) -> pd.Series:
        pids = set()
        for s in slugs:
            if s in pt1b:
                pids.update(pt1b[s]["PLAYER_ID"].astype(int).tolist())
        return pd.Series(base.index.isin(pids), index=base.index)

    has_ob  = has_any(_ON_BALL_SLUGS)
    has_pt  = has_any(_PARTNER_SLUGS)
    has_sp  = has_any(_SPACE_SLUGS)
    has_cr  = has_any(_CRASH_SLUGS)
    has_tr  = base.index.isin(pt4["PLAYER_ID"].astype(int).tolist())
    has_tr  = pd.Series(has_tr, index=base.index)
    has_any_cat = has_ob | has_pt | has_sp | has_cr | has_tr
    has_all_cat = has_ob & has_pt & has_sp & has_cr & has_tr

    # Individual playtype flags
    has_iso   = pd.Series(base.index.isin(pt1b.get("iso", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_pnrbh = pd.Series(base.index.isin(pt1b.get("prballhandler", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_post  = pd.Series(base.index.isin(pt1b.get("postup", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_misc  = pd.Series(base.index.isin(pt1b.get("misc", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_rrp   = pd.Series(base.index.isin(pt1b.get("prrollman", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_ho    = pd.Series(base.index.isin(pt1b.get("handoff", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_su    = pd.Series(base.index.isin(pt1b.get("spotup", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_os    = pd.Series(base.index.isin(pt1b.get("offscreen", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_cut   = pd.Series(base.index.isin(pt1b.get("cut", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    has_putb  = pd.Series(base.index.isin(pt1b.get("offrebound", pd.DataFrame()).get("PLAYER_ID", pd.Series()).astype(int).tolist()), index=base.index)
    # Passing: anyone with nba_pass data (non-zero playmaking_plays_ex_tov)
    has_pass  = base["total_playmaking_plays"] > 0

    # Build output dict
    cols: dict[str, pd.Series] = {}

    # Identity
    cols["Player"]       = base["Player"]
    cols["Year"]         = season
    cols["Season type"]  = season_type
    cols["Tm"]           = base["team"]
    cols["Games"]        = gp
    cols["Minutes"]      = mins
    cols["Possessions"]  = poss
    cols["Years experience"] = float("nan")

    # Category PRF/Plays/PC/rORTG
    cols.update(cat_cols("On-ball",          "ob_prf",   "ob_plays",  "ob_pc",   has_ob))
    cols.update(cat_cols("Off-ball: Partner", "pt_prf",   "pt_plays",  "pt_pc",   has_pt))
    cols.update(cat_cols("Off-ball: Space",   "sp_prf",   "sp_plays",  "sp_pc",   has_sp))
    cols.update(cat_cols("Off-ball: Crash",   "cr_prf",   "cr_plays",  "cr_pc",   has_cr))
    cols.update(cat_cols("Transition",        "tr_prf",   "tr_plays",  "tr_pc",         has_tr))
    cols.update(cat_cols("Total",             "tot_prf",  "tot_plays", "tot_pc",  has_any_cat))
    # The plain "Total PC" rates are replaced by the qualified (ex. floor raising) versions;
    # remove them so only the qualified names appear in the output.
    for _rate in ["g", "36", "75"]:
        cols.pop(f"Total PC/{_rate}", None)

    # Total PC ex. floor raising (already computed above as tot_pc)
    for rate, divisor in [("g", gp), ("36", mins.clip(lower=1e-9) / 36),
                           ("75", poss.clip(lower=1e-9) / 75)]:
        cols[f"Total PC/{rate} (ex. floor raising)"] = (
            (base["tot_pc"] / divisor).where(has_any_cat)
        )

    # Scoring and Playmaking aggregates
    cols.update(cat_cols("Scoring",    "sc_prf",   "sc_plays", "sc_pc",   has_any_cat))
    cols.update(cat_cols("Playmaking", "pm_prf",   "pm_plays", "pm_pc",   has_ob))

    # % Playmaking
    cols["% Playmaking"] = (
        (base["pm_plays"] / base["tot_plays"])
        .replace([float("inf"), float("-inf")], float("nan"))
        .where(base["tot_plays"] > 0)
        .where(has_any_cat)
    )

    # Floor raising
    fr_mask = base["floor_raising_pc_per_g"].notna()
    cols["Floor raising PC/g"]   = base["floor_raising_pc_per_g"].where(fr_mask)
    cols["Floor raising PC/36"]  = base["floor_raising_pc_per_36"].where(fr_mask)
    cols["Floor raising PC/75"]  = base["floor_raising_pc_per_75"].where(fr_mask)

    # Floor raising adjustments
    _fr_rates = [("g", gp), ("36", mins.clip(lower=1e-9) / 36),
                 ("75", poss.clip(lower=1e-9) / 75)]
    for rate, divisor in _fr_rates:
        fr_rate = base[f"floor_raising_pc_per_{rate}"].fillna(0.0)
        ob_pc_rate = (base["ob_pc"] / divisor).where(has_ob)
        cols[f"On-ball PC/{rate} (floor raising adj.)"] = (ob_pc_rate + fr_rate).where(has_ob)
    for rate, divisor in _fr_rates:
        fr_rate = base[f"floor_raising_pc_per_{rate}"].fillna(0.0)
        tot_pc_rate = (base["tot_pc"] / divisor).where(has_any_cat)
        cols[f"Total PC/{rate} (floor raising adj.)"] = (tot_pc_rate + fr_rate).where(has_any_cat)

    # Half court
    cols.update(cat_cols("Half court", "hc_prf", "hc_plays", "hc_pc", has_ob | has_pt | has_sp | has_cr))
    for rate, divisor in [("g", gp), ("36", mins.clip(lower=1e-9) / 36),
                           ("75", poss.clip(lower=1e-9) / 75)]:
        fr_col = f"floor_raising_pc_per_{rate}"
        hc_pc_rate = (base["hc_pc"] / divisor).where(has_ob | has_pt | has_sp | has_cr)
        fr_rate = base[fr_col].fillna(0.0)
        cols[f"Half court PC/{rate} (floor raising adj.)"] = (hc_pc_rate + fr_rate).where(has_ob | has_pt | has_sp | has_cr)

    # Off-ball
    cols.update(cat_cols("Off-ball", "offb_prf", "offb_plays", "offb_pc", has_pt | has_sp | has_cr))

    # Creation usage
    tot_plays_g = (base["tot_plays"] / gp).where(has_any_cat)
    ob_plays_g  = (base["ob_plays"]  / gp).where(has_ob)
    offb_plays_g = (base["offb_plays"] / gp).where(has_pt | has_sp | has_cr)
    tr_plays_g  = (base["tr_plays"]  / gp).where(has_tr)
    cols["Total creation usage"]      = (tot_plays_g  * gp / poss.clip(lower=1e-9)).where(has_any_cat)
    cols["On-ball creation usage"]    = (ob_plays_g   * gp / poss.clip(lower=1e-9)).where(has_ob)
    cols["Off-ball creation usage"]   = (offb_plays_g * gp / poss.clip(lower=1e-9)).where(has_pt | has_sp | has_cr)
    cols["Transition creation usage"] = (tr_plays_g   * gp / poss.clip(lower=1e-9)).where(has_tr)
    ob_plays_36 = (base["ob_plays"] / mins.clip(lower=1e-9) * 36).where(has_ob)
    cols["On-ball share"] = (ob_plays_36 / ((36 / 48) * 90)).where(has_ob)

    # On-ball playmaking ratio
    cols["On-ball playmaking ratio"] = (
        (base["est_hc_pm_plays_final"] / base["ob_plays"])
        .replace([float("inf"), float("-inf")], float("nan"))
        .where(base["ob_plays"] > 0)
        .where(has_ob)
    )

    # On-ball Scoring / Playmaking decomposition
    ob_sc_pab = ob_pab.reindex(base.index).fillna(0.0)
    ob_pm_pab = base["hc_playmaking_pab"]
    _ob_rates = [("g", gp), ("36", mins.clip(lower=1e-9) / 36),
                 ("75", poss.clip(lower=1e-9) / 75)]
    for rate, divisor in _ob_rates:
        cols[f"On-ball Scoring PC/{rate}"] = (ob_sc_pab / divisor).where(has_ob)
    for rate, divisor in _ob_rates:
        cols[f"On-ball Playmaking PC/{rate}"] = (ob_pm_pab / divisor).where(has_ob)

    # Has data? flags
    cols["Has data? All"]             = has_all_cat
    cols["Has data? Any category"]    = has_any_cat
    cols["Has data? ISO"]             = has_iso
    cols["Has data? PNRBH"]           = has_pnrbh
    cols["Has data? Post-Up"]         = has_post
    cols["Has data? Misc"]            = has_misc
    cols["Has data? On-ball"]         = has_ob
    cols["Has data? Passing"]         = has_pass
    cols["Has data? Roll and Pop"]    = has_rrp
    cols["Has data? Handoff"]         = has_ho
    cols["Has data? Off-ball: Partner"] = has_pt
    cols["Has data? Spot-Up"]         = has_su
    cols["Has data? Off Screen"]      = has_os
    cols["Has data? Off-ball: Space"] = has_sp
    cols["Has data? Open Rim"]        = has_cut
    cols["Has data? Putbacks"]        = has_putb
    cols["Has data? Off-ball: Crash"] = has_cr
    cols["Has data? Transition"]      = has_tr

    return pd.DataFrame(cols)


# ── Phase 5 ──────────────────────────────────────────────────────────────────


def phase5_floor_raising(
    pt1b: dict[str, pd.DataFrame],
    pt3b: pd.DataFrame,
    df_stg: pd.DataFrame,
    cfg: dict,
) -> pd.DataFrame:
    """
    Phase 5: Floor Raising — quadratic regression bonus for on-ball players
    based on play volume.

    Only computed for players who appear in at least one on-ball playtype
    (ISO, PNRBH, Post-Up, Misc).  Players without on-ball data receive NaN.

    Args:
        pt1b:   Dict of Phase 1b DataFrames keyed by slug.
                Each has PLAYER_ID and scoring_plays columns.
        pt3b:   Phase 3b DataFrame; provides est_hc_pm_plays_final per player.
        df_stg: Staging DataFrame; provides nba_trad__GP, nba_trad__MIN,
                pbp__OffPoss per player.
        cfg:    Config dict (FR_INTERCEPT, FR_LINEAR, FR_QUADRATIC, FR_MIN_POSS).

    Returns:
        DataFrame with columns:
            PLAYER_ID,
            on_ball_plays_total, on_ball_plays_per_g,
            on_ball_plays_per_36, on_ball_plays_per_75,
            raw_fr_per_g, raw_fr_per_36, raw_fr_per_75,
            fr_baseline_per_g, fr_baseline_per_36, fr_baseline_per_75,
            floor_raising_pc_per_g, floor_raising_pc_per_36, floor_raising_pc_per_75
        One row per player who appears in at least one on-ball playtype.
    """
    # ── On-ball scoring plays (sum across ISO, PNRBH, Post-Up, Misc) ─────────
    frames = [
        pt1b[s][["PLAYER_ID", "scoring_plays"]].rename(columns={"scoring_plays": f"sp_{s}"})
        for s in _ON_BALL_SLUGS
        if s in pt1b
    ]
    ob = frames[0]
    for f in frames[1:]:
        ob = ob.merge(f, on="PLAYER_ID", how="outer")
    ob = ob.fillna(0.0)
    ob["ob_scoring_plays"] = sum(ob[f"sp_{s}"] for s in _ON_BALL_SLUGS if f"sp_{s}" in ob.columns)

    # ── HC Playmaking plays (from Phase 3b) ───────────────────────────────────
    pm = pt3b[["PLAYER_ID", "est_hc_pm_plays_final"]].copy()
    ob = ob.merge(pm, on="PLAYER_ID", how="left")
    ob["est_hc_pm_plays_final"] = ob["est_hc_pm_plays_final"].fillna(0.0)

    ob["on_ball_plays_total"] = ob["ob_scoring_plays"] + ob["est_hc_pm_plays_final"]

    # ── Player-level stats from staging ──────────────────────────────────────
    stg_cols = df_stg[["PLAYER_ID", "nba_trad__GP", "nba_trad__MIN", "pbp__OffPoss"]].copy()
    stg_cols = stg_cols.dropna(subset=["PLAYER_ID"]).drop_duplicates(subset=["PLAYER_ID"])
    ob = ob.merge(stg_cols, on="PLAYER_ID", how="left")

    # ── Per-game, per-36, per-75 on-ball plays ────────────────────────────────
    gp   = ob["nba_trad__GP"]
    mins = ob["nba_trad__MIN"]
    poss = ob["pbp__OffPoss"]

    ob["on_ball_plays_per_g"]  = ob["on_ball_plays_total"] / gp
    ob["on_ball_plays_per_36"] = (ob["on_ball_plays_total"] / mins.clip(lower=1e-9)) * 36
    ob["on_ball_plays_per_75"] = (ob["on_ball_plays_total"] / poss.clip(lower=1e-9)) * 75

    # ── Raw floor raising regression ──────────────────────────────────────────
    a = cfg["FR_INTERCEPT"]
    b = cfg["FR_LINEAR"]
    c = cfg["FR_QUADRATIC"]

    for rate in ("per_g", "per_36", "per_75"):
        x = ob[f"on_ball_plays_{rate}"]
        ob[f"raw_fr_{rate}"] = a + b * x + c * x ** 2

    # ── Baseline: mean raw_fr for players with ≥ FR_MIN_POSS possessions ─────
    qual = ob[poss >= cfg["FR_MIN_POSS"]]
    for rate in ("per_g", "per_36", "per_75"):
        baseline = qual[f"raw_fr_{rate}"].mean()
        ob[f"fr_baseline_{rate}"] = baseline
        ob[f"floor_raising_pc_{rate}"] = ob[f"raw_fr_{rate}"] - baseline

    keep_cols = [
        "PLAYER_ID",
        "on_ball_plays_total", "on_ball_plays_per_g",
        "on_ball_plays_per_36", "on_ball_plays_per_75",
        "raw_fr_per_g", "raw_fr_per_36", "raw_fr_per_75",
        "fr_baseline_per_g", "fr_baseline_per_36", "fr_baseline_per_75",
        "floor_raising_pc_per_g", "floor_raising_pc_per_36", "floor_raising_pc_per_75",
    ]
    return ob[keep_cols].reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season",      required=True, help='e.g. "2024-25"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    parser.add_argument("--output",      default=None,
                        help="Override output CSV path (default: assets/data/season/league-table-20YY.csv)")
    args = parser.parse_args()

    season           = args.season
    season_type      = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    staging_path = os.path.join(REPO_ROOT, "assets", "data", "staging",
                                f"{season}__{season_type_slug}.parquet")
    ctg_path = os.path.join(REPO_ROOT, "assets", "data", "raw",
                            season, season_type_slug, "ctg_league_averages.json")
    pct_ast_path = os.path.join(REPO_ROOT, "assets", "data", "raw",
                                season, season_type_slug, "pct_ast_pts_in_pa.json")

    if not os.path.exists(staging_path):
        raise FileNotFoundError(f"Missing staging file: {staging_path}. Run build_stage_season.py first.")
    if not os.path.exists(ctg_path):
        raise FileNotFoundError(f"Missing CTG file: {ctg_path}. Run ctg_league_avgs.py first.")
    if not os.path.exists(pct_ast_path):
        raise FileNotFoundError(
            f"Missing PCT_AST_PTS_IN_PA file: {pct_ast_path}. "
            f"Run scripts/calculate/compute_pct_ast_pts.py first."
        )

    print(f"[INFO] Loading staging: {staging_path}")
    df = pd.read_parquet(staging_path)
    print(f"[INFO] Staging shape: {df.shape[0]} rows × {df.shape[1]} cols")

    import json as _json
    with open(pct_ast_path, encoding="utf-8") as _f:
        _pct_ast_data = _json.load(_f)
    pct_ast_pts_in_pa = _pct_ast_data["values"]["PCT_AST_PTS_IN_PA"]

    cfg = build_config(ctg_path)
    cfg["PCT_AST_PTS_IN_PA"] = pct_ast_pts_in_pa
    print(f"[INFO] CTG_HC_PPP={cfg['CTG_HC_PPP']:.4f}  CTG_TOV_PENALTY={cfg['CTG_TOV_PENALTY']:.4f}")
    print(f"[INFO] PCT_AST_PTS_IN_PA={cfg['PCT_AST_PTS_IN_PA']:.4f}")

    # ScoringShareOfTOV
    df = compute_scoring_share_of_tov(df)

    # Phase 1a
    print("[INFO] Running Phase 1a...")
    pt1a: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        result = phase1a_playtype(df, pt["slug"])
        pt1a[pt["slug"]] = result
        print(f"[OK]   {pt['name']:14s} ({pt['slug']}): {len(result)} players")

    print("[OK] Phase 1a complete.")

    # Phase 2
    print("[INFO] Running Phase 2 (Transition)...")
    pt2, tr_agg = phase2_transition(df, cfg)
    print(f"[OK]   Transition: {len(pt2)} players")
    print(f"[OK]   TR_AVG_PPP={tr_agg['TR_AVG_PPP']:.4f}  "
          f"TR_OVERALL_TOV_RATE={tr_agg['TR_OVERALL_TOV_RATE']:.4f}  "
          f"TR_AVG_PLAYS={tr_agg['TR_AVG_PLAYS']:.2f}  "
          f"TR_PPP_RATIO={tr_agg['TR_PPP_RATIO']:.4f}")
    print("[OK] Phase 2 complete.")

    # Phase 3a
    print("[INFO] Running Phase 3a (Passing)...")
    pt3a, agg3a = phase3a_passing(df, pt1a, pt2, cfg)
    print(f"[OK]   Phase 3a: {len(pt3a)} players")
    print(f"[OK]   LG_AVG_PPPA            = {agg3a['LG_AVG_PPPA']:.4f}")
    print(f"[OK]   lg_avg_onball_tov_rate = {agg3a['lg_avg_onball_tov_rate']:.4f}")
    print(f"[OK]   HC_PPP_w_tov_penalty   = {agg3a['HC_PPP_w_tov_penalty']:.4f}")
    print("[OK] Phase 3a complete.")

    # Phase 1b — now using the real Lg Avg On-ball TOV rate from Phase 3a
    # Putbacks (offrebound) use CTG_HC_PPP as the baseline directly instead of
    # max(CTG_HC_PPP, tab_avg), because second-chance scoring is valued relative
    # to the half-court average, not the already-elevated putback tab average.
    print("[INFO] Running Phase 1b...")
    pt1b: dict[str, pd.DataFrame] = {}
    for pt in PLAYTYPES:
        baseline = cfg["CTG_HC_PPP"] if pt["slug"] == "offrebound" else None
        pt1b[pt["slug"]] = phase1b_playtype(
            pt1a[pt["slug"]], agg3a["lg_avg_onball_tov_rate"], cfg, baseline
        )
        print(f"[OK]   {pt['name']:14s} ({pt['slug']}): PAB range "
              f"[{pt1b[pt['slug']]['pab'].min():.1f}, {pt1b[pt['slug']]['pab'].max():.1f}]")
    print("[OK] Phase 1b complete.")

    # Phase 3b — HC Playmaking PAB (depends on Phase 3a aggregates)
    print("[INFO] Running Phase 3b (HC Playmaking)...")
    pt3b = phase3b_passing(pt3a, agg3a, tr_agg["TR_PPP_RATIO"], cfg)
    pab_range = pt3b["hc_playmaking_pab"]
    print(f"[OK]   Phase 3b: {len(pt3b)} players  "
          f"HC PM PAB range [{pab_range.min():.1f}, {pab_range.max():.1f}]")
    print("[OK] Phase 3b complete.")

    # Phase 4 — Transition Playmaking (depends on Phase 2 + Phase 3b)
    print("[INFO] Running Phase 4 (Transition Playmaking)...")
    pt4 = phase4_transition(pt2, pt3b, tr_agg, cfg)
    pc_range = pt4["points_created"]
    checksum = (pt4["scoring_points_created"] + pt4["playmaking_points_created"] - pt4["points_created"]).abs().max()
    print(f"[OK]   Phase 4: {len(pt4)} players  "
          f"PC range [{pc_range.min():.1f}, {pc_range.max():.1f}]  "
          f"checksum max={checksum:.4f}")
    print("[OK] Phase 4 complete.")

    # Phase 5 — Floor Raising
    print("[INFO] Running Phase 5 (Floor Raising)...")
    pt5 = phase5_floor_raising(pt1b, pt3b, df, cfg)
    fr_range = pt5["floor_raising_pc_per_g"]
    print(f"[OK]   Phase 5: {len(pt5)} players  "
          f"floor raising PC/g range [{fr_range.min():.3f}, {fr_range.max():.3f}]  "
          f"baseline={pt5['fr_baseline_per_g'].iloc[0]:.4f}")
    print("[OK] Phase 5 complete.")

    # Phase 6 — Assemble final output
    print("[INFO] Running Phase 6 (Assemble)...")
    season_type_label = "RS" if season_type == "Regular Season" else "Playoffs"
    out_df = phase6_assemble(pt1b, pt3b, pt4, pt5, df, season, season_type_label)
    print(f"[OK]   Phase 6: {len(out_df)} players × {len(out_df.columns)} columns")

    # Write output CSV
    if args.output:
        out_path = args.output
    else:
        season_year = season.split("-")[1]  # "2024-25" → "25"
        out_filename = f"league-table-20{season_year}.csv"
        out_path = os.path.join(REPO_ROOT, "assets", "data", "season", out_filename)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # Multiply percentage columns by 100 for website display (e.g. 0.643 → 64.3)
    pct_cols = [
        "Total creation usage",
        "On-ball creation usage",
        "Off-ball creation usage",
        "Transition creation usage",
        "On-ball share",
        "On-ball playmaking ratio",
    ]
    for col in pct_cols:
        if col in out_df.columns:
            out_df[col] = out_df[col] * 100

    # Drop ghost rows that have no player name (outer-merge artifacts with all-null data)
    before = len(out_df)
    out_df = out_df[out_df["Player"].notna() & (out_df["Player"].astype(str).str.strip() != "")].copy()
    dropped = before - len(out_df)
    if dropped:
        print(f"[INFO] Dropped {dropped} blank-Player row(s) from output")
    out_df.to_csv(out_path, index=False)
    print(f"[OK]   Written: {out_path}")
    print("[OK] Phase 6 complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
