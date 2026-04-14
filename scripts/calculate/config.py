import json

# ── Fixed constants (season-independent) ─────────────────────────────────────

# Passing & playmaking
PCT_FT_AST_NO_SHOT = 0.2      # Fraction of FT assists without a shot attempt
LG_AVG_PP_FT_AST   = 1.7      # Average points per FT assist
PADDING_VOLUME      = 400      # Pseudo-observations for padded PPPA
HC_CLAMP_MAX        = 0.86859  # Upper bound for half-court scoring %
HC_CLAMP_MIN        = 0.7234   # Lower bound for half-court scoring %

# HC playmaking regression coefficients
HC_REG_INTERCEPT  = -165.3686815
HC_REG_PLAYMAKING =    0.815899448
HC_REG_OBS        =    0.025486432
HC_REG_HCPCT      =  208.2506342

# Transition playmaking regression coefficients
TR_REG_INTERCEPT  =  171.1093094
TR_REG_TRANSITION =    0.015129861
TR_REG_PLAYMAKING =    0.166406246
TR_REG_HCPCT      = -215.8731256

# Floor raising regression coefficients
FR_INTERCEPT  =   0.189572
FR_LINEAR     =   0.227392
FR_QUADRATIC  =  -0.003175
FR_MIN_POSS   = 600           # Minimum possessions for baseline average


def load_ctg_constants(ctg_json_path: str) -> dict:
    """
    Load season-specific CTG values from the raw JSON file produced by ctg_league_avgs.py.

    CTG stores values as "pts per 100 plays" (e.g. 97.8 → 0.978 pts/play).
    Divides by 100 so all downstream formulas operate in per-play units.

    Returns a dict with keys:
        CTG_HC_PPP, CTG_OREB_PCT, CTG_PUTBACK_PPP, CTG_TOV_PENALTY, CTG_TOV_PCT
    """
    with open(ctg_json_path, encoding="utf-8") as f:
        data = json.load(f)
    v = data["values"]

    ctg_hc_ppp      = v["hc_pts_per_play"] / 100
    ctg_oreb_pct    = v["hc_oreb_pct"]     / 100
    ctg_putback_ppp = v["pb_pts_per_play"]  / 100

    return {
        "CTG_HC_PPP":      ctg_hc_ppp,
        "CTG_OREB_PCT":    ctg_oreb_pct,
        "CTG_PUTBACK_PPP": ctg_putback_ppp,
        "CTG_TOV_PENALTY": -ctg_oreb_pct * ctg_putback_ppp,
        "CTG_TOV_PCT":     v["tov_pct"] / 100,
    }


def build_config(ctg_json_path: str) -> dict:
    """
    Return the full config dict passed through the entire calculation pipeline:
    all fixed constants merged with season-specific CTG values.
    """
    return {
        # Fixed
        "PCT_FT_AST_NO_SHOT": PCT_FT_AST_NO_SHOT,
        "LG_AVG_PP_FT_AST":   LG_AVG_PP_FT_AST,
        "PADDING_VOLUME":      PADDING_VOLUME,
        "HC_CLAMP_MAX":        HC_CLAMP_MAX,
        "HC_CLAMP_MIN":        HC_CLAMP_MIN,
        "HC_REG_INTERCEPT":    HC_REG_INTERCEPT,
        "HC_REG_PLAYMAKING":   HC_REG_PLAYMAKING,
        "HC_REG_OBS":          HC_REG_OBS,
        "HC_REG_HCPCT":        HC_REG_HCPCT,
        "TR_REG_INTERCEPT":    TR_REG_INTERCEPT,
        "TR_REG_TRANSITION":   TR_REG_TRANSITION,
        "TR_REG_PLAYMAKING":   TR_REG_PLAYMAKING,
        "TR_REG_HCPCT":        TR_REG_HCPCT,
        "FR_INTERCEPT":        FR_INTERCEPT,
        "FR_LINEAR":           FR_LINEAR,
        "FR_QUADRATIC":        FR_QUADRATIC,
        "FR_MIN_POSS":         FR_MIN_POSS,
        # Season-specific (CTG)
        **load_ctg_constants(ctg_json_path),
    }
