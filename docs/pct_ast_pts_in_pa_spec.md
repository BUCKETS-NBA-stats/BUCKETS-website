# PCT_AST_PTS_IN_PA — Computation Spec (v2)

**Purpose:** Compute the fraction of AST PTS Created that comes from ≤1 dribble shots (i.e., shots that also qualify as potential assists). Used to correct the numerator/denominator mismatch in the PPPA formula.

**Why this is needed:** AST PTS Created includes points from all assists (any number of dribbles), but the PPPA denominator uses Potential AST (≤1 dribble only). Without correction, PPPA is inflated.

**Corrected PPPA formula:**
```
Playmaking Pts  = AST_PTS_Created * PCT_AST_PTS_IN_PA + FT_AST * LG_AVG_PP_FT_AST
Playmaking Plays = Potential_AST + FT_AST * PCT_FT_AST_NO_SHOT + Playmaking_TOVs
PPPA = Playmaking Pts / Playmaking Plays
```

---

## Approach

Use Synergy play type FGM volumes to build up the assisted ≤1 dribble total. For each play type:
1. We know the total FGM (from Phase 1 playtype data)
2. We know whether it's ~always assisted, ~always unassisted, or ambiguous
3. We can estimate what fraction of its FGM falls in ≤1 dribble vs 2+ dribble tracking categories, based on the physical nature of the play

This is robust because the per-play-type distribution estimates are grounded in what the play physically looks like, not aggregate rates.

---

## Data Inputs

### From Phase 1 playtype data (Synergy, per season)

League-wide FGM totals per play type. These are already in the pipeline.

| Play type | Variable | Assist assumption |
|---|---|---|
| Spot-Up | SPOTUP_FGM | Assisted |
| Off Screen | OFFSCREEN_FGM | Assisted |
| PNR Roll/Pop | ROLLPOP_FGM | Assisted |
| Handoff | HANDOFF_FGM | Assisted |
| Cut (Open Rim) | CUT_FGM | Assisted |
| Putbacks | PUTBACK_FGM | Unassisted |
| PNR Ball Handler | PNRBH_FGM | Unassisted |
| Isolation | ISO_FGM | Unassisted |
| Post-Up | POSTUP_FGM | Ambiguous |
| Transition | TRANSITION_FGM | Ambiguous |
| Misc | MISC_FGM | Ambiguous |

### From tracking shot endpoint (LeagueDashPlayerPtShot)

League-wide totals, `PerMode=Totals`.

**By shot type (mutually exclusive, exhaustive):**

| Variable | Source | 2024-25 value |
|---|---|---|
| CS_FGM | GeneralRange="Catch and Shoot" | 26,071 |
| PU_FGM | GeneralRange="Pull Up" | 21,061 |
| LT10_FGM | PlayerDashPtShots "Less Than 10 ft" | 54,770 |
| OTHER_FGM | PlayerDashPtShots "Other" | 472 |
| TOTAL_FGM | Sum of above | 102,374 |

**By dribble count:**

| Variable | DribbleRange filter | 2024-25 value |
|---|---|---|
| DRIB0_FGM | "0 Dribbles" | 48,201 |
| DRIB1_FGM | "1 Dribble" | 12,345 |
| DRIB2PLUS_FGM | "2"+"3-6"+"7+" | 41,828 |

**Confirmed cross-filter:** C&S is 100% 0-dribble.

### From passing/scoring endpoint

| Variable | Source | 2024-25 value |
|---|---|---|
| TOTAL_AST_FGM | SUM(AST) from passing table | 65,312 |
| TOTAL_AST_PTS | SUM(AST_PTS_Created) from passing table | TBD |

---

## Tracking Shot Type Columns

For estimation purposes, we split the 4 tracking types into 5 columns based on dribble count:

| Column | Abbreviation | ≤1 dribble? | Description |
|---|---|---|---|
| Catch & Shoot | CS | Yes | All 0-dribble (confirmed) |
| Pull Up, 1 dribble | PU1 | Yes | Catch, 1 dribble, pull-up jump shot |
| Pull Up, 2+ dribbles | PU2 | No | Off-the-dribble jump shot |
| Inside 10 ft, 0-1 dribble | LT10_01 | Yes | Cuts, lobs, roll finishes, putback tips |
| Inside 10 ft, 2+ dribbles | LT10_2 | No | Drives, post moves |

Other is small enough to ignore (<0.5% of FGM).

---

## Play Type → Tracking Column Distribution

For each play type, estimate the fraction of FGM that lands in each tracking column. These distributions are based on the physical nature of each play.

### Assumed Assisted play types

**Spot-Up:**
Most are catch-and-shoot. A small fraction drive closeouts.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.80 | 0.10 | 0.00 | 0.02 | 0.08 |

Rationale: Spot-up shooters overwhelmingly catch and shoot (C&S). Some take one dribble to step back / sidestep / step in for a midrange pull-up. A few drive closeouts to the rim — almost always requiring 2+ dribbles since the spot-up position is on the perimeter.

**Off Screen:**
Very similar to Spot-Up — come off screen into catch-and-shoot or quick pull-up.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.70 | 0.15 | 0.02 | 0.08 | 0.05 |

Rationale: Slightly more 1-dribble pull-ups than Spot-Up because the player is moving off the screen and may need to gather.

**PNR Roll/Pop:**
Pops are catch-and-shoot. Rolls are inside finishes, almost always 0-1 dribble.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.25 | 0.02 | 0.00 | 0.65 | 0.08 |

Rationale: Roll man catches lob/pass and immediately finishes (0-1 dribble). Pop man catches and shoots (C&S). Very few pull-ups. Some roll men catch and take 2+ dribbles to get to the rim through traffic.

**Handoff:**
Receiver gets handoff and either shoots immediately, probes with 1 dribble, or drives.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.05 | 0.20 | 0.25 | 0.10 | 0.40 |

Rationale: Handoffs have the widest distribution. Many result in pull-ups (1 or 2+ dribbles). Few receivers shoot immediately since the handoff happens in motion. Many drive to the rim — typically requiring 2+ dribbles since the handoff happens above the break.

**Cut (Open Rim):**
Player receives pass while cutting to basket, immediately finishes.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.00 | 0.00 | 0.00 | 1.00 | 0.00 |

Rationale: Cuts are definitionally catch-and-finish at the rim with 0 dribbles.

### Assumed Unassisted play types

These play types are ~0% assisted, so their tracking column distribution doesn't matter for our calculation — they contribute 0 to assisted FGM at any dribble count. Listed here for completeness and for the validation constraint.

**Putbacks:**

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.00 | 0.00 | 0.00 | 0.97 | 0.03 |

**PNR Ball Handler:**

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.00 | 0.10 | 0.30 | 0.05 | 0.55 |

**Isolation:**

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.00 | 0.02 | 0.40 | 0.03 | 0.55 |

### Partial assist rate play types

These play types have mixed assist rates. Each tracking column gets its own assist rate assumption.

**Transition:**
Kickouts to shooters are assisted. Cuts/passes ahead to the rim are assisted. Coast-to-coast drives and pull-ups are mostly unassisted.

The C&S fraction for Transition is **derived from the gap analysis** (see computation logic). The remaining FGM is split as follows:

| Non-CS split | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| Share of non-CS | 0.05 | 0.15 | 0.40 | 0.40 |

Per-column assist rates:

| | CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|---|
| Assist rate | 1.00 | 0.95 | 0.20 | 1.00 | 0.15 |

Rationale: Transition C&S = kickout to open shooter (always assisted). LT10_01 = pass ahead to cutter at rim (always assisted). PU1 = quick pull-up off a pass in transition (almost always assisted). PU2 = pull-up off live dribble (rarely assisted). LT10_2 = coast-to-coast drive (rarely assisted).

**Post-Up:**
Assisted when entry pass leads directly to score. Unassisted when player makes multiple moves.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.02 | 0.03 | 0.05 | 0.30 | 0.60 |

Per-column assist rates:

| | CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|---|
| Assist rate | 1.00 | 0.70 | 0.20 | 0.70 | 0.25 |

Rationale: Post-up C&S = catch entry pass, immediately face up and shoot (assisted). LT10_01 = catch close to basket, immediately score (mostly assisted — entry pass directly to score). PU1 = catch, one dribble step-back or face-up (often assisted). PU2 and LT10_2 = multiple moves after the catch (mostly self-created). These assist rate assumptions are speculative.

**Misc:**
Catch-all category, small volume. Assumed uniform distribution.

| CS | PU1 | PU2 | LT10_01 | LT10_2 |
|---|---|---|---|---|
| 0.20 | 0.20 | 0.20 | 0.20 | 0.20 |

Per-column assist rates: Use same rates as league average across all play types (approximately 0.80 for CS, 0.50 for PU1, 0.20 for PU2, 0.70 for LT10_01, 0.30 for LT10_2). Low impact given small volume.

---

## Computation Logic

### Step 1: Derive Transition C&S fraction from gap analysis

The Synergy play type distributions for all non-Transition play types produce an estimated total C&S FGM. The gap between this estimate and the actual C&S FGM from tracking data is attributed to Transition (plus small adjustments from Post-Up and Misc).

```python
# Sum estimated CS FGM from all non-Transition play types
est_cs_non_transition = 0
for pt in ALL_NON_TRANSITION_PLAYTYPES:
    est_cs_non_transition += PLAYTYPE_FGM[pt] * DISTRIBUTIONS[pt].cs

# Gap = actual CS FGM minus estimated from non-Transition
cs_gap = CS_FGM - est_cs_non_transition

# Transition CS fraction
transition_cs_fgm = max(cs_gap, 0)  # Floor at 0 in case of overestimate
transition_cs_fraction = transition_cs_fgm / TRANSITION_FGM

# Build full Transition distribution
transition_non_cs = 1.0 - transition_cs_fraction
TRANSITION_DISTRIBUTION = (
    transition_cs_fraction,                     # CS
    transition_non_cs * 0.05,                   # PU1
    transition_non_cs * 0.15,                   # PU2
    transition_non_cs * 0.40,                   # LT10_01
    transition_non_cs * 0.40,                   # LT10_2
)
```

Sanity check: `transition_cs_fraction` should be roughly 0.15–0.30. Outside this range, revisit the non-Transition distributions.

### Step 2: Compute assisted FGM at ≤1 dribble from fully-assisted play types

For each "assumed assisted" play type (100% assist rate), all FGM in ≤1 dribble columns count:

```python
DISTRIBUTIONS = {
    "Spot-Up":       (0.80, 0.10, 0.00, 0.02, 0.08),
    "Off Screen":    (0.70, 0.15, 0.02, 0.08, 0.05),
    "PNR Roll/Pop":  (0.25, 0.02, 0.00, 0.65, 0.08),
    "Handoff":       (0.05, 0.20, 0.25, 0.10, 0.40),
    "Cut":           (0.00, 0.00, 0.00, 1.00, 0.00),
}

FULLY_ASSISTED_FGM = {
    "Spot-Up":       SPOTUP_FGM,
    "Off Screen":    OFFSCREEN_FGM,
    "PNR Roll/Pop":  ROLLPOP_FGM,
    "Handoff":       HANDOFF_FGM,
    "Cut":           CUT_FGM,
}

ast_fgm_leq1 = 0
ast_fgm_2plus = 0

for pt, fgm in FULLY_ASSISTED_FGM.items():
    cs, pu1, pu2, lt10_01, lt10_2 = DISTRIBUTIONS[pt]
    ast_fgm_leq1 += fgm * (cs + pu1 + lt10_01)
    ast_fgm_2plus += fgm * (pu2 + lt10_2)
```

### Step 3: Add assisted FGM from partial-assist play types

For Transition, Post-Up, and Misc, multiply FGM × column share × column assist rate:

```python
PARTIAL_PLAYTYPES = {
    "Transition": {
        "fgm": TRANSITION_FGM,
        "distribution": TRANSITION_DISTRIBUTION,  # From Step 1
        "assist_rates": (1.00, 0.95, 0.20, 1.00, 0.15),
    },
    "Post-Up": {
        "fgm": POSTUP_FGM,
        "distribution": (0.02, 0.03, 0.05, 0.30, 0.60),
        "assist_rates": (1.00, 0.70, 0.20, 0.70, 0.25),
    },
    "Misc": {
        "fgm": MISC_FGM,
        "distribution": (0.20, 0.20, 0.20, 0.20, 0.20),
        "assist_rates": (0.80, 0.50, 0.20, 0.70, 0.30),
    },
}

for pt, cfg in PARTIAL_PLAYTYPES.items():
    fgm = cfg["fgm"]
    cs, pu1, pu2, lt10_01, lt10_2 = cfg["distribution"]
    ar_cs, ar_pu1, ar_pu2, ar_lt10_01, ar_lt10_2 = cfg["assist_rates"]

    # Assisted FGM in ≤1 dribble columns
    ast_fgm_leq1 += fgm * (cs * ar_cs + pu1 * ar_pu1 + lt10_01 * ar_lt10_01)

    # Assisted FGM in 2+ dribble columns
    ast_fgm_2plus += fgm * (pu2 * ar_pu2 + lt10_2 * ar_lt10_2)
```

### Step 4: Account for residual (unclassified FGM + stray assists in unassisted types)

Synergy play types don't cover 100% of FGM. The gap (~7%) plus any stray assists within ISO/PNRBH/Putbacks form a small residual.

```python
classified_fgm = sum(fgm for fgm in ALL_PLAYTYPE_FGM.values())
unclassified_fgm = TOTAL_FGM - classified_fgm

classified_ast_fgm = ast_fgm_leq1 + ast_fgm_2plus
residual_ast_fgm = TOTAL_AST_FGM - classified_ast_fgm

# Apply residual rate
RESIDUAL_LEQ1_RATE = 0.55
ast_fgm_leq1 += residual_ast_fgm * RESIDUAL_LEQ1_RATE
ast_fgm_2plus += residual_ast_fgm * (1 - RESIDUAL_LEQ1_RATE)
```

Print the residual size as a fraction of TOTAL_AST_FGM. If >15%, the model needs more work. If <10%, the residual rate assumption has minimal impact.

### Step 5: Compute ratios

```python
PCT_AST_FGM_IN_PA = ast_fgm_leq1 / TOTAL_AST_FGM

# Points version
CS_PPM = (CS_FGM * 2 + CS_FG3M) / CS_FGM         # ~2.9
PU_PPM = (PU_FGM * 2 + PU_FG3M) / PU_FGM         # ~2.4
LT10_PPM = 2.0                                     # Close range ≈ all 2-pointers

# Recompute with points weighting
ast_pts_leq1 = 0

# Fully assisted play types
for pt, fgm in FULLY_ASSISTED_FGM.items():
    cs, pu1, pu2, lt10_01, lt10_2 = DISTRIBUTIONS[pt]
    ast_pts_leq1 += fgm * (cs * CS_PPM + pu1 * PU_PPM + lt10_01 * LT10_PPM)

# Partial assist play types
for pt, cfg in PARTIAL_PLAYTYPES.items():
    fgm = cfg["fgm"]
    cs, pu1, pu2, lt10_01, lt10_2 = cfg["distribution"]
    ar_cs, ar_pu1, ar_pu2, ar_lt10_01, ar_lt10_2 = cfg["assist_rates"]
    ast_pts_leq1 += fgm * (
        cs * ar_cs * CS_PPM
        + pu1 * ar_pu1 * PU_PPM
        + lt10_01 * ar_lt10_01 * LT10_PPM
    )

# Residual points
RESIDUAL_PPM = 2.2
ast_pts_leq1 += residual_ast_fgm * RESIDUAL_LEQ1_RATE * RESIDUAL_PPM

# TOTAL_AST_PTS: pull directly as SUM(AST_PTS_Created) from passing table
PCT_AST_PTS_IN_PA = ast_pts_leq1 / TOTAL_AST_PTS
```

---

## Validation

### Constraint 1: Tracking column totals must reconcile

Sum the estimated FGM allocated to each tracking column across ALL play types (assisted + unassisted + partial). These should roughly match the actual tracking totals from the API, with a gap attributable to unclassified FGM.

```python
for col_idx, col_name in enumerate(["CS", "PU1", "PU2", "LT10_01", "LT10_2"]):
    est = sum(
        fgm * dist[col_idx]
        for pt, (fgm, dist) in all_playtypes_with_distributions.items()
    )
    print(f"{col_name}: estimated={est}, actual={actual[col_name]}")
```

Expected: estimates will be slightly below actuals due to unclassified FGM (~7%). Per-column deltas should be <10% after accounting for this gap.

### Constraint 2: Total assisted FGM

`ast_fgm_leq1 + ast_fgm_2plus` should equal `TOTAL_AST_FGM` (since the residual is allocated to make them sum).

### Constraint 3: Transition CS sanity

`transition_cs_fraction` should be 0.15–0.30. This represents the fraction of transition plays that end in a catch-and-shoot kickout, which is a well-known common transition outcome.

### Constraint 4: Sanity on final ratio

`PCT_AST_PTS_IN_PA` should be in range 0.65–0.85. Outside this range, something is likely wrong.

---

## Sensitivity

The main drivers of uncertainty, in order of impact:

1. **Transition assist rates by column** — Transition is high-volume and the per-column assist rates significantly affect how much of its FGM counts as assisted ≤1 dribble.
2. **Distributions for high-volume assisted play types** (Spot-Up, PNR Roll/Pop, Cut) — defensible since they're physically constrained.
3. **Post-Up assist rates by column** — speculative, but Post-Up is moderate volume so the impact is bounded.
4. **Residual ≤1 dribble rate** — only impacts the unclassified ~7% gap; low impact if the residual is small after modeling all 11 play types.

Low-impact assumptions:
- Distributions for unassisted play types (0% assist rate regardless)
- Misc play type (small volume)
- Exact PPM for residual

---

## Implementation Notes

- All Synergy play type FGM totals come from existing Phase 1 pipeline data.
- Tracking shot totals come from `LeagueDashPlayerPtShot` / `PlayerDashPtShots`, summed league-wide.
- `TOTAL_AST_PTS` should be pulled as `SUM(AST_PTS_Created)` from the passing endpoint, not estimated.
- The Transition C&S fraction is derived from data each season (gap analysis), not hardcoded.
- This computation runs once per season to produce a single constant. It does not need to run daily.
- Store the result alongside other league-average parameters in the pipeline config.
- The play type distributions and assist rates are assumptions that should be reviewed if the game changes significantly (e.g., major rule change affecting handoffs or transition).
- **Note on Synergy vs tracking data:** Synergy play types and tracking shot types come from different classification systems and may not perfectly overlap. The distributions are estimates of correspondence, not exact mappings. The validation constraints in this spec help catch any gross misalignment.
