# BUCKETS Calculation Spec

**Purpose:** Blueprint for the Python pipeline that transforms raw NBA data into the season CSV for the BUCKETS website.
**Last updated:** April 2026
**Scope:** All calculations are per-season. League averages and sums refer to one season. Regular season only for now; playoffs later.

---

## Constants & Parameters

All tunable values in one place. In Python, these live in a config dict.

### League-wide averages (from Cleaning the Glass)

Season-specific values sourced from Cleaning the Glass. The numbers below are 2025-26 placeholders.

| Name | Value | Description |
|------|-------|-------------|
| CTG_HC_PPP | 0.979 | League average half court PPP |
| CTG_OREB_PCT | 0.295 | League average offensive rebound % |
| CTG_PUTBACK_PPP | 1.114 | Putback PPP |
| CTG_TOV_PENALTY | `= -CTG_OREB_PCT * CTG_PUTBACK_PPP` → ≈ -0.329 | Expected lost points per turnover (missed Oreb opportunity) |

### Passing & playmaking parameters

| Name | Value | Description |
|------|-------|-------------|
| PCT_FT_AST_NO_SHOT | 0.2 | Fraction of FT assists without a shot attempt |
| LG_AVG_PP_FT_AST | 1.7 | Average points per FT assist |
| PCT_AST_PTS_IN_PA | computed per season | Fraction of AST PTS Created from ≤1 dribble shots (computed per season from tracking shot data) |
| LG_AVG_PPPA | `= SUM(Playmaking Pts) / SUM(Playmaking Plays ex TOV)` | League average points per playmaking action (computed from data) |
| PADDING_VOLUME | 400 | Pseudo-observations for padded PPPA |
| HC_CLAMP_MAX | 0.86859 | Upper bound for half-court scoring % |
| HC_CLAMP_MIN | 0.7234 | Lower bound for half-court scoring % |

### HC / Transition playmaking regression coefficients

| Name | Value | Used in |
|------|-------|---------|
| HC_REG_INTERCEPT | -165.3686815 | HC playmaking plays regression |
| HC_REG_PLAYMAKING | 0.815899448 | ... coefficient on total playmaking plays |
| HC_REG_OBS | 0.025486432 | ... coefficient on on-ball scoring plays |
| HC_REG_HCPCT | 208.2506342 | ... coefficient on clamped HC% |
| TR_REG_INTERCEPT | 171.1093094 | Transition playmaking plays regression |
| TR_REG_TRANSITION | 0.015129861 | ... coefficient on transition scoring plays |
| TR_REG_PLAYMAKING | 0.166406246 | ... coefficient on total playmaking plays |
| TR_REG_HCPCT | -215.8731256 | ... coefficient on clamped HC% |

### Floor raising regression coefficients

| Name | Value | Description |
|------|-------|-------------|
| FR_INTERCEPT | 0.308596 | Quadratic regression intercept |
| FR_LINEAR | 0.116029 | Coefficient on on-ball plays |
| FR_QUADRATIC | -0.00142 | Coefficient on on-ball plays² |
| FR_MIN_POSS | 600 | Minimum possessions for baseline average |

### Computed parameters (derived during pipeline execution)

These depend on the data and are computed at specific points in the pipeline. Listed here for reference; see the computation order for when each becomes available.

| Name | Formula | Computed after |
|------|---------|----------------|
| Lg Avg On-ball TOV rate | `= (SUM(ISO Scoring TOVs) + SUM(PNRBH Scoring TOVs) + SUM(PostUp Scoring TOVs) + SUM(On-ball Passing TOVs)) / (SUM(ISO Scoring Plays) + SUM(PNRBH Scoring Plays) + SUM(PostUp Scoring Plays) + SUM(est HC playmaking plays (clamped)))` | Phase 3a |
| HC PPP w/ TOV penalty | `= CTG_HC_PPP + Lg Avg On-ball TOV rate * CTG_TOV_PENALTY` | Phase 3a |
| TR_AVG_PPP | `= SUM(Transition PTS) / SUM(Transition Poss)` | Phase 2 |
| TR_OVERALL_TOV_RATE | `= SUM(Transition TOV Total) / SUM(Transition Poss)` | Phase 2 |
| TR_AVG_PLAYS | `= mean(100 * Transition Scoring Plays / Possessions Played)` — average transition scoring plays per 100 team possessions | Phase 2 |
| TR_PPP_RATIO | `= TR_AVG_PPP / CTG_HC_PPP` | Phase 2 |
| PCT_AST_PTS_IN_PA | See docs/pct_ast_pts_in_pa_spec.md | Computed after: staging + tracking shots ingest |

---

## Raw Data Sources

### Per Game general
**Source:** stats.nba.com
**Key columns:** Player, Team, GP, Min
**Provides:** Team, Games, Minutes

### General PBP
**Source:** PBPStats
**Key columns:** Player, OffPoss
**Provides:** Possessions

### Turnovers PBP
**Source:** PBPStats
**Key columns:** Player, Turnovers, LiveBallTurnovers, DeadBallTurnovers, BadPassTurnovers, BadPassOutOfBoundsTurnovers
**Computed column:** ScoringShareOfTOV = `(Turnovers - BadPassTurnovers - BadPassOutOfBoundsTurnovers) / Turnovers`
**Provides:** Playmaking TOVs for Passing (BadPass + BadPassOOB); ScoringShareOfTOV ratio for playtype scoring

### BBI stuff
**Source:** bball-index (manual paste)
**Key columns:** Player, Years experience

### Passing raw data
**Source:** PBPStats
**Key columns:** Player, GP, AST, Secondary AST, Potential AST, AST PTS Created, AST Adj

### Playtype raw data (11 playtypes)
**Source:** stats.nba.com — one dataset per playtype
**Playtypes:** Isolation, PNRBH, Post-UP, Roll and Pop, Spot-Up, Handoffs, Open Rim, Off-Screen, Putbacks, Misc, Transition
**Shared columns:** Player, Team, GP, Poss, PPP, PTS, FGM, FGA, FG%, eFG%, FT Freq%, TOV Freq%, SF Freq%, And One Freq%, Score Freq%, Percentile

---

## Computation Phases

### Phase 1: Standard Playtype Scoring

**One function, called 10 times** — once for each non-Transition playtype.

**Inputs per playtype:** Playtype raw data, ScoringShareOfTOV (joined on Player), Lg Avg On-ball TOV rate, CTG constants

**Per-player calculations (dependency order):**

| Name | Formula | Description |
|------|---------|-------------|
| TOV Total | `round(TOV_Freq_Pct / 100 * Poss, 0)` | Total turnovers |
| Scoring TOVs | `round(TOV Total * ScoringShareOfTOV, 0)` | Scoring-attributed turnovers |
| Scoring Plays | `Poss - TOV Total + Scoring TOVs` | Scoring possessions incl. scoring turnovers |

**Per-playtype aggregate (computed after Scoring Plays for all players):**

`tab_avg_scoring_ppp = SUM(PTS) / SUM(Scoring Plays)` — average Scoring PPP for this playtype.

**Remaining per-player calculations (depend on tab aggregate + Lg Avg On-ball TOV rate):**

| Name | Formula | Description |
|------|---------|-------------|
| xTOVs | `Poss * Lg Avg On-ball TOV rate` | Expected turnovers |
| Scoring PPP baseline | `max(CTG_HC_PPP, tab_avg_scoring_ppp)` for most playtypes; `CTG_HC_PPP` for Putbacks (offrebound) | Higher of league avg and tab avg (Putbacks exception: see note) |
| TOV penalty | `(Scoring TOVs - xTOVs) * CTG_TOV_PENALTY` | Excess scoring TOVs vs expected, penalized |
| PAB | `PTS + TOV penalty - (Scoring PPP baseline * Scoring Plays)` | Points Above Baseline |

**Putbacks baseline exception:** For the `offrebound` playtype (Putbacks), the scoring PPP baseline is `CTG_HC_PPP` rather than `max(CTG_HC_PPP, tab_avg_scoring_ppp)`. Second-chance scoring is valued relative to the general half-court baseline because the team already "used" a possession on the missed shot — the putback opportunity is measured against what any half-court play would produce, not against what putbacks typically produce.

**Outputs used downstream:** PTS, Scoring Plays, PAB, Scoring TOVs — per player per playtype.

---

### Phase 2: Transition Scoring + Aggregates

First pass of Transition. Computes scoring columns and aggregates needed by Phase 3. Playmaking columns are deferred to Phase 4.

**Inputs:** Transition raw data, ScoringShareOfTOV, General PBP (Possessions)

**Per-player calculations:**

| Name | Formula | Description |
|------|---------|-------------|
| TOV Total | `round(TOV_Freq_Pct / 100 * Poss, 0)` | Total turnovers |
| Scoring TOVs | `round(TOV Total * ScoringShareOfTOV, 0)` | Scoring-attributed turnovers |
| Transition Playmaking TOVs | `TOV Total - Scoring TOVs` | Turnovers attributed to playmaking |
| Possessions Played | `[from General PBP OffPoss]` | Total offensive possessions |
| Scoring Plays | `Poss - TOV Total + Scoring TOVs` | Scoring possessions incl. scoring turnovers |

**Tab-level aggregates (computed after per-player columns):**

| Name | Formula |
|------|---------|
| TR_AVG_PPP | `SUM(PTS) / SUM(Poss)` |
| TR_OVERALL_TOV_RATE | `SUM(TOV Total) / SUM(Poss)` |
| TR_AVG_PLAYS | `mean(100 * Scoring Plays / Possessions Played)` — per 100 team possessions |
| TR_PPP_RATIO | `TR_AVG_PPP / CTG_HC_PPP` |

**Outputs used by Phase 3:** Scoring Plays, TOV Total, Transition Playmaking TOVs, plus tab-level aggregates.

---

### Phase 3: Passing & Playmaking

The most complex phase. Computes playmaking value, decomposes into HC vs Transition, and computes HC Playmaking PAB.

**Inputs:** Passing raw data, Turnovers PBP, Phase 1 Scoring Plays (all 10 playtypes), Phase 2 outputs + aggregates

**Note on split execution:** This phase runs in two passes (3a and 3b) because Lg Avg On-ball TOV rate depends on outputs from Phase 3a, and Phase 1b depends on Lg Avg On-ball TOV rate. See computation order below.

**Calculations (dependency order):**

| Name | Formula | Description | Phase |
|------|---------|-------------|-------|
| Playmaking TOVs | `TurnoversPBP BadPass + BadPassOOB` | Lookup from Turnovers PBP | 3a |
| Transition scoring plays | `[from Phase 2 Scoring Plays]` | Lookup | 3a |
| Transition TOV Total | `[from Phase 2 TOV Total]` | Lookup | 3a |
| Transition Playmaking TOVs | `[from Phase 2]` | Lookup | 3a |
| HC Playmaking TOVs | `Playmaking TOVs - Transition Playmaking TOVs` | Half-court playmaking turnovers | 3a |
| FT assist | `AST_Adj - AST - Secondary_AST` | Free-throw assists | 3a |
| Playmaking Plays ex TOV | `Potential_AST + FT assist * PCT_FT_AST_NO_SHOT` | Playmaking plays excluding turnovers | 3a |
| Playmaking Pts | `AST_PTS_Created * PCT_AST_PTS_IN_PA + FT assist * LG_AVG_PP_FT_AST` | Total playmaking points (PCT_AST_PTS_IN_PA corrects for assists on 2+ dribble shots which are excluded from Potential AST) | 3a |
| Actual PPPA | `Playmaking Pts / Playmaking Plays ex TOV` | Actual points per playmaking action | 3a |
| PPPA (padded) | `(LG_AVG_PPPA * PADDING_VOLUME + Playmaking Pts) / (Playmaking Plays ex TOV + PADDING_VOLUME)` | Stabilized PPPA estimate | 3a |
| OBS plays | `SUM(Scoring Plays from ISO + PNRBH + Post-UP)` | On-ball scoring plays | 3a |
| HC scoring plays | `OBS plays + Spot-Up + Handoffs + Open Rim + Off-Screen + Putbacks + Misc Scoring Plays` | All half-court scoring plays | 3a |
| Total Scoring Plays | `HC scoring plays + Transition scoring plays` | | 3a |
| Total playmaking plays | `Playmaking Plays ex TOV + Playmaking TOVs` | | 3a |
| HC scoring % | `HC scoring plays / Total Scoring Plays` (default to league avg if ÷0) | | 3a |
| HC% (clamped) | `MAX(MIN(HC scoring %, HC_CLAMP_MAX), HC_CLAMP_MIN)` | | 3a |
| est HC playmaking plays | `HC_REG_INTERCEPT + Total playmaking plays * HC_REG_PLAYMAKING + OBS plays * HC_REG_OBS + HC% * HC_REG_HCPCT` | Regression estimate | 3a |
| est Transition playmaking plays | `TR_REG_INTERCEPT + Transition scoring plays * TR_REG_TRANSITION + Total playmaking plays * TR_REG_PLAYMAKING + HC% * TR_REG_HCPCT` | Regression estimate | 3a |
| est HC playmaking plays (clamped) | `min(max(est HC playmaking plays, 0), Total playmaking plays)` | Clamped to [0, total] | 3a |
| est Transition playmaking plays (clamped) | `min(max(est Transition playmaking plays, 0), Total playmaking plays)` | Clamped to [0, total] | 3a |
| On-ball Passing TOVs | `Playmaking TOVs - Transition TOV Total` | Input to Lg Avg On-ball TOV rate | 3a |
| ↓ | **Lg Avg On-ball TOV rate and HC PPP w/ TOV penalty computed here (step 5-6 in execution order)** | | |
| est HC playmaking plays (final) | `if Total playmaking plays = 0 then 0 else est HC (clamped) * (Total playmaking plays / (est HC (clamped) + est Transition (clamped)))` | Normalized so HC + Transition = total | 3b |
| est Transition playmaking plays (final) | `if Total playmaking plays = 0 then 0 else est Transition (clamped) * (Total playmaking plays / (est HC (clamped) + est Transition (clamped)))` | Normalized | 3b |
| xTOV | `est HC playmaking plays (final) * Lg Avg On-ball TOV rate` | Expected turnovers | 3b |
| Est. Actual HC PPPA | `Playmaking Pts / (est Transition playmaking plays (final) * TR_PPP_RATIO + est HC playmaking plays (final))` | Actual HC points per playmaking action | 3b |
| Est. Actual TR PPPA | `Est. Actual HC PPPA * TR_PPP_RATIO` | Actual Transition points per playmaking action | 3b |
| Est. Transition playmaking points | `est Transition playmaking plays (final) * Est. Actual TR PPPA` | | 3b |
| Est. HC playmaking points | `est HC playmaking plays (final) * Est. Actual HC PPPA` | | 3b |
| HC padded PPPA | `(PPPA (padded) * Playmaking Plays ex TOV) / (est Transition playmaking plays (final) * TR_PPP_RATIO + est HC playmaking plays (final))` | Stabilized HC PPPA | 3b |
| TR padded PPPA | `HC padded PPPA * TR_PPP_RATIO` | Stabilized Transition PPPA | 3b |
| HC Playmaking PAB | `HC padded PPPA * (est HC playmaking plays (final) - HC Playmaking TOVs) + (HC Playmaking TOVs * CTG_TOV_PENALTY) - est HC playmaking plays (final) * HC PPP w/ TOV penalty` | HC playmaking points above baseline | 3b |
| HC Playmaking PAB/G | `HC Playmaking PAB / GP` | | 3b |

**Outputs used downstream:** Playmaking Plays ex TOV, Total playmaking plays, est HC playmaking plays (final), est Transition playmaking plays (final), Est. Transition playmaking points, Est. HC playmaking points, HC Playmaking PAB, HC Playmaking PAB/G, On-ball Passing TOVs, est HC playmaking plays (clamped).

---

### Phase 4: Transition Playmaking

Second pass of Transition. Adds playmaking columns and computes Points Created with scoring/playmaking decomposition.

**Inputs:** Phase 2 per-player columns, Phase 3 outputs, Transition aggregates, CTG constants

**Note on column context:** All columns below belong to the Transition player DataFrame. Names like "Playmaking Plays" here refer to Transition-specific values, not the Passing tab values from Phase 3.

**Per-player calculations (dependency order):**

| Name | Formula | Description |
|------|---------|-------------|
| Playmaking Plays | `[from Phase 3: est Transition playmaking plays (final)]` | |
| Playmaking Points | `[from Phase 3: Est. Transition playmaking points]` | |
| Playmaking TOVs | `TOV Total - Scoring TOVs` | Transition playmaking turnovers |
| xPlaymaking TOVs | `Playmaking Plays * TR_OVERALL_TOV_RATE` | Expected playmaking TOVs |
| xTOV | `Poss * TR_OVERALL_TOV_RATE` | Expected total TOVs |
| Transition PRF | `Playmaking Points + PTS` | Total points responsible for |
| Transition Total Plays | `Scoring Plays + Playmaking Plays` | All transition plays |
| Transition Poss/100 | `100 * Transition Total Plays / Possessions Played` | |
| TOV penalty | `TOV Total * CTG_TOV_PENALTY` | |
| PRF w/ TOV penalty | `Transition PRF + TOV penalty` | |
| Plays up to LA/100 | `MIN(TR_AVG_PLAYS, Transition Poss/100)` | Capped at league avg volume |
| Plays above LA/100 | `Transition Poss/100 - Plays up to LA/100` | |
| Baseline PPP | `(Plays up to LA/100 * TR_AVG_PPP + Plays above LA/100 * CTG_HC_PPP) / Transition Poss/100` | Blended baseline |
| PRF baseline | `Baseline PPP * Transition Total Plays` | Expected points at baseline |
| baseline xTOV penalty | `xTOV * CTG_TOV_PENALTY` | |
| PRF baseline w/ xTOV | `PRF baseline + baseline xTOV penalty` | |
| Points created | `PRF w/ TOV penalty - PRF baseline w/ xTOV` | Value added |
| PC/g | `Points created / GP` | Checksum input |

**Scoring / Playmaking decomposition:**

| Name | Formula | Description |
|------|---------|-------------|
| Scoring PRF w/ TOV penalty | `PTS + CTG_TOV_PENALTY * Scoring TOVs` | |
| xScoring TOVs | `xTOV - xPlaymaking TOVs` | |
| Scoring PRF baseline w/ xTOV | `Baseline PPP * Scoring Plays + CTG_TOV_PENALTY * xScoring TOVs` | |
| Scoring points created | `Scoring PRF w/ TOV penalty - Scoring PRF baseline w/ xTOV` | → Final output |
| Scoring PC/g | `Scoring points created / GP` | Checksum input |
| Playmaking PRF w/ TOV penalty | `Playmaking Points + CTG_TOV_PENALTY * Playmaking TOVs` | |
| Playmaking PRF baseline w/ xTOV | `Baseline PPP * Playmaking Plays + CTG_TOV_PENALTY * xPlaymaking TOVs` | |
| Playmaking points created | `Playmaking PRF w/ TOV penalty - Playmaking PRF baseline w/ xTOV` | → Final output |
| Playmaking PC/g | `Playmaking points created / GP` | Checksum input |
| Checksum | `Playmaking PC/g + Scoring PC/g - PC/g` | Should be ≈ 0 |

**Outputs used by Phase 6:**

| Phase 4 output | Internal name in Phase 6 | Used for |
|----------------|--------------------------|---------|
| Transition PRF | `tr_prf` | Transition PRF columns |
| Transition Total Plays | `tr_plays` | Transition Plays columns |
| Scoring points created | `tr_sc_pc` | Scoring aggregate (half-court PAB + `tr_sc_pc`) |
| Playmaking points created | `tr_pm_pc` | Playmaking aggregate (HC PAB + `tr_pm_pc`) |
| `tr_sc_pc + tr_pm_pc` | `tr_pc` | **Transition PC columns; also feeds `tot_pc`** |

`tr_pc` is the true total transition PC (scoring + playmaking), parallel to how `ob_pc` combines on-ball scoring and playmaking.

---

### Phase 5: Floor Raising

Quadratic regression bonus for on-ball players based on play volume.

**Inputs:** Phase 1 on-ball Scoring Plays (ISO + PNRBH + Post-UP + Misc), Phase 3 est HC playmaking plays (final), General PBP (Possessions), Per Game general (Games, Minutes)

**Step 1 — On-ball Plays** (independent of Floor raising):

```python
on_ball_plays_total = SUM(ISO + PNRBH + PostUp + Misc Scoring Plays) + est HC playmaking plays (final)  # per player
on_ball_plays_per_g  = on_ball_plays_total / Games
on_ball_plays_per_36 = (on_ball_plays_per_g * Games) / (Minutes / 36)
on_ball_plays_per_75 = (on_ball_plays_per_g * Games) / (Possessions / 75)
```

**Step 2 — Gate:** Only compute for players who exist in at least one on-ball playtype (ISO, PNRBH, Post-UP, Misc).

**Step 3 — Regression** (same formula applied to /g, /36, /75 inputs):

```python
raw_floor_raising = FR_INTERCEPT + FR_LINEAR * on_ball_plays + FR_QUADRATIC * on_ball_plays**2
```

**Step 4 — Baseline** (mean of raw values for players with ≥ FR_MIN_POSS possessions):

```python
baseline = mean(raw_floor_raising where Possessions >= FR_MIN_POSS)
```

Computed separately for /g, /36, /75.

**Step 5 — Over-baseline:**

```python
floor_raising_pc = raw_floor_raising - baseline
```

**Outputs used by Phase 6:** Floor raising PC per /g, /36, /75.

---

### Phase 6: Assemble Final Output

Joins all phase outputs into the season CSV. Everything becomes DataFrame joins on Player.

**Player identity & basics:**

| Column | Source |
|--------|--------|
| Player | Master player list |
| Year | Season string (e.g., "2025-26") |
| Season type | "RS" |
| Tm | Per Game general → Team |
| Games | Per Game general → GP |
| Minutes | Per Game general → Min |
| Possessions | General PBP → OffPoss |
| Years experience | BBI stuff |

**Has data? flags** (boolean, from checking player existence in each source):

| Flag | True if player exists in… |
|------|---------------------------|
| On-ball | any of ISO, PNRBH, Post-UP, Misc |
| Off-ball: Partner | any of Roll&Pop, Handoffs |
| Off-ball: Space | any of Spot-Up, Off-Screen |
| Off-ball: Crash | any of Open Rim, Putbacks |
| Transition | Transition |
| Passing | Passing |
| Any category | OR of all above |
| All | AND of all above |

**Category groupings** (each gets PRF, Plays, rORTG, PC in /g, /36, /75):

| Category | Source playtypes | PRF = | Plays = | PC = |
|----------|-----------------|-------|---------|------|
| On-ball | ISO + PNRBH + Post-UP + Misc + HC Playmaking | SUM(PTS) + Est. HC playmaking points | SUM(Scoring Plays) + est HC playmaking plays (final) | SUM(PAB) + HC Playmaking PAB |
| Off-ball: Partner | Roll&Pop + Handoffs | SUM(PTS) | SUM(Scoring Plays) | SUM(PAB) |
| Off-ball: Space | Spot-Up + Off-Screen | SUM(PTS) | SUM(Scoring Plays) | SUM(PAB) |
| Off-ball: Crash | Open Rim + Putbacks | SUM(PTS) | SUM(Scoring Plays) | SUM(PAB) |
| Transition | Transition (Phase 4) | Transition PRF | Transition Total Plays | `tr_pc` = Scoring points created + Playmaking points created |

**Aggregates:**

| Aggregate | Definition |
|-----------|------------|
| Total | On-ball + Partner + Space + Crash + Transition |
| Half court | On-ball + Partner + Space + Crash |
| Off-ball | Partner + Space + Crash |
| Scoring | All 11 playtypes: PTS / Scoring Plays / PAB (standard) or **Scoring points created only** (`tr_sc_pc`) for Transition |
| Playmaking | PRF = Est. HC + Est. Transition playmaking points, Plays = Total playmaking plays, PC = HC Playmaking PAB + **Transition Playmaking points created** (`tr_pm_pc`) |
| % Playmaking | Playmaking Plays / Total Plays |

**Identity (holds exactly):**

```
Scoring PC + Playmaking PC + Floor raising PC = Total PC (floor raising adj.)
```

This holds because Scoring and Playmaking together cover all five category PCs (On-ball + Partner + Space + Crash + Transition), which sum to Total PC before the floor-raising adjustment:
- Scoring PC = SUM(10 HC scoring PABs) + `tr_sc_pc`
- Playmaking PC = `hc_playmaking_pab` + `tr_pm_pc`
- Scoring + Playmaking = Total (ex. floor raising)
- Adding Floor raising PC to both sides gives the adjusted identity.

**Floor raising adjustments:**

| Column | Formula |
|--------|---------|
| Floor raising PC | Phase 5 output (per /g, /36, /75) |
| On-ball PC (floor raising adj.) | Floor raising PC + On-ball PC |
| Total PC (floor raising adj.) | On-ball PC adj. + Partner PC + Space PC + Crash PC + Transition PC |

**Creation usage metrics:**

| Column | Formula |
|--------|---------|
| Total creation usage | `(Total Plays/g * Games) / Possessions` |
| On-ball creation usage | `(On-ball Plays/g * Games) / Possessions` |
| Off-ball creation usage | `(Off-ball Plays/g * Games) / Possessions` |
| Transition creation usage | `(Transition Plays/g * Games) / Possessions` |
| On-ball share | `On-ball Plays/36 / ((36/48) * 90)` |

**On-ball scoring vs playmaking decomposition:**

| Column | Source |
|--------|--------|
| On-ball Scoring PC | SUM(PAB from ISO, PNRBH, Post-UP, Misc) |
| On-ball Playmaking PC | From Phase 3 (half-court portion) |

**The /g, /36, /75, rORTG pattern** — applied to every metric group:

```python
per_game = raw_total / games
per_36   = (per_game * games) / (minutes / 36)
per_75   = (per_game * games) / (possessions / 75)
rORTG    = 100 * pc_total / plays_total
```

All gated by the corresponding "Has data?" flag — null if no data for that category.

---

## Computation Order

Lg Avg On-ball TOV rate creates a cross-phase dependency: it needs Phase 1 Scoring TOVs/Plays AND Phase 3's clamped regression outputs. But Phase 1's xTOVs needs Lg Avg On-ball TOV rate. The solution is to split Phases 1 and 3:

```
 0.  compute_pct_ast_pts.py — compute PCT_AST_PTS_IN_PA (runs after staging validation, before build_season.py)
 1.  Load raw data
 2.  Phase 1a:  Standard playtype scoring — partial
               (TOV Total, Scoring TOVs, Scoring Plays only; no xTOVs or PAB yet)
 3.  Phase 2:   Transition scoring + tab-level aggregates
 4.  Phase 3a:  Passing — through clamped regression + On-ball Passing TOVs + HC Playmaking TOVs
 5.  Compute Lg Avg On-ball TOV rate
 6.  Compute HC PPP w/ TOV penalty
 7.  Phase 1b:  Standard playtype scoring — completion (xTOVs, TOV penalty, PAB)
 8.  Phase 3b:  Passing — completion (normalized regression, PPPA estimates, point estimates, HC Playmaking PAB)
 9.  Phase 4:   Transition playmaking
10.  Phase 5:   Floor raising
11.  Phase 6:   Assemble final output
```

## Validation

Compare Python output to a known-good Google Sheets export for the same season:

- Phase 1: PAB per playtype per player
- Phase 3: HC Playmaking PAB per player
- Phase 4: Transition Points created, and Checksum ≈ 0 for all players
- Phase 6: Final CSV row/column counts and spot-check values against existing season file
