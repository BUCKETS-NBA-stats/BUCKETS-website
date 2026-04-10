import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

from nba_api.stats.endpoints import playerdashptshots

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

T = TypeVar("T")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def with_retries(fn: Callable[[], T], label: str, attempts: int = 5) -> T:
    last_err: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i == attempts:
                break
            wait = 2 ** i
            print(f"[WARN] {label} failed (attempt {i}/{attempts}): {e}")
            print(f"[WARN] Waiting {wait}s then retrying...")
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def fetch_tracking_shots(season: str, season_type: str) -> tuple[dict, dict]:
    """
    Single PlayerDashPtShots(player_id=0, team_id=0) call returns all players.
    df[1] = shot types (Catch and Shoot, Pull Ups, Less than 10 ft, Other)
    df[3] = dribble ranges (0 Dribbles, 1 Dribble, 2 Dribbles, 3-6 Dribbles, 7+ Dribbles)
    """
    label = "PlayerDashPtShots league-wide"

    def _call():
        resp = playerdashptshots.PlayerDashPtShots(
            player_id=0,
            team_id=0,
            season=season,
            season_type_all_star=season_type,
            per_mode_simple="Totals",
        )
        dfs = resp.get_data_frames()
        df_shot  = dfs[1]   # shot types
        df_drib  = dfs[3]   # dribble ranges
        if df_shot.empty or df_drib.empty:
            raise ValueError("Empty shot-type or dribble-range dataframe")
        return df_shot, df_drib

    df_shot, df_drib = with_retries(_call, label)

    def shot_sum(shot_type: str, col: str) -> int:
        return int(df_shot.loc[df_shot["SHOT_TYPE"] == shot_type, col].sum())

    def drib_sum(drib_range: str) -> int:
        return int(df_drib.loc[df_drib["DRIBBLE_RANGE"] == drib_range, "FGM"].sum())

    shot_totals = {
        "CS_FGM":  shot_sum("Catch and Shoot", "FGM"),
        "CS_FG3M": shot_sum("Catch and Shoot", "FG3M"),
        "PU_FGM":  shot_sum("Pull Ups", "FGM"),
        "PU_FG3M": shot_sum("Pull Ups", "FG3M"),
    }

    drib_totals = {
        "DRIB0":  drib_sum("0 Dribbles"),
        "DRIB1":  drib_sum("1 Dribble"),
        "DRIB2":  drib_sum("2 Dribbles"),
        "DRIB36": drib_sum("3-6 Dribbles"),
        "DRIB7":  drib_sum("7+ Dribbles"),
    }

    return shot_totals, drib_totals


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull league-wide tracking shot totals from NBA.com"
    )
    parser.add_argument("--season", required=True, help='e.g. "2024-25"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    out_dir = os.path.join(REPO_ROOT, "assets", "data", "raw", season, season_type_slug)
    ensure_dir(out_dir)
    ensure_dir(os.path.join(REPO_ROOT, "reports"))

    print("[INFO] Fetching PlayerDashPtShots (league-wide, all shot types + dribble ranges) ...")
    shot_totals, drib_totals = fetch_tracking_shots(season, season_type)

    drib0_fgm     = drib_totals["DRIB0"]
    drib1_fgm     = drib_totals["DRIB1"]
    drib2plus_fgm = drib_totals["DRIB2"] + drib_totals["DRIB36"] + drib_totals["DRIB7"]
    total_fgm     = drib0_fgm + drib1_fgm + drib2plus_fgm

    print(f"[OK]   CS_FGM={shot_totals['CS_FGM']:,}  CS_FG3M={shot_totals['CS_FG3M']:,}")
    print(f"[OK]   PU_FGM={shot_totals['PU_FGM']:,}  PU_FG3M={shot_totals['PU_FG3M']:,}")
    print(f"[OK]   DRIB0={drib0_fgm:,}  DRIB1={drib1_fgm:,}  DRIB2+={drib2plus_fgm:,}")
    print(f"[OK]   TOTAL_FGM={total_fgm:,}  (= DRIB0+DRIB1+DRIB2+)")

    # -- Validation --
    print()
    print("[INFO] Validating ...")

    cs_pu_sum = shot_totals["CS_FGM"] + shot_totals["PU_FGM"]
    if cs_pu_sum >= total_fgm:
        print(f"[ERROR] CS+PU ({cs_pu_sum:,}) >= TOTAL_FGM ({total_fgm:,})")
        return 1

    if shot_totals["CS_FGM"] > drib0_fgm:
        print(f"[ERROR] CS_FGM ({shot_totals['CS_FGM']:,}) > DRIB0_FGM ({drib0_fgm:,}) -- C&S must be subset of 0-dribble")
        return 1

    print(f"[OK]   CS ({shot_totals['CS_FGM']:,}) + PU ({shot_totals['PU_FGM']:,}) = {cs_pu_sum:,} < TOTAL ({total_fgm:,})")
    print(f"[OK]   CS_FGM ({shot_totals['CS_FGM']:,}) <= DRIB0_FGM ({drib0_fgm:,})")

    # -- Build output --
    values = {
        "CS_FGM":        shot_totals["CS_FGM"],
        "CS_FG3M":       shot_totals["CS_FG3M"],
        "PU_FGM":        shot_totals["PU_FGM"],
        "PU_FG3M":       shot_totals["PU_FG3M"],
        "DRIB0_FGM":     drib0_fgm,
        "DRIB1_FGM":     drib1_fgm,
        "DRIB2PLUS_FGM": drib2plus_fgm,
        "TOTAL_FGM":     total_fgm,
    }

    out = {
        "generated_at_utc": utc_now_iso(),
        "season":            season,
        "season_type":       season_type,
        "source":            "nba.com/stats (PlayerDashPtShots, player_id=0 team_id=0)",
        "values":            values,
    }

    out_path = os.path.join(out_dir, "nba_tracking_shots.json")
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    os.replace(tmp_path, out_path)
    print(f"\n[OK] Wrote {out_path}")

    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season":            season,
        "season_type":       season_type,
        "raw_dir":           out_dir.replace("\\", "/"),
        "raw_files": [
            {
                "id":    "nba_tracking_shots",
                "path":  out_path.replace("\\", "/"),
                "rows":  1,
                "cols":  len(values),
                "ok":    True,
                "error": None,
            }
        ],
        "ok": True,
        "policy": {"level": "league", "mode": "totals", "filters": "shot_type + dribble_range"},
    }
    manifest_path = os.path.join(REPO_ROOT, "reports", "ingest_manifest_tracking_shots.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"[OK] Manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
