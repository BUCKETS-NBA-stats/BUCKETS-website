import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pandas as pd
from nba_api.stats.endpoints import synergyplaytypes

# --- Make repo root importable so we can import scripts/ingest/nba_policy.py ---
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.ingest.nba_policy import validate_playtypes_df  # noqa: E402

T = TypeVar("T")

PLAY_TYPES = [
    "Isolation",
    "PRBallHandler",
    "PRRollman",
    "Postup",
    "Spotup",
    "Handoff",
    "Cut",
    "OffScreen",
    "OffRebound",
    "Misc",
    "Transition",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_parquet(df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, out_path)


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


def fetch_play_type(season: str, season_type: str, play_type: str) -> pd.DataFrame:
    def _call() -> pd.DataFrame:
        resp = synergyplaytypes.SynergyPlayTypes(
            season=season,
            season_type_all_star=season_type,
            play_type_nullable=play_type,
            type_grouping_nullable="offensive",
            player_or_team_abbreviation="P",
            per_mode_simple="Totals",
        )
        return resp.get_data_frames()[0]

    df = with_retries(_call, f"NBA play type [{play_type}]", attempts=5)
    df["PLAY_TYPE"] = play_type
    return df


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help='e.g. "2025-26"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    out_dir = os.path.join("assets", "data", "raw", season, season_type_slug)
    ensure_dir(out_dir)
    ensure_dir("reports")

    frames = []
    for play_type in PLAY_TYPES:
        df = fetch_play_type(season, season_type, play_type)
        frames.append(df)
        print(f"[OK] Fetched play type [{play_type}]: {df.shape[0]} rows")

    df_all = pd.concat(frames, ignore_index=True)
    validate_playtypes_df(df_all, "nba_playtypes")

    out_path = os.path.join(out_dir, "nba_playtypes.parquet")
    atomic_write_parquet(df_all, out_path)
    print(f"[OK] Wrote nba_playtypes: {out_path} ({df_all.shape[0]} rows, {df_all.shape[1]} cols)")

    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "raw_dir": out_dir.replace("\\", "/"),
        "raw_files": [
            {
                "id": "nba_playtypes",
                "path": out_path.replace("\\", "/"),
                "rows": int(df_all.shape[0]),
                "cols": int(df_all.shape[1]),
                "ok": True,
                "error": None,
            }
        ],
        "ok": True,
        "play_types_fetched": PLAY_TYPES,
        "policy": {"level": "player", "type_grouping": "offensive", "mode": "Totals"},
    }

    with open(os.path.join("reports", "playtypes_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
