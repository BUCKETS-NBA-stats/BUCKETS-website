import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import pandas as pd
import requests

# --- Make repo root importable so we can import scripts/ingest/nba_policy.py ---
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.ingest.nba_policy import validate_pbpstats_df  # noqa: E402

T = TypeVar("T")


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


def fetch_pbpstats_totals(season: str, season_type: str) -> pd.DataFrame:
    # SeasonType value uses a literal plus sign (e.g. "Regular+Season"), not a space.
    # requests.get will percent-encode the + as %2B, matching the API's expected format.
    pbp_season_type = "Regular+Season" if season_type == "Regular Season" else "Playoffs"

    def _call() -> pd.DataFrame:
        url = "https://api.pbpstats.com/get-totals/nba"
        params = {"Season": season, "SeasonType": pbp_season_type, "Type": "Player"}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("multi_row_table_data", [])
        if not rows:
            raise ValueError("PBPStats API returned empty multi_row_table_data")
        return pd.DataFrame(rows)

    df = with_retries(_call, "PBPStats totals", attempts=5)
    validate_pbpstats_df(df, "nba_pbpstats")
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

    sources = [
        {
            "id": "nba_pbpstats",
            "filename": "nba_pbpstats.parquet",
            "fetch": lambda: fetch_pbpstats_totals(season, season_type),
        },
    ]

    manifest_files = []

    for src in sources:
        fid = src["id"]
        fpath = os.path.join(out_dir, src["filename"])
        df = src["fetch"]()
        atomic_write_parquet(df, fpath)
        manifest_files.append(
            {
                "id": fid,
                "path": fpath.replace("\\", "/"),
                "rows": int(df.shape[0]),
                "cols": int(df.shape[1]),
                "ok": True,
                "error": None,
            }
        )
        print(f"[OK] Wrote {fid}: {fpath} ({df.shape[0]} rows, {df.shape[1]} cols)")

    manifest = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "raw_dir": out_dir.replace("\\", "/"),
        "raw_files": manifest_files,
        "ok": True,
        "policy": {"level": "player", "type": "totals", "source": "api.pbpstats.com"},
    }

    with open(os.path.join("reports", "pbpstats_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
