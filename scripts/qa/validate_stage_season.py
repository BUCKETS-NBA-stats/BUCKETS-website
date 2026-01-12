import argparse
import json
import os
from datetime import datetime, timezone

import pandas as pd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True)
    parser.add_argument("--season-type", required=True)
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    stage_path = os.path.join("assets", "data", "staging", f"{season}__{season_type_slug}.parquet")
    if not os.path.exists(stage_path):
        raise FileNotFoundError(f"Missing staging file: {stage_path}")

    df = pd.read_parquet(stage_path)

    problems = []

    if "PLAYER_ID" not in df.columns:
        problems.append("Missing PLAYER_ID column in staging.")
    else:
        if df["PLAYER_ID"].isna().any():
            problems.append("Some PLAYER_ID values are blank/NA.")
        if df["PLAYER_ID"].duplicated().any():
            problems.append("Duplicate PLAYER_ID values found (should be unique per season).")

    n = df.shape[0]
    if n < 200 or n > 900:
        problems.append(f"Row count looks wrong: {n} rows (expected roughly 400–650).")

    if "Player" in df.columns:
        blank_pct = float((df["Player"].astype(str).str.strip() == "").mean())
        if blank_pct > 0.05:
            problems.append(f"Too many blank Player names: {blank_pct:.1%}")

    os.makedirs("reports", exist_ok=True)
    report = {
        "checked_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "staging_path": stage_path.replace("\\", "/"),
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "ok": len(problems) == 0,
        "problems": problems,
    }
    with open(os.path.join("reports", "stage_validation.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if problems:
        raise RuntimeError("Stage validation failed:\n- " + "\n- ".join(problems))

    print("Stage validation OK.")


if __name__ == "__main__":
    main()
