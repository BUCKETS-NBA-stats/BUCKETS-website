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

    # Required columns
    for col in ["Season", "SeasonType", "Player", "PLAYER_ID"]:
        if col not in df.columns:
            problems.append(f"Missing required column: {col}")

    # Metadata not blank
    if "Season" in df.columns and (df["Season"].astype(str).str.strip() == "").any():
        problems.append("Some Season values are blank.")
    if "SeasonType" in df.columns and (df["SeasonType"].astype(str).str.strip() == "").any():
        problems.append("Some SeasonType values are blank.")

    # Player-level key checks
    if "PLAYER_ID" in df.columns:
        if df["PLAYER_ID"].isna().any():
            problems.append("Some PLAYER_ID values are blank/NA.")
        if df["PLAYER_ID"].duplicated().any():
            problems.append("Duplicate PLAYER_ID values found (should be unique per season snapshot).")

    # Row count sanity
    n = int(df.shape[0])
    if n < 200 or n > 900:
        problems.append(f"Row count looks wrong: {n} rows (expected roughly 400–650).")

    # Ensure passing merge occurred (columns exist)
    passing_cols = [c for c in df.columns if c.startswith("nba_pass__")]
    if not passing_cols:
        problems.append("No nba_pass__* columns found. Passing data did not merge into staging.")

    # Missing secondary rows are allowed, but should be visible via carry_forward flags/report
    # If a player has all nba_pass__ cols missing, that's OK as long as we didn't lose the entire table.
    if passing_cols:
        missing_pass_count = int(df[passing_cols].isna().all(axis=1).sum())
        # If >90% missing, something is broken (likely wrong endpoint params)
        if missing_pass_count > 0.9 * n:
            problems.append(
                f"Passing columns exist but are missing for almost everyone ({missing_pass_count}/{n}). "
                f"This usually means team-level or error payload merged incorrectly."
            )

    # Player name sanity
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
