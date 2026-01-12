from __future__ import annotations

import pandas as pd


class NBADataPolicyError(RuntimeError):
    pass


def validate_player_totals_df(df: pd.DataFrame, source_id: str, min_rows: int = 150) -> None:
    """
    Hard rules:
      - Must have PLAYER_ID (player-level)
      - Must have a reasonable number of rows (sanity)
    Notes:
      - We keep min_rows conservative because early-season can be smaller.
    """
    if not isinstance(df, pd.DataFrame):
        raise NBADataPolicyError(f"{source_id}: expected DataFrame, got {type(df)}")

    if "PLAYER_ID" not in df.columns:
        raise NBADataPolicyError(
            f"{source_id}: missing PLAYER_ID -> this is not player-level output. "
            f"Fix the endpoint params to force player-level."
        )

    n = int(df.shape[0])
    if n < min_rows:
        raise NBADataPolicyError(
            f"{source_id}: too few rows ({n}). This often indicates team-level data, "
            f"a filtered query, or an endpoint error payload."
        )
