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


def validate_playtypes_df(df: pd.DataFrame, source_id: str, min_rows: int = 500) -> None:
    """
    Hard rules for the concatenated play-type DataFrame (long format):
      - Must have PLAYER_ID (player-level)
      - Must have PLAY_TYPE (confirms it's the long-format combined output)
      - Must have a reasonable number of rows (11 play types × ~45+ players each)
    """
    if not isinstance(df, pd.DataFrame):
        raise NBADataPolicyError(f"{source_id}: expected DataFrame, got {type(df)}")

    if "PLAYER_ID" not in df.columns:
        raise NBADataPolicyError(
            f"{source_id}: missing PLAYER_ID -> this is not player-level output. "
            f"Fix player_or_team_abbreviation='P' in the play-type ingestion."
        )

    if "PLAY_TYPE" not in df.columns:
        raise NBADataPolicyError(
            f"{source_id}: missing PLAY_TYPE column -> DataFrame was not assembled correctly."
        )

    n = int(df.shape[0])
    if n < min_rows:
        raise NBADataPolicyError(
            f"{source_id}: too few rows ({n}). Expected 11 play types × player rows. "
            f"This often indicates team-level data, a filtered query, or an endpoint error payload."
        )


def validate_pbpstats_df(df: pd.DataFrame, source_id: str, min_rows: int = 150) -> None:
    """
    Hard rules for the PBPStats totals DataFrame:
      - Must have EntityId (PBPStats player identifier)
      - Must have Name (used for downstream name matching to PLAYER_ID)
      - Must have a reasonable number of rows (full season is 400–600 players)
    Notes:
      - min_rows is conservative; the Stat= leaderboard endpoints return only ~16 rows.
        A ValidationError here usually means the wrong API endpoint or params were used.
    """
    if not isinstance(df, pd.DataFrame):
        raise NBADataPolicyError(f"{source_id}: expected DataFrame, got {type(df)}")

    if "EntityId" not in df.columns:
        raise NBADataPolicyError(
            f"{source_id}: missing EntityId column. "
            f"Ensure Type=Player is set and the correct endpoint is used."
        )

    if "Name" not in df.columns:
        raise NBADataPolicyError(
            f"{source_id}: missing Name column. Cannot perform downstream name matching without it."
        )

    n = int(df.shape[0])
    if n < min_rows:
        raise NBADataPolicyError(
            f"{source_id}: too few rows ({n}). Full season should have 400–600 players. "
            f"If ~16 rows, the Stat= leaderboard endpoint was used instead of the full totals endpoint. "
            f"Ensure no Stat= parameter is included in the request."
        )
