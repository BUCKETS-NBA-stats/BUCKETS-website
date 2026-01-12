from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd


def repair_mojibake(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if any(ch in s for ch in ("Ã", "Ä", "Â", "â", "€", "œ", "�", "Å")):
        for enc in ("latin1", "cp1252"):
            try:
                return s.encode(enc).decode("utf-8")
            except UnicodeError:
                pass
    return s


def strip_accents(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s_norm = unicodedata.normalize("NFKD", s)
    s_no = "".join(ch for ch in s_norm if not unicodedata.combining(ch))
    return (
        s_no.replace("Ł", "L").replace("ł", "l")
            .replace("Đ", "D").replace("đ", "d")
            .replace("Ø", "O").replace("ø", "o")
            .replace("ß", "ss")
    )


def clean_display(s: str) -> str:
    # Keep your “site display name” accentless & mojibake-proof
    return strip_accents(repair_mojibake(s))


def make_key(s: str) -> str:
    # Key used for matching across sources (lower + alnum only)
    s = clean_display(s).lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", default="mappings/name_fixer_inputs", help="Folder of per-season name fixer CSVs")
    p.add_argument("--out-dir", default="mappings", help="Where to write players.csv and player_aliases.csv")
    args = p.parse_args()

    input_dir = Path(args.inputs)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise SystemExit(f"No input CSVs found in {input_dir}")

    # Master column (required)
    master_col = "NBA Names"

    # Optional source columns. Include whatever exists per file.
    # (You can add more here later if you used other sources.)
    source_columns = [
        ("bbi", "BBI Names"),
        ("pbp", "PBP stats names"),
        ("syn", "Synergy names"),
    ]

    alias_rows = []

    for f in files:
        encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

        last_err = None
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(
                    f,
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                )
                break
            except UnicodeDecodeError as e:
                last_err = e
        else:
            raise SystemExit(f"Could not decode Name Fixer CSV {f.name}. Last error: {last_err}")


        if master_col not in df.columns:
            raise SystemExit(f"{f.name} is missing required master column: '{master_col}'")

        # Filter to only the sources that actually exist in THIS file
        active_sources = [(code, col) for (code, col) in source_columns if col in df.columns]

        for _, r in df.iterrows():
            nba_disp = clean_display(r.get(master_col, ""))
            if not nba_disp:
                continue

            player_key = make_key(nba_disp)
            display_name = nba_disp

            # Always include the NBA name as an alias too
            alias_rows.append({
                "player_key": player_key,
                "display_name": display_name,
                "source": "nba",
                "source_name": nba_disp,
                "source_key": make_key(nba_disp),
            })

            for source, col in active_sources:
                raw = (r.get(col, "") or "").strip()
                if not raw:
                    continue
                src_disp = clean_display(raw)
                alias_rows.append({
                    "player_key": player_key,
                    "display_name": display_name,
                    "source": source,
                    "source_name": src_disp,
                    "source_key": make_key(src_disp),
                })

    aliases = (
        pd.DataFrame(alias_rows)
        .drop_duplicates(subset=["source", "source_key"])
        .reset_index(drop=True)
    )

    # Collision check: same (source, source_key) mapping to >1 player_key
    collisions = (
        aliases.groupby(["source", "source_key"])["player_key"]
        .nunique()
        .reset_index(name="player_key_count")
    )
    collisions = collisions[collisions["player_key_count"] > 1]
    if len(collisions) > 0:
        reports = Path("reports")
        reports.mkdir(parents=True, exist_ok=True)
        collisions.to_csv(reports / "alias_collisions.csv", index=False)
        raise SystemExit("Alias collisions found. See reports/alias_collisions.csv")

    players = (
        aliases[["player_key", "display_name"]]
        .drop_duplicates()
        .sort_values("display_name")
        .reset_index(drop=True)
    )

    players.to_csv(out_dir / "players.csv", index=False)
    aliases.to_csv(out_dir / "player_aliases.csv", index=False)

    print(f"Input files: {len(files)}")
    print(f"Built players: {len(players):,}")
    print(f"Built aliases: {len(aliases):,}")
    print(f"Wrote: {out_dir/'players.csv'}")
    print(f"Wrote: {out_dir/'player_aliases.csv'}")


if __name__ == "__main__":
    main()
