from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd


# ----------------------------
# Name normalization utilities
# ----------------------------
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


def clean_display_name(s: str) -> str:
    # ASCII display (your preference)
    return strip_accents(repair_mojibake(s))


def make_source_key(s: str) -> str:
    # key used for alias lookup
    s = clean_display_name(s).lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def load_canonical_name_map(aliases_path: Path) -> dict[str, str]:
    """
    Returns: {source_key -> display_name}
    """
    if not aliases_path.exists():
        raise SystemExit(f"Missing aliases file: {aliases_path}\n"
                         f"Run: python scripts/build_player_aliases.py")

    aliases = pd.read_csv(aliases_path, dtype=str, keep_default_na=False, na_filter=False)

    required = {"source_key", "display_name"}
    if not required.issubset(set(aliases.columns)):
        raise SystemExit(f"{aliases_path} is missing required columns: {sorted(required)}")

    # last one wins if duplicates sneak in; collisions should be prevented by build_player_aliases.py
    m = {}
    for _, r in aliases.iterrows():
        k = (r.get("source_key") or "").strip()
        v = clean_display_name(r.get("display_name") or "")
        if k and v:
            m[k] = v
    return m


# ----------------------------
# Master build
# ----------------------------
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--season-dir", default="assets/data/season", help="Folder containing per-season CSVs")
    p.add_argument("--output", default="assets/data/league-table-combined.csv", help="Output combined CSV path")
    p.add_argument("--aliases", default="mappings/player_aliases.csv", help="Alias mapping CSV")
    p.add_argument("--keys", default="Player,Year,Season type,Tm", help="Comma-separated key columns for de-dupe")
    args = p.parse_args()

    season_dir = Path(args.season_dir)
    output_path = Path(args.output)
    aliases_path = Path(args.aliases)

    files = sorted(season_dir.glob("*.csv"))
    if not files:
        raise SystemExit(
            f"No season CSVs found in: {season_dir}\n"
            f"Add at least one .csv like: {season_dir / '2024-25.csv'}"
        )

    canonical_map = load_canonical_name_map(aliases_path)
    keys = [k.strip() for k in args.keys.split(",") if k.strip()]

    dfs: list[pd.DataFrame] = []
    unmapped_rows: list[dict] = []

    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

    for f in files:
        # Read robustly, keep blanks as blanks (no NaN)
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
            raise SystemExit(f"Could not decode CSV file {f}. Last error: {last_err}")

        # Drop rows that are completely blank across all columns (Google Sheets export artifact)
        blank_row_mask = df.apply(lambda col: col.astype(str).str.strip().eq("")).all(axis=1)
        df = df.loc[~blank_row_mask].copy()

        # Enforce invariants
        for col in ["Year", "Season type"]:
            if col in df.columns:
                bad = df[col].astype(str).str.strip().eq("")
                if bad.any():
                    examples = df.loc[bad, ["Player", "Year", "Season type", "Tm"]].head(10)
                    raise SystemExit(
                        f"{f.name}: Found rows where '{col}' is blank (should never happen). "
                        f"First examples:\n{examples.to_string(index=False)}"
                    )

        if "Player" not in df.columns:
            raise SystemExit(f"{f.name}: Missing required column 'Player'")

        bad_player = df["Player"].astype(str).str.strip().eq("")
        if bad_player.any():
            examples = df.loc[bad_player, ["Player", "Year", "Season type", "Tm"]].head(10)
            raise SystemExit(
                f"{f.name}: Found non-blank rows where 'Player' is blank. "
                f"First examples:\n{examples.to_string(index=False)}"
            )

        # Clean mojibake/accents in Player, then canonicalize via mappings/player_aliases.csv
        df["Player"] = df["Player"].astype(str).map(clean_display_name)
        src_keys = df["Player"].map(make_source_key)

        mapped = src_keys.map(canonical_map)

        # record any unmapped names (we keep them, but you get a report)
        is_unmapped = mapped.isna() & src_keys.astype(str).str.strip().ne("")
        if is_unmapped.any():
            for name, sk in zip(df.loc[is_unmapped, "Player"], src_keys.loc[is_unmapped]):
                unmapped_rows.append({
                    "file": f.name,
                    "player": name,
                    "source_key": sk,
                })

        df["Player"] = mapped.fillna(df["Player"])

        # Optional uniqueness check within each season file
        if keys and all(k in df.columns for k in keys):
            if df.duplicated(subset=keys).any():
                dups = df[df.duplicated(subset=keys, keep=False)].sort_values(keys).head(10)
                raise SystemExit(
                    f"Duplicate key rows found in {f.name} using keys={keys}.\n"
                    f"First few duplicates:\n{dups[keys].to_string(index=False)}"
                )

        dfs.append(df)

    master = pd.concat(dfs, ignore_index=True)

    # De-dupe across all seasons
    if keys and all(k in master.columns for k in keys):
        master = master.drop_duplicates(subset=keys, keep="last").copy()

    # Stable sort
    sort_cols = [c for c in ["Year", "Season type", "Player", "Tm"] if c in master.columns]
    if sort_cols:
        master = master.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # Write reports if needed
    if unmapped_rows:
        reports_dir = Path("reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(unmapped_rows).drop_duplicates().to_csv(
            reports_dir / "unmapped_player_names.csv",
            index=False
        )
        print(f"WARNING: {len(unmapped_rows)} unmapped player name(s). See reports/unmapped_player_names.csv")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(output_path, index=False, na_rep="")

    print(f"Read {len(files)} season file(s) from: {season_dir}")
    print(f"Wrote combined CSV: {output_path}")
    print(f"Rows: {len(master):,} | Cols: {master.shape[1]}")


if __name__ == "__main__":
    main()
