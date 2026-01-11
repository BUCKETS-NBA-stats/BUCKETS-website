from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import re
import unicodedata



def load_contract_json(contract_path: Path) -> dict | None:
    if not contract_path or not contract_path.exists():
        return None
    try:
        return json.loads(contract_path.read_text(encoding="utf-8"))
    except Exception:
        # Try UTF-8 with BOM
        return json.loads(contract_path.read_text(encoding="utf-8-sig"))


def expected_columns_from_contract(contract: dict | None) -> list[str] | None:
    """
    Supports a few shapes:
    - {"columns": [...]}
    - {"column_names": [...]}
    - Starter-kit shape: {"base_columns":[...], "metric_columns":[...], "availability_columns":[{"name":...},...]}
    - {"column_details":[{"index":1,"name":"..."},...]}
    """
    if not contract:
        return None

    if isinstance(contract.get("columns"), list):
        return [str(c) for c in contract["columns"]]

    if isinstance(contract.get("column_names"), list):
        return [str(c) for c in contract["column_names"]]

    base = contract.get("base_columns") if isinstance(contract.get("base_columns"), list) else []
    metric = contract.get("metric_columns") if isinstance(contract.get("metric_columns"), list) else []

    avail_cols: list[str] = []
    avail = contract.get("availability_columns")
    if isinstance(avail, list):
        for item in avail:
            if isinstance(item, str):
                avail_cols.append(item)
            elif isinstance(item, dict) and "name" in item:
                avail_cols.append(str(item["name"]))

    if (base or metric or avail_cols):
        return [str(c) for c in (base + metric + avail_cols)]

    details = contract.get("column_details")
    if isinstance(details, list):
        try:
            details_sorted = sorted(details, key=lambda x: x.get("index", 0))
            names = [d.get("name") for d in details_sorted if isinstance(d, dict) and d.get("name")]
            if names:
                return [str(n) for n in names]
        except Exception:
            pass

    return None


def pick_paths(args_season_dir: str | None, args_output: str | None, contract: dict | None) -> tuple[Path, Path]:
    # Allow your edited contract.json to supply defaults (season_folder/master_path)
    season_dir = args_season_dir
    output = args_output

    if contract:
        if not season_dir and isinstance(contract.get("season_folder"), str):
            season_dir = contract["season_folder"]
        if not output and isinstance(contract.get("master_path"), str):
            output = contract["master_path"]

    # Final fallbacks (original starter-kit defaults)
    if not season_dir:
        season_dir = "site/data/season"
    if not output:
        output = "site/data/master.csv"

    return Path(season_dir), Path(output)


def enforce_same_columns(df: pd.DataFrame, expected_cols: list[str] | None, reference_cols: list[str]) -> None:
    cols = list(df.columns)

    if expected_cols is not None:
        if cols != expected_cols:
            # Provide a helpful diff summary
            missing = [c for c in expected_cols if c not in cols]
            extra = [c for c in cols if c not in expected_cols]
            raise SystemExit(
                "Column contract mismatch.\n"
                f"- Missing columns (in contract but not file): {missing[:20]}{' ...' if len(missing)>20 else ''}\n"
                f"- Extra columns (in file but not contract): {extra[:20]}{' ...' if len(extra)>20 else ''}\n"
                f"- Contract column count: {len(expected_cols)}\n"
                f"- File column count: {len(cols)}\n"
                "Tip: Ensure the season CSV was exported from the same output tab schema."
            )
    else:
        if cols != reference_cols:
            missing = [c for c in reference_cols if c not in cols]
            extra = [c for c in cols if c not in reference_cols]
            raise SystemExit(
                "Season files do not have identical columns.\n"
                f"- Missing vs first file: {missing[:20]}{' ...' if len(missing)>20 else ''}\n"
                f"- Extra vs first file: {extra[:20]}{' ...' if len(extra)>20 else ''}\n"
                f"- First file columns: {len(reference_cols)} | This file columns: {len(cols)}"
            )

def repair_mojibake(s: str) -> str:
    """
    Fix common UTF-8->cp1252/latin1 mojibake, e.g. "JokiÄ‡" -> "Jokić".
    """
    if not isinstance(s, str):
        return s

    # Heuristic: if it contains typical mojibake markers, try to repair
    if any(ch in s for ch in ("Ã", "Ä", "Â", "â", "€", "œ", "�")):
        for enc in ("latin1", "cp1252"):
            try:
                return s.encode(enc).decode("utf-8")
            except UnicodeError:
                pass
    return s


def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s

    # Decompose accented characters into base + combining mark
    s_norm = unicodedata.normalize("NFKD", s)

    # Drop combining marks
    s_no = "".join(ch for ch in s_norm if not unicodedata.combining(ch))

    # Handle a few common letters that don't always decompose as expected
    s_no = (
        s_no.replace("Ł", "L").replace("ł", "l")
            .replace("Đ", "D").replace("đ", "d")
            .replace("Ø", "O").replace("ø", "o")
            .replace("ß", "ss")
    )
    return s_no


def clean_player_name(s: str) -> str:
    s = repair_mojibake(s)
    s = strip_accents(s)

    # Optional: standardize punctuation/spaces
    s = s.replace(".", "")                 # "R.J." -> "RJ" (optional)
    s = re.sub(r"\s+", " ", s).strip()     # collapse whitespace
    return s


def build_master(season_dir: Path, output_path: Path, contract_path: Path | None, keys: list[str]) -> None:
    contract = load_contract_json(contract_path) if contract_path else None
    expected_cols = expected_columns_from_contract(contract)

    files = sorted(season_dir.glob("*.csv"))
    if not files:
        raise SystemExit(
            f"No season CSVs found in: {season_dir}\n"
            f"Add at least one .csv like: {season_dir / '2024-25.csv'}"
        )

    dfs: list[pd.DataFrame] = []
    reference_cols: list[str] | None = None

    for f in files:
        # Try common encodings (Excel often produces cp1252 on Windows)
        encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]

        last_err = None
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(
                    f,
                    encoding=enc,
                    dtype=str,              # keep everything as strings (Phase 0: no math needed)
                    keep_default_na=False,  # don't convert blanks/NA-like strings into NaN
                    na_filter=False,        # treat empty fields as empty strings, not NaN
                )

                break
            except UnicodeDecodeError as e:
                last_err = e
        else:
            raise SystemExit(f"Could not decode CSV file {f}. Last error: {last_err}")

        if "Player" in df.columns:
            df["Player"] = df["Player"].astype(str).map(clean_player_name)

        # Drop rows that are completely blank across all columns (common from Google Sheets exports)
        blank_row_mask = df.apply(lambda col: col.astype(str).str.strip().eq("")).all(axis=1)
        df = df.loc[~blank_row_mask].copy()

        # Enforce expected invariants
        for col in ["Year", "Season type"]:
            if col in df.columns:
                bad = df[col].astype(str).str.strip().eq("")
                if bad.any():
                    examples = df.loc[bad, ["Player", "Year", "Season type", "Tm"]].head(10)
                    raise SystemExit(
                        f"{f.name}: Found rows where '{col}' is blank (should never happen). "
                        f"First examples:\n{examples.to_string(index=False)}"
                    )

        if "Player" in df.columns:
            bad_player = df["Player"].astype(str).str.strip().eq("")
            if bad_player.any():
                examples = df.loc[bad_player, ["Player", "Year", "Season type", "Tm"]].head(10)
                raise SystemExit(
                    f"{f.name}: Found non-blank rows where 'Player' is blank. "
                    f"First examples:\n{examples.to_string(index=False)}"
                )

        if reference_cols is None:
            reference_cols = list(df.columns)

        enforce_same_columns(df, expected_cols, reference_cols)

        # Uniqueness check (best-effort)
        if keys and all(k in df.columns for k in keys):
            if df.duplicated(subset=keys).any():
                dups = df[df.duplicated(subset=keys, keep=False)].sort_values(keys).head(10)
                raise SystemExit(
                    f"Duplicate key rows found in {f} using keys={keys}.\n"
                    f"First few duplicates:\n{dups[keys].to_string(index=False)}"
                )

        dfs.append(df)

    master = pd.concat(dfs, ignore_index=True)

    # De-dupe across all seasons folder (best-effort)
    if keys and all(k in master.columns for k in keys):
        master = master.drop_duplicates(subset=keys, keep="last").copy()

    # Stable sort if columns exist
    sort_cols = [c for c in ["Year", "Season type", "Player", "Tm"] if c in master.columns]
    if sort_cols:
        master = master.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(output_path, index=False)

    print(f"Read {len(files)} season file(s) from: {season_dir}")
    print(f"Wrote combined CSV: {output_path}")
    print(f"Rows: {len(master):,} | Cols: {master.shape[1]}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--season-dir", default=None, help="Folder containing per-season CSVs (e.g. assets/data/season)")
    p.add_argument("--output", default=None, help="Output combined CSV path (e.g. assets/data/league-table-combined.csv)")
    p.add_argument("--contract", default=None, help="Optional contract JSON to enforce exact column order")
    p.add_argument("--keys", default="Player,Year,Season type,Tm", help="Comma-separated key columns for de-dupe/uniqueness")
    args = p.parse_args()

    contract_path = Path(args.contract) if args.contract else None
    contract_json = load_contract_json(contract_path) if contract_path else None

    season_dir, output_path = pick_paths(args.season_dir, args.output, contract_json)

    keys = [k.strip() for k in args.keys.split(",") if k.strip()]
    build_master(season_dir=season_dir, output_path=output_path, contract_path=contract_path, keys=keys)


if __name__ == "__main__":
    main()
