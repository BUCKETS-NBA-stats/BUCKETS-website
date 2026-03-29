import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Callable, TypeVar

import requests
from bs4 import BeautifulSoup

T = TypeVar("T")

CTG_BASE = "https://cleaningtheglass.com"
CTG_LOGIN_URL = f"{CTG_BASE}/wp-login.php"
CTG_FOURFACTORS_URL = f"{CTG_BASE}/stats/league/fourfactors"
CTG_CONTEXT_URL = f"{CTG_BASE}/stats/league/context"


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


def ctg_season_param(season: str) -> int:
    """Convert pipeline season string to CTG season integer: "2025-26" → 2025."""
    return int(season[:4])


def ctg_season_type_param(season_type: str) -> str:
    """Convert pipeline season type to CTG seasontype param."""
    return "regseason" if season_type == "Regular Season" else "playoffs"


def ctg_login(session: requests.Session, email: str, password: str) -> None:
    """Authenticate with Cleaning the Glass (WordPress login)."""
    # WordPress requires the testcookie to be present before login POST
    session.get(CTG_LOGIN_URL, timeout=30)
    payload = {
        "log": email,
        "pwd": password,
        "wp-submit": "Log In",
        "redirect_to": CTG_FOURFACTORS_URL,
        "testcookie": "1",
    }
    resp = session.post(CTG_LOGIN_URL, data=payload, timeout=30, allow_redirects=True)
    resp.raise_for_status()
    # On success WordPress redirects away from wp-login.php; on failure it stays there
    if "wp-login.php" in resp.url or "loggedout" in resp.url:
        raise RuntimeError(
            "CTG login failed — redirected back to login page. "
            "Check CTG_EMAIL and CTG_PASSWORD environment variables."
        )


def _fetch_page(session: requests.Session, url: str, params: dict) -> str:
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_stat_cells(table_tag, label: str) -> list:
    """Return the td.stat.value cells from the league_averages thead row."""
    thead = table_tag.find("thead")
    if not thead:
        raise ValueError(f"No <thead> found in table '{label}'")
    row = thead.find("tr", class_="league_averages")
    if not row:
        raise ValueError(f"No tr.league_averages found in thead of table '{label}'")
    cells = row.select("td.stat.value")
    if not cells:
        raise ValueError(f"No td.stat.value cells found in league_averages row of '{label}'")
    return cells


def _cell_float(cells: list, idx: int, label: str) -> float:
    """Extract a float from a stat cell, stripping a trailing % if present."""
    try:
        raw = cells[idx].get_text(strip=True)
    except IndexError:
        raise ValueError(
            f"Cell index {idx} out of range ({len(cells)} cells total) in table '{label}'"
        )
    return float(raw.rstrip("%"))


def fetch_fourfactors(session: requests.Session, season: str, season_type: str) -> dict:
    """Fetch the fourfactors page and extract TOV%."""
    params = {
        "season": ctg_season_param(season),
        "seasontype": ctg_season_type_param(season_type),
    }

    def _call() -> dict:
        html = _fetch_page(session, CTG_FOURFACTORS_URL, params)
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="league_four_factors")
        if not table:
            raise ValueError(
                "Could not find table#league_four_factors on the fourfactors page. "
                "Page structure may have changed."
            )
        cells = _parse_stat_cells(table, "league_four_factors")
        # Index 5 = TOV% in the league_averages row
        return {"tov_pct": _cell_float(cells, 5, "league_four_factors")}

    return with_retries(_call, "CTG fourfactors", attempts=5)


def fetch_context(session: requests.Session, season: str, season_type: str) -> dict:
    """Fetch the context page and extract HC Pts/Play, HC OREB%, and Putbacks Pts/Play."""
    params = {
        "season": ctg_season_param(season),
        "seasontype": ctg_season_type_param(season_type),
    }

    def _call() -> dict:
        html = _fetch_page(session, CTG_CONTEXT_URL, params)
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="league_offense_halfcourt_and_putbacks")
        if not table:
            raise ValueError(
                "Could not find table#league_offense_halfcourt_and_putbacks on the context page. "
                "Page structure may have changed."
            )
        cells = _parse_stat_cells(table, "league_offense_halfcourt_and_putbacks")
        # Index 1 = HC Pts/Play, 2 = HC OREB%, 6 = Putbacks Pts/Play
        return {
            "hc_pts_per_play": _cell_float(cells, 1, "league_offense_halfcourt_and_putbacks"),
            "hc_oreb_pct":     _cell_float(cells, 2, "league_offense_halfcourt_and_putbacks"),
            "pb_pts_per_play": _cell_float(cells, 6, "league_offense_halfcourt_and_putbacks"),
        }

    return with_retries(_call, "CTG context", attempts=5)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True, help='e.g. "2025-26"')
    parser.add_argument("--season-type", required=True, help='e.g. "Regular Season"')
    args = parser.parse_args()

    season = args.season
    season_type = args.season_type
    season_type_slug = "regular" if season_type == "Regular Season" else "playoffs"

    email = os.environ.get("CTG_EMAIL", "")
    password = os.environ.get("CTG_PASSWORD", "")

    out_dir = os.path.join("assets", "data", "raw", season, season_type_slug)
    ensure_dir(out_dir)
    ensure_dir("reports")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    if email and password:
        try:
            ctg_login(session, email, password)
            print("[OK] CTG login successful")
        except Exception as e:
            print(
                f"[WARN] CTG login failed: {e}. "
                "Proceeding without auth (league averages are publicly accessible)."
            )
    else:
        print("[INFO] CTG_EMAIL/CTG_PASSWORD not set — fetching without authentication.")

    ff_vals = fetch_fourfactors(session, season, season_type)
    ctx_vals = fetch_context(session, season, season_type)

    values = {**ff_vals, **ctx_vals}

    out_path = os.path.join(out_dir, "ctg_league_averages.json")
    payload = {
        "generated_at_utc": utc_now_iso(),
        "season": season,
        "season_type": season_type,
        "source": "cleaningtheglass.com",
        "ctg_season_param": ctg_season_param(season),
        "ctg_season_type_param": ctg_season_type_param(season_type),
        "values": values,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"[OK] Wrote CTG league averages: {out_path}")
    print(f"[OK] Values: {values}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
