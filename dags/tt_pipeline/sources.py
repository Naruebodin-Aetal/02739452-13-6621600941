"""Fetchers for the three table tennis sources.

Every fetcher returns a list of plain dicts in the unified shape that
``transform.normalize`` expects. Player names and IDs are never returned:
the WTT ranking is reduced to one row per country before it leaves this module
(PDPA: no personal data is stored).
"""

from __future__ import annotations

import os
import urllib.robotparser
from collections import defaultdict
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup

USER_AGENT = os.environ.get(
    "SOURCE_USER_AGENT",
    "TableTennisExamPipeline/1.0 (student project; course 02739452)",
)
TIMEOUT = 30

WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_World_Table_Tennis_Championships_medalists"
OLYMPEDIA_URL = "https://www.olympedia.org/sports/TTE"
WTT_URL = "https://wttcmsapigateway-new.azure-api.net/ttu/Rankings/GetRankingIndividuals"

# Heading id of each per-event medal table on the Wikipedia page.
WIKIPEDIA_TABLES = {
    "Medal_table": "mens_singles",
    "Medal_table_2": "womens_singles",
    "Medal_table_3": "mens_doubles",
    "Medal_table_4": "womens_doubles",
    "Medal_table_5": "mixed_doubles",
}

WTT_TOP_N = 1000


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def ensure_allowed(url: str) -> None:
    """Raise if the site's robots.txt disallows fetching ``url``."""
    parts = urlsplit(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    response = _session().get(robots_url, timeout=TIMEOUT)
    parser = urllib.robotparser.RobotFileParser()
    # A missing robots.txt means everything is allowed.
    parser.parse(response.text.splitlines() if response.ok else [])
    if not parser.can_fetch(USER_AGENT, url):
        raise PermissionError(f"robots.txt disallows {url}")


def _get_html(url: str) -> BeautifulSoup:
    ensure_allowed(url)
    response = _session().get(url, timeout=TIMEOUT)
    response.raise_for_status()
    response.encoding = "utf-8"
    return BeautifulSoup(response.text, "html.parser")


def _medals(text: str) -> float | None:
    # Doubles pairs from two countries split a medal, so Wikipedia shows e.g. 11.5.
    try:
        return float(text.strip().replace(",", ""))
    except ValueError:
        return None


def scrape_wikipedia_world_championships() -> list[dict]:
    """World Championships medal tables, one table per individual event."""
    soup = _get_html(WIKIPEDIA_URL)
    rows: list[dict] = []
    for heading_id, event in WIKIPEDIA_TABLES.items():
        heading = soup.find(id=heading_id)
        if heading is None:
            raise ValueError(f"Wikipedia layout changed: #{heading_id} not found")
        table = heading.find_next("table")
        for tr in table.find_all("tr"):
            nation = tr.find("th", scope="row")
            if nation is None:
                continue
            name = nation.get_text(" ", strip=True)
            if name.lower().startswith("totals"):
                continue
            # Tied ranks share a rowspan cell, so read the medal counts from the end.
            numbers = [td.get_text(strip=True) for td in tr.find_all("td")][-4:]
            gold, silver, bronze, total = (_medals(n) for n in numbers)
            rows.append({
                "source": "wikipedia",
                "competition": "world_championships",
                "event": event,
                "country_name": name,
                "country_code": None,
                "gold": gold,
                "silver": silver,
                "bronze": bronze,
                "medal_total": total,
            })
    return rows


def scrape_olympedia_olympics() -> list[dict]:
    """All-time Olympic table tennis medal table by NOC (country level only)."""
    soup = _get_html(OLYMPEDIA_URL)
    heading = next(
        (h for h in soup.find_all("h2") if "medal table" in h.get_text().lower()),
        None,
    )
    if heading is None:
        raise ValueError("Olympedia layout changed: medal table heading not found")
    # The first table after the heading is the Olympic Games; the next is Youth Olympics.
    table = heading.find_next("table")
    rows: list[dict] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) != 6:
            continue
        name, code, gold, silver, bronze, total = cells
        rows.append({
            "source": "olympedia",
            "competition": "olympics",
            "event": "all_events",
            "country_name": name,
            "country_code": code,
            "gold": _medals(gold),
            "silver": _medals(silver),
            "bronze": _medals(bronze),
            "medal_total": _medals(total),
        })
    return rows


def fetch_wtt_ranking(sub_event: str) -> list[dict]:
    """Senior world ranking (``MS`` or ``WS``) aggregated to one row per country."""
    response = _session().get(
        WTT_URL,
        params={
            "CategoryCode": "SEN",
            "SubEventCode": sub_event,
            "StartRank": 1,
            "EndRank": WTT_TOP_N,
        },
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    players = response.json().get("Result") or []
    if not players:
        raise ValueError(f"WTT returned no ranking rows for {sub_event}")

    by_country: dict[str, dict] = defaultdict(lambda: {"players": 0, "best": None, "points": 0})
    names: dict[str, str] = {}
    week = f"{players[0]['RankingYear']}-W{players[0]['RankingWeek']}"
    for player in players:
        code = player["CountryCode"]
        names[code] = player["CountryName"]
        rank = int(player["RankingPosition"])
        stats = by_country[code]
        stats["players"] += 1
        stats["best"] = rank if stats["best"] is None else min(stats["best"], rank)
        stats["points"] += int(player.get("RankingPointsYTD") or 0)

    return [
        {
            "source": "wtt",
            "competition": "world_ranking",
            "event": sub_event.lower(),
            "country_name": names[code],
            "country_code": code,
            "ranked_players": stats["players"],
            "best_rank": stats["best"],
            "ranking_points": stats["points"],
            "ranking_week": week,
        }
        for code, stats in by_country.items()
    ]
