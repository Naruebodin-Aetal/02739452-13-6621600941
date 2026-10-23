"""Unify rows from all sources, resolve countries across sources, and check quality."""

from __future__ import annotations

# Wikipedia uses country names; Olympedia and WTT already give 3-letter codes.
# Historical teams keep their own IOC/ITTF codes instead of being merged into
# today's countries.
WIKIPEDIA_COUNTRY_CODES = {
    "Austria": "AUT",
    "Belarus": "BLR",
    "Belgium": "BEL",
    "Brazil": "BRA",
    "China": "CHN",
    "Chinese Taipei": "TPE",
    "Croatia": "CRO",
    "Czechoslovakia": "TCH",
    "Denmark": "DEN",
    "East Germany": "GDR",
    "Egypt": "EGY",
    "England": "ENG",
    "France": "FRA",
    "Germany": "GER",
    "Greece": "GRE",
    "Hong Kong": "HKG",
    "Hungary": "HUN",
    "India": "IND",
    "Japan": "JPN",
    "Korea": "COR",  # unified Korea team, 1991
    "Luxembourg": "LUX",
    "Netherlands": "NED",
    "North Korea": "PRK",
    "Poland": "POL",
    "Portugal": "POR",
    "Romania": "ROU",
    "Scotland": "SCO",
    "Singapore": "SGP",
    "South Korea": "KOR",
    "Soviet Union": "URS",
    "Spain": "ESP",
    "Sweden": "SWE",
    "United States": "USA",
    "Wales": "WAL",
    "West Germany": "FRG",
    "Yugoslavia": "YUG",
}

MEDAL_FIELDS = ("gold", "silver", "bronze", "medal_total")
RANKING_FIELDS = ("ranked_players", "best_rank", "ranking_points")
ALL_FIELDS = (
    "source", "competition", "event", "country_code", "country_name",
    *MEDAL_FIELDS, *RANKING_FIELDS, "ranking_week",
)

# When several sources name the same country, show the name from the first one.
NAME_PREFERENCE = ("wtt", "wikipedia", "olympedia")


def normalize(rows: list[dict]) -> list[dict]:
    """Fill missing fields, map names to codes, and drop exact duplicate keys."""
    unified: dict[tuple, dict] = {}
    for raw in rows:
        row = {field: raw.get(field) for field in ALL_FIELDS}
        row["country_name"] = (row["country_name"] or "").strip()
        if not row["country_code"]:
            row["country_code"] = WIKIPEDIA_COUNTRY_CODES.get(row["country_name"])
        if row["country_code"]:
            row["country_code"] = row["country_code"].strip().upper()
        key = (row["source"], row["event"], row["country_code"] or row["country_name"])
        unified[key] = row
    return list(unified.values())


def find_issues(row: dict) -> list[str]:
    issues = []
    if not row["country_code"]:
        issues.append("unmapped_country")
    if row["competition"] == "world_ranking":
        if not row["ranked_players"] or row["ranked_players"] < 1:
            issues.append("no_ranked_players")
        if row["best_rank"] is None or row["best_rank"] < 1:
            issues.append("invalid_rank")
    else:
        if any(row[f] is None or row[f] < 0 for f in MEDAL_FIELDS):
            issues.append("invalid_medal_count")
        elif row["gold"] + row["silver"] + row["bronze"] != row["medal_total"]:
            issues.append("medal_total_mismatch")
    return issues


def split_by_quality(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (valid rows, bad rows with an ``issues`` list)."""
    valid, bad = [], []
    for row in rows:
        issues = find_issues(row)
        if issues:
            bad.append({**row, "issues": issues})
        else:
            valid.append(row)
    return valid, bad


def build_countries(rows: list[dict]) -> list[dict]:
    """One row per country code across all sources (cross-source deduplication)."""
    countries: dict[str, dict] = {}
    rank = {source: i for i, source in enumerate(NAME_PREFERENCE)}
    for row in sorted(rows, key=lambda r: rank.get(r["source"], len(rank))):
        country = countries.setdefault(
            row["country_code"],
            {"country_code": row["country_code"], "country_name": row["country_name"], "sources": []},
        )
        if row["source"] not in country["sources"]:
            country["sources"].append(row["source"])
    return list(countries.values())
