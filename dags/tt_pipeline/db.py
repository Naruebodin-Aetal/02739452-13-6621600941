"""PostgreSQL helpers. Every write is an UPSERT, so re-running a DAG never adds rows."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from tt_pipeline.transform import ALL_FIELDS

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


def connect():
    return psycopg2.connect(
        host=os.environ["DATA_DB_HOST"],
        port=os.environ.get("DATA_DB_PORT", "5432"),
        dbname=os.environ["DATA_DB_NAME"],
        user=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
    )


def create_schema() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))


def upsert_country_stats(rows: list[dict]) -> int:
    columns = list(ALL_FIELDS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in ("source", "event", "country_code"))
    sql = f"""
        INSERT INTO country_stats ({", ".join(columns)}) VALUES %s
        ON CONFLICT (source, event, country_code)
        DO UPDATE SET {updates}, loaded_at = now()
    """
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, [tuple(row[c] for c in columns) for row in rows])
    return len(rows)


def upsert_countries(countries: list[dict]) -> None:
    sql = """
        INSERT INTO countries (country_code, country_name, sources) VALUES %s
        ON CONFLICT (country_code) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            -- keep sources seen in earlier runs if one source is down today
            sources = (SELECT array_agg(DISTINCT s ORDER BY s)
                       FROM unnest(countries.sources || EXCLUDED.sources) AS s),
            updated_at = now()
    """
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, [(c["country_code"], c["country_name"], c["sources"]) for c in countries])


def upsert_quality_issues(bad_rows: list[dict], run_id: str) -> None:
    sql = """
        INSERT INTO quality_issues (source, event, country_name, issues, raw_row, run_id) VALUES %s
        ON CONFLICT (source, event, country_name) DO UPDATE SET
            issues = EXCLUDED.issues, raw_row = EXCLUDED.raw_row,
            run_id = EXCLUDED.run_id, detected_at = now()
    """
    values = [
        (r["source"], r["event"], r["country_name"], r["issues"], json.dumps(r, default=str), run_id)
        for r in bad_rows
    ]
    with connect() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values)


def record_run(run_id: str, state: str, rows_loaded: int | None, bad_rows: int | None,
               source_states: list[tuple[str, str, int | None]]) -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (run_id, state, rows_loaded, bad_rows) VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET state = EXCLUDED.state, rows_loaded = EXCLUDED.rows_loaded,
                bad_rows = EXCLUDED.bad_rows, finished_at = now()
            """,
            (run_id, state, rows_loaded, bad_rows),
        )
        execute_values(
            cur,
            """
            INSERT INTO source_runs (run_id, task_id, state, rows) VALUES %s
            ON CONFLICT (run_id, task_id) DO UPDATE SET state = EXCLUDED.state, rows = EXCLUDED.rows
            """,
            [(run_id, task_id, task_state, rows) for task_id, task_state, rows in source_states],
        )


def table_counts() -> dict[str, int]:
    with connect() as conn, conn.cursor() as cur:
        counts = {}
        for table in ("country_stats", "countries", "quality_issues"):
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        cur.execute("""
            SELECT count(*) FROM (
                SELECT 1 FROM country_stats GROUP BY source, event, country_code HAVING count(*) > 1
            ) d
        """)
        counts["duplicate_keys"] = cur.fetchone()[0]
    return counts
