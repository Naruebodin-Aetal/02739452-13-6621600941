-- Idempotent schema; the DAG runs this at the start of every run.

CREATE TABLE IF NOT EXISTS countries (
    country_code TEXT PRIMARY KEY,
    country_name TEXT NOT NULL,
    sources      TEXT[] NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unified schema for all three sources. Medal columns are NUMERIC because
-- doubles pairs from two countries share a medal (e.g. 11.5).
CREATE TABLE IF NOT EXISTS country_stats (
    source         TEXT NOT NULL,
    competition    TEXT NOT NULL,
    event          TEXT NOT NULL,
    country_code   TEXT NOT NULL,
    country_name   TEXT NOT NULL,
    gold           NUMERIC(6, 1),
    silver         NUMERIC(6, 1),
    bronze         NUMERIC(6, 1),
    medal_total    NUMERIC(6, 1),
    ranked_players INTEGER,
    best_rank      INTEGER,
    ranking_points INTEGER,
    ranking_week   TEXT,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event, country_code)
);

CREATE TABLE IF NOT EXISTS quality_issues (
    source       TEXT NOT NULL,
    event        TEXT NOT NULL,
    country_name TEXT NOT NULL,
    issues       TEXT[] NOT NULL,
    raw_row      JSONB NOT NULL,
    run_id       TEXT NOT NULL,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, event, country_name)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id      TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
    rows_loaded INTEGER,
    bad_rows    INTEGER,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_runs (
    run_id   TEXT NOT NULL,
    task_id  TEXT NOT NULL,
    state    TEXT NOT NULL,
    rows     INTEGER,
    PRIMARY KEY (run_id, task_id)
);

-- One row per country with every source side by side.
CREATE OR REPLACE VIEW country_summary AS
SELECT
    c.country_code,
    c.country_name,
    array_length(c.sources, 1) AS source_count,
    COALESCE(SUM(s.gold)        FILTER (WHERE s.competition = 'world_championships'), 0) AS wtc_gold,
    COALESCE(SUM(s.medal_total) FILTER (WHERE s.competition = 'world_championships'), 0) AS wtc_medals,
    COALESCE(SUM(s.gold)        FILTER (WHERE s.competition = 'olympics'), 0)            AS olympic_gold,
    COALESCE(SUM(s.medal_total) FILTER (WHERE s.competition = 'olympics'), 0)            AS olympic_medals,
    COALESCE(SUM(s.ranked_players) FILTER (WHERE s.event = 'ms'), 0)                     AS men_ranked,
    COALESCE(SUM(s.ranked_players) FILTER (WHERE s.event = 'ws'), 0)                     AS women_ranked,
    MIN(s.best_rank)                                                                     AS best_rank
FROM countries c
JOIN country_stats s USING (country_code)
GROUP BY c.country_code, c.country_name, c.sources;
