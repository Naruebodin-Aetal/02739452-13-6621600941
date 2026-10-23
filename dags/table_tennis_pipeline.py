"""Table tennis strength by country: World Championships, Olympics and WTT ranking.

create_tables
  -> extract (TaskGroup, runs in parallel)
       scrape_wikipedia_world_championships   web scraping
       scrape_olympedia_olympics              web scraping
       fetch_wtt_ranking[MS, WS]              API, Dynamic Task Mapping
  -> merge_sources        runs even if some sources failed (ALL_DONE)
  -> check_quality        branch: load_postgres, or quarantine_bad_rows first
  -> load_postgres        UPSERT into country_stats and countries
  -> validate_database
  -> record_run           writes the pipeline status page data (ALL_DONE)
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from tt_pipeline import db, sources, transform

EXTRACT_TASKS = (
    "extract.scrape_wikipedia_world_championships",
    "extract.scrape_olympedia_olympics",
    "extract.fetch_wtt_ranking",
)
MIN_ROWS = 100


@dag(
    dag_id="table_tennis_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["exam", "table-tennis"],
)
def table_tennis_pipeline():
    @task
    def create_tables():
        db.create_schema()

    @task_group(group_id="extract")
    def extract():
        @task
        def scrape_wikipedia_world_championships():
            return sources.scrape_wikipedia_world_championships()

        @task
        def scrape_olympedia_olympics():
            return sources.scrape_olympedia_olympics()

        @task
        def fetch_wtt_ranking(sub_event: str):
            return sources.fetch_wtt_ranking(sub_event)

        scrape_wikipedia_world_championships()
        scrape_olympedia_olympics()
        fetch_wtt_ranking.expand(sub_event=["MS", "WS"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge_sources(ti=None):
        rows: list[dict] = []
        for task_id in EXTRACT_TASKS:
            pulled = ti.xcom_pull(task_ids=task_id)
            # A mapped task returns one value per map index; failed ones are None.
            batches = pulled if task_id.endswith("fetch_wtt_ranking") else [pulled]
            for batch in batches or []:
                rows.extend(batch or [])
        if not rows:
            raise AirflowFailException("Every source failed; nothing to merge")
        valid, bad = transform.split_by_quality(transform.normalize(rows))
        print(f"merged {len(rows)} rows -> {len(valid)} valid, {len(bad)} bad")
        return {"valid": valid, "bad": bad}

    @task.branch
    def check_quality(merged: dict):
        if merged["bad"]:
            print(f"{len(merged['bad'])} rows failed quality checks")
            return "quarantine_bad_rows"
        return "load_postgres"

    @task
    def quarantine_bad_rows(merged: dict, run_id=None):
        db.upsert_quality_issues(merged["bad"], run_id)

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_postgres(merged: dict):
        valid = merged["valid"]
        db.upsert_countries(transform.build_countries(valid))
        return db.upsert_country_stats(valid)

    @task
    def validate_database():
        counts = db.table_counts()
        print(counts)
        if counts["duplicate_keys"]:
            raise AirflowFailException(f"Duplicate keys in country_stats: {counts}")
        if counts["country_stats"] < MIN_ROWS:
            raise AirflowFailException(f"Expected at least {MIN_ROWS} rows: {counts}")
        return counts

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_run(dag_run=None, ti=None):
        source_states = []
        for extract_ti in dag_run.get_task_instances():
            if not extract_ti.task_id.startswith("extract."):
                continue
            name = extract_ti.task_id.removeprefix("extract.")
            if extract_ti.map_index >= 0:
                name = f"{name}[{extract_ti.map_index}]"
            rows = ti.xcom_pull(task_ids=extract_ti.task_id, map_indexes=extract_ti.map_index)
            source_states.append((name, str(extract_ti.state), len(rows) if rows else None))

        load_ti = dag_run.get_task_instance("load_postgres")
        validate_ti = dag_run.get_task_instance("validate_database")
        merged = ti.xcom_pull(task_ids="merge_sources") or {}
        loaded_ok = validate_ti and validate_ti.state == TaskInstanceState.SUCCESS
        any_source_failed = any(state != TaskInstanceState.SUCCESS for _, state, _ in source_states)
        state = "failed" if not loaded_ok else "partial" if any_source_failed else "success"

        db.record_run(
            dag_run.run_id,
            state,
            ti.xcom_pull(task_ids="load_postgres") if load_ti else None,
            len(merged.get("bad", [])),
            source_states,
        )
        if state == "failed":
            raise AirflowFailException("Load or validation failed; see earlier tasks")

    created = create_tables()
    extracted = extract()
    merged = merge_sources()
    branch = check_quality(merged)
    quarantined = quarantine_bad_rows(merged)
    loaded = load_postgres(merged)
    validated = validate_database()

    created >> extracted >> merged
    branch >> [quarantined, loaded]
    quarantined >> loaded >> validated >> record_run()


table_tennis_pipeline()
