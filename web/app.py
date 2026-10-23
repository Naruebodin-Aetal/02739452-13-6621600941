"""Table tennis dashboard. Reads PostgreSQL directly on every page load."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Table Tennis by Country", page_icon="🏓", layout="wide")

COMPETITION_LABELS = {
    "world_championships": "World Championships",
    "olympics": "Olympics",
    "world_ranking": "WTT World Ranking",
}
EVENT_LABELS = {
    "mens_singles": "Men's singles",
    "womens_singles": "Women's singles",
    "mens_doubles": "Men's doubles",
    "womens_doubles": "Women's doubles",
    "mixed_doubles": "Mixed doubles",
    "all_events": "All events",
    "ms": "Men's singles ranking",
    "ws": "Women's singles ranking",
}


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg2.connect(
        host=os.environ["DATA_DB_HOST"],
        port=os.environ.get("DATA_DB_PORT", "5432"),
        dbname=os.environ["DATA_DB_NAME"],
        user=os.environ["DATA_DB_USER"],
        password=os.environ["DATA_DB_PASSWORD"],
    ) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def tables_ready() -> bool:
    try:
        return not query("SELECT to_regclass('country_summary') IS NULL AS missing").iloc[0, 0]
    except psycopg2.OperationalError:
        return False


def dashboard_page() -> None:
    st.title("🏓 Table tennis strength by country")
    st.caption("World Championships medals (Wikipedia) · Olympic medals (Olympedia) · WTT world ranking top 1000 (WTT API)")

    summary = query("SELECT * FROM country_summary")
    stats = query("SELECT * FROM country_stats")
    for col in ("gold", "silver", "bronze", "medal_total"):
        stats[col] = stats[col].astype(float)
    for col in summary.columns[3:]:
        summary[col] = pd.to_numeric(summary[col])

    with st.sidebar:
        st.header("Filters")
        competition = st.selectbox(
            "Competition",
            list(COMPETITION_LABELS),
            format_func=COMPETITION_LABELS.get,
        )
        events = sorted(stats.loc[stats.competition == competition, "event"].unique())
        chosen_events = st.multiselect("Event", events, default=events, format_func=lambda e: EVENT_LABELS.get(e, e))
        countries = st.multiselect("Country", sorted(summary.country_name))
        top_n = st.slider("Top N countries in chart", 5, 30, 15)

    view = stats[(stats.competition == competition) & stats.event.isin(chosen_events)]
    if countries:
        view = view[view.country_name.isin(countries)]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Countries (all sources)", len(summary))
    k2.metric("World Championships medals", f"{summary.wtc_medals.sum():,.0f}")
    k3.metric("Olympic medals", f"{summary.olympic_medals.sum():,.0f}")
    leader = summary.sort_values(["wtc_gold", "olympic_gold"], ascending=False).iloc[0]
    k4.metric("Most gold medals", leader.country_name, f"{leader.wtc_gold + leader.olympic_gold:,.0f} gold")

    if view.empty:
        st.info("No rows match the filters.")
        return

    if competition == "world_ranking":
        chart_data = (view.groupby("country_name", as_index=False)["ranked_players"].sum()
                      .nlargest(top_n, "ranked_players"))
        fig = px.bar(chart_data, x="country_name", y="ranked_players",
                     labels={"country_name": "Country", "ranked_players": "Players in top 1000"})
    else:
        chart_data = (view.groupby("country_name", as_index=False)[["gold", "silver", "bronze", "medal_total"]].sum()
                      .nlargest(top_n, "medal_total"))
        fig = px.bar(
            chart_data.melt(id_vars="country_name", value_vars=["gold", "silver", "bronze"],
                            var_name="medal", value_name="count"),
            x="country_name", y="count", color="medal",
            color_discrete_map={"gold": "#d4a017", "silver": "#a8a9ad", "bronze": "#b0723c"},
            labels={"country_name": "Country", "count": "Medals"},
        )
    st.subheader(f"{COMPETITION_LABELS[competition]}: top {top_n}")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Data")
    shown = view.drop(columns=["loaded_at"]).dropna(axis=1, how="all")
    st.dataframe(shown, use_container_width=True, hide_index=True)

    st.subheader("Countries across sources")
    st.dataframe(summary.sort_values("wtc_medals", ascending=False), use_container_width=True, hide_index=True)


def status_page() -> None:
    st.title("Pipeline status")
    runs = query("SELECT * FROM pipeline_runs ORDER BY finished_at DESC LIMIT 20")
    if runs.empty:
        st.info("No DAG run has finished yet.")
        return
    latest = runs.iloc[0]
    c1, c2, c3 = st.columns(3)
    c1.metric("Latest run", latest.state.upper())
    c2.metric("Rows loaded", int(latest.rows_loaded or 0))
    c3.metric("Rows quarantined", int(latest.bad_rows or 0))
    st.caption(f"run_id: {latest.run_id} · finished {latest.finished_at:%Y-%m-%d %H:%M %Z}")

    st.subheader("Sources in the latest run")
    st.dataframe(query("SELECT task_id, state, rows FROM source_runs WHERE run_id = %s ORDER BY task_id",
                       (latest.run_id,)), use_container_width=True, hide_index=True)
    st.subheader("Recent runs")
    st.dataframe(runs, use_container_width=True, hide_index=True)
    st.subheader("Rows that failed quality checks")
    st.dataframe(query("SELECT source, event, country_name, issues, run_id, detected_at FROM quality_issues"),
                 use_container_width=True, hide_index=True)


if not tables_ready():
    st.warning("Waiting for the first Airflow DAG run to create and fill the tables. Refresh in a minute.")
    st.stop()

page = st.sidebar.radio("Page", ["Dashboard", "Pipeline status"])
dashboard_page() if page == "Dashboard" else status_page()
