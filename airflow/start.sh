#!/usr/bin/env bash
set -euo pipefail

# Stale PID files from an unclean stop block the webserver from starting.
rm -f "${AIRFLOW_HOME:-/opt/airflow}"/airflow-*.pid

airflow db migrate

if ! airflow users create \
    --username "$AIRFLOW_ADMIN_USER" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@localhost; then
    airflow users reset-password --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD"
fi

exec airflow standalone
