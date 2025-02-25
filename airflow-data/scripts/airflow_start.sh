#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --no-cache-dir -r /opt/airflow/requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username airflow \
    --firstname airflow \
    --lastname airflow \
    --role Admin \
    --email airflow@airflow.com \
    --password airflow
fi

$(command -v airflow) db upgrade

exec airflow webserver