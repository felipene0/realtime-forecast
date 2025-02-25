#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --no-cache-dir -r /opt/airflow/requirements.txt
fi

$(command -v airflow) db upgrade

exec airflow webserver