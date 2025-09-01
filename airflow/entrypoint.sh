#!/bin/bash
set -e

# (optionnel) mini attente Postgres
sleep 2

pip install --no-cache-dir -r /requirements.txt || true

# Initialise Airflow DB si pas encore initialisée
if [ ! -f "/opt/airflow/airflow.db_initialized" ]; then
  airflow db init && touch /opt/airflow/airflow.db_initialized
fi

# Crée l'utilisateur admin si non existant
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

exec airflow webserver