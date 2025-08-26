#!/bin/bash

pip install -r /requirements.txt

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