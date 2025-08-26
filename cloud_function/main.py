from flask import Response
import logging
from datetime import datetime
import os
import pandas as pd
from google.cloud import bigquery
from scripts_data.scraper import scrape_reviews
from scripts_data.cleaner import clean_csv
from api.classify import run as classify_and_store

# === Variables globales ===
PROJECT_ID = os.getenv("PROJECT_ID", "trustpilot-satisfaction")
TARGET_TABLE = f"{PROJECT_ID}.reviews_dataset.reviews"
TEMP_TABLE = f"{PROJECT_ID}.reviews_dataset.temp_reviews"
SCRAPER_MODE = os.getenv("SCRAPER_MODE", "csv")
INPUT_FILE = "/tmp/avis_boutique.csv"
OUTPUT_FILE = "/tmp/avis_boutique_clean.csv"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def upload_to_bigquery(csv_path: str, target_table_id: str):
    df = pd.read_csv(csv_path)
    expected_columns = ['review_id', 'rating', 'content', 'author', 'publication_date', 'scrape_date']
    if list(df.columns) != expected_columns:
        raise ValueError(f"Le CSV ne contient pas les colonnes attendues : {expected_columns}")

    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce').dt.date
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce').dt.date

    client = bigquery.Client()
    load_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, TEMP_TABLE, job_config=load_config).result()
    logger.info(f"Données chargées dans {TEMP_TABLE}.")

    merge_query = f"""
        MERGE `{target_table_id}` T
        USING `{TEMP_TABLE}` S
        ON T.author = S.author AND T.content = S.content AND T.publication_date = S.publication_date
        WHEN NOT MATCHED THEN
            INSERT (review_id, rating, content, author, publication_date, scrape_date)
            VALUES (S.review_id, S.rating, S.content, S.author, S.publication_date, S.scrape_date)
    """
    client.query(merge_query).result()
    logger.info(f"Données fusionnées dans {target_table_id}.")


def run_pipeline():
    logger.info(f"Début du pipeline à {datetime.utcnow().isoformat()}")
    logger.info(f"Mode scraping sélectionné : {SCRAPER_MODE}")

    logger.info("Scraping Trustpilot en ligne...")
    scrape_reviews(mode="csv")

    logger.info("Nettoyage des données CSV...")
    clean_stats = clean_csv(INPUT_FILE, OUTPUT_FILE)
    logger.info(f"Statistiques de nettoyage : {clean_stats}")

    logger.info("Upload des données dans BigQuery...")
    upload_to_bigquery(OUTPUT_FILE, TARGET_TABLE)

    today = datetime.utcnow().date().isoformat()
    logger.info("Lancement de l'analyse Claude...")
    classify_and_store(scrape_date=today)

    logger.info("Pipeline complet exécuté avec succès.")


# === Point d'entrée Cloud Function ===
def main(request):
    try:
        run_pipeline()
        return Response("OK: Pipeline exécuté avec succès.", mimetype="text/plain; charset=utf-8", status=200)
    except EnvironmentError as e:
        logger.error(f"Erreur de configuration de la clé API : {e}", exc_info=True)
        return Response(f"Erreur de configuration de la clé API : {e}", mimetype="text/plain; charset=utf-8", status=500)
    except Exception as e:
        logger.error(f"Erreur critique dans le pipeline : {e}", exc_info=True)
        return Response(f"Erreur : {e}", mimetype="text/plain; charset=utf-8", status=500)
