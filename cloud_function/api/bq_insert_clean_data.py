import pandas as pd
from google.cloud import bigquery
import os
import logging

logger = logging.getLogger(__name__)

def deduplicate_reviews():
    client = bigquery.Client()

    dedup_query = """
        WITH duplicates AS (
            SELECT review_id
            FROM (
                SELECT review_id,
                       ROW_NUMBER() OVER (
                         PARTITION BY author, content, publication_date
                         ORDER BY scrape_date ASC
                       ) AS rn
                FROM `trustpilot-satisfaction.reviews_dataset.reviews`
            )
            WHERE rn > 1
        )
        SELECT COUNT(*) AS nb_to_delete, ARRAY_AGG(review_id) AS ids
        FROM duplicates
    """
    result = client.query(dedup_query).result()
    row = list(result)[0]
    nb_to_delete = row["nb_to_delete"]
    review_ids = row["ids"]

    if nb_to_delete == 0:
        logger.info("Aucun doublon à supprimer dans la table reviews")
        return

    delete_query = """
        DELETE FROM `trustpilot-satisfaction.reviews_dataset.reviews`
        WHERE review_id IN UNNEST(@review_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("review_ids", "STRING", review_ids)
        ]
    )
    delete_job = client.query(delete_query, job_config=job_config)
    delete_job.result()

    logger.info(f"{nb_to_delete} doublons supprimés dans la table reviews")


def insert_clean_reviews_to_bq():
    path = "/tmp/avis_boutique_clean.csv"
    table_id = "trustpilot-satisfaction.reviews_dataset.reviews"

    if not os.path.exists(path):
        logger.error(f"Le fichier {path} n'existe pas.")
        return

    df = pd.read_csv(path)

    if df.empty:
        logger.warning("Le fichier nettoyé est vide. Rien à insérer.")
        return

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open(path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    try:
        job.result()
        logger.info(f"{df.shape[0]} lignes insérées dans {table_id}")
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion dans BigQuery : {e}", exc_info=True)


def main(request=None):
    insert_clean_reviews_to_bq()
    deduplicate_reviews()
    return "Insertion et déduplication terminées", 200
