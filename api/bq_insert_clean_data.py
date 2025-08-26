
import pandas as pd
from google.cloud import bigquery
import os


from google.cloud import bigquery

def deduplicate_reviews():
    client = bigquery.Client()

    # Étape 1 : Identifier les doublons à supprimer
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
        print("Aucun doublon à supprimer dans la table reviews")
        return

    # Étape 2 : Supprimer les doublons trouvés
    delete_query = f"""
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

    print(f" {nb_to_delete} doublons supprimés dans la table reviews")


def insert_clean_reviews_to_bq():
    path = "/opt/airflow/project/data/avis_boutique_clean.csv"
    table_id = "trustpilot-satisfaction.reviews_dataset.reviews"

    if not os.path.exists(path):
        print(f"Le fichier {path} n'existe pas.")
        return

    df = pd.read_csv(path)

    if df.empty:
        print("Le fichier nettoyé est vide. Rien à insérer dans BigQuery.")
        return

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # ou WRITE_TRUNCATE
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    with open(path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    try:
        job.result()  # Attendre la fin
    except Exception as e:
        print(f"***Erreur lors de l'insertion dans BigQuery : {e}")
        return
    
    print(f"{df.shape[0]} lignes insérées dans {table_id}")
