
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
    MAIN_TABLE_ID = "trustpilot-satisfaction.reviews_dataset.reviews"
    TEMP_TABLE_ID = "trustpilot-satisfaction.reviews_dataset.temp_reviews" # Assurez-vous que cette table existe

    if not os.path.exists(path):
        print(f"Le fichier {path} n'existe pas.")
        return

    df = pd.read_csv(path)

    if df.empty:
        print("Le fichier nettoyé est vide. Rien à insérer dans BigQuery.")
        return

    client = bigquery.Client()

    # --- Étape 1: Charger les nouvelles données dans la table temporaire (temp_reviews) ---
    job_config_temp = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Vider la table temporaire avant chaque chargement
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    print(f"Chargement de {df.shape[0]} lignes dans la table temporaire {TEMP_TABLE_ID}...")
    with open(path, "rb") as source_file:
        job_temp = client.load_table_from_file(source_file, TEMP_TABLE_ID, job_config=job_config_temp)

    try:
        job_temp.result() # Attendre la fin du chargement dans la table temporaire
        print(f"{job_temp.output_rows} lignes chargées dans {TEMP_TABLE_ID}.")
    except Exception as e:
        print(f"***Erreur lors du chargement dans la table temporaire {TEMP_TABLE_ID} : {e}")
        return

    # --- Étape 2: Exécuter l'opération MERGE pour insérer/mettre à jour dans la table principale ---
    merge_query = f"""
        MERGE INTO `{MAIN_TABLE_ID}` AS T
        USING `{TEMP_TABLE_ID}` AS S
        ON
            T.content = S.content AND
            T.publication_date = S.publication_date AND
            T.author = S.author
        WHEN MATCHED THEN
            -- Si un avis existe déjà (même content, publication_date, author), ne rien faire
            DO NOTHING
        WHEN NOT MATCHED THEN
            -- Si l'avis n'existe pas, l'insérer
            INSERT (
                review_id, rating, content, author, publication_date, scrape_date
            )
            VALUES (
                S.review_id, S.rating, S.content, S.author, S.publication_date, S.scrape_date
            );
    """

    print(f"Exécution de l'opération MERGE vers {MAIN_TABLE_ID}...")
    query_job = client.query(merge_query)

    try:
        query_job.result() # Attendre la fin de l'opération MERGE
        print(f"Opération MERGE terminée pour {MAIN_TABLE_ID}.")
        # Vous pouvez ajouter ici une requête pour compter les lignes insérées/mises à jour si nécessaire
    except Exception as e:
        print(f"***Erreur lors de l'opération MERGE vers {MAIN_TABLE_ID} : {e}")
        return

    # --- Étape 3: Nettoyage (optionnel, car WRITE_TRUNCATE gère déjà le nettoyage avant chargement) ---
    # Si vous voulez vider explicitement la table temporaire après le MERGE, vous pouvez ajouter:
    # client.query(f"TRUNCATE TABLE `{TEMP_TABLE_ID}`").result()
    # print(f"Table temporaire {TEMP_TABLE_ID} vidée.")

    print(f"Processus d'insertion dédupliquée terminé pour {MAIN_TABLE_ID}.")
