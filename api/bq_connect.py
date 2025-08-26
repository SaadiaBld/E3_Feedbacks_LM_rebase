from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from dotenv import load_dotenv
load_dotenv(dotenv_path="/opt/airflow/.env")


def get_verbatims_by_date(scrape_date: str) -> list[dict]:
    try:
        client = bigquery.Client()
        query = f"""
            SELECT review_id, content
            FROM `trustpilot-satisfaction.reviews_dataset.reviews`
            WHERE content IS NOT NULL
              AND scrape_date = DATE('{scrape_date}')
            
        """
        query_job = client.query(query)
        return [{"review_id": row["review_id"], "content": row["content"]} for row in query_job.result()]

    except DefaultCredentialsError:
        print(" Erreur : impossible de se connecter à BigQuery. Vérifie ton authentification avec `gcloud auth application-default login`.")
        return []

    except Exception as e:
        print(f" Erreur lors de la requête BigQuery : {e}")
        return []

