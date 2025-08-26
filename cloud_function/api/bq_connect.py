from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from dotenv import load_dotenv
import os
from typing import List, Dict
import logging

# Les variables d'environnement doivent être définies dans l'environnement GCP.

logger = logging.getLogger(__name__)

def get_verbatims_by_date(scrape_date: str) -> List[Dict[str, str]]:
    if not scrape_date:
        raise ValueError("La date de scraping est obligatoire.")

    try:
        client = bigquery.Client()
        query = f"""
            SELECT review_id, content
            FROM `trustpilot-satisfaction.reviews_dataset.reviews`
            WHERE content IS NOT NULL
              AND scrape_date = DATE('{scrape_date}')
        """
        query_job = client.query(query)
        results = [{"review_id": row["review_id"], "content": row["content"]} for row in query_job.result()]
        logger.info(f"{len(results)} verbatims récupérés pour la date {scrape_date}")
        return results

    except DefaultCredentialsError:
        logger.error("Erreur : impossible de se connecter à BigQuery. Vérifie ton authentification GCP.")
        return []

    except Exception as e:
        logger.error(f"Erreur lors de la requête BigQuery : {e}", exc_info=True)
        return []
