import os
from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

from scripts_data.scraper import scrape_reviews
from scripts_data.cleaner import clean_csv

# === Chargement des variables d’environnement ===
dotenv_path = os.path.join(os.path.dirname(__file__), "config/.env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    print("⚠️ Aucun fichier .env trouvé. On utilisera les variables d’environnement GCP.")

# === Constantes ===
INPUT_CSV = "/tmp/avis_boutique.csv"
OUTPUT_CSV = "/tmp/avis_boutique_clean.csv"
PROJECT_ID = os.getenv("PROJECT_ID", "trustpilot-satisfaction")
TARGET_TABLE = f"{PROJECT_ID}.reviews_dataset.reviews"
TEMP_TABLE = f"{PROJECT_ID}.reviews_dataset.temp_reviews"

# === Fonction d'upload vers BigQuery ===
def upload_to_bigquery(csv_path, target_table_id):
    df = pd.read_csv(csv_path)
    df['publication_date'] = pd.to_datetime(df['publication_date'], errors='coerce').dt.date
    df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce').dt.date

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    client.load_table_from_dataframe(df, TEMP_TABLE, job_config=job_config).result()
    print(f"✅ Données chargées dans la table temporaire {TEMP_TABLE}.")

    merge_query = f"""
    MERGE `{target_table_id}` T
    USING `{TEMP_TABLE}` S
    ON T.review_id = S.review_id
    WHEN NOT MATCHED THEN
      INSERT (review_id, rating, content, author, publication_date, scrape_date)
      VALUES (S.review_id, S.rating, S.content, S.author, S.publication_date, S.scrape_date)
    """
    client.query(merge_query).result()
    print(f"✅ Données fusionnées dans la table {target_table_id}.")

# === Pipeline principal ===
def main_pipeline():
    print(f"▶ Lancement du pipeline à {datetime.now().isoformat()}")
    print("🔍 Scraping des avis Trustpilot (mode prod)...")
    scrape_reviews(mode="prod")  # Remplace mode='csv' pour un vrai scraping Trustpilot

    print("🧼 Nettoyage des données...")
    clean_csv(INPUT_CSV, OUTPUT_CSV)

    print("📤 Upload dans BigQuery...")
    upload_to_bigquery(OUTPUT_CSV, TARGET_TABLE)
    print("✅ Pipeline terminé avec succès.")

# === Point d’entrée Cloud Function ===
def run_pipeline(request):
    try:
        main_pipeline()
        return ("✅ Pipeline exécuté avec succès.", 200)
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return (f"❌ Erreur : {e}", 500)
