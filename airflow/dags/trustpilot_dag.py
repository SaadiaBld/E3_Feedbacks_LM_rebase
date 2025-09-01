from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Corrigé
from datetime import datetime, timedelta
import pendulum, sys, os, logging, time
from scripts_data.scraper import scrape_reviews
from scripts_data.cleaner import clean_data
from scripts_data.main import main as run_full_scraper_pipeline
from api.bq_insert_clean_data import insert_clean_reviews_to_bq
from dotenv import load_dotenv

from monitoring.metrics import ANALYSIS_DURATION, push_metrics_to_gateway

MONITORING_JOB = "verbatim_pipeline"
MONITORING_INSTANCE = os.getenv("MONITORING_INSTANCE", "dev")

# Chargement des variables d'environnement
load_dotenv("/opt/airflow/project/.env")

# Ajout des chemins 
sys.path.append("/opt/airflow/project/scripts_data")
sys.path.append("/opt/airflow/project/api")

try:
    from api.analyze_and_insert import process_and_insert_all
    PROCESS_AVAILABLE = True
except FileNotFoundError as e:
    logging.error(f" Fichier manquant empêchant l'import : {e}")
    process_and_insert_all = None
    PROCESS_AVAILABLE = False

print("***Fichier .env chargé")
print("***Mode scraping :", os.getenv("SCRAPER_MODE"))
print("***Fichier d’entrée :", os.getenv("INPUT_CSV"))


# Wrappers pour les fonctions de scraping et d'analyse afin de les adapter à Airflow
def wrapper_run_scraper(**context):
    scrape_date = context["ds"]
    print(f"Wrapper Scraper : scrape_date = {scrape_date}")
    scrape_reviews()


def wrapper_process_and_insert(**context):
    scrape_date = context["ds"]
    print(f"Wrapper Analyse/Insert : scrape_date = {scrape_date}")
    print("Fichier de credentials GCP : ", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    if process_and_insert_all:
        start_time = time.time()
        process_and_insert_all(scrape_date=scrape_date)
        end_time = time.time()
        duration = end_time - start_time
        ANALYSIS_DURATION.observe(duration)
    else:
        raise RuntimeError("Fonction d’analyse non disponible. Vérifiez le fichier de credentials.")


# Paramètres par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id='trustpilot_pipeline',
    default_args=default_args,
    description='Pipeline : Scraper → Nettoyage → Claude → BQ',
    schedule=None, #'0 6 * * 1',
    start_date=datetime(2025, 6, 1, tzinfo=pendulum.timezone("Europe/Paris")),
    catchup=False,
    tags=['trustpilot', 'nlp', 'bq'],
    doc_md="""
    ### Pipeline Trustpilot
    Ce DAG scrape les avis Trustpilot de Leroy Merlin, les nettoie, les insère dans BigQuery, puis les analyse via Claude.
    """,
) as dag:

    # Scraping
    scrape_task = PythonOperator(
        task_id='scrape_trustpilot_reviews',
        python_callable=wrapper_run_scraper,
    )

    # Nettoyage
    clean_task = PythonOperator(
        task_id='clean_reviews',
        python_callable=clean_data,
    op_kwargs={
        "input_file": "/opt/airflow/project/data/avis_boutique.csv",
        "output_file": "/opt/airflow/project/data/avis_boutique_clean.csv",
    },
    )

    # Insertion des avis nettoyés dans BQ
    insert_task = PythonOperator(
    task_id="insert_clean_reviews_to_bq",
    python_callable=insert_clean_reviews_to_bq,
    )

    # Analyse / Insertion ou Dummy si process indisponible
    if PROCESS_AVAILABLE:
        analyze_insert_task = PythonOperator(
            task_id='analyze_and_insert',
            python_callable=wrapper_process_and_insert,
        )
    else:
        analyze_insert_task = EmptyOperator(task_id='skip_analyze_insert_due_to_missing_cred') # Corrigé

def wrapper_push_metrics(**context):
    push_metrics_to_gateway(job_name=MONITORING_JOB, instance=MONITORING_INSTANCE)


# New task for pushing metrics
push_metrics_task = PythonOperator(
    task_id='push_metrics',
    python_callable=wrapper_push_metrics,
    trigger_rule="all_done", # Added trigger_rule
)

# Orchestration
scrape_task >> clean_task >> insert_task >> analyze_insert_task >> push_metrics_task
