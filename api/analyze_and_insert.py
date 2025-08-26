###pour airlow, décommente la ligne suivante :
import os, time, uuid
from dotenv import load_dotenv
from api.bq_connect import get_verbatims_by_date
from api.claude_interface import classify_with_claude
from google.cloud import bigquery
from datetime import datetime
from monitoring.metrics import log_analysis_metrics, monitor_start, push_metrics_to_gateway
from prometheus_client import push_to_gateway, REGISTRY


if __name__ == "__main__" or os.getenv("AIRFLOW_RUN_METRICS") == "true":
    monitor_start(port=8000)

# Charger .env avec conditions: Si on est dans un test, utilise toujours le .env local
if "PYTEST_CURRENT_TEST" in os.environ:
    dotenv_path = ".env"
else:
    dotenv_path = "/opt/airflow/.env" if os.getenv("AIRFLOW_HOME") else ".env"

load_dotenv(dotenv_path=dotenv_path)

# Vérifier que les variables importantes sont bien présentes
def get_project_id():
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise ValueError("La variable PROJECT_ID est absente de l'environnement.")
    return project_id

def get_gcp_credentials_path():
    gcp_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not gcp_credentials or not os.path.isfile(gcp_credentials):
        raise FileNotFoundError(f"Fichier de credentials GCP introuvable : {gcp_credentials}")
    return gcp_credentials

def load_topic_ids():
    client = bigquery.Client(project=get_project_id())

    query = """
        SELECT topic_label, topic_id
        FROM `trustpilot-satisfaction.reviews_dataset.topics`
    """
    results = client.query(query).result()
    return {row.topic_label: row.topic_id for row in results}


def insert_topic_analysis(review_id: str, theme_scores: list[dict], label_to_id: dict):
    client = bigquery.Client(project=get_project_id())
    rows_to_insert = []
    unknown_topics = []

    if len(theme_scores) == 0:
        print("Aucun thème détecté")
        return

    for item in theme_scores:
        theme = item["theme"]
        note = item["note"]
        topic_id = label_to_id.get(theme)

        #verification de la présence du thème dans la table topics
        if not topic_id:
            print(f"Thème inconnu dans la table topics : {theme}")
            unknown_topics.append(theme)
            continue

        # Vérification de la validité de la note
        if not isinstance(note, (int, float)) or not (1 <= note <= 5):
            print(f"Note invalide pour {theme} : {note}")
            continue

        # Score de satisfaction normalisé (1 = très insatisfait, 0 = très satisfait)
        score_0_1 = round((5 - note) / 4, 2)

        # Label de satisfaction
        if score_0_1 >= 0.85:
            label = "Très négatif"
        elif score_0_1 >= 0.65 and score_0_1 < 0.85:
            label = "Négatif"
        elif score_0_1 >= 0.4 and score_0_1 < 0.65:
            label = "Neutre"
        elif score_0_1 >= 0.25 and score_0_1 < 0.4:
            label = "Positif"
        else:
            label = "Très positif"

        rows_to_insert.append({
            "id": str(uuid.uuid4()),
            "review_id": review_id,
            "topic_id": topic_id,  # Le thème détecté devient la valeur de topic_id
            "score_sentiment": note,
            "label_sentiment": label,
            "score_0_1": score_0_1
        })
    if not rows_to_insert:
        print("⚠️ Aucun thème à insérer")
        return
    
    errors = client.insert_rows_json("trustpilot-satisfaction.reviews_dataset.topic_analysis", rows_to_insert)

    if errors:
        print(f"Erreurs d'insertion : {errors}")
    else:
        print(f"{len(rows_to_insert)} lignes insérées pour review {review_id}")
        print("Thèmes détectés :")
        for r in rows_to_insert:
            print(f" - {r['topic_id']} (note : {r['score_sentiment']})")

    return {
        "insert_errors": bool(errors),
        "new_topics": unknown_topics
    }

def run_analysis(scrape_date: str):
    verbatims = get_verbatims_by_date(scrape_date)
    print(f"📊 Verbatims récupérés : {len(verbatims)}")
    for v in verbatims:
        print(f"- {v['review_id']}: {v['content'][:60]}...")

    if not verbatims:
        print("⚠️ Aucun verbatim trouvé pour la date, test avec un faux.")
        return

    label_to_id = load_topic_ids()

    print(f"{len(verbatims)} verbatims trouvés pour la date : {scrape_date}")

    for i, v in enumerate(verbatims):
        print(f"\n🟦 Verbatim {i+1} :\n{v['content']}")
        
        start = time.time()  # début de chrono

        try:
            theme_scores = classify_with_claude(v["content"])

            # # Afficher la réponse brute de Claude pour debug
            # print("\n Réponse brute de Claude :")
            # print(theme_scores)

            if theme_scores:
                result = insert_topic_analysis(
                    review_id=v["review_id"],
                    theme_scores=theme_scores,
                    label_to_id=label_to_id
                )
                # Enregistrement des métriques Prometheus
                log_analysis_metrics(
                    verbatim_text=v["content"],
                    duration=time.time() - start,
                    error=False,
                    empty=False,
                    new_topics=result["new_topics"],
                    bq_error=result["insert_errors"]
                )

            else:
                print("❌ Analyse non exploitable (voir claude_errors.log)")
                log_analysis_metrics(
                    verbatim_text=v["content"],
                    duration=time.time() - start,
                    error=False,
                    empty=True  # Claude n’a rien renvoyé
                )


        except Exception as e:
            print(f"❌ Erreur lors de l'analyse du verbatim {v['review_id']} : {e}")
            log_analysis_metrics(
                verbatim_text=v["content"],
                duration=time.time() - start,
                error=True
            )


def process_and_insert_all(scrape_date: str = None):
    """Fonction appelée dans le DAG Airflow."""
    from monitoring.metrics import monitor_start , push_metrics_to_gateway
    monitor_start()

    # S'assurer que les credentials existent AVANT les appels GCP
    get_gcp_credentials_path()
    
    if not scrape_date:
        scrape_date = datetime.utcnow().date().isoformat()
    print(f"Lancement du traitement pour la date : {scrape_date}")

    run_analysis(scrape_date=scrape_date)
    print(f"✅ Traitement terminé pour {scrape_date}")

    # Pousser les métriques vers le PushGateway
    push_metrics_to_gateway()