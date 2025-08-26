import os
import time
import uuid
from datetime import datetime
from dotenv import load_dotenv
import logging

from .bq_connect import get_verbatims_by_date
from .claude_interface import classify_with_claude
from google.cloud import bigquery

# Les variables d'environnement (ex: PROJECT_ID) doivent être définies
# directement dans la configuration de la Cloud Function.

logger = logging.getLogger(__name__)

def get_project_id():
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise ValueError("La variable PROJECT_ID est absente de l'environnement.")
    return project_id

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

    for item in theme_scores:
        theme = item["theme"]
        note = item["note"]
        topic_id = label_to_id.get(theme)

        if not topic_id:
            logger.warning(f"Thème inconnu dans la table topics : {theme}")
            unknown_topics.append(theme)
            continue

        if not isinstance(note, (int, float)) or not (1 <= note <= 5):
            logger.warning(f"Note invalide pour {theme} : {note}")
            continue

        score_0_1 = round((5 - note) / 4, 2)

        if score_0_1 >= 0.85:
            label = "Très négatif"
        elif score_0_1 >= 0.65:
            label = "Négatif"
        elif score_0_1 >= 0.4:
            label = "Neutre"
        elif score_0_1 >= 0.25:
            label = "Positif"
        else:
            label = "Très positif"

        rows_to_insert.append({
            "id": str(uuid.uuid4()),
            "review_id": review_id,
            "topic_id": topic_id,
            "score_sentiment": note,
            "label_sentiment": label,
            "score_0_1": score_0_1
        })

    if not rows_to_insert:
        logger.info("Aucun thème à insérer")
        return

    errors = client.insert_rows_json("trustpilot-satisfaction.reviews_dataset.topic_analysis", rows_to_insert)

    if errors:
        logger.error(f"Erreurs d'insertion : {errors}")
    else:
        logger.info(f"{len(rows_to_insert)} lignes insérées pour review {review_id}")

    return {
        "insert_errors": bool(errors),
        "new_topics": unknown_topics
    }

def run_analysis(scrape_date: str):
    verbatims = get_verbatims_by_date(scrape_date)
    logger.info(f"{len(verbatims)} verbatims récupérés pour {scrape_date}")

    if not verbatims:
        logger.info("Aucun verbatim trouvé pour la date.")
        return

    label_to_id = load_topic_ids()

    for i, v in enumerate(verbatims):
        logger.info(f"Verbatim {i+1} : {v['content'][:60]}...")

        start = time.time()
        try:
            theme_scores = classify_with_claude(v["content"])
            if theme_scores:
                result = insert_topic_analysis(
                    review_id=v["review_id"],
                    theme_scores=theme_scores,
                    label_to_id=label_to_id
                )
            else:
                logger.warning("Claude n’a rien renvoyé")
        except Exception as e:
            logger.error(f"Erreur lors de l'analyse : {e}", exc_info=True)

def process_and_insert_all(scrape_date: str = None):
    if not scrape_date:
        scrape_date = datetime.utcnow().date().isoformat()
    logger.info(f"Début du traitement pour {scrape_date}")
    run_analysis(scrape_date)
    logger.info(f"Fin du traitement")

# ✅ Entrée pour Cloud Function
def main(request=None):
    scrape_date = request.args.get("scrape_date") if request else None
    process_and_insert_all(scrape_date)
    return "✅ Cloud Function exécutée avec succès."

# ✅ Entrée pour exécution manuelle locale
if __name__ == "__main__":
    process_and_insert_all()
