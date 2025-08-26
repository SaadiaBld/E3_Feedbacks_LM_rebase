import os, uuid
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
import logging

# Import interne (doit fonctionner avec ton arborescence cloud_function/)
from .bq_connect import get_verbatims_by_date as get_verbatims_from_bq
from api.claude_interface import classify_with_claude

logger = logging.getLogger(__name__)

# === Fonctions de chargement et de traitement ===

def load_topic_ids() -> Dict[str, str]:
    """Charge les mappings de topic_label vers topic_id depuis BigQuery."""
    client = bigquery.Client()
    query = """
        SELECT topic_label, topic_id
        FROM `trustpilot-satisfaction.reviews_dataset.topics`
    """
    results = client.query(query).result()
    return {row.topic_label: row.topic_id for row in results}

def process_verbatims(verbatims: List[Dict], label_to_id: Dict[str, str], scrape_date: str) -> (List[Dict], set):
    """Traite les verbatims, renvoie les lignes à insérer et les thèmes inconnus."""
    rows_to_insert = []
    unknown_themes = set()
    
    for i, v in enumerate(verbatims):
        logger.info(f"Verbatim {i+1} : {v['content']}")
        annotations = classify_with_claude(v['content'])

        if not annotations:
            logger.warning("Analyse non exploitable pour ce verbatim.")
            continue

        for entry in annotations:
            theme = entry["theme"]
            note = entry["note"]
            topic_id = label_to_id.get(theme)

            if not topic_id:
                logger.warning(f"Thème inconnu : '{theme}'. Il sera ignoré.")
                unknown_themes.add(theme)
                continue
            
            if not isinstance(note, (int, float)) or not (1 <= note <= 5):
                logger.warning(f"Note invalide pour {theme} : {note}. Elle sera ignorée.")
                continue

            score_0_1 = round((5 - note) / 4, 2)
            if score_0_1 >= 0.85: label = "Très négatif"
            elif score_0_1 >= 0.65: label = "Négatif"
            elif score_0_1 >= 0.4: label = "Neutre"
            elif score_0_1 >= 0.25: label = "Positif"
            else: label = "Très positif"

            rows_to_insert.append({
                "id": str(uuid.uuid4()),
                "review_id": v["review_id"],
                "topic_id": topic_id,
                "score_sentiment": note,
                "label_sentiment": label,
                "score_0_1": score_0_1
            })
            logger.info(f"Thème : {theme}, Note : {note}")
            
    return rows_to_insert, unknown_themes

# === Fonctions BigQuery ===

def insert_into_bigquery(data: List[Dict]):
    """Insère les données traitées dans BigQuery."""
    if not data:
        logger.warning("Aucune donnée valide à insérer. Vérifiez les thèmes inconnus ci-dessous.")
        return
        
    client = bigquery.Client()
    table_id = "trustpilot-satisfaction.reviews_dataset.topic_analysis"

    errors = client.insert_rows_json(table_id, data)
    if not errors:
        logger.info(f"{len(data)} lignes insérées avec succès dans {table_id}.")
    else:
        logger.error(f"Erreurs d’insertion dans BigQuery : {errors}")

# === Point d'entrée principal ===

def run(scrape_date: str):
    """Orchestre l'analyse et l'insertion."""
    verbatims = get_verbatims_from_bq(scrape_date=scrape_date)
    logger.info(f"{len(verbatims)} verbatims trouvés à analyser pour la date {scrape_date}.")

    if not verbatims:
        return

    label_to_id = load_topic_ids()
    rows_to_insert, unknown_themes = process_verbatims(verbatims, label_to_id, scrape_date)
    
    insert_into_bigquery(rows_to_insert)

    if unknown_themes:
        logger.warning("--- RÉSUMÉ DES THÈMES INCONNUS ---")
        logger.warning("Les thèmes suivants ont été détectés par Claude mais n'existent pas dans votre table 'topics':")
        for theme in sorted(list(unknown_themes)):
            logger.warning(f"- {theme}")
        logger.warning("Veuillez les ajouter à la table `trustpilot-satisfaction.reviews_dataset.topics` pour que les analyses futures soient enregistrées.")

# === Point d'entrée pour Google Cloud Function ===

def classify_trigger(event, context):
    """Déclenchement par Cloud Scheduler (via Pub/Sub)"""
    today = datetime.utcnow().date().isoformat()
    logger.info(f"Lancement de l’analyse Claude pour la date : {today}")
    run(scrape_date=today)
