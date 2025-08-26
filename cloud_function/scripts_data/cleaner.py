import pandas as pd
import re
import os
from datetime import datetime
from flask import jsonify, Request
import logging

# === Configuration du logger pour Cloud Functions (stdout uniquement) ===
logger = logging.getLogger(__name__)

def clean_emojis(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticônes
        "\U0001F300-\U0001F5FF"  # symboles et pictogrammes
        "\U0001F680-\U0001F6FF"  # transports et cartes
        "\U0001F1E0-\U0001F1FF"  # drapeaux (iOS)
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def clean_text(text):
    if pd.isna(text):
        return ""
    text = clean_emojis(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def clean_csv(input_file, output_file):
    df = pd.read_csv(input_file, dtype=str)

    expected_cols = ['review_id', 'rating', 'content', 'author', 'publication_date', 'scrape_date']
    if set(df.columns) != set(expected_cols):
        raise ValueError(f"Le fichier CSV a une structure invalide : {list(df.columns)}. Attendu : {expected_cols}")

    df['content'] = df['content'].astype(str).apply(clean_text)

    df = df[~df['content'].isin(["?", "", None])]
    df = df[~df['content'].isna()]
    df = df[df['content'].str.strip() != ""]
    df = df.drop_duplicates(subset=['author', 'content', 'publication_date'], keep='first')

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    try:
        df.to_csv(output_file, index=False, encoding='utf-8')
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde du fichier nettoyé : {e}")
        raise

    logger.info(f"Fichier nettoyé sauvegardé dans {output_file}")

def clean_data(input_file=None, output_file=None):
    input_file = input_file or "/tmp/avis_boutique.csv"
    output_file = output_file or "/tmp/avis_boutique_clean.csv"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"{input_file} introuvable. Lance d'abord le scraper.")
    clean_csv(input_file, output_file)
    logger.info(f"INPUT_CSV = {input_file}")
    logger.info(f"OUTPUT_CSV = {output_file}")

def clean_csv_http(request: Request):
    """
    Fonction HTTP pour Cloud Function.
    Nettoie un fichier CSV situé dans /tmp.
    Entrée possible (JSON ou params GET) :
      {
        "input_path": "/tmp/avis_boutique.csv",
        "output_path": "/tmp/avis_boutique_clean.csv"
      }
    """
    try:
        input_path = output_path = None

        if request.is_json:
            data = request.get_json(silent=True) or {}
            input_path = data.get("input_path")
            output_path = data.get("output_path")
        else:
            input_path = request.args.get("input_path")
            output_path = request.args.get("output_path")

        input_path = input_path or "/tmp/avis_boutique.csv"
        output_path = output_path or "/tmp/avis_boutique_clean.csv"

        clean_data(input_path, output_path)

        return jsonify({
            "status": "success",
            "input": input_path,
            "output": output_path
        }), 200

    except Exception as e:
        logger.exception("Erreur pendant le nettoyage du CSV")
        return jsonify({"status": "error", "message": str(e)}), 500

