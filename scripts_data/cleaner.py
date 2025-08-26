import pandas as pd
import re
import os
from datetime import datetime

def clean_emojis(text):
    # Suppression simple d'emojis (exemple générique, à adapter si tu veux plus précis)
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
    # Supprime emojis
    text = clean_emojis(text)
    # Supprime les espaces multiples et sauts de ligne
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def clean_csv(input_file, output_file):
    # Charger le CSV
    df = pd.read_csv(input_file, dtype=str)
    rows_before = len(df)
    # Vérification du nombre de colonnes
    expected_cols = ['review_id', 'rating', 'content', 'author', 'publication_date', 'scrape_date']
    if list(df.columns) != expected_cols:
        raise ValueError(f"Le fichier CSV a une structure invalide : {list(df.columns)}. Attendu : {expected_cols}")
    # Supprimer les lignes où content est NaN, vide ou juste un "?" AVANT le nettoyage
    df.dropna(subset=['content'], inplace=True)
    df = df[df['content'].str.strip().isin(["?", ""]) == False]

    # Nettoyer les commentaires restants
    df['content'] = df['content'].apply(clean_text)

    #supprimer les lignes en double pour les champs 'content' et 'author' similaire
    df = df.drop_duplicates(subset=['author', 'content'], keep='first')
    
    # Sauvegarder le CSV nettoyé
    if isinstance(output_file, (str, os.PathLike)):
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        #Supprimer le fichier s’il existe déjà (et potentiellement bloqué en lecture seule)
        if os.path.exists(output_file):
            try:
                os.remove(output_file)
            except PermissionError as e:
                print(f" Impossible de supprimer le fichier : {output_file} : {e}")
                raise
    
    rows_after = len(df)
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Fichier nettoyé sauvegardé dans {output_file}")
    return {"rows_before": rows_before, "rows_after": rows_after, "rows_removed": rows_before - rows_after}


def clean_data(input_file=None, output_file=None):
    """Nettoyage du fichier CSV des avis Trustpilot."""
               
    input_file = input_file or "/opt/airflow/project/data/avis_boutique.csv"
    output_file = output_file or "/opt/airflow/project/data/avis_boutique_clean.csv"
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"{input_file} introuvable. Lance d'abord le scraper.")
    clean_csv(input_file, output_file)
    print(f"INPUT_CSV = {input_file}")
    print(f"OUTPUT_CSV = {output_file}")

if __name__ == "__main__":
    clean_data()
