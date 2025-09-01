import pandas as pd
import pytest
from io import StringIO
from scripts_data.cleaner import clean_csv

# Données d'exemple pour les tests
CSV_VALID_DATA = (
    "review_id,rating,content,author,publication_date,scrape_date\n"
    "1,5,Super produit!,Alice,2024-01-01,2024-01-02\n"
    "2,1,Nul,Bob,2024-01-02,2024-01-02"
)

CSV_WITH_EMPTY_LINES = (
    "review_id,rating,content,author,publication_date,scrape_date\n"
    "1,5,Super!,Alice,2024-01-01,2024-01-02\n"
    "2,3,,Bob,2024-01-01,2024-01-02\n"  # Ligne avec contenu vide
    "3,4,OK,Charlie,2024-01-01,2024-01-02\n"
)

def test_clean_csv_output_structure():
    """Vérifie que la structure du CSV de sortie est correcte."""
    # ici ce qui se passe c'est qu'on simule des fichiers en mémoire avec StringIO
    input_file = StringIO(CSV_VALID_DATA)
    output_file = StringIO()

    # on va appeler la fonction de nettoyage
    clean_csv(input_file, output_file)
    output_file.seek(0)  # Revenir au début du "fichier" en mémoire pour le lire
    df = pd.read_csv(output_file)

    # Assert
    expected_columns = ['review_id', 'rating', 'content', 'author', 'publication_date', 'scrape_date']
    assert list(df.columns) == expected_columns
    assert not df.isnull().any().any(), "Le fichier nettoyé ne devrait pas contenir de valeurs nulles"
    assert len(df) == 2

def test_clean_csv_removes_empty_lines():
    """Vérifie que les lignes avec un contenu vide sont bien supprimées."""
    # on va simuler des fichiers en mémoire avec StringIO. 
    # StringIO permet de créer un "fichier" en mémoire
    input_file = StringIO(CSV_WITH_EMPTY_LINES)
    output_file = StringIO()

    # appel de la fonction de nettoyage
    clean_csv(input_file, output_file)
    output_file.seek(0) # Revenir au début du "fichier" en mémoire pour le lire
    df = pd.read_csv(output_file)

    # on va vérifier que la ligne avec le contenu vide a été supprimée
    assert len(df) == 2, "La ligne avec le contenu vide aurait dû être supprimée"
    assert not df['content'].isnull().any(), "La colonne content ne devrait pas avoir de nuls"
