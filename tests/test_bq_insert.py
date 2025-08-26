#Tester l'integration des données dans big query - tests d'intégration simulés
import uuid, pytest, os
from unittest.mock import patch
from api.analyze_and_insert import insert_topic_analysis

pytestmark = pytest.mark.integration

@pytest.mark.skipif(not os.getenv("PROJECT_ID"), reason="PROJECT_ID manquant pour ce test")
@patch("api.analyze_and_insert.bigquery.Client")
def test_insert_topic_analysis_valid(mock_bq_client_class):
    # Mock du client et du retour de insert_rows_json
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []  # pas d'erreur

    # Input
    review_id = "review123"
    theme_scores = [
        {"theme": "Livraison et retrait", "note": 1},
        {"theme": "Retour et remboursement", "note": 3}
    ]
    label_to_id = {
        "Livraison et retrait": 101,
        "Retour et remboursement": 102
    }

    insert_topic_analysis(review_id, theme_scores, label_to_id)

    # Vérification des appels
    assert mock_bq_client.insert_rows_json.called
    args, _ = mock_bq_client.insert_rows_json.call_args

    # Vérifie la table ciblée
    assert args[0] == "trustpilot-satisfaction.reviews_dataset.topic_analysis"

    # Vérifie que chaque ligne contient les bonnes clés
    rows = args[1]
    assert isinstance(rows, list)
    assert all("review_id" in row and "topic_id" in row for row in rows)
    assert rows[0]["review_id"] == "review123"
    assert rows[0]["topic_id"] == 101
    assert rows[1]["topic_id"] == 102


# Test d'insertion avec un thème inconnu
@patch("api.analyze_and_insert.bigquery.Client")
def test_insert_topic_analysis_unknown_theme(mock_bq_client_class):
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []
    review_id = "review456"
    theme_scores = [
        {"theme": "Livraison et retrait", "note": 1},
        {"theme": "Service client", "note": 4}  # Thème inconnu
    ]
    label_to_id = {
        "Livraison et retrait": 101
    }
    insert_topic_analysis(review_id, theme_scores, label_to_id)
    # Vérifie que l'insertion n'a pas été appelée pour le thème inconnu
    assert mock_bq_client.insert_rows_json.call_count == 1


# Test d'insertion avec aucun thème détecté
@patch("api.analyze_and_insert.bigquery.Client")
def test_insert_topic_analysis_no_themes(mock_bq_client_class):
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []
    review_id = "review789"
    theme_scores = []
    label_to_id = {
        "Livraison et retrait": 101,
        "Retour et remboursement": 102
    }
    insert_topic_analysis(review_id, theme_scores, label_to_id)
    # Vérifie que l'insertion n'a pas été appelée
    assert mock_bq_client.insert_rows_json.call_count == 0


# Test d'insertion avec des notes invalides
@patch("api.analyze_and_insert.bigquery.Client")
def test_insert_topic_analysis_invalid_notes(mock_bq_client_class):
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []
    review_id = "review101"
    theme_scores = [
        {"theme": "Livraison et retrait", "note": 6},  # Note invalide
        {"theme": "Retour et remboursement", "note": -1}  # Note invalide
    ]
    label_to_id = {
        "Livraison et retrait": 101,
        "Retour et remboursement": 102
    }
    insert_topic_analysis(review_id, theme_scores, label_to_id)
    # Vérifie que l'insertion n'a pas été appelée pour les notes invalides
    assert mock_bq_client.insert_rows_json.call_count == 0

# Test d'insertion avec des notes valides
@patch("api.analyze_and_insert.bigquery.Client")
def test_insert_topic_analysis_valid_notes(mock_bq_client_class):
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.insert_rows_json.return_value = []

    review_id = "review102"
    theme_scores = [
        {"theme": "Livraison et retrait", "note": 4.5},
        {"theme": "Retour et remboursement", "note": 3.0}
    ]
    label_to_id = {
        "Livraison et retrait": 101,
        "Retour et remboursement": 102
    }

    insert_topic_analysis(review_id, theme_scores, label_to_id)

    assert mock_bq_client.insert_rows_json.call_count == 1

    # Vérifie le contenu des données insérées
    rows = mock_bq_client.insert_rows_json.call_args[0][1]
    assert rows[0]["review_id"] == "review102"
    assert rows[0]["score_0_1"] == round((5 - 4.5) / 4, 2)
    assert rows[0]["label_sentiment"] == "Très positif"

    assert rows[1]["topic_id"] == 102
    assert rows[1]["score_0_1"] == round((5 - 3.0) / 4, 2)
    assert rows[1]["label_sentiment"] == "Neutre"



