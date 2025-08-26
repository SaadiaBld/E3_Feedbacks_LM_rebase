import pytest
from unittest.mock import patch, MagicMock
from google.auth.exceptions import DefaultCredentialsError
from api.bq_connect import get_verbatims_by_date

def create_mock_row(data):
    # Configure le mock pour qu'il se comporte comme un dictionnaire
    # en réponse à l'accès par clé (ex: row['review_id'])
    row = MagicMock()
    row.__getitem__.side_effect = data.__getitem__
    return row

@patch('api.bq_connect.bigquery.Client')
def test_get_verbatims_by_date_success(mock_bq_client_class):
    """Vérifie que la fonction formate correctement les résultats de BigQuery."""
    # Arrange
    mock_bq_client = mock_bq_client_class.return_value
    mock_query_result = [
        create_mock_row({"review_id": "id1", "content": "Avis 1"}),
        create_mock_row({"review_id": "id2", "content": "Avis 2"})
    ]
    mock_bq_client.query.return_value.result.return_value = mock_query_result

    # Act
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # Assert
    mock_bq_client.query.assert_called_once()
    assert len(results) == 2
    assert results[0] == {"review_id": "id1", "content": "Avis 1"}
    assert results[1] == {"review_id": "id2", "content": "Avis 2"}

@patch('api.bq_connect.bigquery.Client')
def test_get_verbatims_by_date_no_results(mock_bq_client_class):
    """Vérifie que la fonction retourne une liste vide si la requête ne renvoie rien."""
    # Arrange
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.query.return_value.result.return_value = []

    # Act
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # Assert
    assert results == []

@patch('api.bq_connect.bigquery.Client')
def test_get_verbatims_by_date_credential_error(mock_bq_client_class):
    """Vérifie que la fonction gère bien les erreurs d'authentification GCP."""
    # Arrange
    mock_bq_client_class.side_effect = DefaultCredentialsError("Test credentials error")

    # Act
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # Assert
    assert results == []
