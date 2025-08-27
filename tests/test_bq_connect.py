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

@patch('api.bq_connect.bigquery.Client') #patch sert à remplacer une classe ou une fonction par un mock qui va simuler son comportement
def test_get_verbatims_by_date_success(mock_bq_client_class):
    """Vérifie que la fonction formate correctement les résultats de BigQuery."""
    # on va simuler le client BigQuery et la méthode query
    mock_bq_client = mock_bq_client_class.return_value
    # créé des résultats fictifs
    mock_query_result = [
        create_mock_row({"review_id": "id1", "content": "Avis 1"}),
        create_mock_row({"review_id": "id2", "content": "Avis 2"})
    ]
    #on simule le retour de la méthode query().result()
    mock_bq_client.query.return_value.result.return_value = mock_query_result

    # appel de la fonction à tester
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # ici, on vérifie que la méthode query a été appelée une fois
    # et que les résultats sont bien formatés comme attendu
    mock_bq_client.query.assert_called_once()
    assert len(results) == 2
    assert results[0] == {"review_id": "id1", "content": "Avis 1"}
    assert results[1] == {"review_id": "id2", "content": "Avis 2"}

@patch('api.bq_connect.bigquery.Client')
def test_get_verbatims_by_date_no_results(mock_bq_client_class):
    """Vérifie que la fonction retourne une liste vide si la requête ne renvoie rien."""
    # on simule le client BigQuery et la méthode query afin qu'elle retourne une liste vide
    mock_bq_client = mock_bq_client_class.return_value
    mock_bq_client.query.return_value.result.return_value = []

    # on appelle la fonction à tester et on vérifie qu'elle retourne une liste vide
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # on verifie que results est bien une liste vide
    assert results == []

@patch('api.bq_connect.bigquery.Client')
def test_get_verbatims_by_date_credential_error(mock_bq_client_class):
    """Vérifie que la fonction gère bien les erreurs d'authentification GCP."""
    # idem que précédemment, mais cette fois on simule une erreur d'authentification
    mock_bq_client_class.side_effect = DefaultCredentialsError("Test credentials error")

    # appel de la fonction à tester
    results = get_verbatims_by_date(scrape_date="2024-01-01")

    # on verifie que results est bien une liste vide
    assert results == []
