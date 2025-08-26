import pytest
from unittest.mock import patch
from api import classify

@patch('api.classify.classify_with_claude')
@patch('api.classify.get_verbatims_from_bq')
def test_run_calls_dependencies(mock_get_verbatims, mock_classify_claude):
    """
    Vérifie que la fonction run appelle bien ses dépendances : 
    1. Récupérer les verbatims.
    2. Appeler Claude pour chaque verbatim.
    """
    # Arrange: Simuler le retour de la fonction qui récupère les verbatims
    mock_verbatims = [
        "Ceci est le premier avis.",
        "Ceci est le deuxième avis."
    ]
    mock_get_verbatims.return_value = mock_verbatims

    # Simuler le retour de la fonction de classification
    mock_classify_claude.return_value = {
        "themes": [{"theme": "Test", "note": 3.0}],
        "note": 3.0
    }

    # Act: Lancer la fonction à tester
    classify.run(scrape_date="2024-01-01")

    # Assert: Vérifier que les fonctions ont été appelées correctement
    mock_get_verbatims.assert_called_once_with(scrape_date="2024-01-01")
    assert mock_classify_claude.call_count == len(mock_verbatims)
    mock_classify_claude.assert_any_call("Ceci est le premier avis.")
    mock_classify_claude.assert_any_call("Ceci est le deuxième avis.")

@patch('api.classify.classify_with_claude')
@patch('api.classify.get_verbatims_from_bq')
def test_run_no_verbatims(mock_get_verbatims, mock_classify_claude):
    """
    Vérifie que la fonction de classification n'est pas appelée si aucun verbatim n'est retourné.
    """
    # Arrange: Simuler un retour vide
    mock_get_verbatims.return_value = []

    # Act
    classify.run(scrape_date="2024-01-01")

    # Assert
    mock_get_verbatims.assert_called_once_with(scrape_date="2024-01-01")
    mock_classify_claude.assert_not_called()
