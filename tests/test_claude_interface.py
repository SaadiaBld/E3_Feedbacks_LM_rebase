import pytest
from unittest.mock import patch, MagicMock
from api.claude_interface import classify_with_claude, validate_claude_response
import json

# Mock de la réponse réussie de Claude
class MockMessageContent:
    def __init__(self, text):
        self.text = text

class MockResponse:
    def __init__(self, text_content):
        self.content = [MockMessageContent(text_content)]

def test_classify_with_claude_success():
    """Teste que classify_with_claude gère une réponse réussie de Claude."""
    mock_claude_output = {
        "themes": [
            {"theme": "Service client / SAV", "note": 4.0},
            {"theme": "Qualité des produits", "note": 3.0}
        ]
    }
    mock_claude_output_str = json.dumps(mock_claude_output)

    with patch('api.claude_interface.client.messages.create') as mock_create_method:
        # Configure le mock pour retourner une réponse réussie
        mock_create_method.return_value = MockResponse(mock_claude_output_str)
        
        verbatim = "Le service était bon mais le produit un peu décevant."
        result = classify_with_claude(verbatim)
        
        # Vérifie que le résultat est bien celui attendu après validation
        assert result == [
            {"theme": "Service client / SAV", "note": 4.0},
            {"theme": "Qualité des produits", "note": 3.0}
        ]
        # Vérifie que la méthode create a bien été appelée
        mock_create_method.assert_called_once()

def test_validate_claude_response_valid_json():
    """Teste que validate_claude_response gère un JSON valide."""
    valid_json_str = json.dumps({
        "themes": [
            {"theme": "Prix et promotions", "note": 3.5}
        ]
    })
    expected_result = [{"theme": "Prix et promotions", "note": 3.5}]
    assert validate_claude_response(valid_json_str) == expected_result

def test_validate_claude_response_invalid_theme():
    """Teste que validate_claude_response rejette un thème inconnu."""
    invalid_theme_json_str = json.dumps({
        "themes": [
            {"theme": "Thème Inconnu", "note": 2.0}
        ]
    })
    assert validate_claude_response(invalid_theme_json_str) is None

def test_validate_claude_response_invalid_note():
    """Teste que validate_claude_response rejette une note invalide."""
    invalid_note_json_str = json.dumps({
        "themes": [
            {"theme": "Livraison et retrait", "note": 6.0}
        ]
    })
    assert validate_claude_response(invalid_note_json_str) is None

def test_validate_claude_response_malformed_json():
    """Teste que validate_claude_response lève une erreur pour un JSON malformé."""
    malformed_json_str = "{themes: [}"
    with pytest.raises(ValueError, match="Réponse Claude invalide"):
        validate_claude_response(malformed_json_str)

def test_validate_claude_response_missing_themes_key():
    """Teste que validate_claude_response rejette un JSON sans la clé 'themes'."""
    missing_themes_json_str = json.dumps({"other_key": []})
    assert validate_claude_response(missing_themes_json_str) is None

def test_validate_claude_response_empty_themes_list():
    """Teste que validate_claude_response retourne None pour une liste de thèmes vide."""
    empty_themes_json_str = json.dumps({"themes": []})
    assert validate_claude_response(empty_themes_json_str) is None

# --- Nouveaux tests pour validate_claude_response ---

def test_validate_claude_response_multiple_themes():
    """Teste que validate_claude_response gère correctement plusieurs thèmes."""
    json_str = json.dumps({
        "themes": [
            {"theme": "Service client / SAV", "note": 4.0},
            {"theme": "Qualité des produits", "note": 2.5}
        ]
    })
    expected = [
        {"theme": "Service client / SAV", "note": 4.0},
        {"theme": "Qualité des produits", "note": 2.5}
    ]
    assert validate_claude_response(json_str) == expected

def test_validate_claude_response_decimal_note():
    """Teste que validate_claude_response gère correctement les notes décimales."""
    json_str = json.dumps({
        "themes": [
            {"theme": "Prix et promotions", "note": 3.75}
        ]
    })
    expected = [{"theme": "Prix et promotions", "note": 3.75}]
    assert validate_claude_response(json_str) == expected

def test_validate_claude_response_boundary_notes():
    """Teste que validate_claude_response gère les notes aux limites (1.0 et 5.0)."""
    json_str = json.dumps({
        "themes": [
            {"theme": "Livraison et retrait", "note": 1.0},
            {"theme": "Expérience d'achat en ligne", "note": 5.0}
        ]
    })
    expected = [
        {"theme": "Livraison et retrait", "note": 1.0},
        {"theme": "Expérience d'achat en ligne", "note": 5.0}
    ]
    assert validate_claude_response(json_str) == expected

def test_validate_claude_response_themes_not_list():
    """Teste que validate_claude_response rejette si 'themes' n'est pas une liste."""
    json_str = json.dumps({"themes": "not_a_list"})
    assert validate_claude_response(json_str) is None

def test_validate_claude_response_item_not_dict():
    """Teste que validate_claude_response rejette si un élément de thème n'est pas un dict."""
    json_str = json.dumps({"themes": ["not_a_dict"]})
    assert validate_claude_response(json_str) is None

def test_validate_claude_response_missing_theme_key_in_item():
    """Teste que validate_claude_response rejette si un élément de thème manque la clé 'theme'."""
    json_str = json.dumps({"themes": [{"note": 3.0}]})
    assert validate_claude_response(json_str) is None

def test_validate_claude_response_missing_note_key_in_item():
    """Teste que validate_claude_response rejette si un élément de thème manque la clé 'note'."""
    json_str = json.dumps({"themes": [{"theme": "Prix et promotions"}]})
    assert validate_claude_response(json_str) is None

def test_validate_claude_response_note_not_number():
    """Teste que validate_claude_response rejette si la note n'est pas un nombre."""
    json_str = json.dumps({"themes": [{"theme": "Prix et promotions", "note": "trois"}]})
    assert validate_claude_response(json_str) is None

def test_validate_claude_response_empty_string_input():
    """Teste que validate_claude_response lève une erreur pour une chaîne vide."""
    with pytest.raises(ValueError, match="Réponse Claude invalide"):
        validate_claude_response("")

def test_validate_claude_response_non_json_string_input():
    """Teste que validate_claude_response lève une erreur pour une chaîne non-JSON."""
    with pytest.raises(ValueError, match="Réponse Claude invalide"):
        validate_claude_response("Ceci n'est pas du JSON")
