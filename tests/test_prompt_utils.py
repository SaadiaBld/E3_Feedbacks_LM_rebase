import pytest
from api.prompt_utils import build_prompt

def test_build_prompt_inserts_verbatim():
    """Vérifie que l'avis client (verbatim) est correctement inséré dans le prompt."""
    verbatim_input = "Le service client était fantastique et a résolu mon problème rapidement."
    
    expected_verbatim_format = f"""
"{verbatim_input}"""
    
    # On génère le prompt
    actual_prompt = build_prompt(verbatim_input)
    
    # On vérifie que le verbatim formaté est bien présent dans le résultat
    assert expected_verbatim_format in actual_prompt

def test_build_prompt_contains_all_themes():
    """Vérifie que tous les thèmes définis sont listés dans le prompt."""
    # Importe les thèmes depuis le module pour les comparer
    from api.prompt_utils import THEMES

    # Le verbatim n'a pas d'importance pour ce test
    verbatim_input = "test"
    actual_prompt = build_prompt(verbatim_input)

    # Vérifie que le nom de chaque thème est présent dans le prompt généré
    for theme in THEMES:
        assert theme['nom'] in actual_prompt
