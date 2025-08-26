import pytest
from api.claude_interface import classify_with_claude

pytestmark = pytest.mark.integration
import os

# --- Golden Set de tests d'intégration ---
# Ces tests appellent réellement l'API Claude et nécessitent une ANTHROPIC_API_KEY valide.
# Ils sont destinés à être exécutés moins fréquemment (ex: CI nocturne, avant déploiement).

# Vérifie si la clé API est disponible pour exécuter ces tests
pytestmark = pytest.mark.skipif(
    not os.getenv("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY non définie. Les tests d'intégration Claude nécessitent une clé API réelle."
)

GOLDEN_SET = [
    {
        "verbatim": "Le service client était excellent, très réactif et à l'écoute.",
        "expected_result": [{"theme": "Service client / SAV", "note": 5.0}]
    },
    {
        "verbatim": "Produit reçu cassé et le remboursement a pris des semaines.",
        "expected_result": [
            {"theme": "Retour et remboursement", "note": 1.0},
            {"theme": "Qualité des produits", "note": 1.0}
        ]
    },
    {
        "verbatim": "Commande arrivée 1 mois aprés la date prévue, c'est indamissible!",
        "expected_result": [
            {"theme": "Livraison et retrait", "note": 1.0}
        ]
    },
    {
        "verbatim": "J'ai adoré l'expérience d'achat en ligne, le site est très intuitif.",
        "expected_result": [{"theme": "Expérience d'achat en ligne", "note": 5.0}]
    }
]

@pytest.mark.parametrize("test_case", GOLDEN_SET)
def test_claude_golden_set(test_case):
    """Teste l'intégration réelle avec Claude en utilisant un jeu de données 'golden'."""
    verbatim = test_case["verbatim"]
    expected_result = test_case["expected_result"]

    print(f"\nTesting verbatim: {verbatim}")
    actual_result = classify_with_claude(verbatim)
    print(f"Actual result: {actual_result}")

    # Assurez-vous que le résultat n'est pas None (en cas d'erreur API ou de validation)
    assert actual_result is not None

    # Vérifie que la longueur des listes de thèmes correspond
    assert len(actual_result) == len(expected_result)

    # Vérifie que chaque thème et sa note correspondent. L'ordre peut varier, donc on trie.
    # On convertit en set de tuples pour une comparaison insensible à l'ordre
    actual_set = {tuple(sorted(d.items())) for d in actual_result}
    expected_set = {tuple(sorted(d.items())) for d in expected_result}

    assert actual_set == expected_set
