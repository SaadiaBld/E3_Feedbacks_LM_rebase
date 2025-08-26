# on mocke dans ce fichier les variables d'environnement et les appels os.path.isfile
# pour simuler un environnement de test sans dépendances externes
# project/tests/conftest.py

import pytest
from unittest.mock import patch

@pytest.fixture(autouse=True, scope="session")
def patch_env_and_files():
    """
    Patch global pour éviter l'erreur de credentials GCP lors de l'import des modules.
    """
    with patch("os.path.isfile", return_value=True),patch("os.path.exists", return_value=True),          patch("os.getenv", side_effect=lambda k, d=None: "dummy_anthropic_key" if k == "ANTHROPIC_API_KEY" else ("dummy" if "GOOGLE_APPLICATION_CREDENTIALS" in k or "PROJECT_ID" in k else d)),          patch("dotenv.load_dotenv", return_value=True):
        yield