import anthropic, os, json, logging, unicodedata, re
from .prompt_utils import build_prompt, THEMES
from typing import Optional, List, Dict, Union
from google.cloud import secretmanager # Added import

# Assurez-vous que ANTHROPIC_API_KEY est définie dans l'environnement GCP.

# Ajoutez cette fonction quelque part en haut du fichier, après les imports
def assert_ascii_headers(headers_dict: dict):
    for k, v in headers_dict.items():
        try:
            (v or "").encode("ascii")
        except UnicodeEncodeError as e:
            raise RuntimeError("Header non-ASCII: " + k + "=" + repr(v) + " → " + str(e))

def _sanitize_ascii(s: str) -> str:
    # Normalise, enlève accents et chars non ASCII "visibles"
    s_norm = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s_norm = s_norm.strip()
    # Sécurité: interdit espaces internes/guillemets dans une API key
    if re.search(r'[\s"\'<>]', s_norm):
        raise ValueError("API key contient des caractères interdits (espaces/guillemets).")
    # Vérification ASCII stricte (devrait passer après normalisation)
    s_norm.encode("ascii")
    return s_norm


# === Récupération de la clé API Anthropic depuis Secret Manager ===
SECRET_ID = "ANTHROPIC_API_KEY"
PROJECT_ID = os.getenv("GCP_PROJECT") # Ou os.getenv("GOOGLE_CLOUD_PROJECT")

if not PROJECT_ID:
    raise EnvironmentError("La variable d'environnement GCP_PROJECT n'est pas définie. Elle est nécessaire pour accéder à Secret Manager.")

logger = logging.getLogger("claude_logger") # Existing logger
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

try:
    client_sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client_sm.access_secret_version(request={"name": name})
    raw_key = response.payload.data.decode("UTF-8")
    api_key = _sanitize_ascii(raw_key)
    logger.info("Clé API Anthropic récupérée et validée (ASCII).")
except Exception as e:
    logger.error(f"Erreur lors de la récupération/validation de la clé API : {e}", exc_info=True)
    raise EnvironmentError(f"Impossible d'utiliser la clé API Anthropic : {e}")

# Headers ASCII only (évite tout caractère accentué dans User-Agent)
SAFE_HEADERS = {"User-Agent": "trustpilot-pipeline/1.0"}

# Appelez la fonction d'assertion avant d'initialiser le client
assert_ascii_headers(SAFE_HEADERS) # <-- AJOUT DE CETTE LIGNE

client = anthropic.Anthropic(
    api_key=api_key, # On utilise la clé déjà récupérée
    default_headers=SAFE_HEADERS,
)
THEME_LABELS = {t["nom"] for t in THEMES}


def classify_with_claude(verbatim: str) -> Optional[List[Dict[str, Union[str, float]]]]:
    prompt = build_prompt(verbatim)
    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=500,
            temperature=0,
            system="Tu es un assistant d'analyse de satisfaction client.",
            messages=[{"role": "user", "content": prompt}]
        )

        content = response.content[0].text.strip()
        logger.info(f"Réponse Claude brute : {content}")

        validated = validate_claude_response(content)
        if not validated:
            logger.warning(f"Réponse non valide : {content}")
        return validated

    except Exception as e:
        logger.error(f"❌ Erreur API Claude : {e}")
        raise



def validate_claude_response(response_text: str) -> Optional[List[Dict[str, Union[str, float]]]]:
    try:
        data = json.loads(response_text)

        if "themes" not in data or not isinstance(data["themes"], list):
            logger.warning(f"⚠️ Clé 'themes' manquante ou invalide : {response_text}")
            return None

        results = []
        for item in data["themes"]:
            if not isinstance(item, dict):
                logger.warning(f"⚠️ Élément non structuré : {item}")
                continue

            theme = item.get("theme")
            note = item.get("note")

            if theme not in THEME_LABELS:
                logger.warning(f"⚠️ Thème inconnu : {theme}")
                continue

            if not isinstance(note, (int, float)) or not (1 <= note <= 5):
                logger.warning(f"⚠️ Note invalide pour {theme} : {note}")
                continue

            results.append({"theme": theme, "note": note})

        return results if results else None

    except json.JSONDecodeError as e:
        logger.error(f"❌ Erreur JSON : {e} dans : {response_text}")
        raise ValueError("Réponse Claude invalide")
