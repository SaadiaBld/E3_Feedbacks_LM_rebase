import anthropic, os, json, logging, sys
from dotenv import load_dotenv
from pathlib import Path
from .prompt_utils import build_prompt, THEMES

#dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')

dotenv_path = Path(__file__).resolve().parent.parent / ".env"
if os.getenv("PYTEST_RUNNING"):
    # Les variables d'environnement seront mockées par pytest
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
else:
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
    else:
        raise FileNotFoundError(f"Le fichier .env est introuvable à l'emplacement : {dotenv_path}")

api_key = os.getenv("ANTHROPIC_API_KEY") or ""

client = anthropic.Anthropic(api_key=api_key, timeout=30.0)
THEME_LABELS = {t["nom"] for t in THEMES}

# Log setup
logger = logging.getLogger("claude_logger")
logger.setLevel(logging.INFO)
logger.propagate = False

# Ne pas créer de fichier de log pendant les tests pour éviter les problèmes de permission
if 'pytest' not in sys.modules:
    handler = logging.FileHandler("claude_errors.log", encoding="utf-8")
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

# Fonction principale pour classifier les verbatims avec Claude
def classify_with_claude(verbatim: str) -> list[dict] | None:
    prompt = build_prompt(verbatim)
    try:
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=500,
            temperature=0,
            system="Tu es un assistant d’analyse de satisfaction client.",
            messages=[{"role": "user", "content": prompt}]
        )

        content = response.content[0].text.strip()
        
        logger.info(f"Réponse brute de Claude : {content}")

        validated = validate_claude_response(content)
        if not validated:
            logger.warning(f"Réponse non valide : {content}")
        return validated

    except Exception as e:
        logger.error(f"Erreur API Claude : {e}")
        raise



def validate_claude_response(response_text: str) -> list[dict] | None:
    try:
        data = json.loads(response_text)

        if "themes" not in data or not isinstance(data["themes"], list):
            logger.warning(f"Clé 'themes' manquante ou invalide dans : {response_text}")
            return None

        results = []

        for item in data["themes"]:
            if not isinstance(item, dict):
                logger.warning(f"Item non structuré correctement : {item}")
                continue

            theme = item.get("theme")
            note = item.get("note")

            if theme not in THEME_LABELS:
                logger.warning(f"Thème inconnu : {theme}")
                continue

            if not isinstance(note, (int, float)) or not (1 <= note <= 5):
                logger.warning(f"Note invalide pour {theme} : {note}")
                continue

            results.append({"theme": theme, "note": note})

        return results if results else None

    except json.JSONDecodeError as e:
        logger.error(f"Erreur JSON : {e} dans : {response_text}")
        raise ValueError("Réponse Claude invalide")
