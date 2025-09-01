from prometheus_client import Counter, Gauge, Histogram, start_http_server, delete_from_gateway
from prometheus_client import REGISTRY, push_to_gateway
import socket

# -------------------------
# MÉTRIQUES PRINCIPALES
# -------------------------
# Total de verbatims analysés
VERBATIMS_ANALYZED = Counter('verbatims_analyzed_total', 'Total des verbatims analysés')

# Durée de traitement d’un verbatim
ANALYSIS_DURATION = Histogram(
    'verbatim_analysis_duration_seconds',
    'Durée d’analyse d’un verbatim (s)',
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10)
)
# Erreurs de parsing JSON dans la réponse de Claude
ERRORS_JSON = Counter('errors_json_total', "Nombre d'erreurs JSON")

# Nombre de verbatims avec réponse vide (None) de Claude
CLAUDE_EMPTY_RESPONSES = Counter('claude_response_empty_total', "Réponses vides retournées par Claude")

# Taille du dernier verbatim (en nombre de caractères)
CURRENT_VERBATIM_SIZE = Gauge('current_verbatim_size', 'Taille du verbatim actuellement analysé')

# Histogramme des tailles de verbatims (par classe)
VERBATIM_LENGTH = Histogram(
    "verbatim_length_chars",
    "Longueur du verbatim (en caractères)",
    buckets=(50, 100, 200, 300, 500, 800, 1200, 2000)
)
# Nouveaux thèmes détectés non présents dans la table topics
NEW_TOPICS_DETECTED = Counter('new_topics_detected_total', 'Nouveaux thèmes non reconnus par le modèle')

# Erreurs à l’insertion dans BigQuery
BQ_INSERT_ERRORS = Counter('bq_insert_errors_total', "Erreurs survenues lors de l'insertion dans BigQuery")

# Verbatims ignorés (trop courts, déjà traités, etc.)
VERBATIMS_SKIPPED = Counter('verbatims_skipped_total', "Verbatims ignorés dans le pipeline")

# appels à Claude (total et par statut : succès, erreur) 
CLAUDE_CALLS = Counter("claude_calls_total", "Appels à Claude", ["status"])

# -------------------------
# FONCTION D’EXPORT SERVER
# -------------------------

def monitor_start(port=8000):
    """ Démarre le serveur HTTP pour exporter les métriques Prometheus.
    Par défaut, écoute sur le port 8000."""
    try:
        # on teste si le port est déjà utilisé 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #socket permet de créer une connexion réseau 
            if s.connect_ex(("localhost", port)) != 0: #si ==0 alors le port est utilisé
                start_http_server(port)
                print(f"Exporter Prometheus sur http://localhost:{port}/metrics")
            else:
                print(f"Exporter déjà en cours sur le port {port}")
    except Exception as e:
        print(f"Impossible de démarrer Prometheus : {e}")


# -------------------------
# LOGIQUE DE MISE À JOUR DES MÉTRIQUES
# -------------------------

def log_analysis_metrics(verbatim_text: str, duration: float, error=False, empty=False, new_topics=None, bq_error=False):
    """
    Met à jour les métriques Prometheus après le traitement d’un verbatim.
    
    - verbatim_text : texte du verbatim
    - duration : durée d’analyse
    - error : True si erreur JSON
    - empty : True si réponse vide de Claude
    - new_topics : liste de thèmes non reconnus
    - bq_error : True si insertion BQ échouée
    """
    #VERBATIMS_ANALYZED.inc()
    ANALYSIS_DURATION.observe(duration)

    # Taille du verbatim (en brut)
    size = len(verbatim_text)
    CURRENT_VERBATIM_SIZE.set(size)

    VERBATIM_LENGTH.observe(size)

    if error:
        ERRORS_JSON.inc()
        CLAUDE_CALLS.labels(status="error").inc()
    elif empty:
        CLAUDE_EMPTY_RESPONSES.inc()
        CLAUDE_CALLS.labels(status="error").inc()
    else:
        CLAUDE_CALLS.labels(status="success").inc()

    if new_topics:
        NEW_TOPICS_DETECTED.inc(len(new_topics))

    if bq_error:
        BQ_INSERT_ERRORS.inc()

    print(f" VERBATIMS_ANALYZED avant: {VERBATIMS_ANALYZED._value.get()}")
    VERBATIMS_ANALYZED.inc()
    print(f" VERBATIMS_ANALYZED après: {VERBATIMS_ANALYZED._value.get()}")


def push_metrics_to_gateway(job_name="verbatim_pipeline", instance="dev"):
    # 1) on nettoie le groupe précédent (même job/instance)
    delete_from_gateway(
        'http://pushgateway:9091',
        job=job_name,
        grouping_key={'instance': instance},
    )
    # 2) on pousse le snapshot du run
    try:
        push_to_gateway(
            'http://pushgateway:9091',
            job=job_name,
            registry=REGISTRY,
            grouping_key={'instance': instance},
        )
        print(f" Métriques poussées vers le PushGateway pour le job : {job_name}, instance: {instance}")
    except Exception as e:
        print(f" Erreur lors du push Prometheus : {e}")

