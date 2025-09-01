# exporter.py: lance un serveur HTTP pour exposer les métriques Prometheus sur le port 8000 pour y afficher les métriques en continu 
from prometheus_client import REGISTRY, Counter, push_to_gateway, delete_from_gateway
from metrics import (
    VERBATIMS_ANALYZED,
    ERRORS_JSON,
    ANALYSIS_DURATION,
    VERBATIM_LENGTH,
    CURRENT_VERBATIM_SIZE,
    CLAUDE_EMPTY_RESPONSES,
    NEW_TOPICS_DETECTED,
    BQ_INSERT_ERRORS,
    VERBATIMS_SKIPPED,
    CLAUDE_CALLS,
)

JOB_NAME = "verbatim_pipeline"
GROUPING = {"instance": socket.gethostname()}  # ajoute d'autres clés utiles: {"scrape_date":"2024-01-01"}

def run_batch():
    # --- ton traitement ici ---
    # démo: on “simule” quelques métriques
    VERBATIMS_ANALYZED.inc(12)
    CURRENT_VERBATIM_SIZE.set(180)
    VERBATIM_LENGTH.observe(180)
    CLAUDE_CALLS.labels(status="success").inc(12)

    # push final (atomique) :
    push_to_gateway("http://pushgateway:9091", job=JOB_NAME, registry=REGISTRY, grouping_key=GROUPING)

    # (optionnel) si tu NE veux PAS laisser d’anciennes séries visibles :
    delete_from_gateway("http://pushgateway:9091", job=JOB_NAME, grouping_key=GROUPING)

if __name__ == "__main__":
    run_batch()

# # Démarre le serveur d'export 
# monitor_start(port=8000)

# # Boucle infinie pour garder le script en vie et permettre la consultation des métriques
# while True:
#     time.sleep(10)