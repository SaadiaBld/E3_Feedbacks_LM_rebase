from metrics import (
    VERBATIMS_ANALYZED,
    ERRORS_JSON,
    ANALYSIS_DURATION,
    VERBATIM_SIZE_BUCKET,
    CURRENT_VERBATIM_SIZE,
    CLAUDE_EMPTY_RESPONSES,
    NEW_TOPICS_DETECTED,
    BQ_INSERT_ERRORS,
    CLAUDE_SUCCESS_RATIO,
    monitor_start
)
import time

monitor_start(port=8000)

#le fichier exporter me permet de definir les metriques qui m'interessent et que je veux suivre grace au monitoring 
#on utilise la lirbraiie prometheus_client; les metriques sont exposées sur le port 8000 via /metrics

while True:
    time.sleep(10)