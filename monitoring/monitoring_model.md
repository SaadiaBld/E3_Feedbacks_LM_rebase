# 🧠 C11 — Monitorer un modèle d'intelligence artificielle

## 🎯 Objectif de cette documentation

Cette documentation vise à démontrer que la chaîne de monitorage du modèle d’analyse des verbatims est **en état de marche**, et répond aux critères de la compétence **C11** du référentiel Data Engineer RNCP.

---

## 📊 Métriques monitorées

Les métriques collectées et exposées via Prometheus sont :

| Nom Prometheus                        | Description |
|--------------------------------------|-------------|
| `verbatims_analyzed_total`           | Total de verbatims analysés par Claude |
| `verbatims_skipped_total`            | Verbatims ignorés (trop courts, doublons, etc.) |
| `verbatim_analysis_duration_seconds` | Durée d’analyse d’un verbatim (summary) |
| `errors_json_total`                  | Nombre d’erreurs JSON dans la réponse Claude |
| `claude_response_empty_total`        | Réponses vides retournées par Claude |
| `current_verbatim_size`              | Taille en caractères du dernier verbatim |
| `verbatims_by_length_bucket`         | Répartition des tailles de verbatims (court, moyen, long) |
| `new_topics_detected_total`          | Nouveaux thèmes non présents dans la base de référence |
| `bq_insert_errors_total`             | Erreurs lors de l’insertion dans BigQuery |
| `claude_call_success_ratio`          | Ratio de succès des appels Claude (%) |

📌 Chaque métrique est mise à jour automatiquement à chaque passage dans le pipeline d’analyse.

---

## 🗂️ Arborescence utile au monitoring

```bash
monitoring/
│
├── exporter.py               # Expose les métriques personnalisées
├── prometheus.yml            # Configuration du scraping Prometheus
├── Dockerfile                # Image Docker dédiée à l’exporter

```

## 🧬 Deux mécanismes complémentaires de collecte

Le projet utilise deux méthodes de collecte Prometheus, adaptées aux différents besoins :

| Méthode         | Description |
|----------------|-------------|
| `exporter.py`  | Expose les métriques globales et continues sur `/metrics` (serveur Python HTTP sur le port `8000`) |
| `PushGateway`  | Permet de "pousser" des métriques ponctuelles à la fin des tâches batchs Airflow |

📌 Ces deux méthodes sont complémentaires : l’exporter sert à mesurer les tendances, la gateway sert à suivre l’exécution ponctuelle des DAGs.


## 🛠️ Outils utilisés

| Composant         | Rôle |
|-------------------|------|
| `prometheus_client` | Collecte et expose les métriques depuis Python |
| **Pushgateway**    | Intermédiaire entre le job Airflow et Prometheus |
| **Prometheus**     | Agrège, stocke et interroge les métriques |
| **Grafana**        | Affiche les métriques sur des dashboards interactifs |
| **Airflow**        | Orchestration des tâches (Scraping, Nettoyage, Analyse, Insertion) |

---

## ♿ Accessibilité et diffusion

- **Grafana** a été choisi pour sa compatibilité avec les standards d’accessibilité (navigation clavier, contrastes, etc.)
- La documentation est disponible au format Markdown, lisible par tous les éditeurs, convertibles en PDF ou HTML.
- Le dashboard est **accessibles via navigateur local** pour tous les intervenants du projet.

---

## 🧪 Environnement de test

La chaîne de monitorage a d'abord été **testée dans un environnement bac à sable Docker**, comprenant :
services:
prometheus
grafana
pushgateway
exporter (python)
airflow (webserver + scheduler)

## 📦 Installation de la chaîne de monitoring

1. Cloner le dépôt :
    ```bash
   git clone https://github.com/...
   ```

2. Lancer les services
    ```bash
    docker compose up --build
    ```

3. Accéder aux interfaces :

    Airflow : http://localhost:8080

    Prometheus : http://localhost:9090

    Grafana : http://localhost:3000

    PushGateway Prometheus : http://localhost:9091/

Notes complémentaires

    Le fichier .env doit être bien monté dans les conteneurs Airflow pour activer SCRAPER_MODE=prod

    Le fichier monitoring/exporter.py expose les métriques en continu via start_http_server(8000)

    La configuration Prometheus est dans monitoring/prometheus.yml