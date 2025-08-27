# Feedbacks_LM

## Description

Ce projet vise à scraper automatiquement les avis clients de Trustpilot pour une grande enseigne, nettoyer les données, puis les stocker pour analyses (LLM, résumé de texte, analyse de sentiments...).

Le but est aussi d'automatiser la récupération des avis via Google Cloud Platform (GCP) chaque semaine.


## Fonctionnalités principales

    Scraping multi-pages d'avis Trustpilot.

    Extraction des informations cibles et nettoyage.

    Export vers fichier .csv prêt pour traitement en IA.


## Technologies utilisées

    Python 3.11+

    Librairies :

        requests

        beautifulsoup4

        pandas

        re

    Git / GitHub

    Google Cloud Platform (prochainement)

## Comment utiliser ce projet

Cloner le dépôt :
```bash
git clone https://github.com/SaadiaBld/Feedbacks_LM.git
cd Feedbacks_LM
```

Créer un environnement virtuel (recommandé) :
```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows
```

Installer les dépendances :
```bash
pip install -r requirements.txt
```

Lancer le scraping :
```bash
python scraper.py
```

Nettoyer les données :
```bash
python cleaner.py
```

## 🧪 Tests

Ce projet est couvert par une suite de tests unitaires et d'intégration. Les tests sont conçus pour être lancés **à l'intérieur de l'environnement Docker** pour garantir la cohérence.

Le fichier de configuration `pytest.ini` est pré-configuré pour :
- Ignorer les tests internes des librairies présentes dans le dossier `archives/`.
- Exclure par défaut les tests d'intégration avec l'API Claude (`-k "not claude"`) pour éviter les coûts involontaires.

### Lancer les tests dans Docker

**Étape 1 : Démarrer les conteneurs**

Assurez-vous que vos conteneurs sont démarrés en arrière-plan. Si vous venez de modifier le `docker-compose.yaml`, utilisez `--force-recreate`.
```bash
docker compose up -d --force-recreate
```

**Étape 2 : Exécuter les commandes de test**

Toutes les commandes suivantes s'exécutent depuis la racine de votre projet.

**1. Lancer les tests "sûrs" (tout sauf Claude)**

Cette commande exécute tous les tests unitaires et d'intégration qui n'engendrent pas de coûts. C'est la commande à utiliser le plus souvent.
```bash
docker compose exec webserver pytest
```
*(Note : les options `-v` et `-k "not claude"` sont appliquées automatiquement grâce au `pytest.ini`)*

**2. Lancer UNIQUEMENT les tests d'intégration Claude**

Cette commande ne lance que les tests payants. Vous devez fournir votre clé d'API.
```bash
docker compose exec -e ANTHROPIC_API_KEY="votre_cle_api" webserver pytest -k "claude"
```

## 📈 Monitoring

La documentation complète du monitoring est disponible ici :
➡️ [monitoring/C11_monitoring_model.md](monitoring/C11_monitoring_model.md)