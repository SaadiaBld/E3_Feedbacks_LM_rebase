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

## 📈 Monitoring

La documentation complète du monitoring est disponible ici :
➡️ [monitoring/C11_monitoring_model.md](monitoring/C11_monitoring_model.md)