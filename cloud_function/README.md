# Cloud Function

Ce dossier contient le code d'une Cloud Function Google Cloud, conçue pour être déployée en tant que service serverless. La fonction principale est de traiter et d'analyser des données de manière automatisée en réponse à des événements ou des requêtes HTTP.

## Contenu du dossier

- **`main.py`**: Ce fichier est le point d'entrée de la Cloud Function. Il contient la logique principale qui est exécutée lorsque la fonction est déclenchée. Il orchestre les appels aux autres modules et services.

- **`requirements.txt`**: Ce fichier liste toutes les dépendances Python nécessaires pour que la fonction s'exécute correctement. Lors du déploiement, Google Cloud installe automatiquement ces dépendances.

- **`function.zip`**: Il s'agit d'une archive compressée contenant le code source de la fonction et ses dépendances. Ce fichier est utilisé pour déployer la fonction sur Google Cloud.

- **`api/`**: Ce sous-dossier contient les modules liés à l'interaction avec des API externes, notamment :
  - **`claude_interface.py`**: Pour communiquer avec l'API de Claude pour l'analyse de texte.
  - **`bq_connect.py`** et **`bq_insert_clean_data.py`**: Pour la connexion et l'insertion de données dans BigQuery.

- **`config/`**: Ce dossier est destiné à contenir les fichiers de configuration, tels que les clés d'API ou les identifiants de projet. **Note :** Les fichiers sensibles de ce dossier sont ignorés par Git pour des raisons de sécurité.

- **`scripts_data/`**: Ce dossier contient des scripts pour le traitement des données, comme le nettoyage (`cleaner.py`) et le scraping (`scraper.py`).

## Déploiement

Pour déployer cette fonction, vous pouvez utiliser la Google Cloud CLI (`gcloud`) avec la commande suivante, en vous assurant que votre projet et votre authentification sont correctement configurés :

```bash
gcloud functions deploy NOM_DE_LA_FONCTION --runtime python310 --trigger-http --allow-unauthenticated --source . --entry-point a_definir
```
