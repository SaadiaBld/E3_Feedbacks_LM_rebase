# monitoring/Dockerfile: construit une image Docker pour le service d'export de métriques Prometheus

FROM python:3.10-slim

# Définit le répertoire de travail dans le conteneur
WORKDIR /app

# Copier seulement les fichiers nécessaires
COPY . ./

# Installer les dépendances (prometheus_client, etc.) pour que l'exporteur puisse fonctionner
RUN pip install --no-cache-dir prometheus_client

# rend le port 8000 disponible et accessible pour que d'autres services (comme Prometheus) puissent y accéder et récupérer les métriques
EXPOSE 8000

# Instruction pour Démarrer le serveur d'export de métriques
CMD ["python", "exporter.py"]
