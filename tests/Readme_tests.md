# 🧪 Tests automatisés du projet Trustpilot NLP

Ce dossier contient les tests automatisés liés au pipeline Airflow + Claude + BigQuery.  
Ces tests visent à répondre à la compétence **C12** du référentiel :  
> *Programmer les tests automatisés d’un modèle d’intelligence artificielle…*

---

## 📁 Contenu du dossier

- `test_dag_workflow.py`  
  ✔️ Tests du DAG Airflow `trustpilot_pipeline` (import, tâches, dépendances, opérateurs).

- `test_analyze_and_insert.py`  
  ✔️ Tests de la fonction `process_and_insert_all()` (mock Claude et BigQuery).

- `test_claude_validation.py`  
  ✔️ Tests unitaires de `validate_claude_response()` pour vérifier la cohérence des JSON de Claude.

---

## ▶️ Exécution des tests

Lancez tous les tests depuis le répertoire `/opt/airflow` dans le container :

```bash
PYTHONPATH=/opt/airflow pytest project/tests -v

Ou un fichier spécifique:

PYTHONPATH=/opt/airflow pytest project/tests/test_claude_validation.py -v


Dépendances nécessaires

Assurez-vous que les dépendances suivantes sont installées dans l’environnement de test :

pip install pytest pytest-mock

    ✅ Si vous utilisez Docker, vous pouvez les inclure dans votre requirements.txt ou les installer via le Dockerfile Airflow.

🎯 Objectifs pédagogiques – Compétence C12
Critère attendu	Couverture actuelle dans le projet
📌 Cas à tester bien définis	✅ Tests DAG, Claude, BigQuery, JSON validés
🔧 Outils de test adaptés	✅ pytest, pytest-mock, mocks internes
📈 Tests intégrés, bonne couverture	✅ Tests unitaires et d’intégration inclus
🚀 Exécution sans erreur	✅ Tous les tests passent via pytest
🗂️ Sources versionnées dans Git	✅ Projet synchronisé avec dépôt Git distant
📚 Documentation claire et accessible	✅ Ce README fournit les consignes d’exécution et dépendances

## ✅ Couverture des tests – Compétence C12

Ce dossier couvre l’ensemble des tests automatisés attendus pour valider la compétence **C12** du référentiel :

### 1. Environnement et stratégie
- **Framework utilisé** : `pytest`
- **Mocking** : `unittest.mock.patch` (Claude, BigQuery, environnement `.env`)
- **Chargement des DAGs** : via `airflow.models.DagBag`
- **Tests exécutés dans un conteneur Airflow Docker avec credentials mockés**

### 2. Jeux de tests couverts

| Type de test | Nom du test | Description |
|--------------|-------------|-------------|
| ✅ DAG | `test_dag_import` | Le DAG peut être importé sans erreur |
| ✅ DAG | `test_task_ids` | Toutes les tâches attendues sont présentes |
| ✅ DAG | `test_dependencies` | L’ordre d’exécution des tâches est cohérent |
| ✅ DAG | `test_dag_is_acyclic` | Le DAG est acyclique |
| ✅ DAG | `test_operator_type` | La tâche utilise le bon opérateur Airflow |
| ✅ Pipeline | `test_process_and_insert_all` | Mock complet de la fonction clé du pipeline |
| ✅ Modèle IA | `test_validate_claude_response_*` | Vérifie la validité des JSON renvoyés par Claude |

### 3. Exécution des tests

```bash
# Depuis le conteneur Docker Airflow
PYTHONPATH=/opt/airflow pytest project/tests/ -v
