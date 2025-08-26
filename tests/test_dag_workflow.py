import pytest
from unittest.mock import patch

# Protéger les imports Airflow pour une exécution locale possible
try:
    from airflow.models import DagBag
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
    airflow_installed = True
except ImportError:
    DagBag = None
    PythonOperator = None
    EmptyOperator = None
    airflow_installed = False

@pytest.fixture(scope="module")
@pytest.mark.skipif(not airflow_installed, reason="Airflow not installed")
def dagbag():
    # Le DagBag charge les dags depuis le dossier, l'import direct n'est pas nécessaire
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)

@pytest.mark.skipif(not airflow_installed, reason="Airflow not installed")
def test_dag_import_and_structure(dagbag):
    """Vérifie que le DAG est bien importé, sans cycle, et a les bonnes tâches."""
    dag = dagbag.get_dag("trustpilot_pipeline")
    assert dag is not None, "Le DAG 'trustpilot_pipeline' n'a pas été trouvé."
    assert dagbag.import_errors == {}, f"Erreurs d'importation du DAG: {dagbag.import_errors}"

    expected_tasks = {"scrape_trustpilot_reviews", "clean_reviews", "insert_clean_reviews_to_bq", "analyze_and_insert"}
    assert expected_tasks.issubset(dag.task_ids), f"Des tâches attendues sont manquantes: {expected_tasks - set(dag.task_ids)}"

@pytest.mark.skipif(not airflow_installed, reason="Airflow not installed")
def test_dag_dependencies(dagbag):
    """Vérifie l'enchaînement correct des tâches dans le DAG."""
    dag = dagbag.get_dag("trustpilot_pipeline")
    assert dag is not None, "DAG introuvable"

    scrape_task = dag.get_task("scrape_trustpilot_reviews")
    clean_task = dag.get_task("clean_reviews")
    insert_task = dag.get_task("insert_clean_reviews_to_bq")
    analyze_task = dag.get_task("analyze_and_insert")

    # Vérifie les dépendances en amont (upstream)
    assert clean_task.upstream_task_ids == {scrape_task.task_id}
    assert insert_task.upstream_task_ids == {clean_task.task_id}
    assert analyze_task.upstream_task_ids == {insert_task.task_id}

@pytest.mark.skipif(not airflow_installed, reason="Airflow not installed")
def test_operator_types(dagbag):
    """Vérifie que les tâches utilisent les bons opérateurs Airflow."""
    dag = dagbag.get_dag("trustpilot_pipeline")
    assert dag is not None, "DAG introuvable"

    # Vérifie que les tâches principales sont des PythonOperator
    python_tasks = ["scrape_trustpilot_reviews", "clean_reviews", "insert_clean_reviews_to_bq", "analyze_and_insert"]
    for task_id in python_tasks:
        task = dag.get_task(task_id)
        # Gère le cas où une tâche est un EmptyOperator (skip)
        if isinstance(task, PythonOperator) or isinstance(task, EmptyOperator):
            assert True
        else:
            pytest.fail(f"La tâche {task_id} devrait être un PythonOperator ou EmptyOperator, mais est un {type(task).__name__}")