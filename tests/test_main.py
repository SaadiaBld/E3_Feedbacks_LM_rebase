import pytest
from unittest.mock import patch, MagicMock
import os
import pandas as pd
from scripts_data.main import main, upload_to_bigquery

@pytest.fixture(autouse=True)
def cleanup_env_vars(request):
    """Fixture to clean up environment variables after each test."""
    original_scraper_mode = os.environ.get("SCRAPER_MODE")
    original_csv_input_path = os.environ.get("CSV_INPUT_PATH")

    def finalizer():
        if original_scraper_mode is None:
            if "SCRAPER_MODE" in os.environ:
                del os.environ["SCRAPER_MODE"]
        else:
            os.environ["SCRAPER_MODE"] = original_scraper_mode

        if original_csv_input_path is None:
            if "CSV_INPUT_PATH" in os.environ:
                del os.environ["CSV_INPUT_PATH"]
        else:
            os.environ["CSV_INPUT_PATH"] = original_csv_input_path
    request.addfinalizer(finalizer)


@patch('scripts_data.main.scrape_reviews')
@patch('scripts_data.main.clean_csv')
@patch('scripts_data.main.upload_to_bigquery')
@patch('scripts_data.main.load_dotenv') # Mock load_dotenv to prevent actual .env loading
def test_main_prod_mode(mock_load_dotenv, mock_upload_to_bigquery, mock_clean_csv, mock_scrape_reviews):
    """
    Test the main pipeline execution in 'prod' (scraping) mode.
    """
    # Arrange
    # Patch the module-level 'mode' variable directly
    with patch('scripts_data.main.mode', "prod"):
        # Mock the existence of the scraped CSV file
        with patch('os.path.exists', return_value=True):
            # Act
            main()

            # Assert
            mock_scrape_reviews.assert_called_once_with(mode="prod")
            mock_clean_csv.assert_called_once_with(
                "/opt/airflow/data/avis_boutique.csv",
                "/opt/airflow/data/avis_nettoyes.csv"
            )
            mock_upload_to_bigquery.assert_called_once_with(
                "/opt/airflow/data/avis_nettoyes.csv",
                "trustpilot-satisfaction.reviews_dataset.reviews"
            )

@patch('scripts_data.main.scrape_reviews')
@patch('scripts_data.main.clean_csv')
@patch('scripts_data.main.upload_to_bigquery')
@patch('scripts_data.main.load_dotenv') # Mock load_dotenv to prevent actual .env loading
def test_main_csv_mode(mock_load_dotenv, mock_upload_to_bigquery, mock_clean_csv, mock_scrape_reviews):
    """
    Test the main pipeline execution in 'csv' mode.
    """
    # Arrange
    # Patch the module-level 'mode' variable directly
    with patch('scripts_data.main.mode', "csv"):
        # Patch os.getenv for CSV_INPUT_PATH
        with patch('os.getenv', side_effect=lambda key, default=None: "/path/to/test_input.csv" if key == "CSV_INPUT_PATH" else os.environ.get(key, default)):
            # Act
            main()

            # Assert
            mock_scrape_reviews.assert_not_called() # Scraper should not be called in CSV mode
            mock_clean_csv.assert_called_once_with(
                "/path/to/test_input.csv", # Should use the CSV_INPUT_PATH
                "/opt/airflow/data/avis_nettoyes.csv"
            )
            mock_upload_to_bigquery.assert_called_once_with(
                "/opt/airflow/data/avis_nettoyes.csv",
                "trustpilot-satisfaction.reviews_dataset.reviews"
            )

@patch('scripts_data.main.bigquery.Client')
@patch('scripts_data.main.pd.read_csv')
def test_upload_to_bigquery_success(mock_read_csv, mock_bq_client, capsys):
    """
    Test successful upload of data to BigQuery.
    """
    # Arrange
    csv_path = "dummy_path.csv"
    target_table_id = "test_project.test_dataset.test_table"

    # Create a dummy DataFrame that matches expected columns
    mock_df = pd.DataFrame({
        'review_id': ['id1', 'id2'],
        'rating': [5, 4],
        'content': ['good', 'ok'],
        'author': ['author1', 'author2'],
        'publication_date': ['2023-01-01', '2023-01-02'],
        'scrape_date': ['2023-01-03', '2023-01-04']
    })
    mock_read_csv.return_value = mock_df

    # Mock BigQuery client methods
    mock_client_instance = mock_bq_client.return_value
    mock_client_instance.load_table_from_dataframe.return_value.result.return_value = None
    mock_client_instance.query.return_value.result.return_value = None

    # Act
    upload_to_bigquery(csv_path, target_table_id)

    # Assert
    mock_read_csv.assert_called_once_with(csv_path)
    mock_bq_client.assert_called_once() # Ensure client is instantiated
    mock_client_instance.load_table_from_dataframe.assert_called_once()
    mock_client_instance.query.assert_called_once()

    captured = capsys.readouterr()
    assert f"Données chargées dans la table temporaire trustpilot-satisfaction.reviews_dataset.temp_reviews." in captured.out
    assert f"Données fusionnées dans la table {target_table_id}." in captured.out
