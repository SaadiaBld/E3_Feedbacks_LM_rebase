import pytest
from unittest.mock import patch, MagicMock
from google.cloud import bigquery
from api.bq_insert_clean_data import deduplicate_reviews

@patch('api.bq_insert_clean_data.bigquery.Client')
def test_deduplicate_reviews_no_duplicates(mock_bq_client, capsys):
    """
    Test deduplicate_reviews when no duplicates are found.
    """
    # Arrange
    mock_client_instance = mock_bq_client.return_value
    mock_query_result = MagicMock()
    mock_query_result.__iter__.return_value = [{"nb_to_delete": 0, "ids": []}]
    mock_client_instance.query.return_value.result.return_value = mock_query_result

    # Act
    deduplicate_reviews()

    # Assert
    mock_client_instance.query.assert_called_once() # Query to identify duplicates
    assert mock_client_instance.query.call_args[0][0].strip().startswith("WITH duplicates AS") # Check query content
    assert mock_client_instance.query.call_count == 1 # Only one query should be made
    captured = capsys.readouterr()
    assert "Aucun doublon à supprimer dans la table reviews" in captured.out

@patch('api.bq_insert_clean_data.bigquery.Client')
def test_deduplicate_reviews_with_duplicates(mock_bq_client, capsys):
    """
    Test deduplicate_reviews when duplicates are found and successfully deleted.
    """
    # Arrange
    mock_client_instance = mock_bq_client.return_value

    # Mock the first query result (identifying duplicates)
    mock_first_query_result = MagicMock()
    mock_first_query_result.__iter__.return_value = [{"nb_to_delete": 2, "ids": ["id1", "id2"]}]
    
    # Configure the mock client to return different results for consecutive queries
    # First call to query() is for identification, second is for deletion
    mock_client_instance.query.side_effect = [
        MagicMock(result=MagicMock(return_value=mock_first_query_result)), # For identification query
        MagicMock(result=MagicMock(return_value=None)) # For deletion query (result() is called)
    ]

    # Act
    deduplicate_reviews()

    # Assert
    # Verify that query was called twice
    assert mock_client_instance.query.call_count == 2

    # Verify the first query (identification)
    first_call_args = mock_client_instance.query.call_args_list[0][0][0]
    assert first_call_args.strip().startswith("WITH duplicates AS")

    # Verify the second query (deletion)
    second_call_args = mock_client_instance.query.call_args_list[1][0][0]
    assert second_call_args.strip().startswith("DELETE FROM")
    assert "WHERE review_id IN UNNEST(@review_ids)" in second_call_args
    
    # Corrected assertion for query parameters
    second_call_kwargs = mock_client_instance.query.call_args_list[1][1]
    assert "job_config" in second_call_kwargs
    job_config = second_call_kwargs["job_config"]
    assert isinstance(job_config, bigquery.QueryJobConfig)
    assert len(job_config.query_parameters) == 1
    param = job_config.query_parameters[0]
    assert param.name == "review_ids"
    assert param.array_type == "STRING" # Corrected from param.type
    assert param.values == ["id1", "id2"]

    captured = capsys.readouterr()
    assert "2 doublons supprimés dans la table reviews" in captured.out

@patch('api.bq_insert_clean_data.bigquery.Client')
def test_deduplicate_reviews_bigquery_error(mock_bq_client, capsys):
    """
    Test deduplicate_reviews handles BigQuery errors during identification.
    """
    # Arrange
    mock_client_instance = mock_bq_client.return_value
    mock_client_instance.query.side_effect = Exception("BigQuery connection error")

    # Act & Assert
    with pytest.raises(Exception, match="BigQuery connection error"):
        deduplicate_reviews()

    mock_client_instance.query.assert_called_once() # Query to identify duplicates
    captured = capsys.readouterr()
    assert "BigQuery connection error" not in captured.out # Error should be raised, not printed
