import pytest
from unittest.mock import patch, MagicMock
from api.analyze_and_insert import insert_topic_analysis

# Mock bigquery.Client to prevent actual BigQuery calls
@patch('api.analyze_and_insert.bigquery.Client')
@patch('api.analyze_and_insert.get_project_id', return_value='test-project')
def test_insert_topic_analysis_empty_theme_scores(mock_get_project_id, mock_bq_client):
    """
    Test that insert_topic_analysis handles empty theme_scores correctly.
    """
    # Arrange
    review_id = "test_review_id"
    theme_scores = []
    label_to_id = {}

    # Act
    insert_topic_analysis(review_id, theme_scores, label_to_id)

    # Assert
    # Verify that no BigQuery insert call was made
    mock_bq_client.return_value.insert_rows_json.assert_not_called()

@patch('api.analyze_and_insert.bigquery.Client')
@patch('api.analyze_and_insert.get_project_id', return_value='test-project')
def test_insert_topic_analysis_unknown_topic(mock_get_project_id, mock_bq_client, capsys):
    """
    Test that insert_topic_analysis handles unknown topics correctly.
    """
    # Arrange
    review_id = "test_review_id_2"
    theme_scores = [{"theme": "UnknownTheme", "note": 3}]
    label_to_id = {"KnownTheme": "123"} # "UnknownTheme" is not in label_to_id

    # Act
    insert_topic_analysis(review_id, theme_scores, label_to_id)

    # Assert
    # Verify that no BigQuery insert call was made for the unknown topic
    mock_bq_client.return_value.insert_rows_json.assert_not_called()
    captured = capsys.readouterr()
    assert "Thème inconnu dans la table topics : UnknownTheme" in captured.out

@patch('api.analyze_and_insert.bigquery.Client')
@patch('api.analyze_and_insert.get_project_id', return_value='test-project')
def test_insert_topic_analysis_invalid_note(mock_get_project_id, mock_bq_client, capsys):
    """
    Test that insert_topic_analysis handles invalid notes correctly.
    """
    # Arrange
    review_id = "test_review_id_3"
    theme_scores = [
        {"theme": "KnownTheme1", "note": 0},    # Invalid: below 1
        {"theme": "KnownTheme2", "note": 6},    # Invalid: above 5
        {"theme": "KnownTheme3", "note": "abc"} # Invalid: not a number
    ]
    label_to_id = {"KnownTheme1": "1", "KnownTheme2": "2", "KnownTheme3": "3"}

    # Act
    insert_topic_analysis(review_id, theme_scores, label_to_id)

    # Assert
    mock_bq_client.return_value.insert_rows_json.assert_not_called()
    captured = capsys.readouterr()
    assert "Note invalide pour KnownTheme1 : 0" in captured.out
    assert "Note invalide pour KnownTheme2 : 6" in captured.out
    assert "Note invalide pour KnownTheme3 : abc" in captured.out
