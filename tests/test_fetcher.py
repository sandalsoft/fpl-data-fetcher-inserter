import pytest
from unittest.mock import patch, Mock
from src.fetcher import fetch_bootstrap_data


def test_fetch_bootstrap_data_returns_dict():
    """Test that fetch_bootstrap_data returns a dictionary."""
    data = fetch_bootstrap_data()
    assert isinstance(data, dict)


def test_fetch_bootstrap_data_has_expected_keys():
    """Test that fetch_bootstrap_data returns dict with expected keys."""
    data = fetch_bootstrap_data()

    # Check for main expected keys from the API
    expected_keys = ['elements', 'teams', 'events', 'element_types']

    for key in expected_keys:
        assert key in data, f"Key '{key}' not found in bootstrap data"


def test_fetch_bootstrap_data_teams_structure():
    """Test that teams data has expected structure."""
    data = fetch_bootstrap_data()

    teams = data.get('teams', [])
    assert isinstance(teams, list)
    assert len(teams) > 0  # Should have teams

    # Check first team has expected fields
    if teams:
        team = teams[0]
        assert 'id' in team
        assert 'name' in team
        assert 'short_name' in team


def test_fetch_bootstrap_data_elements_structure():
    """Test that elements (players) data has expected structure."""
    data = fetch_bootstrap_data()

    elements = data.get('elements', [])
    assert isinstance(elements, list)
    assert len(elements) > 0  # Should have players

    # Check first element has expected fields
    if elements:
        element = elements[0]
        assert 'id' in element
        assert 'first_name' in element
        assert 'second_name' in element
        assert 'team' in element


@patch('src.fetcher.requests.get')
def test_fetch_bootstrap_data_http_error_handling(mock_get):
    """Test that HTTP errors are properly handled."""
    # Mock a failed response
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("HTTP Error")
    mock_get.return_value = mock_response

    with pytest.raises(Exception):
        fetch_bootstrap_data()
