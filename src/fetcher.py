import requests
from typing import Dict, Any, Optional, List
from .config import get_config
from .utils import get_logger
import json

logger = get_logger(__name__)


class FPLAPIError(Exception):
    """Custom exception for FPL API errors."""
    pass


def fetch_endpoint(endpoint: str) -> Dict[str, Any]:
    """Fetch data from any FPL API endpoint.

    Args:
        endpoint: The API endpoint path (e.g., "/fixtures/", "/bootstrap-static/")

    Returns:
        Dict containing the API response data

    Raises:
        FPLAPIError: If API request fails with descriptive error message
    """
    config = get_config()

    # Clean up endpoint to ensure consistent format
    if not endpoint.startswith('/'):
        endpoint = '/' + endpoint
    if not endpoint.endswith('/'):
        endpoint = endpoint + '/'

    url = f"{config['fpl_api_url']}{endpoint}"

    logger.info(f"Fetching data from: {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raises HTTPError for bad responses

        data = response.json()
        logger.info(f"Successfully fetched data from {endpoint}")

        return data

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error fetching {endpoint}: {e.response.status_code} {e.response.reason}"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e

    except requests.exceptions.ConnectionError as e:
        error_msg = f"Connection error fetching {endpoint}: Unable to connect to FPL API"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e

    except requests.exceptions.Timeout as e:
        error_msg = f"Timeout error fetching {endpoint}: Request timed out after 30 seconds"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e

    except requests.exceptions.RequestException as e:
        error_msg = f"Request error fetching {endpoint}: {str(e)}"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e

    except ValueError as e:
        error_msg = f"JSON decode error for {endpoint}: Invalid JSON response"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e


def fetch_bootstrap_data() -> Dict[str, Any]:
    """Fetch bootstrap-static data from FPL API.

    Returns:
        Dict containing the bootstrap data from FPL API

    Raises:
        FPLAPIError: If API request fails
    """
    return fetch_endpoint("/bootstrap-static/")


def fetch_fixtures_data() -> Optional[List[Dict]]:
    """Fetches the fixtures data from FPL API."""
    return fetch_endpoint("/fixtures/")


def fetch_player_history(player_id: int) -> Optional[List[Dict]]:
    """Fetch player history data from FPL API.

    Args:
        player_id: The player's FPL ID

    Returns:
        List containing the player's history data

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/element-summary/{player_id}/"
    
    try:
        data = fetch_endpoint(endpoint)
        # The API returns a dict with 'history' key containing the list of gameweek data
        return data.get('history', [])
    except FPLAPIError as e:
        logger.error(f"Failed to fetch player history for player {player_id}: {e}")
        return None


def fetch_current_gameweek_id(bootstrap_data: Dict[str, Any]) -> Optional[int]:
    """Get the current gameweek ID from bootstrap data.

    Args:
        bootstrap_data: The bootstrap-static JSON response

    Returns:
        Current gameweek ID or None if not found
    """
    if 'events' not in bootstrap_data:
        return None
    
    for event in bootstrap_data['events']:
        if event.get('is_current', False):
            return event['id']
    
    return None
