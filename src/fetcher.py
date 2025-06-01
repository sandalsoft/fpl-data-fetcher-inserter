import requests
from typing import Dict, Any
from .config import get_config
from .utils import get_logger

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
