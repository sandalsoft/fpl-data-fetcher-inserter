import requests
from typing import Dict, Any, Optional, List, Tuple
from .config import get_config
from .utils import get_logger
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
    start_time = time.time()
    logger.info("🚀 Starting bootstrap data fetch...")

    try:
        data = fetch_endpoint("/bootstrap-static/")
        duration = time.time() - start_time
        logger.info(
            f"✅ Bootstrap data fetch completed in {duration:.2f} seconds")
        return data
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"❌ Bootstrap data fetch failed after {duration:.2f} seconds: {e}")
        raise


def fetch_fixtures_data() -> Optional[List[Dict]]:
    """Fetches the fixtures data from FPL API."""
    start_time = time.time()
    logger.info("🚀 Starting fixtures data fetch...")

    try:
        data = fetch_endpoint("/fixtures/")
        duration = time.time() - start_time
        logger.info(
            f"✅ Fixtures data fetch completed in {duration:.2f} seconds")
        return data
    except Exception as e:
        duration = time.time() - start_time
        logger.error(
            f"❌ Fixtures data fetch failed after {duration:.2f} seconds: {e}")
        raise


def fetch_fixtures_by_gameweek(event_id: int) -> Optional[List[Dict]]:
    """Fetch fixtures data for a specific gameweek from FPL API.

    Args:
        event_id: The gameweek ID

    Returns:
        List containing fixture data for the specified gameweek

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/fixtures?event={event_id}"

    try:
        # Use the generic endpoint fetcher but override the URL formatting
        config = get_config()
        url = f"{config['fpl_api_url']}{endpoint}"

        logger.info(f"Fetching data from: {url}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()
        logger.info(f"Successfully fetched fixtures for gameweek {event_id}")
        return data
    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP error fetching fixtures for gameweek {event_id}: {e.response.status_code} {e.response.reason}"
        logger.error(error_msg)
        raise FPLAPIError(error_msg) from e
    except Exception as e:
        logger.error(f"Failed to fetch fixtures for gameweek {event_id}: {e}")
        return None


def fetch_gameweek_live_data(event_id: int) -> Optional[Dict[str, Any]]:
    """Fetch live statistics for every player in the specified gameweek.

    Args:
        event_id: The gameweek ID

    Returns:
        Dict containing live statistics for all players in the gameweek

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/event/{event_id}/live/"

    try:
        data = fetch_endpoint(endpoint)
        logger.info(f"Successfully fetched live data for gameweek {event_id}")
        return data
    except FPLAPIError as e:
        logger.error(f"Failed to fetch live data for gameweek {event_id}: {e}")
        return None


def fetch_manager_data(session: Optional[requests.Session] = None) -> Optional[Dict[str, Any]]:
    """Fetch data for the authenticated manager.

    Args:
        session: Optional requests session with authentication cookies

    Returns:
        Dict containing manager data

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = "/me/"

    try:
        if session:
            # Use authenticated session if provided
            config = get_config()
            url = f"{config['fpl_api_url']}{endpoint}"

            logger.info(f"Fetching authenticated data from: {url}")
            response = session.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            logger.info("Successfully fetched manager data")
            return data
        else:
            # Note: This endpoint requires authentication, so it will likely fail
            logger.warning(
                "Attempting to fetch manager data without authentication - this may fail")
            data = fetch_endpoint(endpoint)
            return data
    except FPLAPIError as e:
        logger.error(f"Failed to fetch manager data: {e}")
        return None


def fetch_league_cup_status(league_id: int) -> Optional[Dict[str, Any]]:
    """Fetch cup status information for a specified league.

    Args:
        league_id: The league ID

    Returns:
        Dict containing cup status information

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/league/{league_id}/cup-status/"

    try:
        data = fetch_endpoint(endpoint)
        logger.info(f"Successfully fetched cup status for league {league_id}")
        return data
    except FPLAPIError as e:
        logger.error(f"Failed to fetch cup status for league {league_id}: {e}")
        return None


def fetch_entry_data(entry_id: int) -> Optional[Dict[str, Any]]:
    """Fetch data for a specific team entry.

    Args:
        entry_id: The team entry ID

    Returns:
        Dict containing entry data

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/entry/{entry_id}/"

    try:
        data = fetch_endpoint(endpoint)
        logger.info(f"Successfully fetched entry data for entry {entry_id}")
        return data
    except FPLAPIError as e:
        logger.error(f"Failed to fetch entry data for entry {entry_id}: {e}")
        return None


def fetch_h2h_matches(league_id: int, entry_id: int, page: int = 1) -> Optional[Dict[str, Any]]:
    """Fetch head-to-head matches for a given league and entry.

    Args:
        league_id: The league ID
        entry_id: The entry ID
        page: The page number (default: 1)

    Returns:
        Dict containing head-to-head matches data

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/"

    try:
        data = fetch_endpoint(endpoint)
        logger.info(
            f"Successfully fetched H2H matches for league {league_id}, entry {entry_id}, page {page}")
        return data
    except FPLAPIError as e:
        logger.error(
            f"Failed to fetch H2H matches for league {league_id}, entry {entry_id}, page {page}: {e}")
        return None


def fetch_league_standings(league_id: int) -> Optional[Dict[str, Any]]:
    """Fetch classic league standings.

    Args:
        league_id: The league ID

    Returns:
        Dict containing league standings

    Raises:
        FPLAPIError: If API request fails
    """
    endpoint = f"/leagues-classic/{league_id}/standings/"

    try:
        data = fetch_endpoint(endpoint)
        logger.info(f"Successfully fetched standings for league {league_id}")
        return data
    except FPLAPIError as e:
        logger.error(f"Failed to fetch standings for league {league_id}: {e}")
        return None


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
        logger.error(
            f"Failed to fetch player history for player {player_id}: {e}")
        return None


def fetch_player_history_batch(player_ids: List[int], max_workers: int = 10, delay_between_batches: float = 0.1) -> List[Tuple[int, Optional[List[Dict]]]]:
    """Fetch player history data for multiple players concurrently.

    Args:
        player_ids: List of player IDs to fetch history for
        max_workers: Maximum number of concurrent requests (default: 10)
        delay_between_batches: Delay between batches in seconds (default: 0.1)

    Returns:
        List of tuples containing (player_id, history_data)
    """
    start_time = time.time()
    logger.info(
        f"🚀 Starting player history batch fetch for {len(player_ids)} players with {max_workers} concurrent workers")

    def fetch_single_player(player_id: int) -> Tuple[int, Optional[List[Dict]]]:
        """Fetch history for a single player."""
        try:
            history_data = fetch_player_history(player_id)
            return (player_id, history_data)
        except Exception as e:
            logger.error(f"Error fetching history for player {player_id}: {e}")
            return (player_id, None)

    results = []

    # Process in batches to avoid overwhelming the API
    batch_size = max_workers * 5  # Process 5 batches worth at a time

    for i in range(0, len(player_ids), batch_size):
        batch = player_ids[i:i + batch_size]
        logger.info(
            f"Processing batch {i//batch_size + 1}/{(len(player_ids) + batch_size - 1)//batch_size} ({len(batch)} players)")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks in the current batch
            futures = {executor.submit(
                fetch_single_player, player_id): player_id for player_id in batch}

            # Collect results as they complete
            batch_results = []
            for future in as_completed(futures):
                player_id = futures[future]
                try:
                    result = future.result()
                    batch_results.append(result)
                    if len(batch_results) % 10 == 0:
                        logger.info(
                            f"Completed {len(batch_results)}/{len(batch)} players in current batch")
                except Exception as e:
                    logger.error(f"Future failed for player {player_id}: {e}")
                    batch_results.append((player_id, None))

            results.extend(batch_results)

        # Small delay between batches to be respectful to the API
        if i + batch_size < len(player_ids):
            time.sleep(delay_between_batches)

    duration = time.time() - start_time
    successful_fetches = sum(1 for _, data in results if data is not None)
    logger.info(
        f"✅ Player history batch fetch completed in {duration:.2f} seconds")
    logger.info(
        f"📊 Successfully fetched {successful_fetches}/{len(results)} players ({(successful_fetches/len(results)*100):.1f}% success rate)")
    return results


def fetch_independent_endpoints_parallel() -> Dict[str, Any]:
    """Fetch independent API endpoints in parallel for better performance.

    Returns:
        Dict containing results from parallel endpoint fetching:
        - 'bootstrap_data': Bootstrap-static data
        - 'fixtures_data': Fixtures data
        - 'errors': List of any errors encountered
    """
    start_time = time.time()
    logger.info("🚀 Starting parallel fetch of independent endpoints...")

    def fetch_bootstrap() -> Tuple[str, Any]:
        """Fetch bootstrap data with error handling."""
        try:
            data = fetch_bootstrap_data()
            return ('bootstrap_data', data)
        except Exception as e:
            logger.error(f"Failed to fetch bootstrap data: {e}")
            return ('bootstrap_error', str(e))

    def fetch_fixtures() -> Tuple[str, Any]:
        """Fetch fixtures data with error handling."""
        try:
            data = fetch_fixtures_data()
            return ('fixtures_data', data)
        except Exception as e:
            logger.error(f"Failed to fetch fixtures data: {e}")
            return ('fixtures_error', str(e))

    results = {
        'bootstrap_data': None,
        'fixtures_data': None,
        'errors': []
    }

    # Execute both fetches in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks
        future_bootstrap = executor.submit(fetch_bootstrap)
        future_fixtures = executor.submit(fetch_fixtures)

        # Collect results
        for future in as_completed([future_bootstrap, future_fixtures]):
            try:
                key, value = future.result()
                if key.endswith('_error'):
                    results['errors'].append(f"{key}: {value}")
                else:
                    results[key] = value
            except Exception as e:
                results['errors'].append(f"Unexpected error: {str(e)}")

    # Log results
    duration = time.time() - start_time
    if results['bootstrap_data']:
        logger.info("✓ Bootstrap data fetched successfully")
    if results['fixtures_data']:
        logger.info("✓ Fixtures data fetched successfully")
    if results['errors']:
        logger.warning(
            f"Encountered {len(results['errors'])} errors during parallel fetch")

    logger.info(
        f"✅ Parallel endpoint fetching completed in {duration:.2f} seconds")
    return results


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
