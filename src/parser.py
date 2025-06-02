from typing import Dict, List, Any
from .models import Team, Player, Gameweek, Fixture
from .utils import get_logger

logger = get_logger(__name__)


def parse_teams(data: Dict[str, Any]) -> List[Team]:
    """Parse teams data from bootstrap-static API response.

    Args:
        data: The bootstrap-static JSON response

    Returns:
        List of Team objects

    Raises:
        KeyError: If expected 'teams' key is missing
        ValidationError: If team data doesn't match Team model
    """
    logger.info("Parsing teams data")

    if 'teams' not in data:
        raise KeyError("'teams' key not found in bootstrap data")

    teams_data = data['teams']
    teams = []

    for team_data in teams_data:
        try:
            team = Team(**team_data)
            teams.append(team)
        except Exception as e:
            logger.error(
                f"Failed to parse team data for team ID {team_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(teams)} teams")
    return teams


def parse_players(data: Dict[str, Any]) -> List[Player]:
    """Parse players (elements) data from bootstrap-static API response.

    Args:
        data: The bootstrap-static JSON response

    Returns:
        List of Player objects

    Raises:
        KeyError: If expected 'elements' key is missing
        ValidationError: If player data doesn't match Player model
    """
    logger.info("Parsing players data")

    if 'elements' not in data:
        raise KeyError("'elements' key not found in bootstrap data")

    players_data = data['elements']
    players = []

    for player_data in players_data:
        try:
            player = Player(**player_data)
            players.append(player)
        except Exception as e:
            logger.error(
                f"Failed to parse player data for player ID {player_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(players)} players")
    return players


def parse_gameweeks(data: Dict[str, Any]) -> List[Gameweek]:
    """Parse gameweeks (events) data from bootstrap-static API response.

    Args:
        data: The bootstrap-static JSON response

    Returns:
        List of Gameweek objects

    Raises:
        KeyError: If expected 'events' key is missing
        ValidationError: If gameweek data doesn't match Gameweek model
    """
    logger.info("Parsing gameweeks data")

    if 'events' not in data:
        raise KeyError("'events' key not found in bootstrap data")

    events_data = data['events']
    gameweeks = []

    for event_data in events_data:
        try:
            gameweek = Gameweek(**event_data)
            gameweeks.append(gameweek)
        except Exception as e:
            logger.error(
                f"Failed to parse gameweek data for event ID {event_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(gameweeks)} gameweeks")
    return gameweeks


def parse_fixtures(data: List[Dict[str, Any]]) -> List[Fixture]:
    """Parse fixtures data from fixtures API response.

    Args:
        data: The fixtures JSON response (list of fixture objects)

    Returns:
        List of Fixture objects

    Raises:
        ValidationError: If fixture data doesn't match Fixture model
    """
    logger.info("Parsing fixtures data")

    if not isinstance(data, list):
        raise TypeError("Fixtures data should be a list")

    fixtures = []

    for fixture_data in data:
        try:
            fixture = Fixture(**fixture_data)
            fixtures.append(fixture)
        except Exception as e:
            logger.error(
                f"Failed to parse fixture data for fixture ID {fixture_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(fixtures)} fixtures")
    return fixtures
