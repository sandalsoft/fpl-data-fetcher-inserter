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
