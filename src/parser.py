from typing import Dict, List, Any
from .models import Event, Player, PlayerStats, PlayerHistory, Team, Gameweek, Fixture
from .utils import get_logger

logger = get_logger(__name__)


def parse_events(data: Dict[str, Any]) -> List[Event]:
    """Parse events (gameweeks) data from bootstrap-static API response.

    Args:
        data: The bootstrap-static JSON response

    Returns:
        List of Event objects

    Raises:
        KeyError: If expected 'events' key is missing
        ValidationError: If event data doesn't match Event model
    """
    logger.info("Parsing events data")

    if 'events' not in data:
        raise KeyError("'events' key not found in bootstrap data")

    events_data = data['events']
    events = []

    for event_data in events_data:
        try:
            # Map to simplified Event model
            event = Event(
                id=event_data['id'],
                name=event_data['name'],
                deadline_time=event_data['deadline_time'],
                finished=event_data['finished'],
                average_entry_score=event_data.get('average_entry_score')
            )
            events.append(event)
        except Exception as e:
            logger.error(
                f"Failed to parse event data for event ID {event_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(events)} events")
    return events


def parse_players(data: Dict[str, Any]) -> List[Player]:
    """Parse players data from bootstrap-static API response (simplified for new schema).

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
            # Map to simplified Player model
            player = Player(
                id=player_data['id'],
                code=player_data['code'],
                first_name=player_data['first_name'],
                second_name=player_data['second_name'],
                team_id=player_data['team'],  # Map 'team' to 'team_id'
                element_type=player_data['element_type'],
                now_cost=player_data['now_cost']
            )
            players.append(player)
        except Exception as e:
            logger.error(
                f"Failed to parse player data for player ID {player_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(players)} players")
    return players


def parse_player_stats(data: Dict[str, Any], gameweek_id: int) -> List[PlayerStats]:
    """Parse player statistics data from bootstrap-static API response.

    Args:
        data: The bootstrap-static JSON response
        gameweek_id: The current gameweek ID for the stats

    Returns:
        List of PlayerStats objects

    Raises:
        KeyError: If expected 'elements' key is missing
        ValidationError: If player stats data doesn't match PlayerStats model
    """
    logger.info(f"Parsing player stats data for gameweek {gameweek_id}")

    if 'elements' not in data:
        raise KeyError("'elements' key not found in bootstrap data")

    players_data = data['elements']
    player_stats = []

    for player_data in players_data:
        try:
            # Convert string values to appropriate types
            def safe_float(value):
                if value is None or value == "":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            def safe_int(value):
                if value is None or value == "":
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None

            stats = PlayerStats(
                player_id=player_data['id'],
                gameweek_id=gameweek_id,
                total_points=safe_int(player_data.get('total_points')),
                form=safe_float(player_data.get('form')),
                selected_by_percent=safe_float(player_data.get('selected_by_percent')),
                transfers_in=safe_int(player_data.get('transfers_in')),
                transfers_out=safe_int(player_data.get('transfers_out')),
                minutes=safe_int(player_data.get('minutes')),
                goals_scored=safe_int(player_data.get('goals_scored')),
                assists=safe_int(player_data.get('assists')),
                clean_sheets=safe_int(player_data.get('clean_sheets')),
                goals_conceded=safe_int(player_data.get('goals_conceded')),
                own_goals=safe_int(player_data.get('own_goals')),
                penalties_saved=safe_int(player_data.get('penalties_saved')),
                penalties_missed=safe_int(player_data.get('penalties_missed')),
                yellow_cards=safe_int(player_data.get('yellow_cards')),
                red_cards=safe_int(player_data.get('red_cards')),
                saves=safe_int(player_data.get('saves')),
                bonus=safe_int(player_data.get('bonus')),
                bps=safe_int(player_data.get('bps')),
                influence=safe_float(player_data.get('influence')),
                creativity=safe_float(player_data.get('creativity')),
                threat=safe_float(player_data.get('threat')),
                ict_index=safe_float(player_data.get('ict_index')),
                starts=safe_int(player_data.get('starts')),
                expected_goals=safe_float(player_data.get('expected_goals')),
                expected_assists=safe_float(player_data.get('expected_assists')),
                expected_goal_involvements=safe_float(player_data.get('expected_goal_involvements')),
                expected_goals_conceded=player_data.get('expected_goals_conceded')
            )
            player_stats.append(stats)
        except Exception as e:
            logger.error(
                f"Failed to parse player stats for player ID {player_data.get('id', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(player_stats)} player stats")
    return player_stats


def parse_player_history(data: List[Dict[str, Any]], player_id: int) -> List[PlayerHistory]:
    """Parse player history data from element-summary API response.

    Args:
        data: The element-summary history JSON response
        player_id: The player ID for the history

    Returns:
        List of PlayerHistory objects

    Raises:
        ValidationError: If player history data doesn't match PlayerHistory model
    """
    logger.info(f"Parsing player history data for player {player_id}")

    if not isinstance(data, list):
        raise TypeError("Player history data should be a list")

    player_history = []

    for history_data in data:
        try:
            # Convert string values to appropriate types
            def safe_float(value):
                if value is None or value == "":
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None

            def safe_int(value):
                if value is None or value == "":
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None

            history = PlayerHistory(
                player_id=player_id,
                gameweek_id=safe_int(history_data.get('round')),
                opponent_team=safe_int(history_data.get('opponent_team')),
                was_home=history_data.get('was_home'),
                kickoff_time=history_data.get('kickoff_time'),
                total_points=safe_int(history_data.get('total_points')),
                value=safe_int(history_data.get('value')),
                selected=safe_int(history_data.get('selected')),
                transfers_balance=safe_int(history_data.get('transfers_balance')),
                transfers_in=safe_int(history_data.get('transfers_in')),
                transfers_out=safe_int(history_data.get('transfers_out')),
                minutes=safe_int(history_data.get('minutes')),
                goals_scored=safe_int(history_data.get('goals_scored')),
                assists=safe_int(history_data.get('assists')),
                clean_sheets=safe_int(history_data.get('clean_sheets')),
                goals_conceded=safe_int(history_data.get('goals_conceded')),
                own_goals=safe_int(history_data.get('own_goals')),
                penalties_saved=safe_int(history_data.get('penalties_saved')),
                penalties_missed=safe_int(history_data.get('penalties_missed')),
                yellow_cards=safe_int(history_data.get('yellow_cards')),
                red_cards=safe_int(history_data.get('red_cards')),
                saves=safe_int(history_data.get('saves')),
                bonus=safe_int(history_data.get('bonus')),
                bps=safe_int(history_data.get('bps')),
                influence=safe_float(history_data.get('influence')),
                creativity=safe_float(history_data.get('creativity')),
                threat=safe_float(history_data.get('threat')),
                ict_index=safe_float(history_data.get('ict_index')),
                starts=safe_int(history_data.get('starts')),
                expected_goals=safe_float(history_data.get('expected_goals')),
                expected_assists=safe_float(history_data.get('expected_assists')),
                expected_goal_involvements=safe_float(history_data.get('expected_goal_involvements')),
                expected_goals_conceded=safe_float(history_data.get('expected_goals_conceded'))
            )
            player_history.append(history)
        except Exception as e:
            logger.error(
                f"Failed to parse player history for player ID {player_id}, round {history_data.get('round', 'unknown')}: {e}")
            raise

    logger.info(f"Successfully parsed {len(player_history)} player history entries")
    return player_history


# Legacy parsers - keeping for backwards compatibility
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
