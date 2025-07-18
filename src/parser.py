from typing import Dict, List, Any
from .models import (
    Event, Player, PlayerStats, PlayerHistory, Team, Gameweek, Fixture,
    GameweekLiveData, GameweekLivePlayer, GameweekLivePlayerStats, GameweekLivePlayerExplain,
    ManagerData, LeagueCupStatus, EntryData, H2HMatch, H2HMatchData,
    LeagueStandings, LeagueStandingEntry
)
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
    """Parse players data from bootstrap-static API response (full schema model).

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
        if player_data.get('element_type', 0) > 4:
            logger.debug(
                f"Skipping non-player element ID {player_data.get('id', 'unknown')}")
            continue
        try:
            # Helper function to safely get string values
            def safe_str(value, default='0.0'):
                if value is None or value == "":
                    return default
                return str(value)

            # Helper function to safely get integer values
            def safe_int(value, default=0):
                if value is None or value == "":
                    return default
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default

            # Map to full Player model
            player = Player(
                id=player_data['id'],
                code=player_data['code'],
                first_name=player_data['first_name'],
                second_name=player_data['second_name'],
                web_name=player_data['web_name'],
                team=player_data['team'],
                team_code=player_data['team_code'],
                element_type=player_data['element_type'],
                now_cost=player_data['now_cost'],
                total_points=safe_int(player_data.get('total_points', 0)),
                status=player_data.get('status', 'a'),

                # Performance stats
                minutes=safe_int(player_data.get('minutes', 0)),
                goals_scored=safe_int(player_data.get('goals_scored', 0)),
                assists=safe_int(player_data.get('assists', 0)),
                clean_sheets=safe_int(player_data.get('clean_sheets', 0)),
                goals_conceded=safe_int(player_data.get('goals_conceded', 0)),
                own_goals=safe_int(player_data.get('own_goals', 0)),
                penalties_saved=safe_int(
                    player_data.get('penalties_saved', 0)),
                penalties_missed=safe_int(
                    player_data.get('penalties_missed', 0)),
                yellow_cards=safe_int(player_data.get('yellow_cards', 0)),
                red_cards=safe_int(player_data.get('red_cards', 0)),
                saves=safe_int(player_data.get('saves', 0)),
                bonus=safe_int(player_data.get('bonus', 0)),

                # String-based stats
                form=safe_str(player_data.get('form', '0.0')),
                points_per_game=safe_str(
                    player_data.get('points_per_game', '0.0')),
                selected_by_percent=safe_str(
                    player_data.get('selected_by_percent', '0.0')),
                value_form=safe_str(player_data.get('value_form', '0.0')),
                value_season=safe_str(player_data.get('value_season', '0.0')),
                expected_goals=safe_str(
                    player_data.get('expected_goals'), '0.00'),
                expected_assists=safe_str(
                    player_data.get('expected_assists'), '0.00'),
                expected_goal_involvements=safe_str(
                    player_data.get('expected_goal_involvements'), '0.00'),
                expected_goals_conceded=safe_str(
                    player_data.get('expected_goals_conceded'), '0.00'),
                influence=safe_str(player_data.get('influence', '0.0')),
                creativity=safe_str(player_data.get('creativity', '0.0')),
                threat=safe_str(player_data.get('threat', '0.0')),
                ict_index=safe_str(player_data.get('ict_index', '0.0')),

                # Transfer stats
                transfers_in=safe_int(player_data.get('transfers_in', 0)),
                transfers_out=safe_int(player_data.get('transfers_out', 0)),
                transfers_in_event=safe_int(
                    player_data.get('transfers_in_event', 0)),
                transfers_out_event=safe_int(
                    player_data.get('transfers_out_event', 0)),
                event_points=safe_int(player_data.get('event_points', 0)),

                # Optional nullable fields
                chance_of_playing_this_round=player_data.get(
                    'chance_of_playing_this_round'),
                chance_of_playing_next_round=player_data.get(
                    'chance_of_playing_next_round'),
                news=player_data.get('news'),
                news_added=player_data.get('news_added'),
                squad_number=player_data.get('squad_number'),
                photo=player_data.get('photo')
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
        if player_data.get('element_type', 0) > 4:
            logger.debug(
                f"Skipping non-player element ID {player_data.get('id', 'unknown')}")
            continue
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
                selected_by_percent=safe_float(
                    player_data.get('selected_by_percent')),
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
                expected_assists=safe_float(
                    player_data.get('expected_assists')),
                expected_goal_involvements=safe_float(
                    player_data.get('expected_goal_involvements')),
                expected_goals_conceded=player_data.get(
                    'expected_goals_conceded')
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
                transfers_balance=safe_int(
                    history_data.get('transfers_balance')),
                transfers_in=safe_int(history_data.get('transfers_in')),
                transfers_out=safe_int(history_data.get('transfers_out')),
                minutes=safe_int(history_data.get('minutes')),
                goals_scored=safe_int(history_data.get('goals_scored')),
                assists=safe_int(history_data.get('assists')),
                clean_sheets=safe_int(history_data.get('clean_sheets')),
                goals_conceded=safe_int(history_data.get('goals_conceded')),
                own_goals=safe_int(history_data.get('own_goals')),
                penalties_saved=safe_int(history_data.get('penalties_saved')),
                penalties_missed=safe_int(
                    history_data.get('penalties_missed')),
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
                expected_assists=safe_float(
                    history_data.get('expected_assists')),
                expected_goal_involvements=safe_float(
                    history_data.get('expected_goal_involvements')),
                expected_goals_conceded=safe_float(
                    history_data.get('expected_goals_conceded'))
            )
            player_history.append(history)
        except Exception as e:
            logger.error(
                f"Failed to parse player history for player ID {player_id}, round {history_data.get('round', 'unknown')}: {e}")
            raise

    logger.info(
        f"Successfully parsed {len(player_history)} player history entries")
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


# New parsers for additional API endpoints
def parse_gameweek_live_data(data: Dict[str, Any]) -> GameweekLiveData:
    """Parse gameweek live data from event/{event_id}/live/ API response.

    Args:
        data: The live gameweek JSON response

    Returns:
        GameweekLiveData object

    Raises:
        KeyError: If expected 'elements' key is missing
        ValidationError: If data doesn't match GameweekLiveData model
    """
    logger.info("Parsing gameweek live data")

    if 'elements' not in data:
        raise KeyError("'elements' key not found in live data")

    elements_data = data['elements']
    elements = []

    for element_data in elements_data:
        try:
            # Helper function to safely get values
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

            def safe_bool(value):
                if value is None:
                    return None
                return bool(value)

            # Parse stats
            stats_data = element_data.get('stats', {})
            stats = GameweekLivePlayerStats(
                goals_scored=safe_int(stats_data.get('goals_scored')),
                assists=safe_int(stats_data.get('assists')),
                own_goals=safe_int(stats_data.get('own_goals')),
                penalties_saved=safe_int(stats_data.get('penalties_saved')),
                penalties_missed=safe_int(stats_data.get('penalties_missed')),
                yellow_cards=safe_int(stats_data.get('yellow_cards')),
                red_cards=safe_int(stats_data.get('red_cards')),
                saves=safe_int(stats_data.get('saves')),
                bonus=safe_int(stats_data.get('bonus')),
                bps=safe_int(stats_data.get('bps')),
                influence=safe_float(stats_data.get('influence')),
                creativity=safe_float(stats_data.get('creativity')),
                threat=safe_float(stats_data.get('threat')),
                ict_index=safe_float(stats_data.get('ict_index')),
                total_points=safe_int(stats_data.get('total_points')),
                in_dreamteam=safe_bool(stats_data.get('in_dreamteam')),
                minutes=safe_int(stats_data.get('minutes'))
            )

            # Parse explain
            explain_data = element_data.get('explain', [])
            explain = []
            for explain_item in explain_data:
                explain_entry = GameweekLivePlayerExplain(
                    fixture=safe_int(explain_item.get('fixture')),
                    stats=explain_item.get('stats', [])
                )
                explain.append(explain_entry)

            element = GameweekLivePlayer(
                id=element_data['id'],
                stats=stats,
                explain=explain
            )
            elements.append(element)
        except Exception as e:
            logger.error(
                f"Failed to parse live data for element ID {element_data.get('id', 'unknown')}: {e}")
            raise

    live_data = GameweekLiveData(elements=elements)
    logger.info(f"Successfully parsed live data for {len(elements)} elements")
    return live_data


def parse_manager_data(data: Dict[str, Any]) -> ManagerData:
    """Parse manager data from /me/ API response.

    Args:
        data: The manager JSON response

    Returns:
        ManagerData object

    Raises:
        ValidationError: If data doesn't match ManagerData model
    """
    logger.info("Parsing manager data")

    try:
        manager = ManagerData(**data)
        logger.info(
            f"Successfully parsed manager data for {manager.player_first_name} {manager.player_last_name}")
        return manager
    except Exception as e:
        logger.error(f"Failed to parse manager data: {e}")
        raise


def parse_league_cup_status(data: Dict[str, Any]) -> LeagueCupStatus:
    """Parse league cup status from /league/{league_id}/cup-status/ API response.

    Args:
        data: The league cup status JSON response

    Returns:
        LeagueCupStatus object

    Raises:
        ValidationError: If data doesn't match LeagueCupStatus model
    """
    logger.info("Parsing league cup status data")

    try:
        cup_status = LeagueCupStatus(**data)
        logger.info(
            f"Successfully parsed cup status for league {cup_status.name}")
        return cup_status
    except Exception as e:
        logger.error(f"Failed to parse league cup status: {e}")
        raise


def parse_entry_data(data: Dict[str, Any]) -> EntryData:
    """Parse entry data from /entry/{entry_id}/ API response.

    Args:
        data: The entry JSON response

    Returns:
        EntryData object

    Raises:
        ValidationError: If data doesn't match EntryData model
    """
    logger.info("Parsing entry data")

    try:
        entry = EntryData(**data)
        logger.info(
            f"Successfully parsed entry data for {entry.player_first_name} {entry.player_last_name}")
        return entry
    except Exception as e:
        logger.error(f"Failed to parse entry data: {e}")
        raise


def parse_h2h_matches(data: Dict[str, Any]) -> H2HMatchData:
    """Parse H2H matches data from /h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/ API response.

    Args:
        data: The H2H matches JSON response

    Returns:
        H2HMatchData object

    Raises:
        ValidationError: If data doesn't match H2HMatchData model
    """
    logger.info("Parsing H2H matches data")

    try:
        h2h_data = H2HMatchData(**data)
        logger.info(f"Successfully parsed {len(h2h_data.results)} H2H matches")
        return h2h_data
    except Exception as e:
        logger.error(f"Failed to parse H2H matches data: {e}")
        raise


def parse_league_standings(data: Dict[str, Any]) -> LeagueStandings:
    """Parse league standings from /leagues-classic/{league_id}/standings/ API response.

    Args:
        data: The league standings JSON response

    Returns:
        LeagueStandings object

    Raises:
        ValidationError: If data doesn't match LeagueStandings model
    """
    logger.info("Parsing league standings data")

    try:
        # Extract the standings data from the response
        standings_data = data.get('standings', data)

        # The API response structure might have the results nested differently
        if 'results' in standings_data:
            league_standings = LeagueStandings(**standings_data)
        else:
            # Handle case where standings data is structured differently
            league_standings = LeagueStandings(
                has_next=data.get('has_next', False),
                page=data.get('page', 1),
                results=data.get('results', [])
            )

        logger.info(
            f"Successfully parsed {len(league_standings.results)} league standings entries")
        return league_standings
    except Exception as e:
        logger.error(f"Failed to parse league standings: {e}")
        raise
