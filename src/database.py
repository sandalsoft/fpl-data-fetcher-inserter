import psycopg2
from psycopg2.extensions import connection
from typing import Optional, List
from .config import get_config
from .utils import get_logger
from .models import Event, Player, PlayerStats, PlayerHistory, Team, Gameweek, Fixture
import re
import json

logger = get_logger(__name__)


def get_connection() -> connection:
    """Get a PostgreSQL database connection.

    Returns:
        psycopg2 connection object

    Raises:
        psycopg2.Error: If connection fails
    """
    config = get_config()

    connection_params = {
        'host': config['db_host'],
        'port': config['db_port'],
        'database': config['db_name'],
        'user': config['db_user'],
        'password': config['db_password']
    }

    logger.info(
        f"Connecting to database at {config['db_host']}:{config['db_port']}/{config['db_name']}")

    try:
        conn = psycopg2.connect(**connection_params)
        logger.info("Database connection established successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def get_cursor(conn: connection):
    """Get a cursor from the database connection.

    Args:
        conn: Database connection object

    Returns:
        Database cursor
    """
    return conn.cursor()


def close_connection(conn: Optional[connection]) -> None:
    """Close database connection safely.

    Args:
        conn: Database connection to close (can be None)
    """
    if conn is not None:
        try:
            conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


def execute_schema(conn: connection, schema_file: str = "sql/schema.sql") -> None:
    """Execute SQL schema file on the database.

    Args:
        conn: Database connection
        schema_file: Path to schema SQL file

    Raises:
        psycopg2.Error: If schema execution fails
        FileNotFoundError: If schema file not found
    """
    logger.info(f"Executing schema from {schema_file}")

    try:
        with open(schema_file, 'r') as f:
            schema_sql = f.read()

        with conn.cursor() as cursor:
            # Split the SQL content by semicolons, but be careful with functions
            # that use dollar quotes or single quotes which can contain semicolons
            statements = []
            current_statement = ""
            in_dollar_quote = False
            in_single_quote = False
            dollar_tag = ""

            lines = schema_sql.split('\n')
            for line in lines:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('--'):
                    continue

                current_statement += line + "\n"

                # Check for dollar quote start/end using regex to handle tagged quotes
                dollar_matches = re.findall(r'\$(\w*)\$', line)
                if dollar_matches and not in_single_quote:
                    if not in_dollar_quote:
                        # Starting dollar quote
                        in_dollar_quote = True
                        # First match is the opening tag
                        dollar_tag = dollar_matches[0]
                    else:
                        # Check if any match closes our current dollar quote
                        if dollar_tag in dollar_matches:
                            in_dollar_quote = False
                            dollar_tag = ""

                # Check for single quote functions (AS 'function_body')
                if not in_dollar_quote:
                    if "AS '" in line and not in_single_quote:
                        in_single_quote = True
                    elif line.strip() == "';" and in_single_quote:
                        in_single_quote = False

                # If we hit a semicolon and we're not in any quote, end the statement
                if line.endswith(';') and not in_dollar_quote and not in_single_quote:
                    statements.append(current_statement.strip())
                    current_statement = ""

            # Add any remaining statement
            if current_statement.strip():
                statements.append(current_statement.strip())

            # Execute each statement separately
            for i, statement in enumerate(statements):
                if statement:
                    try:
                        cursor.execute(statement)
                        logger.debug(
                            f"Executed statement {i+1}/{len(statements)}")
                    except psycopg2.Error as e:
                        logger.error(f"Failed to execute statement {i+1}: {e}")
                        logger.error(
                            f"Statement content: {statement[:200]}...")
                        raise

            conn.commit()

        logger.info("Schema executed successfully")

    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_file}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Failed to execute schema: {e}")
        conn.rollback()
        raise


# New functions for the updated schema
def insert_events(conn: connection, events: List[Event]) -> None:
    """Insert event data into the database.

    Args:
        conn: Database connection
        events: List of Event objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not events:
        logger.info("No events to insert")
        return

    logger.info(f"Inserting {len(events)} events into database")

    insert_sql = """
        INSERT INTO events (
            id, name, deadline_time, finished, average_entry_score
        ) VALUES (
            %(id)s, %(name)s, %(deadline_time)s, %(finished)s, %(average_entry_score)s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            deadline_time = EXCLUDED.deadline_time,
            finished = EXCLUDED.finished,
            average_entry_score = EXCLUDED.average_entry_score
    """

    try:
        with conn.cursor() as cursor:
            # Convert Event objects to dictionaries for psycopg2
            events_data = [event.model_dump() for event in events]
            cursor.executemany(insert_sql, events_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(events)} events")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert events: {e}")
        conn.rollback()
        raise


def insert_players_new(conn: connection, players: List[Player]) -> None:
    """Insert player data into the database (new schema).

    Args:
        conn: Database connection
        players: List of Player objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not players:
        logger.info("No players to insert")
        return

    logger.info(f"Inserting {len(players)} players into database")

    insert_sql = """
        INSERT INTO players (
            id, code, first_name, second_name, team_id, element_type, now_cost
        ) VALUES (
            %(id)s, %(code)s, %(first_name)s, %(second_name)s, %(team_id)s, %(element_type)s, %(now_cost)s
        )
        ON CONFLICT (id) DO UPDATE SET
            code = EXCLUDED.code,
            first_name = EXCLUDED.first_name,
            second_name = EXCLUDED.second_name,
            team_id = EXCLUDED.team_id,
            element_type = EXCLUDED.element_type,
            now_cost = EXCLUDED.now_cost
    """

    try:
        with conn.cursor() as cursor:
            # Convert Player objects to dictionaries for psycopg2
            players_data = [player.model_dump() for player in players]
            cursor.executemany(insert_sql, players_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(players)} players")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert players: {e}")
        conn.rollback()
        raise


def insert_player_stats(conn: connection, player_stats: List[PlayerStats]) -> None:
    """Insert player statistics data into the database.

    Args:
        conn: Database connection
        player_stats: List of PlayerStats objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not player_stats:
        logger.info("No player stats to insert")
        return

    logger.info(f"Inserting {len(player_stats)} player stats into database")

    insert_sql = """
        INSERT INTO player_stats (
            player_id, gameweek_id, total_points, form, selected_by_percent,
            transfers_in, transfers_out, minutes, goals_scored, assists,
            clean_sheets, goals_conceded, own_goals, penalties_saved,
            penalties_missed, yellow_cards, red_cards, saves, bonus, bps,
            influence, creativity, threat, ict_index, starts, expected_goals,
            expected_assists, expected_goal_involvements, expected_goals_conceded
        ) VALUES (
            %(player_id)s, %(gameweek_id)s, %(total_points)s, %(form)s, %(selected_by_percent)s,
            %(transfers_in)s, %(transfers_out)s, %(minutes)s, %(goals_scored)s, %(assists)s,
            %(clean_sheets)s, %(goals_conceded)s, %(own_goals)s, %(penalties_saved)s,
            %(penalties_missed)s, %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s,
            %(influence)s, %(creativity)s, %(threat)s, %(ict_index)s, %(starts)s, %(expected_goals)s,
            %(expected_assists)s, %(expected_goal_involvements)s, %(expected_goals_conceded)s
        )
        ON CONFLICT (player_id, gameweek_id) DO UPDATE SET
            total_points = EXCLUDED.total_points,
            form = EXCLUDED.form,
            selected_by_percent = EXCLUDED.selected_by_percent,
            transfers_in = EXCLUDED.transfers_in,
            transfers_out = EXCLUDED.transfers_out,
            minutes = EXCLUDED.minutes,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            goals_conceded = EXCLUDED.goals_conceded,
            own_goals = EXCLUDED.own_goals,
            penalties_saved = EXCLUDED.penalties_saved,
            penalties_missed = EXCLUDED.penalties_missed,
            yellow_cards = EXCLUDED.yellow_cards,
            red_cards = EXCLUDED.red_cards,
            saves = EXCLUDED.saves,
            bonus = EXCLUDED.bonus,
            bps = EXCLUDED.bps,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ict_index = EXCLUDED.ict_index,
            starts = EXCLUDED.starts,
            expected_goals = EXCLUDED.expected_goals,
            expected_assists = EXCLUDED.expected_assists,
            expected_goal_involvements = EXCLUDED.expected_goal_involvements,
            expected_goals_conceded = EXCLUDED.expected_goals_conceded
    """

    try:
        with conn.cursor() as cursor:
            # Convert PlayerStats objects to dictionaries for psycopg2
            stats_data = [stats.model_dump() for stats in player_stats]
            cursor.executemany(insert_sql, stats_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(player_stats)} player stats")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert player stats: {e}")
        conn.rollback()
        raise


def insert_player_history(conn: connection, player_history: List[PlayerHistory]) -> None:
    """Insert player history data into the database.

    Args:
        conn: Database connection
        player_history: List of PlayerHistory objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not player_history:
        logger.info("No player history to insert")
        return

    logger.info(f"Inserting {len(player_history)} player history entries into database")

    insert_sql = """
        INSERT INTO player_history (
            player_id, gameweek_id, opponent_team, was_home, kickoff_time,
            total_points, value, selected, transfers_balance, transfers_in,
            transfers_out, minutes, goals_scored, assists, clean_sheets,
            goals_conceded, own_goals, penalties_saved, penalties_missed,
            yellow_cards, red_cards, saves, bonus, bps, influence, creativity,
            threat, ict_index, starts, expected_goals, expected_assists,
            expected_goal_involvements, expected_goals_conceded
        ) VALUES (
            %(player_id)s, %(gameweek_id)s, %(opponent_team)s, %(was_home)s, %(kickoff_time)s,
            %(total_points)s, %(value)s, %(selected)s, %(transfers_balance)s, %(transfers_in)s,
            %(transfers_out)s, %(minutes)s, %(goals_scored)s, %(assists)s, %(clean_sheets)s,
            %(goals_conceded)s, %(own_goals)s, %(penalties_saved)s, %(penalties_missed)s,
            %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s, %(influence)s, %(creativity)s,
            %(threat)s, %(ict_index)s, %(starts)s, %(expected_goals)s, %(expected_assists)s,
            %(expected_goal_involvements)s, %(expected_goals_conceded)s
        )
        ON CONFLICT (player_id, gameweek_id) DO UPDATE SET
            opponent_team = EXCLUDED.opponent_team,
            was_home = EXCLUDED.was_home,
            kickoff_time = EXCLUDED.kickoff_time,
            total_points = EXCLUDED.total_points,
            value = EXCLUDED.value,
            selected = EXCLUDED.selected,
            transfers_balance = EXCLUDED.transfers_balance,
            transfers_in = EXCLUDED.transfers_in,
            transfers_out = EXCLUDED.transfers_out,
            minutes = EXCLUDED.minutes,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            goals_conceded = EXCLUDED.goals_conceded,
            own_goals = EXCLUDED.own_goals,
            penalties_saved = EXCLUDED.penalties_saved,
            penalties_missed = EXCLUDED.penalties_missed,
            yellow_cards = EXCLUDED.yellow_cards,
            red_cards = EXCLUDED.red_cards,
            saves = EXCLUDED.saves,
            bonus = EXCLUDED.bonus,
            bps = EXCLUDED.bps,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ict_index = EXCLUDED.ict_index,
            starts = EXCLUDED.starts,
            expected_goals = EXCLUDED.expected_goals,
            expected_assists = EXCLUDED.expected_assists,
            expected_goal_involvements = EXCLUDED.expected_goal_involvements,
            expected_goals_conceded = EXCLUDED.expected_goals_conceded
    """

    try:
        with conn.cursor() as cursor:
            # Convert PlayerHistory objects to dictionaries for psycopg2
            history_data = [history.model_dump() for history in player_history]
            cursor.executemany(insert_sql, history_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(player_history)} player history entries")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert player history: {e}")
        conn.rollback()
        raise


# Legacy functions - keeping for backwards compatibility
def insert_teams(conn: connection, teams: List[Team]) -> None:
    """Insert team data into the database.

    Args:
        conn: Database connection
        teams: List of Team objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not teams:
        logger.info("No teams to insert")
        return

    logger.info(f"Inserting {len(teams)} teams into database")

    insert_sql = """
        INSERT INTO teams (
            id, name, short_name, code, draw, loss, played, points, position,
            strength, win, unavailable, strength_overall_home, strength_overall_away,
            strength_attack_home, strength_attack_away, strength_defence_home,
            strength_defence_away, pulse_id, form, team_division
        ) VALUES (
            %(id)s, %(name)s, %(short_name)s, %(code)s, %(draw)s, %(loss)s, %(played)s,
            %(points)s, %(position)s, %(strength)s, %(win)s, %(unavailable)s,
            %(strength_overall_home)s, %(strength_overall_away)s, %(strength_attack_home)s,
            %(strength_attack_away)s, %(strength_defence_home)s, %(strength_defence_away)s,
            %(pulse_id)s, %(form)s, %(team_division)s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            short_name = EXCLUDED.short_name,
            code = EXCLUDED.code,
            draw = EXCLUDED.draw,
            loss = EXCLUDED.loss,
            played = EXCLUDED.played,
            points = EXCLUDED.points,
            position = EXCLUDED.position,
            strength = EXCLUDED.strength,
            win = EXCLUDED.win,
            unavailable = EXCLUDED.unavailable,
            strength_overall_home = EXCLUDED.strength_overall_home,
            strength_overall_away = EXCLUDED.strength_overall_away,
            strength_attack_home = EXCLUDED.strength_attack_home,
            strength_attack_away = EXCLUDED.strength_attack_away,
            strength_defence_home = EXCLUDED.strength_defence_home,
            strength_defence_away = EXCLUDED.strength_defence_away,
            pulse_id = EXCLUDED.pulse_id,
            form = EXCLUDED.form,
            team_division = EXCLUDED.team_division
    """

    try:
        with conn.cursor() as cursor:
            # Convert Team objects to dictionaries for psycopg2
            teams_data = [team.model_dump() for team in teams]

            cursor.executemany(insert_sql, teams_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(teams)} teams")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert teams: {e}")
        conn.rollback()
        raise


def insert_players(conn: connection, players: List[Player]) -> None:
    """Insert player data into the database (legacy function).

    Args:
        conn: Database connection
        players: List of Player objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not players:
        logger.info("No players to insert")
        return

    logger.info(f"Inserting {len(players)} players into database")

    insert_sql = """
        INSERT INTO players (
            id, first_name, second_name, web_name, team, team_code, element_type,
            now_cost, total_points, status, code, minutes, goals_scored, assists,
            clean_sheets, goals_conceded, own_goals, penalties_saved, penalties_missed,
            yellow_cards, red_cards, saves, bonus, form, points_per_game,
            selected_by_percent, value_form, value_season, expected_goals,
            expected_assists, expected_goal_involvements, expected_goals_conceded,
            influence, creativity, threat, ict_index, transfers_in, transfers_out,
            transfers_in_event, transfers_out_event, event_points,
            chance_of_playing_this_round, chance_of_playing_next_round, news,
            news_added, squad_number, photo
        ) VALUES (
            %(id)s, %(first_name)s, %(second_name)s, %(web_name)s, %(team)s, %(team_code)s,
            %(element_type)s, %(now_cost)s, %(total_points)s, %(status)s, %(code)s,
            %(minutes)s, %(goals_scored)s, %(assists)s, %(clean_sheets)s,
            %(goals_conceded)s, %(own_goals)s, %(penalties_saved)s, %(penalties_missed)s,
            %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(form)s,
            %(points_per_game)s, %(selected_by_percent)s, %(value_form)s, %(value_season)s,
            %(expected_goals)s, %(expected_assists)s, %(expected_goal_involvements)s,
            %(expected_goals_conceded)s, %(influence)s, %(creativity)s, %(threat)s,
            %(ict_index)s, %(transfers_in)s, %(transfers_out)s, %(transfers_in_event)s,
            %(transfers_out_event)s, %(event_points)s, %(chance_of_playing_this_round)s,
            %(chance_of_playing_next_round)s, %(news)s, %(news_added)s, %(squad_number)s,
            %(photo)s
        )
        ON CONFLICT (id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            second_name = EXCLUDED.second_name,
            web_name = EXCLUDED.web_name,
            team = EXCLUDED.team,
            team_code = EXCLUDED.team_code,
            element_type = EXCLUDED.element_type,
            now_cost = EXCLUDED.now_cost,
            total_points = EXCLUDED.total_points,
            status = EXCLUDED.status,
            code = EXCLUDED.code,
            minutes = EXCLUDED.minutes,
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
            clean_sheets = EXCLUDED.clean_sheets,
            goals_conceded = EXCLUDED.goals_conceded,
            own_goals = EXCLUDED.own_goals,
            penalties_saved = EXCLUDED.penalties_saved,
            penalties_missed = EXCLUDED.penalties_missed,
            yellow_cards = EXCLUDED.yellow_cards,
            red_cards = EXCLUDED.red_cards,
            saves = EXCLUDED.saves,
            bonus = EXCLUDED.bonus,
            form = EXCLUDED.form,
            points_per_game = EXCLUDED.points_per_game,
            selected_by_percent = EXCLUDED.selected_by_percent,
            value_form = EXCLUDED.value_form,
            value_season = EXCLUDED.value_season,
            expected_goals = EXCLUDED.expected_goals,
            expected_assists = EXCLUDED.expected_assists,
            expected_goal_involvements = EXCLUDED.expected_goal_involvements,
            expected_goals_conceded = EXCLUDED.expected_goals_conceded,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ict_index = EXCLUDED.ict_index,
            transfers_in = EXCLUDED.transfers_in,
            transfers_out = EXCLUDED.transfers_out,
            transfers_in_event = EXCLUDED.transfers_in_event,
            transfers_out_event = EXCLUDED.transfers_out_event,
            event_points = EXCLUDED.event_points,
            chance_of_playing_this_round = EXCLUDED.chance_of_playing_this_round,
            chance_of_playing_next_round = EXCLUDED.chance_of_playing_next_round,
            news = EXCLUDED.news,
            news_added = EXCLUDED.news_added,
            squad_number = EXCLUDED.squad_number,
            photo = EXCLUDED.photo
    """

    try:
        with conn.cursor() as cursor:
            # Convert Player objects to dictionaries for psycopg2
            players_data = [player.model_dump() for player in players]

            cursor.executemany(insert_sql, players_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(players)} players")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert players: {e}")
        conn.rollback()
        raise


def insert_gameweeks(conn: connection, gameweeks: List[Gameweek]) -> None:
    """Insert gameweek data into the database.

    Args:
        conn: Database connection
        gameweeks: List of Gameweek objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not gameweeks:
        logger.info("No gameweeks to insert")
        return

    logger.info(f"Inserting {len(gameweeks)} gameweeks into database")

    insert_sql = """
        INSERT INTO gameweeks (
            id, name, deadline_time, finished, is_previous, is_current, is_next,
            release_time, average_entry_score, data_checked, highest_scoring_entry,
            deadline_time_epoch, deadline_time_game_offset, highest_score,
            cup_leagues_created, h2h_ko_matches_created, can_enter, can_manage,
            released, ranked_count, transfers_made, most_selected,
            most_transferred_in, most_captained, most_vice_captained, top_element
        ) VALUES (
            %(id)s, %(name)s, %(deadline_time)s, %(finished)s, %(is_previous)s,
            %(is_current)s, %(is_next)s, %(release_time)s, %(average_entry_score)s,
            %(data_checked)s, %(highest_scoring_entry)s, %(deadline_time_epoch)s,
            %(deadline_time_game_offset)s, %(highest_score)s, %(cup_leagues_created)s,
            %(h2h_ko_matches_created)s, %(can_enter)s, %(can_manage)s, %(released)s,
            %(ranked_count)s, %(transfers_made)s, %(most_selected)s,
            %(most_transferred_in)s, %(most_captained)s, %(most_vice_captained)s,
            %(top_element)s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            deadline_time = EXCLUDED.deadline_time,
            finished = EXCLUDED.finished,
            is_previous = EXCLUDED.is_previous,
            is_current = EXCLUDED.is_current,
            is_next = EXCLUDED.is_next,
            release_time = EXCLUDED.release_time,
            average_entry_score = EXCLUDED.average_entry_score,
            data_checked = EXCLUDED.data_checked,
            highest_scoring_entry = EXCLUDED.highest_scoring_entry,
            deadline_time_epoch = EXCLUDED.deadline_time_epoch,
            deadline_time_game_offset = EXCLUDED.deadline_time_game_offset,
            highest_score = EXCLUDED.highest_score,
            cup_leagues_created = EXCLUDED.cup_leagues_created,
            h2h_ko_matches_created = EXCLUDED.h2h_ko_matches_created,
            can_enter = EXCLUDED.can_enter,
            can_manage = EXCLUDED.can_manage,
            released = EXCLUDED.released,
            ranked_count = EXCLUDED.ranked_count,
            transfers_made = EXCLUDED.transfers_made,
            most_selected = EXCLUDED.most_selected,
            most_transferred_in = EXCLUDED.most_transferred_in,
            most_captained = EXCLUDED.most_captained,
            most_vice_captained = EXCLUDED.most_vice_captained,
            top_element = EXCLUDED.top_element
    """

    try:
        with conn.cursor() as cursor:
            # Convert Gameweek objects to dictionaries for psycopg2
            gameweeks_data = [gameweek.model_dump() for gameweek in gameweeks]

            cursor.executemany(insert_sql, gameweeks_data)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(gameweeks)} gameweeks")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert gameweeks: {e}")
        conn.rollback()
        raise


def insert_fixtures(conn: connection, fixtures: List[Fixture]) -> None:
    """Insert fixture data into the database.

    Args:
        conn: Database connection
        fixtures: List of Fixture objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not fixtures:
        logger.info("No fixtures to insert")
        return

    logger.info(f"Inserting {len(fixtures)} fixtures into database")

    insert_sql = """
        INSERT INTO fixtures (
            id, code, event, kickoff_time, team_h, team_a, team_h_score,
            team_a_score, finished, finished_provisional, started, minutes,
            provisional_start_time, team_h_difficulty, team_a_difficulty,
            pulse_id, stats
        ) VALUES (
            %(id)s, %(code)s, %(event)s, %(kickoff_time)s, %(team_h)s, %(team_a)s,
            %(team_h_score)s, %(team_a_score)s, %(finished)s, %(finished_provisional)s,
            %(started)s, %(minutes)s, %(provisional_start_time)s, %(team_h_difficulty)s,
            %(team_a_difficulty)s, %(pulse_id)s, %(stats)s
        )
        ON CONFLICT (id) DO UPDATE SET
            code = EXCLUDED.code,
            event = EXCLUDED.event,
            kickoff_time = EXCLUDED.kickoff_time,
            team_h = EXCLUDED.team_h,
            team_a = EXCLUDED.team_a,
            team_h_score = EXCLUDED.team_h_score,
            team_a_score = EXCLUDED.team_a_score,
            finished = EXCLUDED.finished,
            finished_provisional = EXCLUDED.finished_provisional,
            started = EXCLUDED.started,
            minutes = EXCLUDED.minutes,
            provisional_start_time = EXCLUDED.provisional_start_time,
            team_h_difficulty = EXCLUDED.team_h_difficulty,
            team_a_difficulty = EXCLUDED.team_a_difficulty,
            pulse_id = EXCLUDED.pulse_id,
            stats = EXCLUDED.stats
    """

    try:
        with conn.cursor() as cursor:
            # Convert Fixture objects to dictionaries for psycopg2
            fixtures_data = []
            for fixture in fixtures:
                fixture_dict = fixture.model_dump()
                # Convert stats list to JSON string for JSONB field
                if 'stats' in fixture_dict and fixture_dict['stats'] is not None:
                    fixture_dict['stats'] = json.dumps(fixture_dict['stats'])
                else:
                    fixture_dict['stats'] = '[]'  # Default empty JSON array
                fixtures_data.append(fixture_dict)

            cursor.executemany(insert_sql, fixtures_data)
            conn.commit()

        logger.info(f"Successfully inserted/updated {len(fixtures)} fixtures")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert fixtures: {e}")
        conn.rollback()
        raise


class DatabaseManager:
    """Context manager for database connections."""

    def __init__(self):
        self.conn: Optional[connection] = None

    def __enter__(self) -> connection:
        self.conn = get_connection()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Exception in database context: {exc_val}")
            if self.conn:
                self.conn.rollback()
        close_connection(self.conn)
