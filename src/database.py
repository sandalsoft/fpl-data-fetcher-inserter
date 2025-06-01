import psycopg2
from psycopg2.extensions import connection
from typing import Optional, List
from .config import get_config
from .utils import get_logger
from .models import Team, Player

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
            cursor.execute(schema_sql)
            conn.commit()

        logger.info("Schema executed successfully")

    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_file}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Failed to execute schema: {e}")
        conn.rollback()
        raise


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
    """Insert player data into the database.

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
