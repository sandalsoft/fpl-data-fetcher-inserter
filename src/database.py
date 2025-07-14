import psycopg2
from psycopg2.extensions import connection
from typing import Optional, List
from .config import get_config
from .utils import get_logger
from .models import (
    Event, Player, PlayerStats, PlayerHistory, Team, Gameweek, Fixture,
    GameweekLiveData, GameweekLivePlayer, ManagerData, LeagueCupStatus,
    EntryData, H2HMatchData, H2HMatch, LeagueStandings
)
import re
import json
import time
from io import StringIO
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

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

    start_time = time.time()
    logger.info(f"🏗️ Starting database insert: {len(events)} events")

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

        duration = time.time() - start_time
        logger.info(
            f"✅ Database insert completed: {len(events)} events in {duration:.2f} seconds")
        logger.info(
            f"📊 Events insert rate: {len(events)/duration:.1f} records/second")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert events: {e}")
        conn.rollback()
        raise


def insert_teams_new(conn: connection, teams: List[Team]) -> None:
    """Insert team data into the database (new schema).

    Args:
        conn: Database connection
        teams: List of Team objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not teams:
        logger.info("No teams to insert")
        return

    start_time = time.time()
    logger.info(f"🏗️ Starting database insert: {len(teams)} teams")

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

            # Execute in batches to handle large datasets
            batch_size = 100  # Smaller batch size for teams
            for i in range(0, len(teams_data), batch_size):
                batch = teams_data[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    logger.debug(
                        f"Inserted team batch {i//batch_size + 1} ({len(batch)} teams)")
                except psycopg2.IntegrityError as e:
                    logger.error(
                        f"Integrity constraint violation in team batch {i//batch_size + 1}: {e}")
                    # Try to identify the specific problematic record
                    for j, team_data in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, team_data)
                        except psycopg2.IntegrityError as inner_e:
                            logger.error(
                                f"Failed to insert team {team_data.get('id', 'unknown')}: {inner_e}")
                            continue
                except psycopg2.DataError as e:
                    logger.error(
                        f"Data type error in team batch {i//batch_size + 1}: {e}")
                    raise
                except psycopg2.Error as e:
                    logger.error(
                        f"Database error in team batch {i//batch_size + 1}: {e}")
                    raise

            conn.commit()

        duration = time.time() - start_time
        logger.info(
            f"✅ Database insert completed: {len(teams)} teams in {duration:.2f} seconds")
        logger.info(
            f"📊 Teams insert rate: {len(teams)/duration:.1f} records/second")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert teams: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting teams: {e}")
        conn.rollback()
        raise


def insert_gameweeks_new(conn: connection, gameweeks: List[Gameweek]) -> None:
    """Insert gameweek data into the database (new schema).

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

            # Execute in batches to handle large datasets
            batch_size = 100  # Smaller batch size for gameweeks
            for i in range(0, len(gameweeks_data), batch_size):
                batch = gameweeks_data[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    logger.debug(
                        f"Inserted gameweek batch {i//batch_size + 1} ({len(batch)} gameweeks)")
                except psycopg2.IntegrityError as e:
                    logger.error(
                        f"Integrity constraint violation in gameweek batch {i//batch_size + 1}: {e}")
                    # Try to identify the specific problematic record
                    for j, gameweek_data in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, gameweek_data)
                        except psycopg2.IntegrityError as inner_e:
                            logger.error(
                                f"Failed to insert gameweek {gameweek_data.get('id', 'unknown')}: {inner_e}")
                            continue
                except psycopg2.DataError as e:
                    logger.error(
                        f"Data type error in gameweek batch {i//batch_size + 1}: {e}")
                    raise
                except psycopg2.Error as e:
                    logger.error(
                        f"Database error in gameweek batch {i//batch_size + 1}: {e}")
                    raise

            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(gameweeks)} gameweeks")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert gameweeks: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting gameweeks: {e}")
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

    start_time = time.time()
    logger.info(f"🏗️ Starting database insert: {len(players)} players")

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

            # Execute in batches to handle large datasets
            batch_size = 1000
            for i in range(0, len(players_data), batch_size):
                batch = players_data[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    logger.debug(
                        f"Inserted batch {i//batch_size + 1} ({len(batch)} players)")
                except psycopg2.IntegrityError as e:
                    logger.error(
                        f"Integrity constraint violation in batch {i//batch_size + 1}: {e}")
                    # Try to identify the specific problematic record
                    for j, player_data in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, player_data)
                        except psycopg2.IntegrityError as inner_e:
                            logger.error(
                                f"Failed to insert player {player_data.get('id', 'unknown')}: {inner_e}")
                            # Continue with other records
                            continue
                except psycopg2.DataError as e:
                    logger.error(
                        f"Data type error in batch {i//batch_size + 1}: {e}")
                    raise
                except psycopg2.Error as e:
                    logger.error(
                        f"Database error in batch {i//batch_size + 1}: {e}")
                    raise

            conn.commit()

        duration = time.time() - start_time
        logger.info(
            f"✅ Database insert completed: {len(players)} players in {duration:.2f} seconds")
        logger.info(
            f"📊 Players insert rate: {len(players)/duration:.1f} records/second")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert players: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting players: {e}")
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

    start_time = time.time()
    logger.info(
        f"🏗️ Starting database insert: {len(player_stats)} player stats")

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

            # Execute in batches to handle large datasets
            batch_size = 1000
            for i in range(0, len(stats_data), batch_size):
                batch = stats_data[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    logger.debug(
                        f"Inserted player stats batch {i//batch_size + 1} ({len(batch)} records)")
                except psycopg2.IntegrityError as e:
                    logger.error(
                        f"Integrity constraint violation in player stats batch {i//batch_size + 1}: {e}")
                    # Try to identify the specific problematic record
                    for j, stats_data_item in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, stats_data_item)
                        except psycopg2.IntegrityError as inner_e:
                            logger.error(
                                f"Failed to insert player stats for player {stats_data_item.get('player_id', 'unknown')}, gameweek {stats_data_item.get('gameweek_id', 'unknown')}: {inner_e}")
                            continue
                except psycopg2.DataError as e:
                    logger.error(
                        f"Data type error in player stats batch {i//batch_size + 1}: {e}")
                    raise
                except psycopg2.Error as e:
                    logger.error(
                        f"Database error in player stats batch {i//batch_size + 1}: {e}")
                    raise

            conn.commit()

        duration = time.time() - start_time
        logger.info(
            f"✅ Database insert completed: {len(player_stats)} player stats in {duration:.2f} seconds")
        logger.info(
            f"📊 Player stats insert rate: {len(player_stats)/duration:.1f} records/second")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert player stats: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting player stats: {e}")
        conn.rollback()
        raise


def insert_player_history_optimized(conn: connection, player_history: List[PlayerHistory]) -> bool:
    """Insert player history data into the database using optimized batch operations.

    Args:
        conn: Database connection
        player_history: List of PlayerHistory objects to insert

    Returns:
        bool: True if VACUUM ANALYZE should be run after this operation

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not player_history:
        logger.info("No player history to insert")
        return False

    logger.info(
        f"Inserting {len(player_history)} player history entries using optimized batch method")

    from .config import get_config
    config = get_config()

    # Deduplicate the data before processing to avoid conflicts
    # Keep the latest entry for each (player_id, gameweek_id) combination
    seen = {}
    deduplicated_history = []

    for player_hist in player_history:
        key = (player_hist.player_id, player_hist.gameweek_id)
        if key not in seen:
            seen[key] = player_hist
            deduplicated_history.append(player_hist)
        else:
            # Replace if this entry has more recent or complete data
            # For now, we'll just keep the first occurrence
            pass

    if len(deduplicated_history) != len(player_history):
        logger.info(
            f"Deduplicated {len(player_history)} records to {len(deduplicated_history)} records")
        player_history = deduplicated_history

    # Process in batches to avoid memory issues and improve performance
    batch_size = config.get('batch_size', 1000)
    total_processed = 0
    start_time = time.time()

    try:
        with conn.cursor() as cursor:
            # Prepare the INSERT statement with ON CONFLICT handling
            insert_sql = """
                INSERT INTO player_history (
                    player_id, gameweek_id, opponent_team, was_home, kickoff_time,
                    total_points, value, selected, transfers_balance, transfers_in,
                    transfers_out, minutes, goals_scored, assists, clean_sheets,
                    goals_conceded, own_goals, penalties_saved, penalties_missed,
                    yellow_cards, red_cards, saves, bonus, bps, influence, creativity,
                    threat, ict_index, starts, expected_goals, expected_assists,
                    expected_goal_involvements, expected_goals_conceded
                ) VALUES %s
                ON CONFLICT (player_id, gameweek_id) 
                DO UPDATE SET
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

            # Process data in batches
            for i in range(0, len(player_history), batch_size):
                batch = player_history[i:i + batch_size]
                batch_start = time.time()

                # Prepare batch data
                batch_data = []
                for player_hist in batch:
                    batch_data.append((
                        player_hist.player_id,
                        player_hist.gameweek_id,
                        player_hist.opponent_team,
                        player_hist.was_home,
                        player_hist.kickoff_time,
                        player_hist.total_points,
                        player_hist.value,
                        player_hist.selected,
                        player_hist.transfers_balance,
                        player_hist.transfers_in,
                        player_hist.transfers_out,
                        player_hist.minutes,
                        player_hist.goals_scored,
                        player_hist.assists,
                        player_hist.clean_sheets,
                        player_hist.goals_conceded,
                        player_hist.own_goals,
                        player_hist.penalties_saved,
                        player_hist.penalties_missed,
                        player_hist.yellow_cards,
                        player_hist.red_cards,
                        player_hist.saves,
                        player_hist.bonus,
                        player_hist.bps,
                        player_hist.influence,
                        player_hist.creativity,
                        player_hist.threat,
                        player_hist.ict_index,
                        player_hist.starts,
                        player_hist.expected_goals,
                        player_hist.expected_assists,
                        player_hist.expected_goal_involvements,
                        player_hist.expected_goals_conceded
                    ))

                # Use execute_values for efficient batch insert
                from psycopg2.extras import execute_values
                execute_values(cursor, insert_sql, batch_data,
                               template=None, page_size=batch_size)

                batch_time = time.time() - batch_start
                total_processed += len(batch)

                logger.debug(
                    f"Processed batch {i//batch_size + 1}: {len(batch)} records in {batch_time:.2f}s")

            # Log performance metrics
            total_time = time.time() - start_time
            logger.info(
                f"Successfully inserted/updated {total_processed} player history entries")
            logger.info(f"Total operation time: {total_time:.2f}s")
            logger.info(
                f"Performance: {total_processed/total_time:.0f} records/second")

            # Return information about whether vacuum should be run
            should_vacuum = (config.get('enable_vacuum_after_bulk', True) and
                             total_processed > config.get('vacuum_threshold', 1000))

            if should_vacuum:
                logger.info(
                    f"Will run VACUUM ANALYZE after transaction commit for {total_processed} records")

            return should_vacuum

    except psycopg2.Error as e:
        logger.error(f"Failed to insert player history (optimized): {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error inserting player history (optimized): {e}")
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

    from .config import get_config
    config = get_config()

    # Use optimized version for large datasets based on configuration
    threshold = config.get('bulk_insert_threshold', 100)
    should_vacuum = False

    if len(player_history) > threshold:
        logger.info(
            f"Using optimized insertion for {len(player_history)} records (threshold: {threshold})")
        should_vacuum = insert_player_history_optimized(conn, player_history)
    else:
        # Fallback to original method for smaller datasets
        logger.info(
            f"Using standard insertion for {len(player_history)} records (threshold: {threshold})")
        insert_player_history_standard(conn, player_history)

    # Run VACUUM ANALYZE outside of the transaction if needed
    if should_vacuum:
        logger.info("Running VACUUM ANALYZE to optimize table for queries")
        vacuum_analyze_table(conn, "player_history")


def insert_player_history_standard(conn: connection, player_history: List[PlayerHistory]) -> None:
    """Insert player history data using the original method (renamed for fallback).

    Args:
        conn: Database connection
        player_history: List of PlayerHistory objects to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    logger.info(
        f"Inserting {len(player_history)} player history entries into database")

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

            # Execute in batches to handle large datasets
            batch_size = 1000
            for i in range(0, len(history_data), batch_size):
                batch = history_data[i:i + batch_size]
                try:
                    cursor.executemany(insert_sql, batch)
                    logger.debug(
                        f"Inserted player history batch {i//batch_size + 1} ({len(batch)} records)")
                except psycopg2.IntegrityError as e:
                    logger.error(
                        f"Integrity constraint violation in player history batch {i//batch_size + 1}: {e}")
                    # Try to identify the specific problematic record
                    for j, history_data_item in enumerate(batch):
                        try:
                            cursor.execute(insert_sql, history_data_item)
                        except psycopg2.IntegrityError as inner_e:
                            logger.error(
                                f"Failed to insert player history for player {history_data_item.get('player_id', 'unknown')}, gameweek {history_data_item.get('gameweek_id', 'unknown')}: {inner_e}")
                            continue
                except psycopg2.DataError as e:
                    logger.error(
                        f"Data type error in player history batch {i//batch_size + 1}: {e}")
                    raise
                except psycopg2.Error as e:
                    logger.error(
                        f"Database error in player history batch {i//batch_size + 1}: {e}")
                    raise

            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(player_history)} player history entries")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert player history: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting player history: {e}")
        conn.rollback()
        raise


# Legacy functions - keeping for backwards compatibility
def insert_gameweeks(conn: connection, gameweeks: List[Gameweek]) -> None:
    """Insert gameweek data into the database (legacy function).

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


def insert_gameweek_live_data(conn: connection, live_data: GameweekLiveData, gameweek_id: int) -> None:
    """Insert gameweek live data into the database.

    Args:
        conn: Database connection
        live_data: GameweekLiveData object to insert
        gameweek_id: The gameweek ID for this live data

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not live_data.elements:
        logger.info("No live data elements to insert")
        return

    logger.info(
        f"Inserting {len(live_data.elements)} live data elements for gameweek {gameweek_id}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS gameweek_live_data (
            id SERIAL PRIMARY KEY,
            gameweek_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            goals_scored INTEGER,
            assists INTEGER,
            own_goals INTEGER,
            penalties_saved INTEGER,
            penalties_missed INTEGER,
            yellow_cards INTEGER,
            red_cards INTEGER,
            saves INTEGER,
            bonus INTEGER,
            bps INTEGER,
            influence FLOAT,
            creativity FLOAT,
            threat FLOAT,
            ict_index FLOAT,
            total_points INTEGER,
            in_dreamteam BOOLEAN,
            minutes INTEGER,
            explain_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(gameweek_id, player_id)
        );
    """

    insert_sql = """
        INSERT INTO gameweek_live_data (
            gameweek_id, player_id, goals_scored, assists, own_goals, penalties_saved,
            penalties_missed, yellow_cards, red_cards, saves, bonus, bps, influence,
            creativity, threat, ict_index, total_points, in_dreamteam, minutes, explain_data
        ) VALUES (
            %(gameweek_id)s, %(player_id)s, %(goals_scored)s, %(assists)s, %(own_goals)s, %(penalties_saved)s,
            %(penalties_missed)s, %(yellow_cards)s, %(red_cards)s, %(saves)s, %(bonus)s, %(bps)s, %(influence)s,
            %(creativity)s, %(threat)s, %(ict_index)s, %(total_points)s, %(in_dreamteam)s, %(minutes)s, %(explain_data)s
        )
        ON CONFLICT (gameweek_id, player_id) DO UPDATE SET
            goals_scored = EXCLUDED.goals_scored,
            assists = EXCLUDED.assists,
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
            total_points = EXCLUDED.total_points,
            in_dreamteam = EXCLUDED.in_dreamteam,
            minutes = EXCLUDED.minutes,
            explain_data = EXCLUDED.explain_data
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Prepare data
            live_data_records = []
            for element in live_data.elements:
                record = {
                    'gameweek_id': gameweek_id,
                    'player_id': element.id,
                    'goals_scored': element.stats.goals_scored,
                    'assists': element.stats.assists,
                    'own_goals': element.stats.own_goals,
                    'penalties_saved': element.stats.penalties_saved,
                    'penalties_missed': element.stats.penalties_missed,
                    'yellow_cards': element.stats.yellow_cards,
                    'red_cards': element.stats.red_cards,
                    'saves': element.stats.saves,
                    'bonus': element.stats.bonus,
                    'bps': element.stats.bps,
                    'influence': element.stats.influence,
                    'creativity': element.stats.creativity,
                    'threat': element.stats.threat,
                    'ict_index': element.stats.ict_index,
                    'total_points': element.stats.total_points,
                    'in_dreamteam': element.stats.in_dreamteam,
                    'minutes': element.stats.minutes,
                    'explain_data': json.dumps([explain.model_dump() for explain in element.explain])
                }
                live_data_records.append(record)

            cursor.executemany(insert_sql, live_data_records)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(live_data_records)} live data records")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert live data: {e}")
        conn.rollback()
        raise


def insert_manager_data(conn: connection, manager_data: ManagerData) -> None:
    """Insert manager data into the database.

    Args:
        conn: Database connection
        manager_data: ManagerData object to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    logger.info(
        f"Inserting manager data for {manager_data.player_first_name} {manager_data.player_last_name}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS manager_data (
            id INTEGER PRIMARY KEY,
            entry_id INTEGER NOT NULL,
            player_first_name VARCHAR(100) NOT NULL,
            player_last_name VARCHAR(100) NOT NULL,
            player_region_id INTEGER,
            player_region_name VARCHAR(100),
            player_region_iso_code_short VARCHAR(10),
            player_region_iso_code_long VARCHAR(10),
            summary_overall_points INTEGER,
            summary_overall_rank INTEGER,
            summary_event_points INTEGER,
            summary_event_rank INTEGER,
            joined_time TIMESTAMP,
            current_event INTEGER,
            total_transfers INTEGER,
            total_loans INTEGER,
            total_loans_active INTEGER,
            transfers_or_loans VARCHAR(50),
            deleted BOOLEAN DEFAULT FALSE,
            email VARCHAR(255),
            entry_email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    insert_sql = """
        INSERT INTO manager_data (
            id, entry_id, player_first_name, player_last_name, player_region_id, player_region_name,
            player_region_iso_code_short, player_region_iso_code_long, summary_overall_points,
            summary_overall_rank, summary_event_points, summary_event_rank, joined_time, current_event,
            total_transfers, total_loans, total_loans_active, transfers_or_loans, deleted, email, entry_email
        ) VALUES (
            %(id)s, %(entry)s, %(player_first_name)s, %(player_last_name)s, %(player_region_id)s, %(player_region_name)s,
            %(player_region_iso_code_short)s, %(player_region_iso_code_long)s, %(summary_overall_points)s,
            %(summary_overall_rank)s, %(summary_event_points)s, %(summary_event_rank)s, %(joined_time)s, %(current_event)s,
            %(total_transfers)s, %(total_loans)s, %(total_loans_active)s, %(transfers_or_loans)s, %(deleted)s, %(email)s, %(entry_email)s
        )
        ON CONFLICT (id) DO UPDATE SET
            entry_id = EXCLUDED.entry_id,
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_region_id = EXCLUDED.player_region_id,
            player_region_name = EXCLUDED.player_region_name,
            player_region_iso_code_short = EXCLUDED.player_region_iso_code_short,
            player_region_iso_code_long = EXCLUDED.player_region_iso_code_long,
            summary_overall_points = EXCLUDED.summary_overall_points,
            summary_overall_rank = EXCLUDED.summary_overall_rank,
            summary_event_points = EXCLUDED.summary_event_points,
            summary_event_rank = EXCLUDED.summary_event_rank,
            joined_time = EXCLUDED.joined_time,
            current_event = EXCLUDED.current_event,
            total_transfers = EXCLUDED.total_transfers,
            total_loans = EXCLUDED.total_loans,
            total_loans_active = EXCLUDED.total_loans_active,
            transfers_or_loans = EXCLUDED.transfers_or_loans,
            deleted = EXCLUDED.deleted,
            email = EXCLUDED.email,
            entry_email = EXCLUDED.entry_email,
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Convert to dict for insertion
            data_dict = manager_data.model_dump()
            cursor.execute(insert_sql, data_dict)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated manager data for ID {manager_data.id}")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert manager data: {e}")
        conn.rollback()
        raise


def insert_league_cup_status(conn: connection, cup_status: LeagueCupStatus) -> None:
    """Insert league cup status into the database.

    Args:
        conn: Database connection
        cup_status: LeagueCupStatus object to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    logger.info(f"Inserting cup status for league {cup_status.name}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS league_cup_status (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            cup_league INTEGER,
            cup_qualified BOOLEAN,
            rank INTEGER,
            entry_rank INTEGER,
            league_type VARCHAR(50),
            scoring VARCHAR(50),
            reprocess_standings BOOLEAN DEFAULT FALSE,
            cup_league_rank INTEGER,
            cup_league_entry_rank INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    insert_sql = """
        INSERT INTO league_cup_status (
            id, name, cup_league, cup_qualified, rank, entry_rank, league_type, scoring,
            reprocess_standings, cup_league_rank, cup_league_entry_rank
        ) VALUES (
            %(id)s, %(name)s, %(cup_league)s, %(cup_qualified)s, %(rank)s, %(entry_rank)s, %(league_type)s, %(scoring)s,
            %(reprocess_standings)s, %(cup_league_rank)s, %(cup_league_entry_rank)s
        )
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            cup_league = EXCLUDED.cup_league,
            cup_qualified = EXCLUDED.cup_qualified,
            rank = EXCLUDED.rank,
            entry_rank = EXCLUDED.entry_rank,
            league_type = EXCLUDED.league_type,
            scoring = EXCLUDED.scoring,
            reprocess_standings = EXCLUDED.reprocess_standings,
            cup_league_rank = EXCLUDED.cup_league_rank,
            cup_league_entry_rank = EXCLUDED.cup_league_entry_rank,
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Convert to dict for insertion
            data_dict = cup_status.model_dump()
            cursor.execute(insert_sql, data_dict)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated cup status for league ID {cup_status.id}")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert cup status: {e}")
        conn.rollback()
        raise


def insert_entry_data(conn: connection, entry_data: EntryData) -> None:
    """Insert entry data into the database.

    Args:
        conn: Database connection
        entry_data: EntryData object to insert

    Raises:
        psycopg2.Error: If insertion fails
    """
    logger.info(
        f"Inserting entry data for {entry_data.player_first_name} {entry_data.player_last_name}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS entry_data (
            id INTEGER PRIMARY KEY,
            joined_time TIMESTAMP,
            started_event INTEGER,
            favourite_team INTEGER,
            player_first_name VARCHAR(100) NOT NULL,
            player_last_name VARCHAR(100) NOT NULL,
            player_region_id INTEGER,
            player_region_name VARCHAR(100),
            player_region_iso_code_short VARCHAR(10),
            player_region_iso_code_long VARCHAR(10),
            summary_overall_points INTEGER,
            summary_overall_rank INTEGER,
            summary_event_points INTEGER,
            summary_event_rank INTEGER,
            current_event INTEGER,
            total_transfers INTEGER,
            total_loans INTEGER,
            total_loans_active INTEGER,
            transfers_or_loans VARCHAR(50),
            deleted BOOLEAN DEFAULT FALSE,
            email VARCHAR(255),
            entry_email VARCHAR(255),
            name VARCHAR(255),
            kit VARCHAR(255),
            last_deadline_bank INTEGER,
            last_deadline_value INTEGER,
            last_deadline_total_transfers INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    insert_sql = """
        INSERT INTO entry_data (
            id, joined_time, started_event, favourite_team, player_first_name, player_last_name,
            player_region_id, player_region_name, player_region_iso_code_short, player_region_iso_code_long,
            summary_overall_points, summary_overall_rank, summary_event_points, summary_event_rank,
            current_event, total_transfers, total_loans, total_loans_active, transfers_or_loans,
            deleted, email, entry_email, name, kit, last_deadline_bank, last_deadline_value,
            last_deadline_total_transfers
        ) VALUES (
            %(id)s, %(joined_time)s, %(started_event)s, %(favourite_team)s, %(player_first_name)s, %(player_last_name)s,
            %(player_region_id)s, %(player_region_name)s, %(player_region_iso_code_short)s, %(player_region_iso_code_long)s,
            %(summary_overall_points)s, %(summary_overall_rank)s, %(summary_event_points)s, %(summary_event_rank)s,
            %(current_event)s, %(total_transfers)s, %(total_loans)s, %(total_loans_active)s, %(transfers_or_loans)s,
            %(deleted)s, %(email)s, %(entry_email)s, %(name)s, %(kit)s, %(last_deadline_bank)s, %(last_deadline_value)s,
            %(last_deadline_total_transfers)s
        )
        ON CONFLICT (id) DO UPDATE SET
            joined_time = EXCLUDED.joined_time,
            started_event = EXCLUDED.started_event,
            favourite_team = EXCLUDED.favourite_team,
            player_first_name = EXCLUDED.player_first_name,
            player_last_name = EXCLUDED.player_last_name,
            player_region_id = EXCLUDED.player_region_id,
            player_region_name = EXCLUDED.player_region_name,
            player_region_iso_code_short = EXCLUDED.player_region_iso_code_short,
            player_region_iso_code_long = EXCLUDED.player_region_iso_code_long,
            summary_overall_points = EXCLUDED.summary_overall_points,
            summary_overall_rank = EXCLUDED.summary_overall_rank,
            summary_event_points = EXCLUDED.summary_event_points,
            summary_event_rank = EXCLUDED.summary_event_rank,
            current_event = EXCLUDED.current_event,
            total_transfers = EXCLUDED.total_transfers,
            total_loans = EXCLUDED.total_loans,
            total_loans_active = EXCLUDED.total_loans_active,
            transfers_or_loans = EXCLUDED.transfers_or_loans,
            deleted = EXCLUDED.deleted,
            email = EXCLUDED.email,
            entry_email = EXCLUDED.entry_email,
            name = EXCLUDED.name,
            kit = EXCLUDED.kit,
            last_deadline_bank = EXCLUDED.last_deadline_bank,
            last_deadline_value = EXCLUDED.last_deadline_value,
            last_deadline_total_transfers = EXCLUDED.last_deadline_total_transfers,
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Convert to dict for insertion
            data_dict = entry_data.model_dump()
            cursor.execute(insert_sql, data_dict)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated entry data for ID {entry_data.id}")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert entry data: {e}")
        conn.rollback()
        raise


def insert_h2h_matches(conn: connection, h2h_data: H2HMatchData, league_id: int) -> None:
    """Insert H2H matches data into the database.

    Args:
        conn: Database connection
        h2h_data: H2HMatchData object to insert
        league_id: The league ID for these matches

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not h2h_data.results:
        logger.info("No H2H matches to insert")
        return

    logger.info(
        f"Inserting {len(h2h_data.results)} H2H matches for league {league_id}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS h2h_matches (
            id INTEGER PRIMARY KEY,
            entry_1_entry INTEGER NOT NULL,
            entry_1_name VARCHAR(255),
            entry_1_player_name VARCHAR(255),
            entry_1_points INTEGER,
            entry_1_win INTEGER,
            entry_1_draw INTEGER,
            entry_1_loss INTEGER,
            entry_1_total INTEGER,
            entry_2_entry INTEGER NOT NULL,
            entry_2_name VARCHAR(255),
            entry_2_player_name VARCHAR(255),
            entry_2_points INTEGER,
            entry_2_win INTEGER,
            entry_2_draw INTEGER,
            entry_2_loss INTEGER,
            entry_2_total INTEGER,
            is_knockout BOOLEAN DEFAULT FALSE,
            league_id INTEGER NOT NULL,
            winner INTEGER,
            seed_value INTEGER,
            event_id INTEGER,
            tiebreak VARCHAR(50),
            is_bye BOOLEAN DEFAULT FALSE,
            knockout_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """

    insert_sql = """
        INSERT INTO h2h_matches (
            id, entry_1_entry, entry_1_name, entry_1_player_name, entry_1_points, entry_1_win, entry_1_draw,
            entry_1_loss, entry_1_total, entry_2_entry, entry_2_name, entry_2_player_name, entry_2_points,
            entry_2_win, entry_2_draw, entry_2_loss, entry_2_total, is_knockout, league_id, winner, seed_value,
            event_id, tiebreak, is_bye, knockout_name
        ) VALUES (
            %(id)s, %(entry_1_entry)s, %(entry_1_name)s, %(entry_1_player_name)s, %(entry_1_points)s, %(entry_1_win)s, %(entry_1_draw)s,
            %(entry_1_loss)s, %(entry_1_total)s, %(entry_2_entry)s, %(entry_2_name)s, %(entry_2_player_name)s, %(entry_2_points)s,
            %(entry_2_win)s, %(entry_2_draw)s, %(entry_2_loss)s, %(entry_2_total)s, %(is_knockout)s, %(league_id)s, %(winner)s, %(seed_value)s,
            %(event_id)s, %(tiebreak)s, %(is_bye)s, %(knockout_name)s
        )
        ON CONFLICT (id) DO UPDATE SET
            entry_1_entry = EXCLUDED.entry_1_entry,
            entry_1_name = EXCLUDED.entry_1_name,
            entry_1_player_name = EXCLUDED.entry_1_player_name,
            entry_1_points = EXCLUDED.entry_1_points,
            entry_1_win = EXCLUDED.entry_1_win,
            entry_1_draw = EXCLUDED.entry_1_draw,
            entry_1_loss = EXCLUDED.entry_1_loss,
            entry_1_total = EXCLUDED.entry_1_total,
            entry_2_entry = EXCLUDED.entry_2_entry,
            entry_2_name = EXCLUDED.entry_2_name,
            entry_2_player_name = EXCLUDED.entry_2_player_name,
            entry_2_points = EXCLUDED.entry_2_points,
            entry_2_win = EXCLUDED.entry_2_win,
            entry_2_draw = EXCLUDED.entry_2_draw,
            entry_2_loss = EXCLUDED.entry_2_loss,
            entry_2_total = EXCLUDED.entry_2_total,
            is_knockout = EXCLUDED.is_knockout,
            league_id = EXCLUDED.league_id,
            winner = EXCLUDED.winner,
            seed_value = EXCLUDED.seed_value,
            event_id = EXCLUDED.event_id,
            tiebreak = EXCLUDED.tiebreak,
            is_bye = EXCLUDED.is_bye,
            knockout_name = EXCLUDED.knockout_name
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Prepare data
            matches_data = []
            for match in h2h_data.results:
                match_dict = match.model_dump()
                match_dict['league_id'] = league_id
                match_dict['event_id'] = match.event
                matches_data.append(match_dict)

            cursor.executemany(insert_sql, matches_data)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(matches_data)} H2H matches")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert H2H matches: {e}")
        conn.rollback()
        raise


def insert_league_standings(conn: connection, standings: LeagueStandings, league_id: int) -> None:
    """Insert league standings into the database.

    Args:
        conn: Database connection
        standings: LeagueStandings object to insert
        league_id: The league ID for these standings

    Raises:
        psycopg2.Error: If insertion fails
    """
    if not standings.results:
        logger.info("No league standings to insert")
        return

    logger.info(
        f"Inserting {len(standings.results)} league standings for league {league_id}")

    # Create table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS league_standings (
            id INTEGER NOT NULL,
            league_id INTEGER NOT NULL,
            event_total INTEGER,
            player_name VARCHAR(255),
            rank INTEGER,
            last_rank INTEGER,
            rank_sort INTEGER,
            total INTEGER,
            entry_id INTEGER,
            entry_name VARCHAR(255),
            page INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, league_id)
        );
    """

    insert_sql = """
        INSERT INTO league_standings (
            id, league_id, event_total, player_name, rank, last_rank, rank_sort, total, entry_id, entry_name, page
        ) VALUES (
            %(id)s, %(league_id)s, %(event_total)s, %(player_name)s, %(rank)s, %(last_rank)s, %(rank_sort)s, %(total)s, %(entry)s, %(entry_name)s, %(page)s
        )
        ON CONFLICT (id, league_id) DO UPDATE SET
            event_total = EXCLUDED.event_total,
            player_name = EXCLUDED.player_name,
            rank = EXCLUDED.rank,
            last_rank = EXCLUDED.last_rank,
            rank_sort = EXCLUDED.rank_sort,
            total = EXCLUDED.total,
            entry_id = EXCLUDED.entry_id,
            entry_name = EXCLUDED.entry_name,
            page = EXCLUDED.page,
            updated_at = CURRENT_TIMESTAMP
    """

    try:
        with conn.cursor() as cursor:
            # Create table
            cursor.execute(create_table_sql)

            # Prepare data
            standings_data = []
            for entry in standings.results:
                entry_dict = entry.model_dump()
                entry_dict['league_id'] = league_id
                entry_dict['page'] = standings.page
                standings_data.append(entry_dict)

            cursor.executemany(insert_sql, standings_data)
            conn.commit()

        logger.info(
            f"Successfully inserted/updated {len(standings_data)} league standings")

    except psycopg2.Error as e:
        logger.error(f"Failed to insert league standings: {e}")
        conn.rollback()
        raise


def optimize_connection_for_bulk_operations(conn: connection) -> None:
    """Optimize database connection settings for bulk operations.

    Args:
        conn: Database connection to optimize
    """
    try:
        with conn.cursor() as cursor:
            # Optimize PostgreSQL settings for bulk inserts
            optimization_queries = [
                "SET synchronous_commit = OFF",  # Faster commits during bulk ops
                "SET wal_buffers = '16MB'",      # Larger WAL buffers
                "SET maintenance_work_mem = '256MB'",  # More memory for maintenance ops
                "SET work_mem = '128MB'",        # More memory for sorting/hashing
            ]

            for query in optimization_queries:
                try:
                    cursor.execute(query)
                    logger.debug(f"Applied optimization: {query}")
                except psycopg2.Error as e:
                    # Some settings might not be available in all PostgreSQL versions
                    logger.debug(
                        f"Could not apply optimization '{query}': {e}")

        logger.debug("Database connection optimized for bulk operations")

    except psycopg2.Error as e:
        logger.warning(f"Failed to optimize connection settings: {e}")


def vacuum_analyze_table(conn: connection, table_name: str) -> None:
    """Run VACUUM ANALYZE on a table to optimize query performance.

    Args:
        conn: Database connection
        table_name: Name of the table to vacuum and analyze
    """
    old_autocommit = None
    try:
        # VACUUM ANALYZE must be run outside of a transaction
        old_autocommit = conn.autocommit
        conn.autocommit = True

        with conn.cursor() as cursor:
            vacuum_sql = f"VACUUM ANALYZE {table_name}"
            logger.debug(f"Running {vacuum_sql}")
            cursor.execute(vacuum_sql)
            logger.info(f"Completed VACUUM ANALYZE on {table_name}")

    except psycopg2.Error as e:
        logger.warning(f"Failed to VACUUM ANALYZE {table_name}: {e}")
    finally:
        # Always restore autocommit setting
        if old_autocommit is not None:
            try:
                conn.autocommit = old_autocommit
            except Exception as e:
                logger.warning(f"Failed to restore autocommit setting: {e}")


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
