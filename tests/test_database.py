import pytest
from unittest.mock import Mock, patch, mock_open
import psycopg2
from src.database import (
    get_connection, get_cursor, close_connection, execute_schema,
    insert_teams, insert_players, DatabaseManager
)
from src.models import Team, Player


class TestDatabaseConnection:
    """Tests for database connection utilities."""

    @patch('src.database.psycopg2.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        conn = get_connection()

        assert conn == mock_conn
        mock_connect.assert_called_once()

    @patch('src.database.psycopg2.connect')
    def test_get_connection_failure(self, mock_connect):
        """Test database connection failure."""
        mock_connect.side_effect = psycopg2.Error("Connection failed")

        with pytest.raises(psycopg2.Error):
            get_connection()

    def test_get_cursor(self):
        """Test getting cursor from connection."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        cursor = get_cursor(mock_conn)

        assert cursor == mock_cursor
        mock_conn.cursor.assert_called_once()

    def test_close_connection_valid(self):
        """Test closing valid connection."""
        mock_conn = Mock()

        close_connection(mock_conn)

        mock_conn.close.assert_called_once()

    def test_close_connection_none(self):
        """Test closing None connection."""
        # Should not raise exception
        close_connection(None)

    @patch('src.database.get_connection')
    @patch('src.database.close_connection')
    def test_database_manager_context(self, mock_close, mock_get_connection):
        """Test DatabaseManager context manager."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with DatabaseManager() as conn:
            assert conn == mock_conn

        mock_close.assert_called_once_with(mock_conn)


class TestSchemaExecution:
    """Tests for schema execution."""

    @patch('builtins.open', mock_open(read_data="CREATE TABLE test;"))
    def test_execute_schema_success(self):
        """Test successful schema execution."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        execute_schema(mock_conn, "test_schema.sql")

        mock_cursor.execute.assert_called_once_with("CREATE TABLE test;")
        mock_conn.commit.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_execute_schema_file_not_found(self):
        """Test schema execution with missing file."""
        mock_conn = Mock()

        with pytest.raises(FileNotFoundError):
            execute_schema(mock_conn, "missing.sql")

    @patch('builtins.open', mock_open(read_data="INVALID SQL;"))
    def test_execute_schema_sql_error(self):
        """Test schema execution with SQL error."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = psycopg2.Error("SQL error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with pytest.raises(psycopg2.Error):
            execute_schema(mock_conn)

        mock_conn.rollback.assert_called_once()


class TestTeamInsertion:
    """Tests for team data insertion."""

    def create_test_teams(self):
        """Create test team data."""
        return [
            Team(
                id=1,
                name='Arsenal',
                short_name='ARS',
                code=3,
                draw=0,
                loss=0,
                played=0,
                points=0,
                position=2,
                strength=5,
                win=0,
                unavailable=False,
                strength_overall_home=1350,
                strength_overall_away=1350,
                strength_attack_home=1390,
                strength_attack_away=1400,
                strength_defence_home=1310,
                strength_defence_away=1300,
                pulse_id=1
            ),
            Team(
                id=2,
                name='Chelsea',
                short_name='CHE',
                code=8,
                draw=0,
                loss=0,
                played=0,
                points=0,
                position=4,
                strength=5,
                win=0,
                unavailable=False,
                strength_overall_home=1300,
                strength_overall_away=1300,
                strength_attack_home=1300,
                strength_attack_away=1300,
                strength_defence_home=1300,
                strength_defence_away=1300,
                pulse_id=2
            )
        ]

    def test_insert_teams_success(self):
        """Test successful team insertion."""
        teams = self.create_test_teams()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_teams(mock_conn, teams)

        # Verify SQL execution
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

        # Verify data passed to executemany
        call_args = mock_cursor.executemany.call_args
        sql_query = call_args[0][0]
        teams_data = call_args[0][1]

        assert "ON CONFLICT (id) DO UPDATE" in sql_query
        assert len(teams_data) == 2
        assert teams_data[0]['name'] == 'Arsenal'
        assert teams_data[1]['name'] == 'Chelsea'

    def test_insert_teams_empty_list(self):
        """Test inserting empty teams list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_teams(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_teams_database_error(self):
        """Test team insertion with database error."""
        teams = self.create_test_teams()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.Error("DB error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with pytest.raises(psycopg2.Error):
            insert_teams(mock_conn, teams)

        mock_conn.rollback.assert_called_once()


class TestPlayerInsertion:
    """Tests for player data insertion."""

    def create_test_players(self):
        """Create test player data."""
        return [
            Player(
                id=1,
                first_name='Test',
                second_name='Player1',
                web_name='Player1',
                team=1,
                team_code=3,
                element_type=3,
                now_cost=80,
                total_points=50,
                status='a',
                code=12345,
                minutes=900,
                goals_scored=5,
                assists=3,
                clean_sheets=2,
                goals_conceded=10,
                own_goals=0,
                penalties_saved=0,
                penalties_missed=0,
                yellow_cards=2,
                red_cards=0,
                saves=0,
                bonus=8
            ),
            Player(
                id=2,
                first_name='Test',
                second_name='Player2',
                web_name='Player2',
                team=2,
                team_code=8,
                element_type=1,
                now_cost=45,
                total_points=30,
                status='a',
                code=67890,
                minutes=450,
                goals_scored=0,
                assists=0,
                clean_sheets=5,
                goals_conceded=5,
                own_goals=0,
                penalties_saved=2,
                penalties_missed=0,
                yellow_cards=1,
                red_cards=0,
                saves=20,
                bonus=5
            )
        ]

    def test_insert_players_success(self):
        """Test successful player insertion."""
        players = self.create_test_players()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_players(mock_conn, players)

        # Verify SQL execution
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

        # Verify data passed to executemany
        call_args = mock_cursor.executemany.call_args
        sql_query = call_args[0][0]
        players_data = call_args[0][1]

        assert "ON CONFLICT (id) DO UPDATE" in sql_query
        assert len(players_data) == 2
        assert players_data[0]['first_name'] == 'Test'
        assert players_data[0]['second_name'] == 'Player1'
        assert players_data[1]['second_name'] == 'Player2'

    def test_insert_players_empty_list(self):
        """Test inserting empty players list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_players(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_players_database_error(self):
        """Test player insertion with database error."""
        players = self.create_test_players()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.Error("DB error")
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with pytest.raises(psycopg2.Error):
            insert_players(mock_conn, players)

        mock_conn.rollback.assert_called_once()

    def test_insert_players_with_nullable_fields(self):
        """Test inserting players with nullable fields."""
        player = Player(
            id=1,
            first_name='John',
            second_name='Doe',
            web_name='Doe',
            team=1,
            team_code=3,
            element_type=1,
            now_cost=45,
            total_points=0,
            status='a',
            code=123,
            minutes=0,
            goals_scored=0,
            assists=0,
            clean_sheets=0,
            goals_conceded=0,
            own_goals=0,
            penalties_saved=0,
            penalties_missed=0,
            yellow_cards=0,
            red_cards=0,
            saves=0,
            bonus=0,
            # Nullable fields
            chance_of_playing_this_round=None,
            chance_of_playing_next_round=None,
            news=None,
            news_added=None,
            squad_number=None,
            photo=None
        )

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_players(mock_conn, [player])

        # Verify execution completed without error
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

        # Verify nullable fields are handled correctly
        call_args = mock_cursor.executemany.call_args
        players_data = call_args[0][1]

        assert players_data[0]['chance_of_playing_this_round'] is None
        assert players_data[0]['news'] is None
        assert players_data[0]['squad_number'] is None
