import pytest
from unittest.mock import Mock, patch, mock_open
import psycopg2
from src.database import (
    get_connection, get_cursor, close_connection, execute_schema,
    insert_teams, insert_players, insert_gameweeks, insert_fixtures, DatabaseManager,
    insert_events, insert_players_new, insert_player_stats, insert_player_history,
    insert_teams_new, insert_gameweeks_new
)
from src.models import Team, Player, Event, PlayerStats, PlayerHistory, Gameweek


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

    def test_close_connection_exception(self):
        """Test closing connection with exception."""
        mock_conn = Mock()
        mock_conn.close.side_effect = Exception("Close failed")

        # Should not raise exception
        close_connection(mock_conn)


class TestSchemaExecution:
    """Tests for schema execution."""

    @patch('builtins.open', mock_open(read_data="CREATE TABLE test;"))
    def test_execute_schema_success(self):
        """Test successful schema execution."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        execute_schema(mock_conn, "test_schema.sql")

        mock_cursor.execute.assert_called_once_with("CREATE TABLE test;")
        mock_conn.commit.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError())
    def test_execute_schema_file_not_found(self, mock_open):
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
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        with pytest.raises(psycopg2.Error):
            execute_schema(mock_conn)

        mock_conn.rollback.assert_called_once()


class TestEventInsertion:
    """Tests for event data insertion (new schema)."""

    def create_test_events(self):
        """Create test event data."""
        return [
            Event(
                id=1,
                name="Gameweek 1",
                deadline_time="2024-08-16T17:30:00Z",
                finished=False,
                average_entry_score=50
            ),
            Event(
                id=2,
                name="Gameweek 2",
                deadline_time="2024-08-23T17:30:00Z",
                finished=True,
                average_entry_score=65
            )
        ]

    def test_insert_events_success(self):
        """Test successful event insertion."""
        events = self.create_test_events()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_events(mock_conn, events)

        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()

        # Verify data passed to executemany
        call_args = mock_cursor.executemany.call_args
        sql_query = call_args[0][0]
        events_data = call_args[0][1]

        assert "ON CONFLICT (id) DO UPDATE" in sql_query
        assert len(events_data) == 2
        assert events_data[0]['name'] == 'Gameweek 1'
        assert events_data[1]['name'] == 'Gameweek 2'

    def test_insert_events_empty_list(self):
        """Test inserting empty events list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_events(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_events_database_error(self):
        """Test event insertion with database error."""
        events = self.create_test_events()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.Error("DB error")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        with pytest.raises(psycopg2.Error):
            insert_events(mock_conn, events)

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

    def test_insert_teams_new_success(self):
        """Test successful team insertion with new schema function."""
        teams = self.create_test_teams()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_teams_new(mock_conn, teams)

        # Verify SQL execution - should be called in batches
        assert mock_cursor.executemany.called
        mock_conn.commit.assert_called_once()

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
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        with pytest.raises(psycopg2.Error):
            insert_teams(mock_conn, teams)

        mock_conn.rollback.assert_called_once()


class TestPlayerInsertion:
    """Tests for player data insertion."""

    def create_test_players(self):
        """Create test player data with full schema."""
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
                bonus=8,
                form='5.0',
                points_per_game='3.5',
                selected_by_percent='12.5',
                transfers_in=1000,
                transfers_out=500,
                transfers_in_event=50,
                transfers_out_event=25,
                event_points=6
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
                bonus=5,
                form='2.5',
                points_per_game='2.0',
                selected_by_percent='8.3'
            )
        ]

    def test_insert_players_new_success(self):
        """Test successful player insertion with new schema function."""
        players = self.create_test_players()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_players_new(mock_conn, players)

        # Verify SQL execution - should be called in batches
        assert mock_cursor.executemany.called
        mock_conn.commit.assert_called_once()

    def test_insert_players_new_empty_list(self):
        """Test inserting empty players list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_players_new(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_players_new_integrity_error(self):
        """Test player insertion with integrity error handling."""
        players = self.create_test_players()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.IntegrityError(
            "Constraint violation")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        # Should handle integrity errors gracefully
        insert_players_new(mock_conn, players)

        # Should still commit after handling errors
        mock_conn.commit.assert_called_once()

    def test_insert_players_new_data_error(self):
        """Test player insertion with data error."""
        players = self.create_test_players()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.DataError(
            "Data type error")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        with pytest.raises(psycopg2.DataError):
            insert_players_new(mock_conn, players)

        mock_conn.rollback.assert_called_once()


class TestPlayerStatsInsertion:
    """Tests for player stats data insertion."""

    def create_test_player_stats(self):
        """Create test player stats data."""
        return [
            PlayerStats(
                player_id=1,
                gameweek_id=1,
                total_points=10,
                form=5.0,
                selected_by_percent=12.5,
                transfers_in=1000,
                transfers_out=500,
                minutes=90,
                goals_scored=1,
                assists=0,
                clean_sheets=1,
                goals_conceded=0,
                own_goals=0,
                penalties_saved=0,
                penalties_missed=0,
                yellow_cards=0,
                red_cards=0,
                saves=0,
                bonus=3,
                bps=25,
                influence=50.0,
                creativity=30.0,
                threat=20.0,
                ict_index=10.0,
                starts=1,
                expected_goals=0.5,
                expected_assists=0.2,
                expected_goal_involvements=0.7,
                expected_goals_conceded="0.3"
            ),
            PlayerStats(
                player_id=2,
                gameweek_id=1,
                total_points=6,
                form=3.0,
                selected_by_percent=8.3,
                minutes=90,
                goals_scored=0,
                assists=0,
                clean_sheets=1,
                goals_conceded=0,
                saves=3,
                bonus=1,
                bps=20
            )
        ]

    def test_insert_player_stats_success(self):
        """Test successful player stats insertion."""
        player_stats = self.create_test_player_stats()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_player_stats(mock_conn, player_stats)

        # Verify SQL execution - should be called in batches
        assert mock_cursor.executemany.called
        mock_conn.commit.assert_called_once()

    def test_insert_player_stats_empty_list(self):
        """Test inserting empty player stats list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_player_stats(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_player_stats_integrity_error(self):
        """Test player stats insertion with integrity error handling."""
        player_stats = self.create_test_player_stats()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.IntegrityError(
            "Constraint violation")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        # Should handle integrity errors gracefully
        insert_player_stats(mock_conn, player_stats)

        # Should still commit after handling errors
        mock_conn.commit.assert_called_once()


class TestPlayerHistoryInsertion:
    """Tests for player history data insertion."""

    def create_test_player_history(self):
        """Create test player history data."""
        return [
            PlayerHistory(
                player_id=1,
                gameweek_id=1,
                opponent_team=2,
                was_home=True,
                kickoff_time="2024-08-16T15:00:00Z",
                total_points=10,
                value=80,
                selected=15000,
                transfers_balance=1000,
                transfers_in=1500,
                transfers_out=500,
                minutes=90,
                goals_scored=1,
                assists=0,
                clean_sheets=1,
                goals_conceded=0,
                own_goals=0,
                penalties_saved=0,
                penalties_missed=0,
                yellow_cards=0,
                red_cards=0,
                saves=0,
                bonus=3,
                bps=25,
                influence=50.0,
                creativity=30.0,
                threat=20.0,
                ict_index=10.0,
                starts=1,
                expected_goals=0.5,
                expected_assists=0.2,
                expected_goal_involvements=0.7,
                expected_goals_conceded=0.3
            ),
            PlayerHistory(
                player_id=2,
                gameweek_id=1,
                opponent_team=1,
                was_home=False,
                kickoff_time="2024-08-16T15:00:00Z",
                total_points=6,
                value=45,
                selected=8000,
                minutes=90,
                goals_scored=0,
                assists=0,
                clean_sheets=1,
                goals_conceded=0,
                saves=3,
                bonus=1,
                bps=20
            )
        ]

    def test_insert_player_history_success(self):
        """Test successful player history insertion."""
        player_history = self.create_test_player_history()

        mock_conn = Mock()
        mock_cursor = Mock()

        # Properly mock the context manager
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        insert_player_history(mock_conn, player_history)

        # Verify SQL execution - should be called in batches
        assert mock_cursor.executemany.called
        mock_conn.commit.assert_called_once()

    def test_insert_player_history_empty_list(self):
        """Test inserting empty player history list."""
        mock_conn = Mock()
        mock_cursor = Mock()

        insert_player_history(mock_conn, [])

        # Should not call database operations
        assert not mock_cursor.executemany.called
        assert not mock_conn.commit.called

    def test_insert_player_history_integrity_error(self):
        """Test player history insertion with integrity error handling."""
        player_history = self.create_test_player_history()

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = psycopg2.IntegrityError(
            "Constraint violation")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)

        # Should handle integrity errors gracefully
        insert_player_history(mock_conn, player_history)

        # Should still commit after handling errors
        mock_conn.commit.assert_called_once()


class TestDatabaseManager:
    """Tests for DatabaseManager context manager."""

    @patch('src.database.get_connection')
    def test_database_manager_success(self, mock_get_connection):
        """Test successful database manager context."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with DatabaseManager() as conn:
            assert conn == mock_conn

        mock_get_connection.assert_called_once()

    @patch('src.database.get_connection')
    def test_database_manager_exception(self, mock_get_connection):
        """Test database manager with exception."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with pytest.raises(ValueError):
            with DatabaseManager() as conn:
                raise ValueError("Test exception")

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()
