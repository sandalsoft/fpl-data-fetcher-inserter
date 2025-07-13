import pytest
from unittest.mock import patch
from src.parser import parse_teams, parse_players, parse_events, parse_player_stats, parse_player_history
from src.models import Team, Player, Event, PlayerStats, PlayerHistory


class TestParseTeams:
    """Tests for parse_teams function."""

    def test_parse_teams_valid_data(self):
        """Test parsing valid teams data."""
        mock_data = {
            'teams': [
                {
                    'id': 1,
                    'name': 'Arsenal',
                    'short_name': 'ARS',
                    'code': 3,
                    'draw': 0,
                    'loss': 0,
                    'played': 0,
                    'points': 0,
                    'position': 2,
                    'strength': 5,
                    'win': 0,
                    'unavailable': False,
                    'strength_overall_home': 1350,
                    'strength_overall_away': 1350,
                    'strength_attack_home': 1390,
                    'strength_attack_away': 1400,
                    'strength_defence_home': 1310,
                    'strength_defence_away': 1300,
                    'pulse_id': 1,
                    'form': None,
                    'team_division': None
                },
                {
                    'id': 2,
                    'name': 'Chelsea',
                    'short_name': 'CHE',
                    'code': 8,
                    'draw': 0,
                    'loss': 0,
                    'played': 0,
                    'points': 0,
                    'position': 4,
                    'strength': 5,
                    'win': 0,
                    'unavailable': False,
                    'strength_overall_home': 1300,
                    'strength_overall_away': 1300,
                    'strength_attack_home': 1300,
                    'strength_attack_away': 1300,
                    'strength_defence_home': 1300,
                    'strength_defence_away': 1300,
                    'pulse_id': 2,
                    'form': None,
                    'team_division': None
                }
            ]
        }

        teams = parse_teams(mock_data)

        assert len(teams) == 2
        assert all(isinstance(team, Team) for team in teams)

        # Check first team
        assert teams[0].id == 1
        assert teams[0].name == 'Arsenal'
        assert teams[0].short_name == 'ARS'

        # Check second team
        assert teams[1].id == 2
        assert teams[1].name == 'Chelsea'
        assert teams[1].short_name == 'CHE'

    def test_parse_teams_missing_key(self):
        """Test error when 'teams' key is missing."""
        mock_data = {'other_key': 'value'}

        with pytest.raises(KeyError, match="'teams' key not found"):
            parse_teams(mock_data)

    def test_parse_teams_empty_list(self):
        """Test parsing empty teams list."""
        mock_data = {'teams': []}

        teams = parse_teams(mock_data)
        assert teams == []

    def test_parse_teams_invalid_data(self):
        """Test error when team data is invalid."""
        mock_data = {
            'teams': [
                {
                    'id': 1,
                    'name': 'Arsenal',
                    # Missing required fields
                }
            ]
        }

        with pytest.raises(Exception):
            parse_teams(mock_data)


class TestParseEvents:
    """Tests for parse_events function."""

    def test_parse_events_valid_data(self):
        """Test parsing valid events data."""
        mock_data = {
            'events': [
                {
                    'id': 1,
                    'name': 'Gameweek 1',
                    'deadline_time': '2024-08-16T17:30:00Z',
                    'finished': False,
                    'average_entry_score': 50
                },
                {
                    'id': 2,
                    'name': 'Gameweek 2',
                    'deadline_time': '2024-08-23T17:30:00Z',
                    'finished': True,
                    'average_entry_score': 65
                }
            ]
        }

        events = parse_events(mock_data)

        assert len(events) == 2
        assert all(isinstance(event, Event) for event in events)

        # Check first event
        assert events[0].id == 1
        assert events[0].name == 'Gameweek 1'
        assert events[0].finished == False
        assert events[0].average_entry_score == 50

        # Check second event
        assert events[1].id == 2
        assert events[1].name == 'Gameweek 2'
        assert events[1].finished == True
        assert events[1].average_entry_score == 65

    def test_parse_events_missing_key(self):
        """Test error when 'events' key is missing."""
        mock_data = {'other_key': 'value'}

        with pytest.raises(KeyError, match="'events' key not found"):
            parse_events(mock_data)

    def test_parse_events_empty_list(self):
        """Test parsing empty events list."""
        mock_data = {'events': []}

        events = parse_events(mock_data)
        assert events == []


class TestParsePlayers:
    """Tests for parse_players function."""

    def test_parse_players_valid_data(self):
        """Test parsing valid players data."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'first_name': 'Player',
                    'second_name': 'One',
                    'web_name': 'Player One',
                    'team': 1,
                    'team_code': 3,
                    'element_type': 3,
                    'now_cost': 80,
                    'total_points': 50,
                    'status': 'a',
                    'code': 12345,
                    'minutes': 900,
                    'goals_scored': 5,
                    'assists': 3,
                    'clean_sheets': 2,
                    'goals_conceded': 10,
                    'own_goals': 0,
                    'penalties_saved': 0,
                    'penalties_missed': 0,
                    'yellow_cards': 2,
                    'red_cards': 0,
                    'saves': 0,
                    'bonus': 8,
                    'form': '5.0',
                    'points_per_game': '3.5',
                    'selected_by_percent': '12.5',
                    'transfers_in': 1000,
                    'transfers_out': 500,
                    'transfers_in_event': 50,
                    'transfers_out_event': 25,
                    'event_points': 6,
                    'value_form': '10.0',
                    'value_season': '6.5',
                    'expected_goals': '2.50',
                    'expected_assists': '1.25',
                    'expected_goal_involvements': '3.75',
                    'expected_goals_conceded': '8.50',
                    'influence': '250.5',
                    'creativity': '180.2',
                    'threat': '120.8',
                    'ict_index': '55.1',
                    'chance_of_playing_this_round': 100,
                    'chance_of_playing_next_round': 100,
                    'news': '',
                    'news_added': None,
                    'squad_number': 10,
                    'photo': 'player.jpg'
                }
            ]
        }

        players = parse_players(mock_data)

        assert len(players) == 1
        assert isinstance(players[0], Player)

        player = players[0]
        assert player.id == 1
        assert player.first_name == 'Player'
        assert player.second_name == 'One'
        assert player.web_name == 'Player One'
        assert player.team == 1
        assert player.team_code == 3
        assert player.element_type == 3
        assert player.now_cost == 80
        assert player.total_points == 50
        assert player.status == 'a'
        assert player.code == 12345
        assert player.goals_scored == 5
        assert player.assists == 3
        assert player.form == '5.0'
        assert player.points_per_game == '3.5'
        assert player.selected_by_percent == '12.5'
        assert player.transfers_in == 1000
        assert player.transfers_out == 500
        assert player.expected_goals == '2.50'
        assert player.chance_of_playing_this_round == 100
        assert player.squad_number == 10

    def test_parse_players_missing_key(self):
        """Test error when 'elements' key is missing."""
        mock_data = {'other_key': 'value'}

        with pytest.raises(KeyError, match="'elements' key not found"):
            parse_players(mock_data)

    def test_parse_players_empty_list(self):
        """Test parsing empty players list."""
        mock_data = {'elements': []}

        players = parse_players(mock_data)
        assert players == []

    def test_parse_players_minimal_required_fields(self):
        """Test parsing with minimal required fields and defaults."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'first_name': 'John',
                    'second_name': 'Doe',
                    'web_name': 'Doe',
                    'team': 1,
                    'team_code': 3,
                    'element_type': 1,
                    'now_cost': 45,
                    'code': 123
                }
            ]
        }

        players = parse_players(mock_data)

        assert len(players) == 1
        player = players[0]

        # Check defaults are applied
        assert player.total_points == 0
        assert player.status == 'a'
        assert player.minutes == 0
        assert player.goals_scored == 0
        assert player.form == "0.0"
        assert player.points_per_game == "0.0"
        assert player.selected_by_percent == "0.0"
        assert player.expected_goals == "0.00"
        assert player.transfers_in == 0
        assert player.transfers_out == 0
        assert player.chance_of_playing_this_round is None
        assert player.news is None

    def test_parse_players_with_null_values(self):
        """Test parsing players with null/empty values."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'first_name': 'John',
                    'second_name': 'Doe',
                    'web_name': 'Doe',
                    'team': 1,
                    'team_code': 3,
                    'element_type': 1,
                    'now_cost': 45,
                    'code': 123,
                    'total_points': None,
                    'form': '',
                    'expected_goals': None,
                    'transfers_in': '',
                    'chance_of_playing_this_round': None,
                    'news': None,
                    'squad_number': None
                }
            ]
        }

        players = parse_players(mock_data)

        assert len(players) == 1
        player = players[0]

        # Check null/empty values are handled correctly
        assert player.total_points == 0  # Default applied
        assert player.form == "0.0"  # Default applied
        assert player.expected_goals == "0.00"  # Default applied
        assert player.transfers_in == 0  # Default applied
        assert player.chance_of_playing_this_round is None  # Nullable field
        assert player.news is None  # Nullable field
        assert player.squad_number is None  # Nullable field

    def test_parse_players_invalid_data(self):
        """Test error when player data is invalid."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'first_name': 'John',
                    # Missing required fields
                }
            ]
        }

        with pytest.raises(Exception):
            parse_players(mock_data)


class TestParsePlayerStats:
    """Tests for parse_player_stats function."""

    def test_parse_player_stats_valid_data(self):
        """Test parsing valid player stats data."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'total_points': 50,
                    'form': '5.0',
                    'selected_by_percent': '12.5',
                    'transfers_in': 1000,
                    'transfers_out': 500,
                    'minutes': 900,
                    'goals_scored': 5,
                    'assists': 3,
                    'clean_sheets': 2,
                    'goals_conceded': 10,
                    'own_goals': 0,
                    'penalties_saved': 0,
                    'penalties_missed': 0,
                    'yellow_cards': 2,
                    'red_cards': 0,
                    'saves': 0,
                    'bonus': 8,
                    'bps': 25,
                    'influence': '250.5',
                    'creativity': '180.2',
                    'threat': '120.8',
                    'ict_index': '55.1',
                    'starts': 10,
                    'expected_goals': '2.50',
                    'expected_assists': '1.25',
                    'expected_goal_involvements': '3.75',
                    'expected_goals_conceded': '8.50'
                }
            ]
        }

        player_stats = parse_player_stats(mock_data, 1)

        assert len(player_stats) == 1
        assert isinstance(player_stats[0], PlayerStats)

        stats = player_stats[0]
        assert stats.player_id == 1
        assert stats.gameweek_id == 1
        assert stats.total_points == 50
        assert stats.form == 5.0
        assert stats.selected_by_percent == 12.5
        assert stats.transfers_in == 1000
        assert stats.transfers_out == 500
        assert stats.minutes == 900
        assert stats.goals_scored == 5
        assert stats.assists == 3
        assert stats.influence == 250.5
        assert stats.creativity == 180.2
        assert stats.threat == 120.8
        assert stats.ict_index == 55.1
        assert stats.starts == 10
        assert stats.expected_goals == 2.50
        assert stats.expected_assists == 1.25
        assert stats.expected_goal_involvements == 3.75
        assert stats.expected_goals_conceded == '8.50'

    def test_parse_player_stats_with_null_values(self):
        """Test parsing player stats with null/empty values."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'total_points': None,
                    'form': '',
                    'selected_by_percent': None,
                    'transfers_in': '',
                    'minutes': None,
                    'goals_scored': '',
                    'influence': None,
                    'expected_goals': '',
                    'expected_goals_conceded': None
                }
            ]
        }

        player_stats = parse_player_stats(mock_data, 1)

        assert len(player_stats) == 1
        stats = player_stats[0]

        # Check null/empty values are handled correctly
        assert stats.total_points is None
        assert stats.form is None
        assert stats.selected_by_percent is None
        assert stats.transfers_in is None
        assert stats.minutes is None
        assert stats.goals_scored is None
        assert stats.influence is None
        assert stats.expected_goals is None
        assert stats.expected_goals_conceded is None

    def test_parse_player_stats_missing_key(self):
        """Test error when 'elements' key is missing."""
        mock_data = {'other_key': 'value'}

        with pytest.raises(KeyError, match="'elements' key not found"):
            parse_player_stats(mock_data, 1)

    def test_parse_player_stats_empty_list(self):
        """Test parsing empty player stats list."""
        mock_data = {'elements': []}

        player_stats = parse_player_stats(mock_data, 1)
        assert player_stats == []


class TestParsePlayerHistory:
    """Tests for parse_player_history function."""

    def test_parse_player_history_valid_data(self):
        """Test parsing valid player history data."""
        mock_data = [
            {
                'round': 1,
                'opponent_team': 2,
                'was_home': True,
                'kickoff_time': '2024-08-16T15:00:00Z',
                'total_points': 10,
                'value': 80,
                'selected': 15000,
                'transfers_balance': 1000,
                'transfers_in': 1500,
                'transfers_out': 500,
                'minutes': 90,
                'goals_scored': 1,
                'assists': 0,
                'clean_sheets': 1,
                'goals_conceded': 0,
                'own_goals': 0,
                'penalties_saved': 0,
                'penalties_missed': 0,
                'yellow_cards': 0,
                'red_cards': 0,
                'saves': 0,
                'bonus': 3,
                'bps': 25,
                'influence': 50.0,
                'creativity': 30.0,
                'threat': 20.0,
                'ict_index': 10.0,
                'starts': 1,
                'expected_goals': 0.5,
                'expected_assists': 0.2,
                'expected_goal_involvements': 0.7,
                'expected_goals_conceded': 0.3
            }
        ]

        player_history = parse_player_history(mock_data, 1)

        assert len(player_history) == 1
        assert isinstance(player_history[0], PlayerHistory)

        history = player_history[0]
        assert history.player_id == 1
        assert history.gameweek_id == 1
        assert history.opponent_team == 2
        assert history.was_home == True
        assert history.kickoff_time == '2024-08-16T15:00:00Z'
        assert history.total_points == 10
        assert history.value == 80
        assert history.selected == 15000
        assert history.transfers_balance == 1000
        assert history.minutes == 90
        assert history.goals_scored == 1
        assert history.assists == 0
        assert history.clean_sheets == 1
        assert history.influence == 50.0
        assert history.creativity == 30.0
        assert history.threat == 20.0
        assert history.ict_index == 10.0
        assert history.starts == 1
        assert history.expected_goals == 0.5
        assert history.expected_assists == 0.2
        assert history.expected_goal_involvements == 0.7
        assert history.expected_goals_conceded == 0.3

    def test_parse_player_history_with_null_values(self):
        """Test parsing player history with null/empty values."""
        mock_data = [
            {
                'round': 1,
                'opponent_team': None,
                'was_home': None,
                'kickoff_time': None,
                'total_points': None,
                'value': '',
                'selected': None,
                'minutes': '',
                'goals_scored': None,
                'influence': '',
                'expected_goals': None
            }
        ]

        player_history = parse_player_history(mock_data, 1)

        assert len(player_history) == 1
        history = player_history[0]

        # Check null/empty values are handled correctly
        assert history.player_id == 1
        assert history.gameweek_id == 1
        assert history.opponent_team is None
        assert history.was_home is None
        assert history.kickoff_time is None
        assert history.total_points is None
        assert history.value is None
        assert history.selected is None
        assert history.minutes is None
        assert history.goals_scored is None
        assert history.influence is None
        assert history.expected_goals is None

    def test_parse_player_history_empty_list(self):
        """Test parsing empty player history list."""
        mock_data = []

        player_history = parse_player_history(mock_data, 1)
        assert player_history == []

    def test_parse_player_history_invalid_data_type(self):
        """Test error when player history data is not a list."""
        mock_data = {'not': 'a list'}

        with pytest.raises(TypeError, match="Player history data should be a list"):
            parse_player_history(mock_data, 1)
