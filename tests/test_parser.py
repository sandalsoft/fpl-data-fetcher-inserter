import pytest
from unittest.mock import patch
from src.parser import parse_teams, parse_players
from src.models import Team, Player


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

    def test_parse_teams_invalid_team_data(self):
        """Test error when team data is invalid."""
        mock_data = {
            'teams': [
                {
                    'id': 1,
                    'name': 'Arsenal',
                    # Missing required fields like 'short_name', 'code', etc.
                }
            ]
        }

        with pytest.raises(Exception):  # Should raise validation error
            parse_teams(mock_data)


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
        assert player.team == 1
        assert player.goals_scored == 5
        assert player.assists == 3

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
                    'total_points': 0,
                    'status': 'a',
                    'code': 123,
                    'minutes': 0,
                    'goals_scored': 0,
                    'assists': 0,
                    'clean_sheets': 0,
                    'goals_conceded': 0,
                    'own_goals': 0,
                    'penalties_saved': 0,
                    'penalties_missed': 0,
                    'yellow_cards': 0,
                    'red_cards': 0,
                    'saves': 0,
                    'bonus': 0
                }
            ]
        }

        players = parse_players(mock_data)

        assert len(players) == 1
        player = players[0]

        # Check defaults are applied
        assert player.form == "0.0"
        assert player.points_per_game == "0.0"
        assert player.selected_by_percent == "0.0"
        assert player.expected_goals == "0.00"

    def test_parse_players_with_null_values(self):
        """Test parsing players with null values in optional fields."""
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
                    'total_points': 0,
                    'status': 'a',
                    'code': 123,
                    'minutes': 0,
                    'goals_scored': 0,
                    'assists': 0,
                    'clean_sheets': 0,
                    'goals_conceded': 0,
                    'own_goals': 0,
                    'penalties_saved': 0,
                    'penalties_missed': 0,
                    'yellow_cards': 0,
                    'red_cards': 0,
                    'saves': 0,
                    'bonus': 0,
                    'chance_of_playing_this_round': None,
                    'chance_of_playing_next_round': None,
                    'news': None,
                    'news_added': None,
                    'squad_number': None,
                    'photo': None
                }
            ]
        }

        players = parse_players(mock_data)

        assert len(players) == 1
        player = players[0]

        # Check null values are handled
        assert player.chance_of_playing_this_round is None
        assert player.news is None
        assert player.squad_number is None

    def test_parse_players_invalid_data(self):
        """Test error when player data is invalid."""
        mock_data = {
            'elements': [
                {
                    'id': 1,
                    'first_name': 'John',
                    # Missing many required fields
                }
            ]
        }

        with pytest.raises(Exception):  # Should raise validation error
            parse_players(mock_data)
