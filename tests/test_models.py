import pytest
from pydantic import ValidationError
from src.models import Event, Player, PlayerStats, PlayerHistory, Gameweek, Fixture


class TestPlayerModel:
    def test_valid_player(self):
        player_data = {
            'id': 1,
            'code': 123,
            'first_name': 'John',
            'second_name': 'Doe',
            'web_name': 'Doe',
            'team': 1,
            'team_code': 3,
            'element_type': 3,
            'now_cost': 50,
            'status': 'a'
        }
        player = Player(**player_data)
        assert player.id == 1
        assert player.status == 'a'

    def test_invalid_status(self):
        player_data = {
            'id': 1,
            'code': 123,
            'first_name': 'John',
            'second_name': 'Doe',
            'web_name': 'Doe',
            'team': 1,
            'team_code': 3,
            'element_type': 3,
            'now_cost': 50,
            'status': 'x'
        }
        with pytest.raises(ValidationError):
            Player(**player_data)

    def test_invalid_now_cost(self):
        player_data = {
            'id': 1,
            'code': 123,
            'first_name': 'John',
            'second_name': 'Doe',
            'web_name': 'Doe',
            'team': 1,
            'team_code': 3,
            'element_type': 3,
            'now_cost': 0,
            'status': 'a'
        }
        with pytest.raises(ValidationError):
            Player(**player_data)

# Add similar test classes for other models like Event, PlayerStats, etc.


class TestEventModel:
    def test_valid_event(self):
        event_data = {
            'id': 1,
            'name': 'Gameweek 1',
            'deadline_time': '2023-08-11T17:30:00Z',
            'finished': False
        }
        event = Event(**event_data)
        assert event.id == 1

    # Add invalid tests as needed

# Continue for PlayerStats, PlayerHistory, Gameweek, Fixture
