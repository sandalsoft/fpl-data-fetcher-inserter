from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel, Field


class Event(BaseModel):
    """Model for FPL event (gameweek) data."""
    id: int
    name: str
    deadline_time: str  # ISO datetime string
    finished: bool
    average_entry_score: Optional[int] = None


class Player(BaseModel):
    """Model for FPL player data (simplified for new schema)."""
    id: int
    code: int
    first_name: str
    second_name: str
    team_id: int  # Changed from 'team' to 'team_id' to match new schema
    element_type: int  # Position type (1=GK, 2=DEF, 3=MID, 4=FWD)
    now_cost: int  # Cost in FPL points (multiply by 0.1 for actual cost)


class PlayerStats(BaseModel):
    """Model for FPL player statistics data."""
    player_id: int
    gameweek_id: int
    total_points: Optional[int] = None
    form: Optional[float] = None
    selected_by_percent: Optional[float] = None
    transfers_in: Optional[int] = None
    transfers_out: Optional[int] = None
    minutes: Optional[int] = None
    goals_scored: Optional[int] = None
    assists: Optional[int] = None
    clean_sheets: Optional[int] = None
    goals_conceded: Optional[int] = None
    own_goals: Optional[int] = None
    penalties_saved: Optional[int] = None
    penalties_missed: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    saves: Optional[int] = None
    bonus: Optional[int] = None
    bps: Optional[int] = None
    influence: Optional[float] = None
    creativity: Optional[float] = None
    threat: Optional[float] = None
    ict_index: Optional[float] = None
    starts: Optional[int] = None
    expected_goals: Optional[float] = None
    expected_assists: Optional[float] = None
    expected_goal_involvements: Optional[float] = None
    expected_goals_conceded: Optional[str] = None


class PlayerHistory(BaseModel):
    """Model for FPL player history data (per-gameweek)."""
    player_id: int
    gameweek_id: int
    opponent_team: Optional[int] = None
    was_home: Optional[bool] = None
    kickoff_time: Optional[str] = None  # ISO datetime string
    total_points: Optional[int] = None
    value: Optional[int] = None  # Cost at that GW
    selected: Optional[int] = None
    transfers_balance: Optional[int] = None
    transfers_in: Optional[int] = None
    transfers_out: Optional[int] = None
    minutes: Optional[int] = None
    goals_scored: Optional[int] = None
    assists: Optional[int] = None
    clean_sheets: Optional[int] = None
    goals_conceded: Optional[int] = None
    own_goals: Optional[int] = None
    penalties_saved: Optional[int] = None
    penalties_missed: Optional[int] = None
    yellow_cards: Optional[int] = None
    red_cards: Optional[int] = None
    saves: Optional[int] = None
    bonus: Optional[int] = None
    bps: Optional[int] = None
    influence: Optional[float] = None
    creativity: Optional[float] = None
    threat: Optional[float] = None
    ict_index: Optional[float] = None
    starts: Optional[int] = None
    expected_goals: Optional[float] = None
    expected_assists: Optional[float] = None
    expected_goal_involvements: Optional[float] = None
    expected_goals_conceded: Optional[float] = None


# Legacy models - keeping for backwards compatibility if needed
class Team(BaseModel):
    """Model for FPL team data."""
    id: int
    name: str
    short_name: str
    code: int
    draw: int
    loss: int
    played: int
    points: int
    position: int
    strength: int
    win: int
    unavailable: bool
    strength_overall_home: int
    strength_overall_away: int
    strength_attack_home: int
    strength_attack_away: int
    strength_defence_home: int
    strength_defence_away: int
    pulse_id: int
    form: Optional[str] = None
    team_division: Optional[str] = None


class Gameweek(BaseModel):
    """Model for FPL gameweek (event) data."""
    id: int
    name: str
    deadline_time: str  # ISO datetime string
    finished: bool
    is_previous: bool
    is_current: bool
    is_next: bool

    # Optional fields
    release_time: Optional[str] = None
    average_entry_score: Optional[int] = None
    data_checked: bool = False
    highest_scoring_entry: Optional[int] = None
    deadline_time_epoch: Optional[int] = None
    deadline_time_game_offset: int = 0
    highest_score: Optional[int] = None
    cup_leagues_created: bool = False
    h2h_ko_matches_created: bool = False
    can_enter: bool = False
    can_manage: bool = False
    released: bool = True
    ranked_count: Optional[int] = None
    transfers_made: int = 0
    most_selected: Optional[int] = None
    most_transferred_in: Optional[int] = None
    most_captained: Optional[int] = None
    most_vice_captained: Optional[int] = None
    top_element: Optional[int] = None


class Fixture(BaseModel):
    """Model for FPL fixture data."""
    id: int
    code: int
    event: Optional[int] = None  # Gameweek ID
    kickoff_time: Optional[str] = None  # ISO datetime string
    team_h: int  # Home team ID
    team_a: int  # Away team ID
    team_h_score: Optional[int] = None
    team_a_score: Optional[int] = None
    finished: bool = False
    finished_provisional: bool = False
    started: bool = False
    minutes: int = 0
    provisional_start_time: bool = False
    team_h_difficulty: Optional[int] = None
    team_a_difficulty: Optional[int] = None
    pulse_id: Optional[int] = None

    # Stats field is complex nested data, keeping as generic for now
    stats: List[Any] = Field(default_factory=list)
