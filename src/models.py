from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    """Model for FPL event (gameweek) data."""
    id: int
    name: str
    deadline_time: str  # ISO datetime string
    finished: bool
    average_entry_score: Optional[int] = None


class Player(BaseModel):
    """Model for FPL player data (simplified for new schema)."""
    id: int = Field(..., gt=0)
    code: int = Field(..., ge=0)
    first_name: str = Field(..., min_length=1)
    second_name: str = Field(..., min_length=1)
    web_name: str = Field(..., min_length=1)
    team: int = Field(..., gt=0)  # Foreign key to teams table - matches schema
    team_code: int = Field(..., ge=0)
    # Position type (1=GK, 2=DEF, 3=MID, 4=FWD)
    element_type: int = Field(..., ge=1, le=4)
    # Cost in FPL points (multiply by 0.1 for actual cost)
    now_cost: int = Field(..., gt=0)
    total_points: int = Field(default=0)
    status: str = Field(default='a')  # Player availability status

    # Performance stats with defaults to match schema
    minutes: int = Field(default=0, ge=0)
    goals_scored: int = Field(default=0, ge=0)
    assists: int = Field(default=0, ge=0)
    clean_sheets: int = Field(default=0, ge=0)
    goals_conceded: int = Field(default=0, ge=0)
    own_goals: int = Field(default=0, ge=0)
    penalties_saved: int = Field(default=0, ge=0)
    penalties_missed: int = Field(default=0, ge=0)
    yellow_cards: int = Field(default=0, ge=0)
    red_cards: int = Field(default=0, ge=0)
    saves: int = Field(default=0, ge=0)
    bonus: int = Field(default=0, ge=0)

    # String-based stats (stored as strings in API) with defaults
    form: str = Field(default='0.0')
    points_per_game: str = Field(default='0.0')
    selected_by_percent: str = Field(default='0.0')
    value_form: str = Field(default='0.0')
    value_season: str = Field(default='0.0')
    expected_goals: str = Field(default='0.00')
    expected_assists: str = Field(default='0.00')
    expected_goal_involvements: str = Field(default='0.00')
    expected_goals_conceded: str = Field(default='0.00')
    influence: str = Field(default='0.0')
    creativity: str = Field(default='0.0')
    threat: str = Field(default='0.0')
    ict_index: str = Field(default='0.0')

    # Transfer stats
    transfers_in: int = Field(default=0, ge=0)
    transfers_out: int = Field(default=0, ge=0)
    transfers_in_event: int = Field(default=0, ge=0)
    transfers_out_event: int = Field(default=0, ge=0)
    event_points: int = Field(default=0)

    # Optional nullable fields
    chance_of_playing_this_round: Optional[int] = Field(None, ge=0, le=100)
    chance_of_playing_next_round: Optional[int] = Field(None, ge=0, le=100)
    news: Optional[str] = None
    news_added: Optional[str] = None  # ISO datetime string
    squad_number: Optional[int] = Field(None, ge=0)
    photo: Optional[str] = None

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v not in ['a', 'd', 'i', 's', 'u', 'n']:
            raise ValueError(f"Invalid player status: {v}")
        return v


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
    total_points: Optional[int] = Field(None)
    value: Optional[int] = None  # Cost at that GW
    selected: Optional[int] = None
    transfers_balance: Optional[int] = Field(None)
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
