from typing import Optional, List, Any
from datetime import datetime
from pydantic import BaseModel, Field


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


class Player(BaseModel):
    """Model for FPL player (element) data."""
    id: int
    first_name: str
    second_name: str
    web_name: str
    team: int
    team_code: int
    element_type: int  # Position type (1=GK, 2=DEF, 3=MID, 4=FWD)
    now_cost: int  # Cost in FPL points (multiply by 0.1 for actual cost)
    total_points: int
    status: str  # Player availability status
    code: int

    # Performance stats
    minutes: int
    goals_scored: int
    assists: int
    clean_sheets: int
    goals_conceded: int
    own_goals: int
    penalties_saved: int
    penalties_missed: int
    yellow_cards: int
    red_cards: int
    saves: int
    bonus: int

    # Optional fields that might be null
    form: str = "0.0"
    points_per_game: str = "0.0"
    selected_by_percent: str = "0.0"
    transfers_in: int = 0
    transfers_out: int = 0
    transfers_in_event: int = 0
    transfers_out_event: int = 0
    event_points: int = 0
    value_form: str = "0.0"
    value_season: str = "0.0"

    # Expected stats (stored as strings in API)
    expected_goals: str = "0.00"
    expected_assists: str = "0.00"
    expected_goal_involvements: str = "0.00"
    expected_goals_conceded: str = "0.00"

    # ICT stats (stored as strings in API)
    influence: str = "0.0"
    creativity: str = "0.0"
    threat: str = "0.0"
    ict_index: str = "0.0"

    # Nullable fields
    chance_of_playing_this_round: Optional[int] = None
    chance_of_playing_next_round: Optional[int] = None
    news: Optional[str] = None
    news_added: Optional[str] = None
    squad_number: Optional[int] = None
    photo: Optional[str] = None


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
