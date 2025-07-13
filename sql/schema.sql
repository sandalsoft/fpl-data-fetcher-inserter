-- Complete FPL Data Schema for Player Tracking Strategy
-- Includes teams, gameweeks (events), players, fixtures (optional for opponent context),
-- player_stats (cumulative snapshots from bootstrap-static),
-- and player_history (per-gameweek details from element-summary).
-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(10) NOT NULL,
    code INTEGER NOT NULL,
    draw INTEGER DEFAULT 0,
    loss INTEGER DEFAULT 0,
    played INTEGER DEFAULT 0,
    points INTEGER DEFAULT 0,
    position INTEGER,
    strength INTEGER,
    win INTEGER DEFAULT 0,
    unavailable BOOLEAN DEFAULT FALSE,
    strength_overall_home INTEGER,
    strength_overall_away INTEGER,
    strength_attack_home INTEGER,
    strength_attack_away INTEGER,
    strength_defence_home INTEGER,
    strength_defence_away INTEGER,
    pulse_id INTEGER,
    form VARCHAR(10),
    team_division VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gameweeks table (events in API)
CREATE TABLE IF NOT EXISTS gameweeks (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    deadline_time TIMESTAMP,
    finished BOOLEAN DEFAULT FALSE,
    is_previous BOOLEAN DEFAULT FALSE,
    is_current BOOLEAN DEFAULT FALSE,
    is_next BOOLEAN DEFAULT FALSE,
    release_time TIMESTAMP,
    average_entry_score INTEGER,
    data_checked BOOLEAN DEFAULT FALSE,
    highest_scoring_entry INTEGER,
    deadline_time_epoch BIGINT,
    deadline_time_game_offset INTEGER DEFAULT 0,
    highest_score INTEGER,
    cup_leagues_created BOOLEAN DEFAULT FALSE,
    h2h_ko_matches_created BOOLEAN DEFAULT FALSE,
    can_enter BOOLEAN DEFAULT FALSE,
    can_manage BOOLEAN DEFAULT FALSE,
    released BOOLEAN DEFAULT TRUE,
    ranked_count INTEGER,
    transfers_made INTEGER DEFAULT 0,
    most_selected INTEGER,
    most_transferred_in INTEGER,
    most_captained INTEGER,
    most_vice_captained INTEGER,
    top_element INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Players table (elements in API)
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    second_name VARCHAR(50) NOT NULL,
    web_name VARCHAR(50) NOT NULL,
    team INTEGER NOT NULL REFERENCES teams(id),
    team_code INTEGER NOT NULL,
    element_type INTEGER NOT NULL,
    -- 1=GK, 2=DEF, 3=MID, 4=FWD
    now_cost INTEGER NOT NULL,
    total_points INTEGER DEFAULT 0,
    status VARCHAR(1) DEFAULT 'a',
    -- Player availability status
    code INTEGER NOT NULL,
    -- Performance stats
    minutes INTEGER DEFAULT 0,
    goals_scored INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    clean_sheets INTEGER DEFAULT 0,
    goals_conceded INTEGER DEFAULT 0,
    own_goals INTEGER DEFAULT 0,
    penalties_saved INTEGER DEFAULT 0,
    penalties_missed INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    saves INTEGER DEFAULT 0,
    bonus INTEGER DEFAULT 0,
    -- String-based stats (stored as strings in API)
    form VARCHAR(10) DEFAULT '0.0',
    points_per_game VARCHAR(10) DEFAULT '0.0',
    selected_by_percent VARCHAR(10) DEFAULT '0.0',
    value_form VARCHAR(10) DEFAULT '0.0',
    value_season VARCHAR(10) DEFAULT '0.0',
    expected_goals VARCHAR(10) DEFAULT '0.00',
    expected_assists VARCHAR(10) DEFAULT '0.00',
    expected_goal_involvements VARCHAR(10) DEFAULT '0.00',
    expected_goals_conceded VARCHAR(10) DEFAULT '0.00',
    influence VARCHAR(10) DEFAULT '0.0',
    creativity VARCHAR(10) DEFAULT '0.0',
    threat VARCHAR(10) DEFAULT '0.0',
    ict_index VARCHAR(10) DEFAULT '0.0',
    -- Transfer stats
    transfers_in INTEGER DEFAULT 0,
    transfers_out INTEGER DEFAULT 0,
    transfers_in_event INTEGER DEFAULT 0,
    transfers_out_event INTEGER DEFAULT 0,
    event_points INTEGER DEFAULT 0,
    -- Optional nullable fields
    chance_of_playing_this_round INTEGER,
    chance_of_playing_next_round INTEGER,
    news TEXT,
    news_added TIMESTAMP,
    squad_number INTEGER,
    photo VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fixtures table (optional for opponent context)
CREATE TABLE IF NOT EXISTS fixtures (
    id INTEGER PRIMARY KEY,
    code INTEGER NOT NULL,
    event INTEGER REFERENCES gameweeks(id),
    -- Gameweek ID (nullable for future fixtures)
    kickoff_time TIMESTAMP,
    team_h INTEGER NOT NULL REFERENCES teams(id),
    -- Home team
    team_a INTEGER NOT NULL REFERENCES teams(id),
    -- Away team
    team_h_score INTEGER,
    team_a_score INTEGER,
    finished BOOLEAN DEFAULT FALSE,
    finished_provisional BOOLEAN DEFAULT FALSE,
    started BOOLEAN DEFAULT FALSE,
    minutes INTEGER DEFAULT 0,
    provisional_start_time BOOLEAN DEFAULT FALSE,
    team_h_difficulty INTEGER,
    team_a_difficulty INTEGER,
    pulse_id INTEGER,
    -- Stats stored as JSONB for complex nested data
    stats JSONB DEFAULT '[]' :: jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Player_stats table (cumulative snapshots from bootstrap-static)
CREATE TABLE IF NOT EXISTS player_stats (
    stat_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    gameweek_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_points INTEGER,
    form REAL,
    selected_by_percent REAL,
    transfers_in INTEGER,
    transfers_out INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    bonus INTEGER,
    bps INTEGER,
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    starts INTEGER,
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded TEXT,
    -- Stored as text if JSON-like
    UNIQUE (player_id, gameweek_id) -- Prevent duplicates
);

-- Player_history table (per-gameweek details from element-summary)
CREATE TABLE IF NOT EXISTS player_history (
    history_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    gameweek_id INTEGER NOT NULL,
    opponent_team INTEGER,
    was_home BOOLEAN,
    kickoff_time TIMESTAMP,
    total_points INTEGER,
    value INTEGER,
    -- Cost at that GW
    selected INTEGER,
    transfers_balance INTEGER,
    transfers_in INTEGER,
    transfers_out INTEGER,
    minutes INTEGER,
    goals_scored INTEGER,
    assists INTEGER,
    clean_sheets INTEGER,
    goals_conceded INTEGER,
    own_goals INTEGER,
    penalties_saved INTEGER,
    penalties_missed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    saves INTEGER,
    bonus INTEGER,
    bps INTEGER,
    influence REAL,
    creativity REAL,
    threat REAL,
    ict_index REAL,
    starts INTEGER,
    expected_goals REAL,
    expected_assists REAL,
    expected_goal_involvements REAL,
    expected_goals_conceded REAL,
    UNIQUE (player_id, gameweek_id) -- Prevent duplicates
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team);

CREATE INDEX IF NOT EXISTS idx_players_element_type ON players(element_type);

CREATE INDEX IF NOT EXISTS idx_fixtures_event ON fixtures(event);

CREATE INDEX IF NOT EXISTS idx_fixtures_teams ON fixtures(team_h, team_a);

CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff ON fixtures(kickoff_time);

CREATE INDEX IF NOT EXISTS idx_gameweeks_current ON gameweeks(is_current)
WHERE
    is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_gameweeks_next ON gameweeks(is_next)
WHERE
    is_next = TRUE;

CREATE INDEX IF NOT EXISTS idx_player_stats_player_gw ON player_stats(player_id, gameweek_id);

CREATE INDEX IF NOT EXISTS idx_player_history_player_gw ON player_history(player_id, gameweek_id);

-- Update triggers for updated_at timestamps
CREATE
OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER LANGUAGE plpgsql AS '
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
';

CREATE TRIGGER update_teams_updated_at BEFORE
UPDATE
    ON teams FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_players_updated_at BEFORE
UPDATE
    ON players FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fixtures_updated_at BEFORE
UPDATE
    ON fixtures FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_gameweeks_updated_at BEFORE
UPDATE
    ON gameweeks FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- For migrations: Example ALTER statements if needed
-- ALTER TABLE player_history ADD COLUMN new_field REAL;
-- ALTER TABLE player_stats ADD COLUMN additional_stat INTEGER;