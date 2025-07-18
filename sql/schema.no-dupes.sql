-- Create events table
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  deadline_time TIMESTAMP NOT NULL,
  finished BOOLEAN NOT NULL,
  average_entry_score INTEGER
);

-- Create players table
CREATE TABLE IF NOT EXISTS players (
  id INTEGER PRIMARY KEY,
  code INTEGER NOT NULL,
  first_name TEXT NOT NULL,
  second_name TEXT NOT NULL,
  team_id INTEGER NOT NULL,
  element_type INTEGER NOT NULL,
  now_cost INTEGER NOT NULL
);

-- Create player_stats table
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

-- For migrations: Example ALTER statements if needed
-- ALTER TABLE players ADD COLUMN new_column TEXT;
-- ALTER TABLE player_stats ADD COLUMN new_stat REAL;