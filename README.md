# FPL Data Fetcher & Inserter

A comprehensive Python application for fetching and storing Fantasy Premier League (FPL) data from the official API.

## Features

### Core Functionality

- **Bootstrap Data**: Fetches general information about teams, players, and gameweeks
- **Player Data**: Retrieves detailed player statistics and historical performance
- **Fixtures**: Gets fixture data for all gameweeks
- **Live Data**: Fetches real-time gameweek statistics

### New API Endpoints (Recently Implemented)

- **Fixtures by Gameweek**: `/fixtures?event={event_id}` - Get fixtures for a specific gameweek
- **Gameweek Live Data**: `/event/{event_id}/live/` - Live statistics for every player in a gameweek
- **Manager Data**: `/me/` - Authenticated manager information
- **Entry Data**: `/entry/{entry_id}/` - Data for a specific team entry
- **League Cup Status**: `/league/{league_id}/cup-status/` - Cup status for leagues
- **H2H Matches**: `/h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/` - Head-to-head matches
- **League Standings**: `/leagues-classic/{league_id}/standings/` - Classic league standings

### Database Integration

- **PostgreSQL Support**: Stores all data in structured PostgreSQL tables
- **Schema Management**: Automatic table creation and updates
- **Data Validation**: Pydantic models ensure data integrity
- **Conflict Resolution**: Upsert operations handle duplicate data

### Performance Optimizations

- **Bulk Database Operations**: Uses PostgreSQL COPY for high-speed bulk inserts
- **Parallel API Fetching**: Concurrent requests with configurable worker pools
- **Optimized Database Connections**: Session-level optimizations for bulk operations
- **Automatic VACUUM**: Post-insert table optimization for query performance
- **Comprehensive Timing**: Detailed performance metrics for all operations
- **Configurable Thresholds**: Adaptive optimization based on data volume

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd fpl-data-fetcher-inserter
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables:

```bash
cp .env.example .env
# Edit .env with your database credentials
```

## Usage

### Basic Usage

```bash
python -m src.app
```

By default, this fetches **ALL available data** including:

- Events (gameweeks) data
- Players data
- Player statistics
- **Player history from element-summary endpoint for ALL players**

### Command Line Options

```bash
# Run with specific data types
python -m src.app --events --players --player-stats

# Dry run (preview without database insertion)
python -m src.app --dry-run

# Use legacy schema
python -m src.app --legacy --teams --fixtures
```

### Available Data Types

#### New Schema (Default)

- `--events`: Process events (gameweeks) data
- `--players`: Process players data
- `--player-stats`: Process player statistics
- `--player-history`: Process player historical data from element-summary endpoint

#### Legacy Schema

- `--teams`: Process teams data
- `--fixtures`: Process fixtures data
- `--gameweeks`: Process gameweeks data

## API Endpoints

### Fetcher Functions

The application provides easy-to-use functions for all FPL API endpoints:

```python
from src.fetcher import (
    fetch_bootstrap_data,
    fetch_fixtures_data,
    fetch_fixtures_by_gameweek,
    fetch_gameweek_live_data,
    fetch_manager_data,
    fetch_entry_data,
    fetch_league_cup_status,
    fetch_h2h_matches,
    fetch_league_standings,
    fetch_player_history
)

# Example usage
bootstrap_data = fetch_bootstrap_data()
live_data = fetch_gameweek_live_data(1)  # Gameweek 1
standings = fetch_league_standings(123)  # League ID 123
```

### Parser Functions

Data parsing with validation:

```python
from src.parser import (
    parse_gameweek_live_data,
    parse_manager_data,
    parse_entry_data,
    parse_league_standings,
    parse_h2h_matches
)

# Example usage
live_data = parse_gameweek_live_data(raw_data)
manager = parse_manager_data(raw_manager_data)
```

### Database Functions

Database insertion with conflict handling:

```python
from src.database import (
    insert_gameweek_live_data,
    insert_manager_data,
    insert_entry_data,
    insert_league_standings,
    insert_h2h_matches
)

# Example usage
with DatabaseManager() as conn:
    insert_gameweek_live_data(conn, live_data, gameweek_id=1)
    insert_league_standings(conn, standings, league_id=123)
```

## Database Schema

The application creates and manages the following tables:

### Core Tables

- `players`: Player information and statistics
- `teams`: Team data
- `gameweeks`: Gameweek/event information
- `fixtures`: Fixture data

### New Tables

- `gameweek_live_data`: Real-time player statistics per gameweek
- `manager_data`: Manager/user information
- `entry_data`: Team entry details
- `league_cup_status`: League cup status information
- `h2h_matches`: Head-to-head match data
- `league_standings`: League standings data

## Testing

Run the comprehensive test suite:

```bash
python test_new_endpoints.py
```

The test suite validates:

- ✅ Fixtures by Gameweek
- ✅ Gameweek Live Data
- ✅ Manager Data
- ✅ Entry Data
- ✅ League Cup Status
- ✅ League Standings
- ⚠️ H2H Matches (requires specific league/entry combinations)

## Configuration

### Environment Variables

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fpl_data
DB_USER=fpl_user
DB_PASSWORD=fpl_password
FPL_API_URL=https://fantasy.premierleague.com/api
PARALLEL_WORKERS=15
```

### Performance Configuration

- **PARALLEL_WORKERS**: Number of concurrent HTTP requests for player history fetching (default: 15)
  - Higher values = faster data collection but more API load
  - Lower values = slower but more API-friendly
  - Recommended range: 5-20 depending on your API rate limits

### Parallelization Strategy

The application uses a multi-level parallelization approach:

1. **Core Endpoints**: Bootstrap and fixtures data are fetched simultaneously
2. **Player History**: All 784 players' historical data fetched concurrently
3. **Batch Processing**: Large datasets processed in manageable chunks
4. **Error Handling**: Individual failures don't stop the entire process

### Authentication

Some endpoints require authentication:

- `/me/` - Requires FPL login session
- H2H matches may require specific permissions

## Performance Features

### Parallel Data Fetching

- **Concurrent Player History**: Fetches player history from `/api/element-summary/{element_id}/` endpoints in parallel
- **Configurable Workers**: Adjustable number of concurrent requests (default: 15)
- **Batch Processing**: Processes large datasets in manageable batches
- **Rate Limiting**: Built-in delays between batches to respect API limits
- **Comprehensive Logging**: Progress tracking for parallel operations

### Speed Improvements

- **784 players**: Fetched in ~30 seconds instead of ~13 minutes (26x faster)
- **27,000+ history entries**: Processed efficiently with concurrent fetching
- **Scalable**: Performance scales with the number of available workers

## Error Handling

The application includes comprehensive error handling:

- **Network Errors**: Automatic retry and timeout handling
- **Data Validation**: Pydantic models ensure data integrity
- **Database Errors**: Transaction rollback on failures
- **API Errors**: Graceful handling of API rate limits and errors
- **Parallel Execution**: Individual request failures don't stop the entire batch

## Architecture

### Functional Programming Patterns

- **Dependency Injection**: Configuration and database connections
- **Currying**: Partial application of common functions
- **Immutable Data**: Pydantic models ensure data consistency
- **Error Handling**: Monadic error handling patterns

### Code Organization

- `src/fetcher.py`: API endpoint functions
- `src/parser.py`: Data parsing and validation
- `src/database.py`: Database operations
- `src/models.py`: Pydantic data models
- `src/config.py`: Configuration management
- `src/utils.py`: Utility functions

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

MIT License - see LICENSE file for details
