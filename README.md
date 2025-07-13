# FPL Data Fetcher & Inserter

A robust Python application that fetches Fantasy Premier League (FPL) data from the official API and stores it in a PostgreSQL database. The system now supports both a new normalized schema and legacy schema for backward compatibility, with comprehensive error handling and flexible CLI options.

## Features

- **Dual Schema Support**: Choose between new normalized schema or legacy schema
- **Complete FPL Data Pipeline**: Fetches, parses, and stores teams, players, gameweeks, and fixtures data
- **Normalized Database Design**: Separate tables for events, players, player stats, and player history
- **Robust Database Integration**: PostgreSQL with automatic schema creation and upsert operations
- **Flexible CLI**: Control what data types to process with command-line flags
- **Dry-Run Mode**: Preview operations without database changes
- **Comprehensive Logging**: Detailed logging with timestamps and module names
- **Error Handling**: Graceful handling of API failures and database errors
- **Data Validation**: Pydantic models ensure data integrity
- **Schema Validation**: Built-in SQL syntax validation and proper PostgreSQL function handling
- **Extensible Architecture**: Modular design for easy feature additions
- **Backwards Compatibility**: Legacy applications continue to work unchanged

## Schema Options

The application supports two database schemas:

### New Schema (Default)

A normalized structure with separate tables:

- **events**: Simplified gameweek data (id, name, deadline_time, finished, average_entry_score)
- **players**: Basic player information (id, code, first_name, second_name, team_id, element_type, now_cost)
- **player_stats**: Player statistics per gameweek (player_id, gameweek_id, statistics...)
- **player_history**: Historical player data per gameweek (player_id, gameweek_id, history...)

### Legacy Schema

The original schema with:

- **teams**: Complete team information
- **players**: Full player data including statistics
- **gameweeks**: Complete gameweek information
- **fixtures**: Match fixtures and results

## Data Processing

The application processes the following FPL data:

### Teams (20 teams)

- Basic information: name, short name, code
- League position and statistics: played, won, drawn, lost, points
- Strength ratings: overall, attack, defence (home/away)
- Form and availability status

### Players (800+ players)

- Personal information: names, team, position type
- Performance statistics: goals, assists, minutes played
- FPL metrics: cost, total points, ownership percentage
- Advanced stats: expected goals, ICT index, form ratings
- Injury and availability information

### Gameweeks (38 gameweeks)

- Gameweek information: name, deadline times
- Status flags: current, previous, next, finished
- Statistics: average scores, highest scores
- Administrative data: transfers made, most selected players

### Fixtures (380+ fixtures)

- Match information: teams, kickoff times, scores
- Status: finished, started, provisional
- Difficulty ratings for both teams
- Detailed match statistics stored as JSON

## Installation

### Prerequisites

- Python 3.12+
- PostgreSQL database
- UV package manager (or pip)

### Setup

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd fpl-data-fetcher-inserter
   ```

2. **Create virtual environment and install dependencies**

   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -r requirements.txt
   ```

3. **Configure database connection**

   ```bash
   cp .env_example .env
   # Edit .env with your PostgreSQL credentials
   ```

4. **Set up PostgreSQL database**

   ```sql
   CREATE DATABASE fpl_data;
   CREATE USER fpl_user WITH PASSWORD 'fpl_password';
   GRANT ALL PRIVILEGES ON DATABASE fpl_data TO fpl_user;
   ```

5. **Validate schema (optional)**

   ```bash
   # Validate SQL schema syntax without database connection
   python validate_schema.py
   ```

## Usage

### Basic Usage

```bash
# Fetch and insert all data using new schema (default)
python -m src.app

# Preview what would be processed (dry-run mode)
python -m src.app --dry-run

# Use legacy schema
python -m src.app --legacy

# Process only specific data types (new schema)
python -m src.app --events --players
python -m src.app --player-stats --player-history

# Process only specific data types (legacy schema)
python -m src.app --teams --players --legacy
python -m src.app --gameweeks --fixtures --legacy

# Verbose output with configuration details
python -m src.app --verbose --dry-run
```

### CLI Options

#### Schema Selection

- `--legacy`: Use legacy schema (teams, players, gameweeks, fixtures)

#### New Schema Data Types

- `--events`: Process events (gameweeks) data
- `--players`: Process players data
- `--player-stats`: Process player statistics data
- `--player-history`: Process player history data

#### Legacy Schema Data Types

- `--teams`: Process teams data (legacy schema)
- `--gameweeks`: Process gameweeks data (legacy schema)
- `--fixtures`: Process fixtures data (legacy schema)

#### General Options

- `--dry-run`: Preview mode - fetch and parse data but skip database insertion
- `--verbose, -v`: Enable verbose logging output
- `--help, -h`: Show help message with examples

**Note**: If no specific data type flags are provided, the application defaults to processing all available data types for the selected schema.

### Configuration

The application uses environment variables loaded from `.env_example`:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fpl_data
DB_USER=fpl_user
DB_PASSWORD=fpl_password
FPL_API_URL=https://fantasy.premierleague.com/api
```

## Database Schema

### New Schema (Default)

The normalized PostgreSQL schema includes:

- **events**: Simplified gameweek data (id, name, deadline_time, finished, average_entry_score)
- **players**: Basic player information (id, code, first_name, second_name, team_id, element_type, now_cost)
- **player_stats**: Player statistics per gameweek with foreign keys to players and events
- **player_history**: Historical player data per gameweek for tracking changes over time

### Legacy Schema

The original PostgreSQL schema includes:

- **teams**: Team information and statistics
- **players**: Comprehensive player data with foreign key to teams
- **gameweeks**: Match week information with deadlines and status flags
- **fixtures**: Match data with teams, scores, and detailed statistics (JSONB)

All tables include:

- Automatic timestamp tracking (`created_at`, `updated_at`)
- Primary keys and foreign key relationships
- Optimized indexes for query performance
- Upsert operations to handle data updates
- Proper PostgreSQL function syntax with dollar-quoted strings

### Schema Validation

The project includes a schema validation utility:

```bash
# Validate schema syntax without database connection
python validate_schema.py
```

This validates:

- PostgreSQL function syntax (dollar quotes, BEGIN/END blocks)
- Table and index definitions
- Trigger creation statements
- Overall SQL structure

## Development

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_fetcher.py -v

# Run with coverage
python -m pytest tests/ --cov=src
```

### Project Structure

```filetree
fpl-data-fetcher-inserter/
├── src/
│   ├── app.py          # Main application runner
│   ├── config.py       # Configuration loading
│   ├── fetcher.py      # FPL API data fetching
│   ├── parser.py       # Data parsing and validation
│   ├── models.py       # Pydantic data models
│   ├── database.py     # Database operations
│   └── utils.py        # Logging utilities
├── tests/              # Test suite
├── sql/                # Database schema
├── validate_schema.py  # Schema validation utility
├── requirements.txt    # Python dependencies
└── .env_example       # Environment configuration template
```

## Architecture

The application follows a modular pipeline architecture:

1. **Configuration**: Load database and API settings
2. **Data Fetching**: Retrieve data from FPL API endpoints (`/bootstrap-static/` and `/fixtures/`)
3. **Data Parsing**: Validate and structure data using Pydantic models
4. **Database Operations**: Insert/update data with upsert logic

The database module includes enhanced schema execution that properly handles PostgreSQL functions with dollar-quoted strings by parsing SQL statements individually rather than executing the entire file as one statement.

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## Troubleshooting

### Common Issues

#### Database Connection Failed

- Verify PostgreSQL is running
- Check database credentials in `.env_example`
- Ensure database and user exist

#### Schema Execution Errors

- Run `python validate_schema.py` to check for syntax issues
- Ensure PostgreSQL functions use proper dollar-quote syntax
- Check that all required tables and indexes are defined

#### API Request Failed

- Check internet connection
- Verify FPL API URL is accessible
- API may be temporarily unavailable during maintenance

#### Import Errors

- Ensure virtual environment is activated
- Run `uv pip install -r requirements.txt`
- Check Python version compatibility

### Logs

The application provides detailed logging for troubleshooting:

- API request/response information
- Data parsing statistics (teams, players, gameweeks, fixtures)
- Database operation results
- Error messages with context

## Future Enhancements

- Player gameweek history data
- Automated scheduling with cron
- Change detection for incremental updates
- Performance optimizations for large datasets
- Additional data endpoints (transfers, chips, etc.)
- Real-time fixture updates

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Acknowledgments

- Fantasy Premier League for providing the public API
- The Python community for excellent data processing libraries

## Using Pydantic Models

The application uses Pydantic models for data validation and structuring. Here are some examples of how to use them:

```python
from src.models import Player

# Create a Player instance
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
print(player)
```

Invalid data will raise a ValidationError:

```python
try:
    invalid_player = Player(id=0, first_name='')
except ValidationError as e:
    print(e)
```

Similar usage applies to other models like Event, PlayerStats, etc.
