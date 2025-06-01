# FPL Data Fetcher & Inserter

A robust Python application that fetches Fantasy Premier League (FPL) data from the official API and stores it in a PostgreSQL database. The system handles teams, players, fixtures, and gameweeks data with comprehensive error handling and flexible CLI options.

## Features

- **Complete FPL Data Pipeline**: Fetches, parses, and stores teams and players data
- **Robust Database Integration**: PostgreSQL with automatic schema creation and upsert operations
- **Flexible CLI**: Control what data types to process with command-line flags
- **Dry-Run Mode**: Preview operations without database changes
- **Comprehensive Logging**: Detailed logging with timestamps and module names
- **Error Handling**: Graceful handling of API failures and database errors
- **Data Validation**: Pydantic models ensure data integrity
- **Extensible Architecture**: Modular design for easy feature additions

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

## Usage

### Basic Usage

```bash
# Fetch and insert all data (teams + players)
python -m src.app

# Preview what would be processed (dry-run mode)
python -m src.app --dry-run

# Process only teams data
python -m src.app --teams

# Process only players data
python -m src.app --players

# Verbose output with configuration details
python -m src.app --verbose --dry-run
```

### CLI Options

- `--dry-run`: Preview mode - fetch and parse data but skip database insertion
- `--teams`: Process only teams data
- `--players`: Process only players data
- `--verbose, -v`: Enable verbose logging output
- `--help, -h`: Show help message with examples

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

## Database Schema

The PostgreSQL schema includes:

- **teams**: Team information and statistics
- **players**: Comprehensive player data with foreign key to teams
- **gameweeks**: Match week information (ready for future use)
- **fixtures**: Match data (ready for future use)

All tables include:

- Automatic timestamp tracking (`created_at`, `updated_at`)
- Primary keys and foreign key relationships
- Optimized indexes for query performance
- Upsert operations to handle data updates

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

```
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
├── requirements.txt    # Python dependencies
└── .env_example       # Environment configuration template
```

## Architecture

The application follows a modular pipeline architecture:

1. **Configuration**: Load database and API settings
2. **Data Fetching**: Retrieve data from FPL API endpoints
3. **Data Parsing**: Validate and structure data using Pydantic models
4. **Database Operations**: Insert/update data with upsert logic

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

## Troubleshooting

### Common Issues

**Database Connection Failed**

- Verify PostgreSQL is running
- Check database credentials in `.env_example`
- Ensure database and user exist

**API Request Failed**

- Check internet connection
- Verify FPL API URL is accessible
- API may be temporarily unavailable during maintenance

**Import Errors**

- Ensure virtual environment is activated
- Run `uv pip install -r requirements.txt`
- Check Python version compatibility

### Logs

The application provides detailed logging for troubleshooting:

- API request/response information
- Data parsing statistics
- Database operation results
- Error messages with context

## Future Enhancements

- Fixture data processing
- Player gameweek history
- Automated scheduling with cron
- Change detection for incremental updates
- Performance optimizations for large datasets
- Additional data endpoints (transfers, chips, etc.)

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
