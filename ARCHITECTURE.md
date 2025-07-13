# FPL Data Fetcher & Inserter - Architecture Documentation

## Overview

The FPL Data Fetcher & Inserter is designed as a modular, pipeline-based application that follows functional programming principles with dependency inversion and minimal complexity. The architecture emphasizes reliability, testability, and extensibility while maintaining a clean separation of concerns.

The application now supports dual schema architectures: a new normalized schema (default) and a legacy schema for backward compatibility. This allows for gradual migration and ensures existing applications continue to work unchanged.

## System Architecture

```ascii
┌─────────────────────────────────────────────────────────────────┐
│                         FPL API                                 │
│              https://fantasy.premierleague.com/api              │
│                                                                 │
│  ┌─────────────────┐              ┌─────────────────┐          │
│  │ /bootstrap-static/│              │   /fixtures/    │          │
│  │ (teams, players, │              │   (fixtures)    │          │
│  │  gameweeks)     │              │                 │          │
│  └─────────────────┘              └─────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    │ HTTP Requests
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                      src/app.py                                 │
│                   Main Orchestrator                             │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────┐ │
│  │   Config    │  │   Fetcher   │  │   Parser    │  │Database │ │
│  │  Loading    │  │  (API Call) │  │ (Validation)│  │Insert   │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    │ SQL Operations
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                          │
│                                                                 │
│  New Schema (Default):                                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │ events  │  │ players │  │player_stats │  │player_history│   │
│  └─────────┘  └─────────┘  └─────────────┘  └─────────────┘   │
│                                                                 │
│  Legacy Schema:                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │  teams  │  │ players │  │gameweeks│  │fixtures │          │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

## Module Design

### 1. Configuration Module (`src/config.py`)

**Purpose**: Centralized configuration management with environment variable loading.

**Key Components**:

- `get_config()`: Pure function returning configuration dictionary
- Environment variable loading from `.env_example`
- Type-safe configuration with default values

**Design Decisions**:

- Functional approach with no global state
- Explicit dependency injection pattern
- Fail-fast on missing critical configuration

### 2. Data Fetching Module (`src/fetcher.py`)

**Purpose**: HTTP client for FPL API with robust error handling and parallel processing capabilities.

**Key Components**:

- `fetch_endpoint(endpoint: str)`: Generic API fetcher
- `fetch_bootstrap_data()`: Specialized bootstrap endpoint
- `fetch_fixtures_data()`: Specialized fixtures endpoint
- `fetch_independent_endpoints_parallel()`: **NEW** - Parallel independent endpoint fetcher
- `fetch_player_history_batch()`: **NEW** - Parallel player history fetcher
- Custom `FPLAPIError` exception class

**Design Decisions**:

- Generic endpoint function enables easy extension
- Custom exceptions for better error handling
- Comprehensive logging for debugging API issues
- Dependency inversion: receives configuration vs. hardcoded URLs
- **Multi-Level Parallel Processing**:
  - Independent endpoints (bootstrap + fixtures) fetched simultaneously
  - Player history data fetched concurrently for all 784 players
  - Uses `ThreadPoolExecutor` for concurrent HTTP requests
- **Rate Limiting**: Built-in batch processing with delays to respect API limits
- **Fault Tolerance**: Individual request failures don't stop entire batches

**Parallel Fetching Architecture**:

**Level 1: Independent Endpoints Parallelization**

```python
def fetch_independent_endpoints_parallel() -> Dict[str, Any]:
    # Fetches bootstrap and fixtures data simultaneously
    # Returns: {'bootstrap_data': ..., 'fixtures_data': ..., 'errors': [...]}
```

**Level 2: Player History Parallelization**

```python
def fetch_player_history_batch(player_ids: List[int], max_workers: int = 10):
    # Process in batches to avoid overwhelming API
    # Use ThreadPoolExecutor for concurrent requests
    # Handle individual failures gracefully
    # Return (player_id, history_data) tuples
```

### 3. Data Parsing Module (`src/parser.py`)

**Purpose**: Data validation and transformation using Pydantic models.

**Key Components**:

- `parse_teams(data: dict)`: Team data parser with validation
- `parse_players(data: dict)`: Player data parser with validation
- `parse_gameweeks(data: dict)`: Gameweek data parser with validation
- `parse_fixtures(data: list)`: Fixture data parser with validation
- Error handling for malformed data

**Design Decisions**:

- Pydantic models ensure data integrity at parse time
- Parser functions are pure (no side effects)
- Graceful handling of missing or null fields
- Comprehensive logging for data validation errors

### 4. Data Models Module (`src/models.py`)

**Purpose**: Pydantic models defining the data schema.

**Key Components**:

- `Team`: Premier League team information
- `Player`: Player statistics and information
- `Gameweek`: Match week data with deadlines and status
- `Fixture`: Match data with teams, scores, and statistics

**Design Decisions**:

- Pydantic models provide automatic validation and serialization
- Optional fields with sensible defaults for API inconsistencies
- String-based fields for API compatibility (expected_goals, etc.)
- Foreign key relationships preserved in models

### 5. Database Module (`src/database.py`)

**Purpose**: PostgreSQL database operations with connection management.

**Key Components**:

- `get_connection()`: Database connection factory
- `DatabaseManager`: Context manager for connection lifecycle
- `insert_teams()`, `insert_players()`, `insert_gameweeks()`, `insert_fixtures()`: Upsert operations
- `execute_schema()`: Enhanced schema management with proper SQL parsing

**Design Decisions**:

- Context managers ensure proper resource cleanup
- ON CONFLICT DO UPDATE for idempotent operations
- Separate connection management from business logic
- Batch operations for performance
- Comprehensive error handling with rollback
- **Enhanced SQL parsing**: Properly handles PostgreSQL functions with dollar-quoted strings
- **JSON handling**: Converts Python objects to JSON for JSONB fields

**Schema Execution Improvements**:

The `execute_schema()` function has been enhanced to properly handle complex PostgreSQL syntax:

- **Statement-by-statement execution**: Splits SQL file into individual statements
- **Dollar-quote awareness**: Correctly handles PostgreSQL function definitions with `$$` delimiters
- **Function parsing**: Recognizes `CREATE OR REPLACE FUNCTION` blocks and executes them as complete units
- **Error isolation**: Individual statement failures provide specific error context
- **Syntax validation**: Built-in validation prevents common SQL syntax errors

This resolves issues with executing schema files containing PostgreSQL functions that use dollar-quoted strings, which cannot be executed as a single large SQL statement.

### 6. Main Application (`src/app.py`)

**Purpose**: Orchestrates the complete data pipeline with CLI interface.

**Key Components**:

- `run_bootstrap_pipeline()`: Main pipeline orchestration
- `parse_args()`: Command-line argument handling
- `main()`: Application entry point

**Design Decisions**:

- Pipeline pattern with clear sequential steps
- Dry-run mode for safe testing
- Selective data processing (teams/players/gameweeks/fixtures)
- Comprehensive CLI with argparse
- Graceful error handling with proper exit codes

### 7. Utilities Module (`src/utils.py`)

**Purpose**: Common utilities, primarily logging configuration.

**Key Components**:

- `get_logger(name: str)`: Logger factory with consistent formatting

**Design Decisions**:

- Centralized logging configuration
- Module-specific loggers for better debugging
- Consistent timestamp and formatting

## Data Flow

The application orchestrates a pipeline to fetch, parse, and store FPL data:

1. **Initialization (`src/app.py:main`)**:

   - Parses command-line arguments (e.g., `--dry-run`, `--teams`, `--players`, `--gameweeks`, `--fixtures`).
   - Sets up logging.

2. **Configuration Loading (`src/config.py`)**:

   - Loads environment variables (database credentials, API URL) using `python-dotenv`.
   - Provides a configuration dictionary accessible throughout the application.

3. **Data Fetching (`src/fetcher.py`)**:

   - `fetch_bootstrap_data()`: Makes an HTTP GET request to the FPL API's `/bootstrap-static/` endpoint. This endpoint provides a large JSON object containing data for teams, players, player types, and gameweeks (events).
   - `fetch_fixtures_data()`: Makes an HTTP GET request to the FPL API's `/fixtures/` endpoint. This endpoint provides a JSON array of all fixtures for the season.
   - Handles potential HTTP errors and returns the JSON response.

4. **Data Parsing (`src/parser.py`)**:

   - `parse_teams(bootstrap_data)`: Extracts team-specific information from the bootstrap data and converts it into a list of `Team` Pydantic models.
   - `parse_players(bootstrap_data)`: Extracts player-specific information, including their team ID and element type (player position), and converts it into a list of `Player` Pydantic models.
   - `parse_gameweeks(bootstrap_data)`: Extracts gameweek (event) information from the `events` key in the bootstrap data and converts it into a list of `Gameweek` Pydantic models.
   - `parse_fixtures(fixtures_data)`: Extracts fixture information from the fixtures data list and converts it into a list of `Fixture` Pydantic models. Handles complex nested statistics data.
   - Pydantic models (`src/models.py`) are used for data validation and structuring.

5. **Database Interaction (`src.database.py`)**:

   - `DatabaseManager`: A context manager to handle database connections (`psycopg2`).
   - `execute_schema(conn)`: Reads and executes SQL commands from `sql/schema.sql` to create necessary tables if they don't already exist. This includes `teams`, `players`, `element_types`, `gameweeks`, and `fixtures`.
   - `insert_teams(conn, teams_data)`: Inserts/updates team data into the `teams` table.
   - `insert_players(conn, players_data)`: Inserts/updates player data into the `players` table.
   - `insert_gameweeks(conn, gameweeks_data)`: Inserts/updates gameweek data into the `gameweeks` table.
   - `insert_fixtures(conn, fixtures_data)`: Inserts/updates fixture data into the `fixtures` table. Converts complex statistics to JSON for JSONB storage.
   - Uses `ON CONFLICT DO UPDATE` (upsert) for `teams`, `players`, `gameweeks`, and `fixtures` to handle re-runs of the script.

6. **Orchestration (`src/app.py:run_bootstrap_pipeline`)**:
   - Calls the fetcher, parser, and database modules in sequence.
   - Handles `dry-run` logic to skip database writes.
   - Manages selective processing of data types based on command-line arguments.
   - Ensures data is inserted in an order that respects foreign key constraints (e.g., teams before players, gameweeks before fixtures if fixtures reference gameweeks).

The `element_types` table is populated once by `schema.sql` as this data is static (Goalkeeper, Defender, Midfielder, Forward).

## Database Schema Design

The application supports two database schemas:

### New Schema (Default)

A normalized design that separates concerns for better scalability and performance.

#### Schema Principles

- **Normalized design** with proper foreign key relationships
- **Separation of concerns** - base data vs. statistics vs. history
- **Automatic timestamping** for audit trails
- **Optimized indexes** for common query patterns
- **Upsert operations** for data freshness

#### Key Tables (New Schema)

##### events

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,           -- FPL event/gameweek ID
    name TEXT NOT NULL,              -- Event name (e.g., "Gameweek 1")
    deadline_time TIMESTAMP NOT NULL, -- Deadline for team changes
    finished BOOLEAN NOT NULL,        -- Whether event is finished
    average_entry_score INTEGER       -- Average score for this event
);
```

##### players

```sql
CREATE TABLE players (
    id INTEGER PRIMARY KEY,          -- FPL player ID
    code INTEGER NOT NULL,           -- Player code
    first_name TEXT NOT NULL,        -- Player first name
    second_name TEXT NOT NULL,       -- Player second name
    team_id INTEGER NOT NULL,        -- Team ID (foreign key)
    element_type INTEGER NOT NULL,   -- Position (1=GK, 2=DEF, 3=MID, 4=FWD)
    now_cost INTEGER NOT NULL        -- Current cost
);
```

##### player_stats

```sql
CREATE TABLE player_stats (
    stat_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    gameweek_id INTEGER NOT NULL,
    -- Performance statistics
    total_points INTEGER,
    form REAL,
    selected_by_percent REAL,
    -- ... additional statistics
    UNIQUE (player_id, gameweek_id)
);
```

##### player_history

```sql
CREATE TABLE player_history (
    history_id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id),
    gameweek_id INTEGER NOT NULL,
    -- Historical data
    opponent_team INTEGER,
    was_home BOOLEAN,
    kickoff_time TIMESTAMP,
    -- ... additional history data
    UNIQUE (player_id, gameweek_id)
);
```

### Legacy Schema

The original schema design for backward compatibility.

#### Schema Principles

- **Normalized design** with proper foreign key relationships
- **Automatic timestamping** for audit trails
- **Optimized indexes** for common query patterns
- **JSONB fields** for complex nested data (fixture stats)
- **Upsert operations** for data freshness

#### Key Tables (Legacy Schema)

##### teams

```sql
CREATE TABLE teams (
    id INTEGER PRIMARY KEY,           -- FPL team ID
    name VARCHAR(100) NOT NULL,       -- Full team name
    short_name VARCHAR(10) NOT NULL,  -- 3-letter abbreviation
    -- ... position, strength, form data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### players

```sql
CREATE TABLE players (
    id INTEGER PRIMARY KEY,                      -- FPL player ID
    team INTEGER NOT NULL REFERENCES teams(id),  -- Foreign key to teams
    first_name VARCHAR(50) NOT NULL,
    second_name VARCHAR(50) NOT NULL,
    -- ... comprehensive player statistics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

##### gameweeks

```sql
CREATE TABLE gameweeks (
    id INTEGER PRIMARY KEY,           -- FPL gameweek ID
    name VARCHAR(50) NOT NULL,        -- Gameweek name (e.g., "Gameweek 1")
    deadline_time TIMESTAMP,          -- Deadline for the gameweek
    is_previous BOOLEAN DEFAULT FALSE,
    is_current BOOLEAN DEFAULT FALSE,
    is_next BOOLEAN DEFAULT FALSE,
    finished BOOLEAN DEFAULT FALSE,
    -- ... other relevant fields from the API
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### fixtures

```sql
CREATE TABLE fixtures (
    id INTEGER PRIMARY KEY,                      -- FPL fixture ID
    event INTEGER REFERENCES gameweeks(id),      -- Gameweek ID the fixture belongs to
    team_h INTEGER NOT NULL REFERENCES teams(id), -- Home team ID
    team_a INTEGER NOT NULL REFERENCES teams(id), -- Away team ID
    kickoff_time TIMESTAMP,                      -- Match kickoff time
    team_h_score INTEGER,                        -- Home team score (can be NULL if not played)
    team_a_score INTEGER,                        -- Away team score (can be NULL if not played)
    finished BOOLEAN DEFAULT FALSE,              -- Boolean flag
    stats JSONB DEFAULT '[]'::jsonb,             -- Complex match statistics
    -- ... other relevant fields from the API
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Indexes and Performance

- `idx_players_team`: Fast player lookups by team
- `idx_players_element_type`: Position-based queries
- `idx_fixtures_event`: Gameweek-based fixture queries
- `idx_fixtures_teams`: Team-based fixture queries
- `idx_fixtures_kickoff`: Time-based fixture queries
- Partial indexes for current/next gameweeks

## Error Handling Strategy

### 1. API Error Handling

- Custom `FPLAPIError` for API-specific issues
- HTTP status code validation
- Retry logic for transient failures (future enhancement)
- Comprehensive logging for debugging

### 2. Database Error Handling

- Transaction rollback on failures
- Connection pooling for reliability (future enhancement)
- Graceful handling of constraint violations
- Detailed error logging with context

### 3. Data Validation

- Pydantic model validation at parse time
- Graceful handling of missing fields
- Null value handling for optional fields
- Validation error aggregation

## Extensibility Design

### 1. Adding New Data Types

```python
# Easy to add new endpoints
def fetch_player_history() -> dict:
    return fetch_endpoint("/element-summary/{player_id}/")

# New parser functions
def parse_player_history(data: dict) -> List[PlayerHistory]:
    # Implementation
    pass

# New insertion functions
def insert_player_history(conn: connection, history: List[PlayerHistory]) -> None:
    # Implementation
    pass
```

### 2. Adding New CLI Options

- argparse structure supports easy flag additions
- Pipeline function accepts parameters for control
- Modular design allows feature toggles

### 3. Data Source Flexibility

- Generic fetcher design supports any HTTP API
- Parser functions are data-source agnostic
- Database operations separated from business logic

## Testing Strategy

### 1. Unit Testing

- **Mocked dependencies** for isolated testing
- **Data-driven tests** with realistic API responses
- **Edge case coverage** for error conditions
- **Pydantic model validation** testing

### 2. Integration Testing

- **End-to-end pipeline** testing with dry-run mode
- **Database operations** with test transactions
- **API integration** with real endpoints (in CI)

### 3. Test Structure

```filetree
tests/
├── test_fetcher.py     # API client tests
├── test_parser.py      # Data parsing tests
├── test_database.py    # Database operation tests
└── test_models.py      # Pydantic model tests (future)
```

## Performance Considerations

### 1. Database Performance

- **Batch operations** for bulk inserts
- **Upsert operations** to avoid duplicates
- **Optimized indexes** for common queries
- **Connection pooling** (future enhancement)

### 2. Memory Management

- **Streaming processing** for large datasets (future)
- **Generator patterns** for data processing
- **Context managers** for resource cleanup

### 3. API Efficiency

- **Single API call** for bootstrap data
- **Separate endpoint** for fixtures data
- **Generic endpoint function** for reuse
- **Error caching** to avoid repeated failures (future)

## Security Considerations

### 1. Configuration Security

- Environment variables for sensitive data
- No hardcoded credentials in source code
- `.env` file exclusion from version control

### 2. Database Security

- Parameterized queries to prevent SQL injection
- Connection string validation
- Proper privilege management (application-level user)

### 3. API Security

- HTTPS enforcement for API calls
- User-Agent headers for API identification
- Rate limiting awareness (future enhancement)

## Deployment Architecture

### 1. Production Deployment

```ascii
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Cron Job      │───▶│  Application    │───▶│   PostgreSQL    │
│  (Scheduler)    │    │    Container    │    │    Database     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                        ┌─────────────────┐
                        │   Log Files     │
                        │  (Monitoring)   │
                        └─────────────────┘
```

### 2. Container Strategy

- **Docker container** for consistent environments
- **Multi-stage builds** for optimized images
- **Health checks** for container orchestration
- **Environment-based configuration**

### 3. Monitoring and Observability

- **Structured logging** with JSON format
- **Metrics collection** for performance monitoring
- **Alert integration** for failure notifications
- **Dashboard integration** for operational visibility

## Future Architectural Enhancements

### 1. Scalability Improvements

- **Message queue integration** for asynchronous processing
- **Database sharding** for large datasets
- **Horizontal scaling** with load balancers
- **Caching layer** for frequently accessed data

### 2. Data Pipeline Enhancements

- **Change detection** for incremental updates
- **Data validation rules** engine
- **ETL workflow orchestration** (Airflow/Prefect)
- **Data quality monitoring**

### 3. API Enhancements

- **Rate limiting** and backoff strategies
- **Circuit breaker** pattern for resilience
- **API versioning** support
- **Authentication** for authenticated endpoints

## Performance Optimizations

The application implements several performance optimizations for handling large-scale FPL data efficiently:

### Database Optimizations

#### Bulk Insert Operations

- **PostgreSQL COPY**: For datasets >100 records, uses COPY operations instead of individual INSERT statements
- **Temporary Tables**: Staging data in temporary tables for efficient upserts
- **Batch Processing**: Configurable batch sizes (default 1000 records) with error isolation
- **Connection Optimization**: Session-level PostgreSQL settings for bulk operations:
  ```sql
  SET synchronous_commit = OFF;
  SET wal_buffers = '16MB';
  SET maintenance_work_mem = '256MB';
  SET work_mem = '128MB';
  ```

#### Post-Insert Optimization

- **Automatic VACUUM ANALYZE**: Runs after bulk inserts >1000 records
- **Configurable Thresholds**: Environment variables control when optimizations activate
- **Performance Monitoring**: Detailed timing and throughput metrics

### API Fetching Optimizations

#### Parallel Processing

- **Concurrent Requests**: ThreadPoolExecutor with configurable worker pools (default 15)
- **Batch Management**: API requests processed in batches to avoid overwhelming endpoints
- **Parallel Endpoints**: Independent API endpoints fetched concurrently
- **Rate Limiting**: Configurable delays between batches (default 0.1s)

#### Error Handling

- **Timeout Management**: 30-second timeouts with proper error handling
- **Retry Logic**: Failed individual requests don't block entire batches
- **Success Tracking**: Detailed success rate reporting

### Performance Monitoring

#### Comprehensive Timing

- **API Endpoint Timing**: Individual and batch operation timing
- **Database Operation Timing**: Insert operations with throughput metrics
- **Pipeline Timing**: End-to-end execution time tracking
- **Performance Metrics**: Records/second rates for all operations

#### Visual Performance Indicators

- 🚀 Operation start indicators
- ✅ Successful completion with timing
- 📊 Performance metrics and statistics
- ❌ Error timing and diagnostics
- 🎉 Pipeline completion summaries

### Configuration Options

Performance behavior is controlled through environment variables:

```bash
# Database optimization settings
BULK_INSERT_THRESHOLD=100           # Records threshold for bulk operations
ENABLE_VACUUM_AFTER_BULK=true     # Auto-vacuum after bulk inserts
VACUUM_THRESHOLD=1000              # Minimum records for vacuum
ENABLE_DB_OPTIMIZATIONS=true      # Enable connection optimizations

# API fetching settings
PARALLEL_WORKERS=15                # Concurrent API request workers
```

### Performance Metrics

Typical performance improvements observed:

- **Player History Insertion**: 10-50x faster with COPY operations vs individual INSERTs
- **API Fetching**: 15x parallel speedup for player history batch fetching
- **Pipeline Execution**: ~40 seconds for full pipeline (784 players, ~27K history records)
- **Throughput**: 500-2000+ records/second for bulk database operations

This optimization framework ensures the application can handle the full FPL dataset efficiently while providing detailed performance insights for monitoring and tuning.
