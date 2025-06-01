# FPL Data Fetcher & Inserter - Architecture Documentation

## Overview

The FPL Data Fetcher & Inserter is designed as a modular, pipeline-based application that follows functional programming principles with dependency inversion and minimal complexity. The architecture emphasizes reliability, testability, and extensibility while maintaining a clean separation of concerns.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         FPL API                                 │
│              https://fantasy.premierleague.com/api              │
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
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│   │  teams  │  │ players │  │gameweeks│  │fixtures │          │
│   └─────────┘  └─────────┘  └─────────┘  └─────────┘          │
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

**Purpose**: HTTP client for FPL API with robust error handling.

**Key Components**:

- `fetch_endpoint(endpoint: str)`: Generic API fetcher
- `fetch_bootstrap_data()`: Specialized bootstrap endpoint
- Custom `FPLAPIError` exception class

**Design Decisions**:

- Generic endpoint function enables easy extension
- Custom exceptions for better error handling
- Comprehensive logging for debugging API issues
- Dependency inversion: receives configuration vs. hardcoded URLs

### 3. Data Parsing Module (`src/parser.py`)

**Purpose**: Data validation and transformation using Pydantic models.

**Key Components**:

- `parse_teams(data: dict)`: Team data parser with validation
- `parse_players(data: dict)`: Player data parser with validation
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
- `Gameweek`: Match week data (future use)
- `Fixture`: Match data (future use)

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
- `insert_teams()`, `insert_players()`: Upsert operations
- `execute_schema()`: Schema management

**Design Decisions**:

- Context managers ensure proper resource cleanup
- ON CONFLICT DO UPDATE for idempotent operations
- Separate connection management from business logic
- Batch operations for performance
- Comprehensive error handling with rollback

### 6. Main Application (`src/app.py`)

**Purpose**: Orchestrates the complete data pipeline with CLI interface.

**Key Components**:

- `run_bootstrap_pipeline()`: Main pipeline orchestration
- `parse_args()`: Command-line argument handling
- `main()`: Application entry point

**Design Decisions**:

- Pipeline pattern with clear sequential steps
- Dry-run mode for safe testing
- Selective data processing (teams/players)
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

## Database Schema Design

### Schema Principles

- **Normalized design** with proper foreign key relationships
- **Automatic timestamping** for audit trails
- **Optimized indexes** for common query patterns
- **JSONB fields** for complex nested data (fixture stats)
- **Upsert operations** for data freshness

### Key Tables

#### teams

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

#### players

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

### Indexes and Performance

- `idx_players_team`: Fast player lookups by team
- `idx_players_element_type`: Position-based queries
- `idx_fixtures_event`: Gameweek-based fixture queries
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
def fetch_fixtures() -> dict:
    return fetch_endpoint("/fixtures/")

# New parser functions
def parse_fixtures(data: dict) -> List[Fixture]:
    # Implementation
    pass

# New insertion functions
def insert_fixtures(conn: connection, fixtures: List[Fixture]) -> None:
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

```
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

```
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

This architecture provides a solid foundation for the current requirements while maintaining flexibility for future enhancements and scale requirements.
