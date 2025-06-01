# Architecture

## Overview

This application is designed to fetch data from the Fantasy Premier League (FPL) API and store it in a PostgreSQL database. The architecture is based on a modular design, separating concerns into distinct components for fetching data, processing it, and interacting with the database.

## FPL API Endpoints

This application is designed to interact with several official Fantasy Premier League API endpoints, including:

- `/bootstrap-static/`: General summary of gameweeks, teams, players, and settings.
- `/fixtures/`: All fixture objects, including statistics for past fixtures.
- `/fixtures/?event={event_id}`: Fixtures for a specific gameweek.
- `/element-summary/{element_id}/`: Detailed data for a specific player.
- `/event/{event_id}/live/`: Live statistics for every player in a gameweek.
- `/me/`: Data for the authenticated manager (requires authentication).
- `/league/{league_id}/cup-status/`: Cup status for a league.
- `/entry/{entry_id}/`: Data for a specific team entry.
- `/h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/`: Head-to-head matches for a league and entry.
- `/leagues-classic/{league_id}/standings/`: Classic league standings.

See `APIs.md` for more details on each endpoint.

## Components

1.  **Data Fetcher (`fetcher.py` or similar)**:

    - Responsible for making HTTP requests to the FPL API endpoints (see above for supported endpoints).
    - Handles API authentication and rate limiting (if applicable).
    - Retrieves raw data in JSON format.

2.  **Data Processor/Parser (`parser.py` or similar - _optional, may be part of inserter_)**:

    - Takes the raw JSON data from the Fetcher.
    - Transforms and cleans the data into a structured format suitable for database insertion.
    - May involve mapping API fields to database columns, handling data type conversions, and validating data integrity.

3.  **Database Inserter/Manager (`database.py` or `db_manager.py`)**:

    - Manages the connection to the PostgreSQL database.
    - Contains functions to create tables (if they don't exist) based on a defined schema.
    - Handles the insertion and updating of data into the respective tables (e.g., `players`, `teams`, `fixtures`, `gameweeks`).
    - Uses an ORM like SQLAlchemy or a database connector like `psycopg2`.

4.  **Main Application Logic (`src/app.py`)**:

    - Orchestrates the overall workflow of the application.
    - Initializes and configures the other components.
    - Calls the Data Fetcher to get data.
    - Passes data to the Processor (if a separate component).
    - Instructs the Database Inserter to store the processed data.
    - Handles command-line arguments or configuration for specific operations (e.g., fetching specific types of data, full refresh vs. incremental update).

5.  **Configuration (`config.py` or environment variables via `.env`)**:

    - Stores settings for database connection (host, port, user, password, database name).
    - Contains API endpoint URLs and any necessary API keys (though API keys are better in `.env`).
    - May include logging configurations.

6.  **Utilities (`utils.py` - _optional_)**:
    - Contains helper functions used across different components (e.g., logging setup, date/time conversions, common data manipulation tasks).

## Data Flow

1.  The `Main Application Logic` is executed.
2.  It reads configuration from environment variables.
3.  It initializes the `Data Fetcher` and `Database Inserter`.
4.  The `Data Fetcher` makes requests to the configured FPL API endpoints.
    - Note: The Data Fetcher may interact with any of the supported FPL API endpoints listed above. The specific endpoints used can be configured as needed for your use case.
5.  The API returns JSON data.
6.  This raw data is passed to the `Data Processor` (or handled within the `Database Inserter` or `Main Application Logic`).
7.  The `Data Processor` transforms the data into a schema-compliant format.
8.  The `Database Inserter` takes the processed data and executes SQL commands (e.g., `INSERT`, `UPDATE`) to store it in the PostgreSQL database tables.
9.  Logging throughout the process records successes, errors, and important events.

## Key Design Principles

- **Modularity**: Each component has a specific responsibility, making the system easier to understand, maintain, and test. Prefer functional programming patterns and the use of functions, objects and dictionaries over classes.
- **Configuration-driven**: Key parameters like API URLs and database credentials are externalized, allowing for easy changes without code modification.
- **Error Handling**: Robust error handling should be implemented in API interactions and database operations.
- **Idempotency (Desirable for Inserter)**: Ideally, running the insertion process multiple times with the same data should not result in duplicate entries or incorrect states (e.g., using `INSERT ... ON CONFLICT DO UPDATE`).
- **Scalability (Consideration)**: While the initial scope might be simple, the design should allow for potential future enhancements, such as handling larger datasets or more frequent updates.

## Database Schema (Example - to be detailed further in SCHEMA.md)

The PostgreSQL database will contain tables such as:

- `teams` (id, name, short_name, ...)
- `players` (id, first_name, second_name, team_id, position, price, ...)
- `fixtures` (id, home_team_id, away_team_id, kickoff_time, gameweek_id, ...)
- `gameweeks` (id, deadline_time, is_current, is_next, ...)
- `player_history` (player_id, gameweek_id, minutes_played, goals_scored, assists, ...)

This schema is illustrative and will be refined based on the data available from the FPL API and the desired analysis capabilities.
