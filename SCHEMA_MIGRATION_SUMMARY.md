# Schema Migration Summary

## Overview
The FPL Data Fetcher & Inserter codebase has been successfully updated to support the new database schema while maintaining backwards compatibility with the legacy schema.

## New Schema Structure
The new schema introduces a normalized structure with separate tables for different types of data:

### Tables:
- `events`: Simplified gameweek/event data (id, name, deadline_time, finished, average_entry_score)
- `players`: Basic player information (id, code, first_name, second_name, team_id, element_type, now_cost)
- `player_stats`: Player statistics per gameweek (player_id, gameweek_id, stats...)
- `player_history`: Historical player data per gameweek (player_id, gameweek_id, history...)

## Key Changes Made

### 1. Models (`src/models.py`)
- **Added new models**: `Event`, `Player` (simplified), `PlayerStats`, `PlayerHistory`
- **Kept legacy models**: `Team`, `Gameweek`, `Fixture` for backwards compatibility
- **Simplified Player model**: Removed many fields that are now in `PlayerStats`

### 2. Parser (`src/parser.py`)
- **Added new parsers**: `parse_events()`, `parse_player_stats()`, `parse_player_history()`
- **Updated existing parsers**: Modified `parse_players()` to work with simplified model
- **Kept legacy parsers**: All original parsers remain functional

### 3. Database (`src/database.py`)
- **Added new functions**: `insert_events()`, `insert_players_new()`, `insert_player_stats()`, `insert_player_history()`
- **Updated schema execution**: Now uses the new `schema.sql` file by default
- **Kept legacy functions**: All original database functions remain functional

### 4. Fetcher (`src/fetcher.py`)
- **Added new functions**: `fetch_player_history()`, `fetch_current_gameweek_id()`
- **Enhanced functionality**: Better support for individual player data fetching

### 5. Main Application (`src/app.py`)
- **Added new pipeline**: `run_new_pipeline()` for the new schema
- **Enhanced command-line interface**: New flags for different data types
- **Maintained legacy pipeline**: `run_bootstrap_pipeline()` still works as before
- **Smart schema selection**: Automatically chooses schema based on command-line flags

### 6. Configuration (`src/config.py`)
- **Fixed environment file**: Now correctly loads from `.env.example` instead of `.env`

### 7. Schema (`sql/schema.sql`)
- **Created new schema file**: Defines the new table structure
- **Normalized design**: Separates player data into base info and statistics

## Usage Examples

### New Schema (Default)
```bash
# Use all new schema features
python -m src.app --dry-run

# Specific data types
python -m src.app --events --dry-run
python -m src.app --players --player-stats --dry-run
python -m src.app --player-history --dry-run
```

### Legacy Schema
```bash
# Use legacy schema
python -m src.app --legacy --dry-run

# Specific legacy data types
python -m src.app --teams --legacy --dry-run
python -m src.app --players --gameweeks --legacy --dry-run
```

## Benefits of New Schema

1. **Normalized Structure**: Better data organization with separate concerns
2. **Scalability**: Easier to add new statistics without modifying core tables
3. **Performance**: More efficient queries for specific data types
4. **Flexibility**: Support for historical data tracking
5. **Backwards Compatibility**: Legacy applications continue to work

## Migration Strategy

The implementation supports both schemas simultaneously:
- **New applications** can use the new schema by default
- **Existing applications** can continue using the legacy schema with `--legacy` flag
- **Gradual migration** is possible by slowly moving components to the new schema

## Testing

Both schemas have been tested with dry-run mode:
- ✅ New schema: Successfully parses and processes events, players, and player stats
- ✅ Legacy schema: Successfully parses and processes teams, players, gameweeks, and fixtures
- ✅ Command-line interface: All new flags work correctly
- ✅ Backwards compatibility: All legacy functionality preserved

## Technical Implementation Notes

- **Error handling**: Robust error handling for both schemas
- **Type conversion**: Safe conversion of string values to appropriate types
- **Database operations**: Upsert operations (INSERT ... ON CONFLICT DO UPDATE)
- **Logging**: Comprehensive logging for debugging and monitoring
- **Validation**: Pydantic models ensure data integrity

## Future Enhancements

1. **Player History**: Currently fetches limited data (first 10 players) for testing
2. **Data Validation**: Additional validation rules can be added to models
3. **Performance Optimization**: Batch operations for large datasets
4. **Monitoring**: Enhanced logging and metrics collection
5. **Data Cleaning**: Automated data quality checks and corrections