#!/usr/bin/env python3
"""
FPL Data Fetcher & Inserter
Main application runner that orchestrates the complete data pipeline.
"""

import sys
import argparse
from typing import Optional
from .config import get_config
from .utils import get_logger
from .fetcher import fetch_bootstrap_data, fetch_fixtures_data, fetch_player_history, fetch_current_gameweek_id
from .parser import (
    parse_events, parse_players, parse_player_stats, parse_player_history,
    parse_teams, parse_gameweeks, parse_fixtures
)
from .database import (
    DatabaseManager, execute_schema,
    insert_events, insert_players_new, insert_player_stats, insert_player_history,
    insert_teams, insert_players, insert_gameweeks, insert_fixtures
)

logger = get_logger(__name__)


def run_new_pipeline(dry_run: bool = False, include_events: bool = True, include_players: bool = True, 
                    include_player_stats: bool = True, include_player_history: bool = False) -> bool:
    """Run the complete data pipeline using the new schema.

    Args:
        dry_run: If True, skip database insertions (preview mode)
        include_events: If True, process and insert events data
        include_players: If True, process and insert players data
        include_player_stats: If True, process and insert player stats data
        include_player_history: If True, process and insert player history data

    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting FPL data pipeline (new schema)...")

    if not any([include_events, include_players, include_player_stats, include_player_history]):
        logger.error("At least one data type must be selected")
        return False

    try:
        # Step 1: Load configuration
        logger.info("Step 1: Loading configuration")
        config = get_config()
        logger.info(f"Configuration loaded - API URL: {config['fpl_api_url']}")

        # Step 2: Fetch bootstrap data from FPL API
        logger.info("Step 2: Fetching bootstrap data from FPL API")
        bootstrap_data = fetch_bootstrap_data()
        logger.info("Bootstrap data fetched successfully")

        # Get current gameweek ID for player stats
        current_gameweek_id = fetch_current_gameweek_id(bootstrap_data)
        if current_gameweek_id is None:
            logger.warning("Could not determine current gameweek ID, using 1 as default")
            current_gameweek_id = 1

        # Step 3: Parse data based on selection
        logger.info("Step 3: Parsing data")
        events = []
        players = []
        player_stats = []
        player_history = []

        if include_events:
            events = parse_events(bootstrap_data)
            logger.info(f"Parsed {len(events)} events")
        else:
            logger.info("Skipping events parsing (not selected)")

        if include_players:
            players = parse_players(bootstrap_data)
            logger.info(f"Parsed {len(players)} players")
        else:
            logger.info("Skipping players parsing (not selected)")

        if include_player_stats:
            player_stats = parse_player_stats(bootstrap_data, current_gameweek_id)
            logger.info(f"Parsed {len(player_stats)} player stats")
        else:
            logger.info("Skipping player stats parsing (not selected)")

        if include_player_history and players:
            logger.info("Fetching player history data...")
            all_player_history = []
            for i, player in enumerate(players[:10]):  # Limit to first 10 players for testing
                logger.info(f"Fetching history for player {player.id} ({i+1}/{min(10, len(players))})")
                history_data = fetch_player_history(player.id)
                if history_data:
                    player_history_entries = parse_player_history(history_data, player.id)
                    all_player_history.extend(player_history_entries)
            
            player_history = all_player_history
            logger.info(f"Parsed {len(player_history)} player history entries")
        else:
            logger.info("Skipping player history parsing (not selected or no players)")

        if dry_run:
            logger.info("DRY RUN MODE: Skipping database operations")
            if events:
                logger.info(f"Would insert {len(events)} events")
                logger.info(f"Sample event: {events[0].name}")
            if players:
                logger.info(f"Would insert {len(players)} players")
                logger.info(f"Sample player: {players[0].first_name} {players[0].second_name}")
            if player_stats:
                logger.info(f"Would insert {len(player_stats)} player stats")
            if player_history:
                logger.info(f"Would insert {len(player_history)} player history entries")

            logger.info("Pipeline completed successfully (dry run)")
            return True

        # Step 4: Insert data into database
        logger.info("Step 4: Inserting data into database")

        with DatabaseManager() as conn:
            # Ensure schema exists
            try:
                execute_schema(conn)
                logger.info("Database schema verified/created")
            except Exception as e:
                logger.warning(f"Schema execution failed (may already exist): {e}")

            # Insert events
            if events:
                insert_events(conn, events)

            # Insert players
            if players:
                insert_players_new(conn, players)

            # Insert player stats
            if player_stats:
                insert_player_stats(conn, player_stats)

            # Insert player history
            if player_history:
                insert_player_history(conn, player_history)

        logger.info("Pipeline completed successfully")
        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


def run_bootstrap_pipeline(dry_run: bool = False, include_teams: bool = True, include_players: bool = True, include_gameweeks: bool = True, include_fixtures: bool = True) -> bool:
    """Run the complete bootstrap data pipeline (legacy function).

    Args:
        dry_run: If True, skip database insertions (preview mode)
        include_teams: If True, process and insert teams data
        include_players: If True, process and insert players data
        include_gameweeks: If True, process and insert gameweeks data
        include_fixtures: If True, process and insert fixtures data

    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting FPL bootstrap data pipeline...")

    if not include_teams and not include_players and not include_gameweeks and not include_fixtures:
        logger.error(
            "At least one data type (teams, players, gameweeks, or fixtures) must be selected")
        return False

    try:
        # Step 1: Load configuration
        logger.info("Step 1: Loading configuration")
        config = get_config()
        logger.info(f"Configuration loaded - API URL: {config['fpl_api_url']}")

        # Step 2: Fetch bootstrap data from FPL API
        logger.info("Step 2: Fetching bootstrap data from FPL API")
        bootstrap_data = fetch_bootstrap_data()
        logger.info("Bootstrap data fetched successfully")

        # Step 2.1: Fetch fixtures data from FPL API
        logger.info("Fetching fixtures data from FPL API")
        fixtures_data_list = []
        if include_fixtures:
            fixtures_data_list = fetch_fixtures_data()
            if not fixtures_data_list:
                logger.warning(
                    "Failed to fetch fixtures data. Proceeding without fixtures.")
            else:
                logger.info("Fixtures data fetched successfully")
        else:
            logger.info("Skipping fixtures fetching (not selected)")

        # Step 3: Parse teams, players, gameweeks, and fixtures based on selection
        logger.info("Step 3: Parsing data")
        teams = []
        players = []
        gameweeks = []
        fixtures = []

        if include_teams:
            teams = parse_teams(bootstrap_data)
            logger.info(f"Parsed {len(teams)} teams")
        else:
            logger.info("Skipping teams parsing (not selected)")

        if include_players:
            players = parse_players(bootstrap_data)
            logger.info(f"Parsed {len(players)} players")
        else:
            logger.info("Skipping players parsing (not selected)")

        if include_gameweeks:
            gameweeks = parse_gameweeks(bootstrap_data)
            logger.info(f"Parsed {len(gameweeks)} gameweeks")
        else:
            logger.info("Skipping gameweeks parsing (not selected)")

        if include_fixtures and fixtures_data_list:
            fixtures = parse_fixtures(fixtures_data_list)
            logger.info(f"Parsed {len(fixtures)} fixtures")
        else:
            logger.info(
                "Skipping fixtures parsing (not selected or data not fetched)")

        if dry_run:
            logger.info("DRY RUN MODE: Skipping database operations")
            if teams:
                logger.info(f"Would insert {len(teams)} teams")
                logger.info(
                    f"Sample team: {teams[0].name} ({teams[0].short_name})")
            if players:
                logger.info(f"Would insert {len(players)} players")
                logger.info(
                    f"Sample player: {players[0].first_name} {players[0].second_name}")
            if gameweeks:
                logger.info(f"Would insert {len(gameweeks)} gameweeks")
                logger.info(
                    f"Sample gameweek: {gameweeks[0].name}")
            if fixtures:
                logger.info(f"Would insert {len(fixtures)} fixtures")
                logger.info(
                    f"Sample fixture: Team {fixtures[0].team_h} vs Team {fixtures[0].team_a}")

            logger.info("Pipeline completed successfully (dry run)")
            return True

        # Step 4: Insert data into database
        logger.info("Step 4: Inserting data into database")

        with DatabaseManager() as conn:
            # Ensure schema exists
            try:
                execute_schema(conn)
                logger.info("Database schema verified/created")
            except Exception as e:
                logger.warning(
                    f"Schema execution failed (may already exist): {e}")

            # Insert teams first (players have foreign key to teams)
            if teams:
                insert_teams(conn, teams)

            # Insert players
            if players:
                insert_players(conn, players)

            # Insert gameweeks
            if gameweeks:
                insert_gameweeks(conn, gameweeks)

            # Insert fixtures
            if fixtures:
                insert_fixtures(conn, fixtures)

        logger.info("Pipeline completed successfully")
        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="FPL Data Fetcher & Inserter - Fetch and store Fantasy Premier League data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Fetch and insert all data using new schema
  %(prog)s --dry-run              # Preview what would be fetched/inserted
  %(prog)s --legacy               # Use legacy schema (teams + players + gameweeks + fixtures)
  %(prog)s --events               # Only fetch and insert events (new schema)
  %(prog)s --players              # Only fetch and insert players (new schema)
  %(prog)s --player-stats         # Only fetch and insert player stats (new schema)
  %(prog)s --player-history       # Only fetch and insert player history (new schema)
  %(prog)s --teams --legacy       # Only fetch teams using legacy schema
  %(prog)s --dry-run --players    # Preview players data only
        """
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mode: fetch and parse data but skip database insertion"
    )

    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Use legacy schema (teams, players, gameweeks, fixtures)"
    )

    # New schema options
    parser.add_argument(
        "--events",
        action="store_true",
        help="Process events data (new schema)"
    )

    parser.add_argument(
        "--player-stats",
        action="store_true",
        help="Process player stats data (new schema)"
    )

    parser.add_argument(
        "--player-history",
        action="store_true",
        help="Process player history data (new schema)"
    )

    # Legacy schema options
    parser.add_argument(
        "--teams",
        action="store_true",
        help="Process teams data (legacy schema)"
    )

    parser.add_argument(
        "--players",
        action="store_true",
        help="Process players data (both schemas)"
    )

    parser.add_argument(
        "--gameweeks",
        action="store_true",
        help="Process gameweeks data (legacy schema)"
    )

    parser.add_argument(
        "--fixtures",
        action="store_true",
        help="Process fixtures data (legacy schema)"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging output"
    )

    return parser.parse_args()


def main():
    """Main entry point for the application."""
    args = parse_args()

    logger.info("FPL Data Fetcher & Inserter starting...")

    # Determine which schema to use
    legacy_args = [args.teams, args.gameweeks, args.fixtures]
    new_args = [args.events, args.player_stats, args.player_history]
    
    use_legacy = args.legacy or any(legacy_args)
    
    if use_legacy:
        logger.info("Using legacy schema")
        # If no specific legacy data types are selected, default to all
        include_teams = args.teams if any(legacy_args) or args.players else True
        include_players = args.players if any(legacy_args) or args.players else True
        include_gameweeks = args.gameweeks if any(legacy_args) or args.players else True
        include_fixtures = args.fixtures if any(legacy_args) or args.players else True

        if args.verbose:
            logger.info(
                f"Legacy configuration: dry_run={args.dry_run}, teams={include_teams}, players={include_players}, gameweeks={include_gameweeks}, fixtures={include_fixtures}")

        success = run_bootstrap_pipeline(
            dry_run=args.dry_run,
            include_teams=include_teams,
            include_players=include_players,
            include_gameweeks=include_gameweeks,
            include_fixtures=include_fixtures
        )
    else:
        logger.info("Using new schema")
        # If no specific new data types are selected, default to events, players, and player_stats
        include_events = args.events if any(new_args) or args.players else True
        include_players = args.players if any(new_args) or args.players else True
        include_player_stats = args.player_stats if any(new_args) or args.players else True
        include_player_history = args.player_history if any(new_args) or args.players else False

        if args.verbose:
            logger.info(
                f"New schema configuration: dry_run={args.dry_run}, events={include_events}, players={include_players}, player_stats={include_player_stats}, player_history={include_player_history}")

        success = run_new_pipeline(
            dry_run=args.dry_run,
            include_events=include_events,
            include_players=include_players,
            include_player_stats=include_player_stats,
            include_player_history=include_player_history
        )

    if success:
        logger.info("Application completed successfully")
        sys.exit(0)
    else:
        logger.error("Application failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
