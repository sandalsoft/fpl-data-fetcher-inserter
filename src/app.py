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
from .fetcher import fetch_bootstrap_data
from .parser import parse_teams, parse_players
from .database import DatabaseManager, insert_teams, insert_players, execute_schema

logger = get_logger(__name__)


def run_bootstrap_pipeline(dry_run: bool = False, include_teams: bool = True, include_players: bool = True) -> bool:
    """Run the complete bootstrap data pipeline.

    Args:
        dry_run: If True, skip database insertions (preview mode)
        include_teams: If True, process and insert teams data
        include_players: If True, process and insert players data

    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting FPL bootstrap data pipeline...")

    if not include_teams and not include_players:
        logger.error(
            "At least one data type (teams or players) must be selected")
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

        # Step 3: Parse teams and players based on selection
        logger.info("Step 3: Parsing data")
        teams = []
        players = []

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
  %(prog)s                        # Fetch and insert all data (teams + players)
  %(prog)s --dry-run              # Preview what would be fetched/inserted
  %(prog)s --teams                # Only fetch and insert teams
  %(prog)s --players              # Only fetch and insert players
  %(prog)s --teams --players      # Explicitly fetch both (same as default)
  %(prog)s --dry-run --players    # Preview players data only
        """
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mode: fetch and parse data but skip database insertion"
    )

    parser.add_argument(
        "--teams",
        action="store_true",
        help="Process teams data (default: process all data types)"
    )

    parser.add_argument(
        "--players",
        action="store_true",
        help="Process players data (default: process all data types)"
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

    # If no specific data types are selected, default to all
    include_teams = args.teams if (args.teams or args.players) else True
    include_players = args.players if (args.teams or args.players) else True

    if args.verbose:
        logger.info(
            f"Configuration: dry_run={args.dry_run}, teams={include_teams}, players={include_players}")

    success = run_bootstrap_pipeline(
        dry_run=args.dry_run,
        include_teams=include_teams,
        include_players=include_players
    )

    if success:
        logger.info("Application completed successfully")
        sys.exit(0)
    else:
        logger.error("Application failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
