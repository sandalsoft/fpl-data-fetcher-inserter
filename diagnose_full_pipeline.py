#!/usr/bin/env python3
"""
Comprehensive diagnostic script that mimics the exact main application flow
"""

from src.database import DatabaseManager, execute_schema, insert_teams_new, insert_players_new, insert_player_stats, insert_player_history, insert_gameweeks_new
from src.parser import parse_players, parse_player_stats, parse_player_history, parse_teams, parse_gameweeks
from src.fetcher import fetch_bootstrap_data, fetch_current_gameweek_id, fetch_player_history_batch, fetch_independent_endpoints_parallel
from src.utils import get_logger
from src.config import get_config
import sys
sys.path.append('.')


logger = get_logger(__name__)


def test_full_pipeline():
    print("🔍 Testing full pipeline exactly like main app...")

    try:
        # Step 1: Load configuration (same as main app)
        print("\n1. Loading configuration...")
        config = get_config()
        print(f"✅ Configuration loaded - API URL: {config['fpl_api_url']}")

        # Step 2: Fetch core data from FPL API in parallel (same as main app)
        print("\n2. Fetching core data from FPL API in parallel...")
        fetch_results = fetch_independent_endpoints_parallel()

        bootstrap_data = fetch_results['bootstrap_data']
        if not bootstrap_data:
            print("❌ Failed to fetch bootstrap data - cannot continue")
            return

        print("✅ Bootstrap data fetched successfully")

        # Get current gameweek ID for player stats (same as main app)
        current_gameweek_id = fetch_current_gameweek_id(bootstrap_data)
        if current_gameweek_id is None:
            print("⚠️  Could not determine current gameweek ID, using 1 as default")
            current_gameweek_id = 1

        # Step 3: Parse data (same as main app)
        print("\n3. Parsing data...")
        gameweeks = parse_gameweeks(bootstrap_data)
        print(f"✅ Parsed {len(gameweeks)} gameweeks")

        teams = parse_teams(bootstrap_data)
        print(f"✅ Parsed {len(teams)} teams")

        players = parse_players(bootstrap_data)
        print(f"✅ Parsed {len(players)} players")

        player_stats = parse_player_stats(bootstrap_data, current_gameweek_id)
        print(f"✅ Parsed {len(player_stats)} player stats")

        # Player history fetching (same as main app)
        print("\n4. Fetching player history data...")
        all_player_history = []
        player_ids = [player.id for player in players]

        # Test with just first 3 players to speed up diagnosis
        test_player_ids = player_ids[:3]
        print(f"📋 Testing with first 3 players for speed: {test_player_ids}")

        max_workers = config.get('parallel_workers', 15)
        history_results = fetch_player_history_batch(
            test_player_ids, max_workers=max_workers)

        successful_fetches = 0
        for player_id, history_data in history_results:
            if history_data:
                player_history_entries = parse_player_history(
                    history_data, player_id)
                all_player_history.extend(player_history_entries)
                successful_fetches += 1

        player_history = all_player_history
        print(
            f"✅ Successfully fetched history for {successful_fetches}/{len(test_player_ids)} players")
        print(f"✅ Parsed {len(player_history)} player history entries")

        # Step 4: Insert data into database (EXACT SAME ORDER as main app)
        print("\n5. Inserting data into database (same order as main app)...")

        # Ensure schema exists
        print("   5.1. Creating/verifying schema...")
        with DatabaseManager() as conn:
            try:
                execute_schema(conn)
                print("   ✅ Database schema verified/created")
            except Exception as e:
                print(f"   ⚠️  Schema execution failed: {e}")

        # Insert gameweeks in separate transaction
        print("   5.2. Inserting gameweeks...")
        try:
            with DatabaseManager() as conn:
                insert_gameweeks_new(conn, gameweeks)
            print("   ✅ Gameweeks inserted successfully")
        except Exception as e:
            print(f"   ❌ Gameweeks insertion failed: {e}")

        # Insert teams in separate transaction
        print("   5.3. Inserting teams...")
        try:
            with DatabaseManager() as conn:
                insert_teams_new(conn, teams)
            print("   ✅ Teams inserted successfully")
        except Exception as e:
            print(f"   ❌ Teams insertion failed: {e}")

        # Insert players in separate transaction
        print("   5.4. Inserting players...")
        try:
            with DatabaseManager() as conn:
                insert_players_new(conn, players)
            print("   ✅ Players inserted successfully")

            # Check if players actually got inserted
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM players WHERE id IN %s", (tuple(test_player_ids),))
                inserted_count = cursor.fetchone()[0]
                print(
                    f"   📊 Verified: {inserted_count}/{len(test_player_ids)} test players in database")

        except Exception as e:
            print(f"   ❌ Players insertion failed: {e}")
            print("   🚨 This could be the root cause!")
            import traceback
            traceback.print_exc()

        # Insert player stats in separate transaction
        print("   5.5. Inserting player stats...")
        try:
            with DatabaseManager() as conn:
                insert_player_stats(conn, player_stats)
            print("   ✅ Player stats inserted successfully")
        except Exception as e:
            print(f"   ❌ Player stats insertion failed: {e}")

        # Insert player history in separate transaction (THIS IS THE CRITICAL STEP)
        print("   5.6. Inserting player history...")
        try:
            with DatabaseManager() as conn:
                insert_player_history(conn, player_history)
            print("   ✅ Player history inserted successfully")

            # Check final count
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM player_history WHERE player_id IN %s", (tuple(test_player_ids),))
                final_count = cursor.fetchone()[0]
                print(
                    f"   📊 Final verification: {final_count} player history records in database")

        except Exception as e:
            print(f"   ❌ Player history insertion failed: {e}")
            print("   🚨 This is likely the root cause!")
            import traceback
            traceback.print_exc()

        print("\n✅ Full pipeline test completed!")

    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_full_pipeline()
