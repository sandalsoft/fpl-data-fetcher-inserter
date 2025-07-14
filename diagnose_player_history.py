#!/usr/bin/env python3
"""
Test player history insertion directly
"""

from src.utils import get_logger
from src.database import DatabaseManager, insert_player_history
from src.parser import parse_player_history
from src.fetcher import fetch_player_history
import sys
sys.path.append('.')


logger = get_logger(__name__)


def test_player_history_insertion():
    print("🔍 Testing player history insertion...")

    # Test with one player
    player_id = 1

    try:
        # 1. Fetch and parse data
        print(f"\n1. Fetching history for player {player_id}...")
        history_data = fetch_player_history(player_id)

        if not history_data:
            print("❌ No history data returned")
            return

        print(f"✅ Got {len(history_data)} raw history records")

        # 2. Parse data
        print("\n2. Parsing history data...")
        player_history = parse_player_history(history_data, player_id)
        print(f"✅ Parsed {len(player_history)} PlayerHistory objects")

        # 3. Test database insertion
        print("\n3. Testing database insertion...")

        # Check current count
        with DatabaseManager() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM player_history WHERE player_id = %s", (player_id,))
                count_before = cursor.fetchone()[0]
                print(f"📊 Records before insertion: {count_before}")

        # Attempt insertion
        print("🚀 Attempting insertion...")
        try:
            with DatabaseManager() as conn:
                insert_player_history(conn, player_history)
                print("✅ Insertion completed without errors")

        except Exception as e:
            print(f"❌ Insertion failed: {e}")
            print(f"   Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()
            return

        # Check count after
        with DatabaseManager() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM player_history WHERE player_id = %s", (player_id,))
                count_after = cursor.fetchone()[0]
                print(f"📊 Records after insertion: {count_after}")

                if count_after > count_before:
                    print(
                        f"✅ Successfully inserted {count_after - count_before} records")

                    # Show sample of inserted data
                    cursor.execute("""
                        SELECT player_id, gameweek_id, total_points, minutes 
                        FROM player_history 
                        WHERE player_id = %s 
                        ORDER BY gameweek_id 
                        LIMIT 3
                    """, (player_id,))

                    sample_rows = cursor.fetchall()
                    print("🔍 Sample inserted records:")
                    for row in sample_rows:
                        print(
                            f"   Player {row[0]}, GW{row[1]}, {row[2]} pts, {row[3]} mins")

                else:
                    print("⚠️  No records were inserted!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_player_history_insertion()
