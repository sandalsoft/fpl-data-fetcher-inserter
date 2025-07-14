#!/usr/bin/env python3
"""
Diagnose transaction commit issue in DatabaseManager
"""

from src.utils import get_logger
from src.database import DatabaseManager, get_connection, close_connection
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


logger = get_logger(__name__)


def test_transaction_commit():
    """Test if transactions are committed properly"""

    # Test 1: Check current row count
    with DatabaseManager() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM player_history")
            initial_count = cursor.fetchone()[0]
            logger.info(f"Initial player_history count: {initial_count}")

    # Test 2: Insert a test record and see if it persists
    with DatabaseManager() as conn:
        with conn.cursor() as cursor:
            # First get a valid player_id from the database
            cursor.execute("SELECT id FROM players LIMIT 1")
            result = cursor.fetchone()
            if result:
                player_id = result[0]
                # Insert a test record that we can easily identify
                cursor.execute("""
                    INSERT INTO player_history (player_id, gameweek_id, total_points)
                    VALUES (%s, 999, 0)
                    ON CONFLICT (player_id, gameweek_id) DO UPDATE SET total_points = EXCLUDED.total_points
                """, (player_id,))
                logger.info(f"Inserted test record for player {player_id}")
            else:
                logger.error("No players found in database")
                return False

    # Test 3: Check if the record persists after connection closes
    with DatabaseManager() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM player_history WHERE gameweek_id = 999")
            test_count = cursor.fetchone()[0]
            logger.info(f"Test record count after reconnection: {test_count}")

            cursor.execute("SELECT COUNT(*) FROM player_history")
            final_count = cursor.fetchone()[0]
            logger.info(f"Final player_history count: {final_count}")

    return test_count > 0


def test_manual_commit():
    """Test with manual commit"""
    logger.info("Testing with manual commit...")

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Get a valid player_id
            cursor.execute("SELECT id FROM players LIMIT 1")
            result = cursor.fetchone()
            if not result:
                logger.error("No players found for manual commit test")
                return False

            player_id = result[0]
            cursor.execute("""
                INSERT INTO player_history (player_id, gameweek_id, total_points)
                VALUES (%s, 888, 5)
                ON CONFLICT (player_id, gameweek_id) DO UPDATE SET total_points = EXCLUDED.total_points
            """, (player_id,))
            conn.commit()  # Manual commit
            logger.info(
                f"Inserted test record with manual commit for player {player_id}")
    finally:
        close_connection(conn)

    # Check if it persists
    with DatabaseManager() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM player_history WHERE gameweek_id = 888")
            test_count = cursor.fetchone()[0]
            logger.info(f"Manual commit test record count: {test_count}")

    return test_count > 0


if __name__ == "__main__":
    logger.info("🔍 Diagnosing transaction commit issue...")

    logger.info("=== Test 1: DatabaseManager without explicit commit ===")
    result1 = test_transaction_commit()
    logger.info(
        f"DatabaseManager test result: {'PASS' if result1 else 'FAIL'}")

    logger.info("=== Test 2: Manual commit ===")
    result2 = test_manual_commit()
    logger.info(f"Manual commit test result: {'PASS' if result2 else 'FAIL'}")

    if not result1 and result2:
        logger.error(
            "❌ CONFIRMED: DatabaseManager is not committing transactions!")
        logger.info("💡 Solution: Add conn.commit() to DatabaseManager.__exit__")
    elif result1:
        logger.info("✅ DatabaseManager is working correctly")
    else:
        logger.error("❌ Both tests failed - there may be a different issue")
