#!/usr/bin/env python3
"""
Test script to verify the schema executes without errors.
"""

import logging
from config import load_config
from database import get_connection, close_connection, execute_schema
import sys
import os
sys.path.append('src')


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_schema():
    """Test schema execution."""
    try:
        # Load configuration
        config = load_config()
        logger.info("Configuration loaded successfully")

        # Get database connection
        conn = get_connection()
        logger.info("Database connection established")

        # Execute schema
        execute_schema(conn)
        logger.info("Schema executed successfully!")

        # Close connection
        close_connection(conn)
        logger.info("Test completed successfully")

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False

    return True


if __name__ == "__main__":
    success = test_schema()
    sys.exit(0 if success else 1)
