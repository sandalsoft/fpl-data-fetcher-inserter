import os
from typing import Dict, Any
from dotenv import load_dotenv

ENV_FILE = ".env"


def get_config() -> Dict[str, Any]:
    """Load configuration from environment variables.

    Returns:
        Dict containing configuration values
    """
    # Load environment variables from .env
    load_dotenv(ENV_FILE)

    config = {
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "5432")),
        "db_name": os.getenv("DB_NAME", "fpl_data"),
        "db_user": os.getenv("DB_USER", "fpl_user"),
        "db_password": os.getenv("DB_PASSWORD", "fpl_password"),
        "fpl_api_url": os.getenv("FPL_API_URL", "https://fantasy.premierleague.com/api"),
        "parallel_workers": int(os.getenv("PARALLEL_WORKERS", "15")),

        # Database optimization settings
        "bulk_insert_threshold": int(os.getenv("BULK_INSERT_THRESHOLD", "100")),
        "enable_vacuum_after_bulk": os.getenv("ENABLE_VACUUM_AFTER_BULK", "true").lower() == "true",
        "vacuum_threshold": int(os.getenv("VACUUM_THRESHOLD", "1000")),
        "enable_db_optimizations": os.getenv("ENABLE_DB_OPTIMIZATIONS", "true").lower() == "true",
    }

    return config
