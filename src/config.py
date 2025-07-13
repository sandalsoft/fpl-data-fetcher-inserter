import os
from typing import Dict, Any
from dotenv import load_dotenv


def get_config() -> Dict[str, Any]:
    """Load configuration from environment variables.

    Returns:
        Dict containing configuration values
    """
    # Load environment variables from .env
    load_dotenv(".env")

    config = {
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "5432")),
        "db_name": os.getenv("DB_NAME", "fpl_data"),
        "db_user": os.getenv("DB_USER", "fpl_user"),
        "db_password": os.getenv("DB_PASSWORD", "fpl_password"),
        "fpl_api_url": os.getenv("FPL_API_URL", "https://fantasy.premierleague.com/api"),
    }

    return config
