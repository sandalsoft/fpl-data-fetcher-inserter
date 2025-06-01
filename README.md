# Fantasy Premier League Data Fetcher and Inserter

## Description

This application fetches Fantasy Premier League (FPL) statistics from the official FPL API and inserts them into a PostgreSQL database. It is designed to help FPL enthusiasts and data analysts to easily access and analyze FPL data.

## Features

- Fetches comprehensive FPL data, including player statistics, team information, fixtures, and more.
- Inserts and updates data efficiently into a PostgreSQL database.
- Provides a configurable setup for database connection and API endpoints.
- Designed for easy extension and customization.

## Supported FPL API Endpoints

The application interacts with several official Fantasy Premier League API endpoints, including:

- **General Information**: `/bootstrap-static/` — Summary of gameweeks, teams, players, and settings.
- **Fixtures**: `/fixtures/` — All fixture objects, including statistics for past fixtures.
- **Fixtures by Gameweek**: `/fixtures/?event={event_id}` — Fixtures for a specific gameweek.
- **Player Summary**: `/element-summary/{element_id}/` — Detailed data for a specific player.
- **Gameweek Live Data**: `/event/{event_id}/live/` — Live statistics for every player in a gameweek.
- **Manager Data**: `/me/` — Data for the authenticated manager (requires authentication).
- **League Cup Status**: `/league/{league_id}/cup-status/` — Cup status for a league.
- **Entry Data**: `/entry/{entry_id}/` — Data for a specific team entry.
- **H2H Matches**: `/h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/` — Head-to-head matches for a league and entry.
- **League Standings**: `/leagues-classic/{league_id}/standings/` — Classic league standings.

See `APIs.md` for more details on each endpoint.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.12 or higher
- Pip (Python package installer)
- PostgreSQL database server
- Git (for cloning the repository)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd fpl-data-fetcher-inserter
    ```
2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Set up environment variables:**
    Copy the `.env.example` file to `.env` and update it with your database credentials and any other necessary configurations.
    ```bash
    cp .env.example .env
    ```
    **Note:** Never commit your actual `.env` file to version control.

## Usage

To run the application and fetch/insert data:

```bash
python main.py
```

You can schedule this script to run periodically using cron jobs or other scheduling tools to keep your database updated.

Note: The application can be configured to fetch data from any of the supported FPL API endpoints. See the 'Supported FPL API Endpoints' section above for details on available endpoints and their usage.

## Environment Variables

The following environment variables are used by the application. These should be defined in your `.env` file:

- `DB_NAME`: The name of your PostgreSQL database.
- `DB_USER`: The username for your PostgreSQL database.
- `DB_PASSWORD`: The password for your PostgreSQL database.
- `DB_HOST`: The host of your PostgreSQL database (e.g., `localhost`).
- `DB_PORT`: The port for your PostgreSQL database (e.g., `5432`).
- `FPL_API_URL`: The base URL for the Fantasy Premier League API (e.g., `https://fantasy.premierleague.com/api/`).

Refer to `.env.example` for a template.

## Contributing

Contributions are welcome! If you have suggestions for improvements or find any issues, please feel free to:

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature-name`).
3.  Make your changes.
4.  Commit your changes (`git commit -m 'Add some feature'`).
5.  Push to the branch (`git push origin feature/your-feature-name`).
6.  Open a Pull Request.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
