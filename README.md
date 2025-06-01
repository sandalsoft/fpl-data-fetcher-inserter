# Fantasy Premier League Data Fetcher and Inserter

## Description

This application fetches Fantasy Premier League (FPL) statistics from the official FPL API and inserts them into a PostgreSQL database. It is designed to help FPL enthusiasts and data analysts to easily access and analyze FPL data.

## Features

- Fetches comprehensive FPL data, including player statistics, team information, fixtures, and more.
- Inserts and updates data efficiently into a PostgreSQL database.
- Provides a configurable setup for database connection and API endpoints.
- Designed for easy extension and customization.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 3.8 or higher
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
