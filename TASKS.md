### **🧱 Setup & Environment**

1. **Create project folder structure**

   - Create: `src/`, `tests/`, `.env.example`, `.gitignore`, `requirements.txt`, `README.md`
   - ✅ Ends when all folders and files exist and are committed to Git.

2. **Define initial dependencies**

   - Add to `requirements.txt`: `requests`, `psycopg2-binary`, `python-dotenv`, `pydantic`, `pytest`
   - ✅ Ends when `pip install -r requirements.txt` works without errors.

3. **Implement environment loading**

   - Create `src/config.py`
   - Load values from `.env.example`: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `FPL_API_URL`
   - ✅ Ends when calling a function like `get_config()` returns expected values.

4. **Add logging utility**

   - Create `src/utils.py` with a `get_logger(name)` function
   - ✅ Ends when logs appear with module names and timestamp.

---

### **🌐 Data Fetching**

5. **Implement basic fetcher for `/bootstrap-static/`**

   - File: `src/fetcher.py`
   - Function: `fetch_bootstrap_data() -> dict`
   - ✅ Ends when sample JSON is fetched and returned.

6. **Add fetcher test**

   - File: `tests/test_fetcher.py`
   - ✅ Ends when test asserts `dict` and some expected keys (`'elements'`, `'teams'`, etc.)

7. **Add support for generic endpoint fetching**

   - Function: `fetch_endpoint(endpoint: str) -> dict`
   - ✅ Ends when `fetch_endpoint("/fixtures/")` returns data.

8. **Handle fetcher HTTP errors**

   - Raise for non-200 responses with descriptive exceptions.
   - ✅ Ends when error on bad endpoint is caught and logged.

---

### **🧮 Data Parsing**

9. **Create data models using Pydantic for `Team`, `Player`, `Fixture`, `Gameweek`**

   - File: `src/models.py`
   - ✅ Ends when JSON can be parsed into Python objects and validated.

10. **Write parser for `teams`**

    - File: `src/parser.py`
    - Function: `parse_teams(data: dict) -> List[Team]`
    - ✅ Ends when parsed list contains all valid teams from the JSON.

11. **Write parser for `players`**

    - Function: `parse_players(data: dict) -> List[Player]`
    - ✅ Ends when all players are parsed with accurate data mapping.

12. **Add unit tests for parsers**

    - File: `tests/test_parser.py`
    - ✅ Ends when data maps are correct and edge cases (missing fields, nulls) are handled.

---

### **🛢 Database Setup**

13. **Create SQL schema file**

    - File: `sql/schema.sql`
    - Tables: `teams`, `players`, `fixtures`, `gameweeks`
    - ✅ Ends when schema runs in psql without error.

14. **Build DB connection utility**

    - File: `src/database.py`
    - Function: `get_connection() -> connection`
    - ✅ Ends when successful connect and cursor returns.

15. **Add function to insert `teams`**

    - Function: `insert_teams(conn, teams: List[Team])`
    - ✅ Ends when team data is inserted and queryable.

16. **Add function to insert `players`**

    - Function: `insert_players(conn, players: List[Player])`
    - ✅ Ends when insert completes with `ON CONFLICT DO UPDATE`.

17. **Add tests with SQLite or mocked PG**

    - ✅ Ends when test inserts are verifiable using SELECTs.

---

### **🚀 Integration Orchestration**

18. **Create main runner**

    - File: `src/app.py`
    - Steps:

      1. Load config
      2. Fetch `/bootstrap-static/`
      3. Parse `teams`, `players`
      4. Insert into DB

    - ✅ Ends when all stages run end-to-end without crash.

19. **Add dry-run mode to runner**

    - Allow no-insert preview for verification.
    - ✅ Ends when CLI flag `--dry-run` skips inserts.

20. **Add command-line arguments**

    - Use `argparse` to toggle data types (e.g., `--players`, `--fixtures`)
    - ✅ Ends when you can control what’s fetched and inserted.

---

### **📅 Future Steps (Post MVP)**

- Add scheduling logic for cron
- Implement endpoints like `/fixtures/`, `/element-summary/{id}/`
- Add change tracking for incremental updates
- Add logging to file + error alerting

---

Let me know if you'd like these output as separate prompts per task or bundled into YAML/JSON/task manifest format.
