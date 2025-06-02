# Key FPL API Endpoints

## General Information

- Endpoint: /bootstrap-static/
- URL: <https://fantasy.premierleague.com/api/bootstrap-static/>
- Description: Provides a summary of gameweeks, FPL phases, teams, players, and game settings.

## Fixtures

- Endpoint: /fixtures/
- URL: <https://fantasy.premierleague.com/api/fixtures/>
- Description: Returns an array of fixture objects. For future fixtures, only summary info is available; for past fixtures, statistics are included.

## Fixtures by Gameweek

- Endpoint: /fixtures/?event={event_id}
- URL: <https://fantasy.premierleague.com/api/fixtures/?event={event_id}>
- Description: Returns fixtures for a specific gameweek.

## Player Summary

- Endpoint: /element-summary/{element_id}/
- URL: <https://fantasy.premierleague.com/api/element-summary/{element_id}/>
- Description: Returns detailed data for a specific player, including current season fixtures and historical season summaries.

## Gameweek Live Data

- Endpoint: /event/{event_id}/live/
- URL: <https://fantasy.premierleague.com/api/event/{event_id}/live/>
- Description: Returns live statistics for every player in the specified gameweek.

## Manager Data

- Endpoint: /me/
- URL: <https://fantasy.premierleague.com/api/me/>
- Description: Returns data for the authenticated manager (requires authentication).

## League Cup Status

- Endpoint: /league/{league_id}/cup-status/
- URL: <https://fantasy.premierleague.com/api/league/{league_id}/cup-status/>
- Description: Returns cup status information for a specified league.

## Additional Endpoints and Libraries

### Entry Data

- Endpoint: /entry/{entry_id}/
- URL: <https://fantasy.premierleague.com/api/entry/{entry_id}/>
- Description: Returns data for a specific team entry.

### H2H Matches

- Endpoint: /h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/
- URL: <https://fantasy.premierleague.com/api/h2h-matches/league/{league_id}/entry/{entry_id}/page/{page}/>
- Description: Returns head-to-head matches for a given league and entry.

### League Standings

- Endpoint: /leagues-classic/{league_id}/standings/
- URL: <https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/>
- Description: Returns classic league standings.
