# Price Fox Service

Price Fox Service fetches product pages, parses prices, and persists daily scrape results.

## Overall Architecture

- `src/main.py` is the CLI entrypoint.
- `src/application/run_pipeline.py` orchestrates fetch -> parse -> persist flows.
- `src/scraper/fetcher.py` is the fetch orchestrator (job preparation, strategy selection, output placement).
- `src/scraper/parser.py` reads fetched `page.html`/`page.txt` and extracts normalized prices.
- `src/application/persist_latest_session.py` writes parsed outputs into storage.
- `src/turso_sync.py` handles Turso pull/push synchronization for the local SQLite catalog DB.

## Turso Sync Integration

Turso integration uses the current Python `libsql` approach for embedded replicas:

- connect/sync through `libsql.connect(<local_db_path>, sync_url=..., auth_token=...)`
- sync is enabled at runtime with `--sync` and values from `config/turso.json`

When `--sync` is used and the local catalog DB file is missing, the app now performs a bootstrap pull from Turso before loading configuration. This allows first-time setup without a pre-existing local database file.

## CLI Usage

Default run:

```bash
python src/main.py
```

Run with Turso sync enabled:

```bash
python src/main.py --sync
```

Parse-only mode:

```bash
python src/main.py --parse-only
```

Collect-only mode:

```bash
python src/main.py --collect-only
```

Use explicit paths:

```bash
python src/main.py --data-path ./data --db-path ./db/database/product-catalog.sqlite
```

Push local DB to Turso as a one-off initial load:

```bash
python src/turso_initial_load.py
```

## CLI Options (`src/main.py`)

- `--data-path <path>`
  - overrides data root path (defaults from `config/settings.py`).
- `--config-path <path>`
  - loads product catalog from JSON file instead of SQLite DB.
- `--db-path <path>`
  - overrides product catalog SQLite DB path.
- `--print-json`
  - prints full pipeline result JSON at the end of run.
- `--parse-only`
  - skips fetch and parses only latest fetched session folder.
- `--collect-only`
  - skips fetch/parse and persists latest scrape session into DB.
- `--sync`
  - enables Turso pre/post sync for DB-backed runs.
  - if local DB is missing, performs bootstrap pull from Turso first.

## CLI Options (`src/turso_initial_load.py`)

- `--db-path <path>`
  - local SQLite DB path to upload (defaults from `config/settings.py`).
- `--turso-config-path <path>`
  - path to Turso JSON config (default: `config/turso.json`).

## Why This Approach

- keeps regular local SQLite workflow intact.
- enables remote synchronization only when explicitly requested.
- supports zero-local-file bootstrap in sync mode for easier environment setup.
