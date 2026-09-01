# Price Fox Service

Price Fox Service fetches product pages, parses prices, and persists daily scrape results.

## Overall Architecture

- `src/main.py` is the CLI entrypoint: it owns the DB connection lifetime and sequences sync -> fetch/parse -> persist.
- `src/scraper/pipeline.py` holds `Scraper` and `run_pipeline`, the fetch -> parse flow.
- `src/scraper/fetcher.py` is the fetch orchestrator only: it builds jobs from the catalog, hands them to a strategy, and files artifacts under `<session>/<product_id>/<url_id>/`.
- `src/scraper/fetch_strategies/` holds the fetching itself, one module per strategy behind `BaseFetchStrategy`. `PlaywrightFetchStrategy` is the only one wired up -- every URL goes through it, and there is no per-domain strategy selection. When a page comes back blocked it walks a recovery chain internally: anti-bot retry -> itbox persistent Chrome -> Jina reader. `JinaFetchStrategy` and `GeminiUrlFetchStrategy` are implemented but not currently selected by anything.
- `src/scraper/parser.py` reads fetched `page.html`/`page.txt` and extracts normalized prices. Out-of-stock pages are detected up front (schema.org `availability` and "Нет в наличии"/"Немає в наявності"/"Out of stock"-style badges in the upper part of the page) and reported as a failed parse (`status: "failed"`, `out_of_stock: true`, no price) instead of yielding a bogus price.
- `src/repositories/` is all DB access: one module per table, plus `persist_latest_session.py`, which writes a parsed session through the repositories and processors and commits.
- `src/turso_sync.py` owns the shared libSQL connection (local-only or Turso embedded replica) for the catalog DB.
- `src/logger.py` is the application-wide logger singleton. Any module does `from logger import logger` and logs; nothing accepts or forwards a logger argument. It self-configures on import, and entry points (`main.py`, `one_time_url.py`, `tests/conftest.py`) call `configure_logging(data_path=...)` once at startup to route output to `log/<data_dir_name>_<yyyymmdd>.log`.

## Turso Sync Integration

`src/turso_sync.py`'s `TursoReplicaConnection` owns a libSQL embedded-replica
connection (`libsql.connect(<local_db_path>, sync_url=..., auth_token=...)`).
`src/main.py` builds one handle and enters it twice -- a short window before the run
(sync + `--once-per-day` guard) and another after it (persist) -- rather than holding
it open across fetch/parse, where an idle replica connection loses its Hrana stream
and fails with `stream not found`:

- Opening it calls `.sync()`, pulling any remote changes made since the last open.
- Every repository/processor writes through that connection, and the write is
  forwarded to the remote on `commit()` -- there is no separate "push" step.
- The connection is passed explicitly to `persist_latest_scrape_results`; it is not
  carried on `CatalogConfig`, which holds only paths and catalog data.
- If the local `product-catalog.sqlite` / `product-catalog.sqlite-info` pair is
  missing or incomplete (first run, or a corrupted local replica), the local replica
  files are removed and the same `connect()` + `.sync()` call bootstraps a fresh full
  copy from Turso.
- On close, the WAL is checkpointed into the main DB file and both `product-catalog.sqlite`
  and `product-catalog.sqlite-info` are copied into `db/database/backups/<yyyy_mm_dd>/`.

Whether this is active is controlled by `config/turso.json`'s `enabled` field. When
disabled, the same connection type is used against the local file only (no sync_url),
so the rest of the pipeline is unaffected either way. `--sync` does not toggle Turso on;
it makes a misconfigured `turso.json` a hard error instead of a silent local-only run.

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

One-time fetch + parse for a single ad-hoc URL (artifacts and parsed JSON land in `data/one-time/`):

```bash
python src/one_time_url.py "https://example.com/product"
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
  - requires Turso to be fully configured; fails the run instead of falling back to local-only.
- `--once_per_day`
  - after the Turso pull, checks `scrape_stats` for a row with today's `session_date` (YYYYMMDD).
  - if such a row exists, the run is treated as already completed for the day and exits early with code 0 before fetch/parse/persist.
  - skipped (with a log line) when the SQLite catalog is not in use (`--config-path` set) or the DB file is missing.

## CLI Options (`src/one_time_url.py`)

- positional `url`
  - URL to fetch and parse through the full pipeline.
- `--db-path <path>`
  - overrides product catalog SQLite DB path.
- `--quiet`
  - suppresses stdout JSON output (the result file is still written).

Output layout: `data/one-time/scrape/<timestamp>/1/1/page.html`, `page.txt`, `metadata.json`, `parsed.json`, plus a combined `data/one-time/scrape/<timestamp>/parsed_output.json`.

## Why This Approach

- keeps regular local SQLite workflow intact when Turso is disabled.
- only ever syncs the delta with the remote (one `.sync()` per run), not the whole database.
- supports zero-local-file bootstrap when the local replica is missing or corrupted.
