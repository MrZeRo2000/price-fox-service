# Price Fox Service

Price Fox Service fetches product pages, parses prices, and persists daily scrape results.

## Overall Architecture

- `src/main.py` is the CLI entrypoint.
- `src/application/run_pipeline.py` orchestrates fetch -> parse -> persist flows.
- `src/scraper/fetcher.py` is the fetch orchestrator (job preparation, strategy selection, output placement).
- `src/scraper/parser.py` reads fetched `page.html`/`page.txt` and extracts normalized prices. Out-of-stock pages are detected up front (schema.org `availability` and "Нет в наличии"/"Немає в наявності"/"Out of stock"-style badges in the upper part of the page) and reported as a failed parse (`status: "failed"`, `out_of_stock: true`, no price) instead of yielding a bogus price.
- `src/application/persist_latest_session.py` writes parsed outputs into storage.
- `src/turso_sync.py` owns the shared libSQL connection (local-only or Turso embedded replica) for the catalog DB.

## Turso Sync Integration

`src/turso_sync.py`'s `TursoReplicaConnection` opens a single libSQL embedded-replica
connection (`libsql.connect(<local_db_path>, sync_url=..., auth_token=...)`) for the
whole pipeline run:

- Opening it calls `.sync()`, pulling any remote changes made since the last run.
- Every repository/processor in the pipeline reads and writes through that same
  connection for the rest of the run. Writes are forwarded to the remote transparently
  as they happen -- there is no separate "push" step.
- If the local `product-catalog.sqlite` / `product-catalog.sqlite-info` pair is
  missing or incomplete (first run, or a corrupted local replica), the local replica
  files are removed and the same `connect()` + `.sync()` call bootstraps a fresh full
  copy from Turso.
- On close, the WAL is checkpointed into the main DB file and both `product-catalog.sqlite`
  and `product-catalog.sqlite-info` are copied into `db/database/backups/<yyyy_mm_dd>/`.
- If a long fetch/parse phase runs between opening the connection and its first write,
  Turso's remote Hrana stream can go stale in the meantime and the write fails with a
  "stream not found" error. `TursoReplicaConnection` catches that specific error,
  reconnects, and retries the failed call once automatically -- this is invisible to
  the rest of the pipeline.

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
  - overrides product catalog SQLite DB path used to load strategy settings.
- `--quiet`
  - suppresses stdout JSON output (the result file is still written).

Output layout: `data/one-time/scrape/<timestamp>/1/1/page.html`, `page.txt`, `metadata.json`, `parsed.json`, plus a combined `data/one-time/scrape/<timestamp>/parsed_output.json`.

## Why This Approach

- keeps regular local SQLite workflow intact when Turso is disabled.
- only ever syncs the delta with the remote (one `.sync()` per run), not the whole database.
- supports zero-local-file bootstrap when the local replica is missing or corrupted.
