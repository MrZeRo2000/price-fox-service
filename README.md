# Price Fox Service

Price Fox Service fetches product pages, parses prices, and persists daily scrape results.

## Overall Architecture

- `src/main.py` is the CLI entrypoint.
- `src/application/run_pipeline.py` orchestrates fetch -> parse -> persist flows.
- `src/scraper/fetcher.py` is the fetch orchestrator (job preparation, strategy selection, output placement).
- `src/scraper/fetch_strategies/playwright_strategy.py` contains the Playwright implementation (current default behavior).
- `src/scraper/fetch_strategies/jina_strategy.py` contains the Jina Reader implementation.
- `src/scraper/fetch_strategies/base.py` defines the `FetchStrategy` contract.
- `src/scraper/parser.py` reads fetched `page.html`/`page.txt` and extracts normalized prices.
- `src/application/persist_latest_session.py` writes parsed outputs into storage.

## Fetcher Strategy Design

The fetcher now uses a strategy-based approach so source acquisition can be switched without changing the rest of the pipeline.

- `PlaywrightFetchStrategy`:
  - keeps the existing procedure as default behavior.
  - performs browser-based scraping with all current reliability logic.
- `JinaFetchStrategy`:
  - optional mode using Jina Reader endpoint `https://r.jina.ai/`.
  - works without API key.
  - stores fetched content in the same output structure so downstream parser flow remains unchanged.

Strategy selection happens through runtime configuration and is consumed in `Fetcher.execute()`, while strategy-specific logic stays in `src/scraper/fetch_strategies/`.

## Jina Mode (No API Key)

Jina mode uses `r.jina.ai/<target-url>` to retrieve LLM-friendly page content.

Limitations/notes:
- unauthenticated mode has stricter service limits.
- this implementation enforces a local client-side rate limiter.
- default limit is `20` requests per minute and is configurable.

## CLI Usage

Default (existing Playwright behavior):

```bash
python src/main.py
```

Use Jina strategy:

```bash
python src/main.py --fetch-strategy jina
```

Use Jina strategy with custom rate limit:

```bash
python src/main.py --fetch-strategy jina --jina-rate-limit-rpm 12
```

## New CLI Options

- `--fetch-strategy {playwright,jina}`
  - `playwright` is default and preserves the current procedure.
  - `jina` enables the no-key Jina Reader strategy.
- `--jina-rate-limit-rpm <int>`
  - default: `20`
  - minimum enforced value: `1`

## Why This Approach

- keeps the stable scraping path untouched.
- allows low-friction fallback/experimentation with Jina.
- makes future providers easy to add by implementing another `FetchStrategy`.
