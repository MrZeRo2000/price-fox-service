"""
One-time URL fetch + parse runner.

Runs the full price-fox pipeline (fetch -> parse) against a single ad-hoc URL,
without touching the configured product catalog. A synthetic single-URL catalog
is built in memory; DB-driven strategy settings (default fetch strategy, Jina
rate limit, per-domain overrides) are still loaded from the regular product
catalog SQLite DB so the run mirrors a production execution path.

Artifacts land under data/one-time/:
    data/one-time/scrape/<timestamp>/1/1/page.html
    data/one-time/scrape/<timestamp>/1/1/page.txt
    data/one-time/scrape/<timestamp>/1/1/metadata.json
    data/one-time/scrape/<timestamp>/1/1/parsed.json
    data/one-time/scrape/<timestamp>/parsed_output.json   (combined result)

Usage:
    ./venv/Scripts/python src/one_time_url.py "https://example.com/product"
    ./venv/Scripts/python src/one_time_url.py "https://example.com/product" --db-path ./db/database/product-catalog.sqlite
    ./venv/Scripts/python src/one_time_url.py "https://example.com/product" --quiet
"""

import argparse
import json
import sys
from pathlib import Path

from cfg import CatalogConfig
from models import CatalogData, CatalogUrl, Product
from scraper import Scraper
from session import resolve_latest_scrape_session_folder


ONE_TIME_PRODUCT_ID = 1
ONE_TIME_URL_ID = 1


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _build_one_time_catalog(url: str) -> CatalogData:
    return CatalogData(
        urls=[CatalogUrl(url_id=ONE_TIME_URL_ID, url=url, is_active=True)],
        categories=[],
        products=[
            Product(
                id=ONE_TIME_PRODUCT_ID,
                name="one-time-url",
                category_ids=[],
                url_ids=[ONE_TIME_URL_ID],
            )
        ],
    )


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch and parse a single URL through the full price-fox pipeline. "
            "Artifacts and parsed JSON land in data/one-time/."
        )
    )
    parser.add_argument("url", help="URL to fetch and parse.")
    parser.add_argument(
        "--db-path",
        default=None,
        help="Override product catalog SQLite DB path used for strategy settings.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress stdout JSON output (file is still written).",
    )
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()

    one_time_root = _project_root() / "data" / "one-time"
    one_time_root.mkdir(parents=True, exist_ok=True)

    catalog_config = CatalogConfig(
        data_path=str(one_time_root),
        config_path=None,
        db_path=args.db_path,
    )
    # The DB-backed catalog is irrelevant for a single ad-hoc URL; swap in a
    # synthetic catalog so Fetcher/Parser still resolve product/url IDs while
    # DB-driven strategy settings (default strategy, jina rate limit, domain
    # overrides) continue to load from product_catalog_db_path.
    catalog_config._product_catalog_data = _build_one_time_catalog(args.url)
    logger = catalog_config.logger

    logger.info(f"One-time URL fetch+parse: {args.url}")
    logger.info(f"Data root: {one_time_root}")

    scraper = Scraper(catalog_config)
    result = scraper.execute()

    session_folder = resolve_latest_scrape_session_folder(one_time_root)
    output_root = session_folder if session_folder is not None else one_time_root
    parsed_output_path = output_root / "parsed_output.json"
    parsed_output_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    logger.info(f"Parsed JSON written to: {parsed_output_path}")

    if not args.quiet:
        parse_results = result.get("parse_results", [])
        print(
            json.dumps(
                parse_results[0] if parse_results else result,
                indent=2,
                ensure_ascii=False,
            )
        )
    return 0


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    sys.exit(main())
