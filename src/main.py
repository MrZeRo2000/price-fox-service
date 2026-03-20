import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from app_logger import create_application_logger
from collector import ScrapeDetailedCollector
from cfg import Configuration
from processor import ScrapeConsolidatedProcessor
from repository import ScrapeDetailedRepository
from scraper import Parser, Scraper


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the price fox scraping pipeline (fetch + parse)."
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Path to data directory. Defaults to project data folder.",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Path to product catalog JSON (testing only). Overrides DB loading.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to product catalog SQLite DB. Defaults to data/db/product-catalog.sqlite.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print full scraper output JSON at the end.",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip fetching and parse only the latest fetched session folder.",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        help="Skip fetch/parse and persist latest scrape session into scrape_detailed table.",
    )
    return parser


def _persist_latest_scrape_results(configuration: Configuration) -> dict:
    logger = configuration.logger
    if configuration.product_catalog_db_path is None:
        logger.warning(
            "Skipping scrape result persistence because product catalog DB path is not configured."
        )
        return {
            "session_date": None,
            "deleted_rows": 0,
            "saved_rows": 0,
            "consolidated": None,
        }

    scrape_detailed_collector = ScrapeDetailedCollector(
        data_path=configuration.data_path,
        logger=logger,
    )
    session_date, rows = scrape_detailed_collector.collect_latest_session_rows()
    if session_date is None:
        logger.info("Skipping scrape result persistence because no scrape session was found.")
        return {
            "session_date": None,
            "deleted_rows": 0,
            "saved_rows": 0,
            "consolidated": None,
        }

    scrape_detailed_repository = ScrapeDetailedRepository(
        db_path=configuration.product_catalog_db_path
    )
    persisted_results = scrape_detailed_repository.replace_session_rows(
        session_date=session_date,
        rows=rows,
    )
    scrape_consolidated_processor = ScrapeConsolidatedProcessor(
        db_path=configuration.product_catalog_db_path
    )
    consolidated_results = scrape_consolidated_processor.replace_for_session(
        session_date=session_date
    )
    logger.info(
        f"Persisted scrape session_date={persisted_results['session_date']} "
        f"(deleted={persisted_results['deleted_rows']}, saved={persisted_results['saved_rows']})."
    )
    logger.info(
        f"Refreshed scrape_consolidated for session_date={consolidated_results['session_date']} "
        f"(deleted={consolidated_results['deleted_rows']}, saved={consolidated_results['saved_rows']})."
    )
    return {
        "session_date": persisted_results["session_date"],
        "deleted_rows": persisted_results["deleted_rows"],
        "saved_rows": persisted_results["saved_rows"],
        "consolidated": consolidated_results,
    }


def main() -> int:
    session_start_datetime = datetime.now()
    parser = _build_parser()
    args = parser.parse_args()
    if args.parse_only and args.collect_only:
        parser.error("--parse-only and --collect-only cannot be used together.")

    resolved_data_path = (
        args.data_path
        if args.data_path is not None
        else str(Path(__file__).resolve().parent.parent / "data")
    )
    logger = create_application_logger(data_path=resolved_data_path)

    try:
        configuration = Configuration(
            data_path=args.data_path,
            config_path=args.config_path,
            db_path=args.db_path,
        )
        logger = configuration.logger
        if args.collect_only:
            persisted_results = _persist_latest_scrape_results(configuration)
            result = {
                "fetch_results": [],
                "parse_results": [],
                "collect_results": persisted_results,
            }
        elif args.parse_only:
            parser = Parser(configuration)
            parse_results = parser.execute()
            result = {
                "fetch_results": [],
                "parse_results": parse_results,
            }
        else:
            scraper = Scraper(configuration)
            result = scraper.execute()
    except Exception as exc:
        logger.error(f"Scraper failed: {exc}")
        return 1

    fetch_results = result.get("fetch_results", [])
    parse_results = result.get("parse_results", [])
    successful_parses = sum(1 for item in parse_results if item.get("status") == "success")

    if args.collect_only:
        logger.info("Collect-only run completed.")
    elif args.parse_only:
        logger.info("Parse-only run completed.")
    else:
        logger.info("Scraper run completed.")
    logger.info(f"Fetched records: {len(fetch_results)}")
    logger.info(f"Parsed records: {len(parse_results)}")
    logger.info(f"Successful parses: {successful_parses}")
    if not args.collect_only:
        _persist_latest_scrape_results(configuration)

    session_root = None
    if fetch_results:
        first_html = fetch_results[0].get("result", {}).get("html")
        if first_html:
            session_root = Path(first_html).parents[2]
    if session_root is not None:
        logger.info(f"Session output folder: {session_root}")

    if args.print_json:
        logger.info(json.dumps(result, indent=2, ensure_ascii=False))

    session_end_datetime = datetime.now()
    session_duration_seconds = int(
        (session_end_datetime - session_start_datetime).total_seconds()
    )
    duration_minutes = session_duration_seconds // 60
    duration_seconds = session_duration_seconds % 60
    logger.info(f"Session start time: {session_start_datetime.isoformat(timespec='seconds')}")
    logger.info(f"Session end time: {session_end_datetime.isoformat(timespec='seconds')}")
    logger.info(f"Session duration: {duration_minutes}m {duration_seconds}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
