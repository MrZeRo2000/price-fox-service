import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from application import persist_latest_scrape_results, run_pipeline
from app_logger import create_application_logger
from cfg import Configuration
from config.settings import resolve_configuration_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the price fox scraping pipeline (fetch + parse)."
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Path to data directory. Defaults to config.settings value.",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Path to product catalog JSON (testing only). Overrides DB loading.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to product catalog SQLite DB. Defaults to config.settings value.",
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


def main() -> int:
    session_start_datetime = datetime.now()
    parser = _build_parser()
    args = parser.parse_args()
    if args.parse_only and args.collect_only:
        parser.error("--parse-only and --collect-only cannot be used together.")

    resolved_settings = resolve_configuration_settings(
        data_path=args.data_path,
        config_path=args.config_path,
        db_path=args.db_path,
    )
    resolved_data_path = resolved_settings.data_path
    logger = create_application_logger(data_path=resolved_data_path)

    try:
        configuration = Configuration(
            data_path=args.data_path,
            config_path=args.config_path,
            db_path=args.db_path,
        )
        logger = configuration.logger
        if args.collect_only:
            result = run_pipeline(
                configuration,
                parse_only=args.parse_only,
                collect_only=args.collect_only,
            )
        else:
            result = run_pipeline(configuration, parse_only=args.parse_only)
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
        persist_latest_scrape_results(configuration)

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
