import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from application import persist_latest_scrape_results, run_pipeline
from logger import create_application_logger
from cfg import Configuration
from config.settings import resolve_configuration_settings
from turso_sync import TursoSyncClient, load_turso_sync_configuration
from version import APP_VERSION


def _log_resolved_configuration(
    logger,
    configuration: Configuration,
    args: argparse.Namespace,
) -> None:
    catalog_source = (
        "json_file"
        if configuration.product_catalog_path is not None
        else "sqlite_database"
    )

    resolved_configuration = {
        "paths": {
            "data_path": configuration.data_path,
            "product_catalog_path": configuration.product_catalog_path,
            "product_catalog_db_path": configuration.product_catalog_db_path,
        },
        "runtime": {
            "parse_only": args.parse_only,
            "collect_only": args.collect_only,
            "print_json": args.print_json,
            "sync": args.sync,
            "catalog_source": catalog_source,
        },
    }
    logger.info("Resolved configuration:")
    logger.info(json.dumps(resolved_configuration, indent=2, ensure_ascii=False))


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
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Enable Turso sync with remote DB (disabled by default).",
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
    logger.info(f"Price Fox version: {APP_VERSION}")

    try:
        turso_sync_client = None
        did_bootstrap_pull = False
        if args.sync and args.config_path is None:
            db_path = resolved_settings.product_catalog_db_path
            turso_config = load_turso_sync_configuration()
            turso_sync_client = TursoSyncClient(
                config=turso_config,
                db_path=db_path,
            )
            if not os.path.exists(db_path):
                logger.info(
                    f"Local catalog DB is missing at '{db_path}'. "
                    "Attempting bootstrap pull from Turso."
                )
                pull_result = turso_sync_client.pull_from_remote()
                if pull_result["status"] == "success":
                    did_bootstrap_pull = True
                    logger.info(
                        f"Turso bootstrap pull completed ({pull_result['direction']}) "
                        f"for DB '{pull_result['db_path']}' via mode '{pull_result.get('mode', 'unknown')}'."
                    )
                elif pull_result["status"] == "skipped":
                    raise ValueError(
                        f"Local catalog DB is missing at '{db_path}', and Turso bootstrap pull "
                        f"was skipped ({pull_result['reason']}). Check "
                        f"'{turso_config.config_path}' and set enabled/url/auth_token."
                    )

        configuration = Configuration(
            data_path=args.data_path,
            config_path=args.config_path,
            db_path=args.db_path,
        )
        logger = configuration.logger
        _log_resolved_configuration(logger, configuration, args)
        if args.sync and configuration.product_catalog_db_path is not None:
            if turso_sync_client is None:
                turso_sync_client = TursoSyncClient(
                    config=configuration.turso,
                    db_path=configuration.product_catalog_db_path,
                )
            if not did_bootstrap_pull:
                pull_result = turso_sync_client.pull_from_remote()
                if pull_result["status"] == "success":
                    logger.info(
                        f"Turso pre-sync completed ({pull_result['direction']}) "
                        f"for DB '{pull_result['db_path']}' via mode '{pull_result.get('mode', 'unknown')}'."
                    )
                elif pull_result["status"] == "skipped":
                    logger.info(
                        f"Turso pre-sync skipped ({pull_result['reason']}) "
                        f"from config '{configuration.turso.config_path}'."
                    )
        elif args.sync:
            logger.info(
                "Turso sync disabled for this run because product catalog is loaded from JSON."
            )
        else:
            logger.info("Turso sync is disabled. Use --sync to enable remote synchronization.")

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
    if turso_sync_client is not None:
        push_result = turso_sync_client.push_to_remote()
        if push_result["status"] == "success":
            logger.info(
                f"Turso post-sync completed ({push_result['direction']}) "
                f"for DB '{push_result['db_path']}' via mode '{push_result.get('mode', 'unknown')}'."
            )
        elif push_result["status"] == "skipped":
            logger.info(
                f"Turso post-sync skipped ({push_result['reason']}) "
                f"from config '{configuration.turso.config_path}'."
            )

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
