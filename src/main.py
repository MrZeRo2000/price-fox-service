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
from turso_sync import (
    TursoSyncClient,
    backup_sqlite_before_cloud_pull,
    load_turso_sync_configuration,
)
from version import APP_VERSION


def _local_backup_note_before_turso_pull(db_path: str) -> str:
    """If the catalog DB exists, back it up and return a log suffix; else ``''``."""
    if not os.path.exists(db_path):
        return ""
    backup = backup_sqlite_before_cloud_pull(db_path)
    if backup.get("status") == "success":
        return f" Local backup: '{backup['backup_path']}'."
    return ""


def _require_turso_pull_success(
    pull_result: dict,
    *,
    phase: str,
    db_path: str,
    turso_config,
    logger,
) -> None:
    if pull_result.get("status") == "success":
        return
    reason = pull_result.get("reason", "unknown")
    msg = (
        f"Turso {phase} pull was required but did not succeed "
        f"(status={pull_result.get('status')}, reason={reason}). "
        f"Local catalog DB path: '{db_path}'. "
        f"Update '{turso_config.config_path}' (set enabled=true and valid url/auth_token), "
        f"or pass --config-path to use a JSON catalog and skip the SQLite DB."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def _require_turso_push_success(push_result: dict, *, turso_config, logger) -> None:
    if push_result.get("status") == "success":
        return
    reason = push_result.get("reason", "unknown")
    msg = (
        f"Turso push was required but did not complete "
        f"(status={push_result.get('status')}, reason={reason}). "
        f"Config: '{turso_config.config_path}'. "
        f"Fix Turso settings or resolve the error above so the local DB is pushed to the remote."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def _log_resolved_configuration(
    logger,
    configuration: Configuration,
    args: argparse.Namespace,
    turso_attempt_db_sync: bool,
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
            "turso_attempt_db_sync": turso_attempt_db_sync,
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
        help=(
            "Request Turso sync with the remote DB. When config/turso.json has enabled, url, "
            "and auth_token, pull before the run and push after are mandatory even without this flag."
        ),
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

    turso_config = load_turso_sync_configuration()
    use_sqlite_catalog = args.config_path is None
    attempt_db_sync = use_sqlite_catalog and (
        turso_config.is_ready or args.sync
    )

    try:
        turso_sync_client = None
        did_bootstrap_pull = False

        if attempt_db_sync:
            db_path = resolved_settings.product_catalog_db_path
            turso_sync_client = TursoSyncClient(
                config=turso_config,
                db_path=db_path,
            )
            if not os.path.exists(db_path):
                logger.info(
                    f"Local catalog DB is missing at '{db_path}'. "
                    "Attempting bootstrap pull from Turso."
                )
                backup_note = ""
                pull_result = turso_sync_client.pull_from_remote()
                if pull_result["status"] == "success":
                    did_bootstrap_pull = True
                    logger.info(
                        f"Turso bootstrap pull completed ({pull_result['direction']}) "
                        f"for DB '{pull_result['db_path']}' via mode "
                        f"'{pull_result.get('mode', 'unknown')}'.{backup_note}"
                    )
                _require_turso_pull_success(
                    pull_result,
                    phase="bootstrap",
                    db_path=db_path,
                    turso_config=turso_config,
                    logger=logger,
                )
                if not os.path.exists(db_path):
                    raise RuntimeError(
                        f"Turso bootstrap pull reported success but local DB is still missing "
                        f"at '{db_path}'."
                    )

        configuration = Configuration(
            data_path=args.data_path,
            config_path=args.config_path,
            db_path=args.db_path,
        )
        logger = configuration.logger
        _log_resolved_configuration(
            logger, configuration, args, turso_attempt_db_sync=attempt_db_sync
        )

        if attempt_db_sync and configuration.product_catalog_db_path is not None:
            if turso_sync_client is None:
                turso_sync_client = TursoSyncClient(
                    config=turso_config,
                    db_path=configuration.product_catalog_db_path,
                )
            if not did_bootstrap_pull:
                backup_note = _local_backup_note_before_turso_pull(
                    configuration.product_catalog_db_path
                )
                pull_result = turso_sync_client.pull_from_remote()
                if pull_result["status"] == "success":
                    logger.info(
                        f"Turso pre-sync completed ({pull_result['direction']}) "
                        f"for DB '{pull_result['db_path']}' via mode "
                        f"'{pull_result.get('mode', 'unknown')}'.{backup_note}"
                    )
                _require_turso_pull_success(
                    pull_result,
                    phase="pre-run",
                    db_path=configuration.product_catalog_db_path,
                    turso_config=turso_config,
                    logger=logger,
                )
        elif args.sync and not use_sqlite_catalog:
            logger.info(
                "Turso sync was not applied: product catalog is loaded from JSON (--config-path)."
            )
        elif turso_config.is_ready and not use_sqlite_catalog:
            logger.info(
                "Turso is fully configured, but pull/push apply only when the catalog is loaded "
                "from SQLite (omit --config-path)."
            )
        elif not attempt_db_sync:
            logger.info(
                "Turso DB sync is off (set enabled/url/auth_token in turso.json for mandatory "
                "pull/push with SQLite, or pass --sync to require sync when the config is incomplete)."
            )

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
    if attempt_db_sync and turso_sync_client is not None:
        try:
            push_result = turso_sync_client.push_to_remote()
            if push_result["status"] == "success":
                logger.info(
                    f"Turso post-sync completed ({push_result['direction']}) "
                    f"for DB '{push_result['db_path']}' via mode "
                    f"'{push_result.get('mode', 'unknown')}'."
                )
            _require_turso_push_success(
                push_result, turso_config=turso_config, logger=logger
            )
        except Exception as exc:
            logger.error(f"Turso push failed: {exc}")
            return 1

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
