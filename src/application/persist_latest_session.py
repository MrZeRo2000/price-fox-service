from cfg import CatalogConfig
from collector import ScrapeDetailedCollector
from processor import (
    ScrapeAnalysisProcessor,
    ScrapeConsolidatedProcessor,
    ScrapeStatsProcessor,
)
from repositories import ScrapeDetailedRepository, VersionsRepository


def persist_latest_scrape_results(catalog_config: CatalogConfig) -> dict:
    logger = catalog_config.logger
    if catalog_config.db_connection is None:
        logger.warning(
            "Skipping scrape result persistence because no product catalog DB connection is available."
        )
        return {
            "session_date": None,
            "deleted_rows": 0,
            "saved_rows": 0,
            "consolidated": None,
            "analysis": None,
            "stats": None,
        }

    scrape_detailed_collector = ScrapeDetailedCollector(
        data_path=catalog_config.data_path,
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
            "analysis": None,
            "stats": None,
        }

    db_connection = catalog_config.db_connection
    scrape_detailed_repository = ScrapeDetailedRepository(db_connection)
    persisted_results = scrape_detailed_repository.replace_session_rows(
        session_date=session_date,
        rows=rows,
    )
    scrape_consolidated_processor = ScrapeConsolidatedProcessor(db_connection)
    consolidated_results = scrape_consolidated_processor.replace_for_session(
        session_date=session_date
    )
    scrape_analysis_processor = ScrapeAnalysisProcessor(db_connection)
    analysis_results = scrape_analysis_processor.refresh()
    scrape_stats_processor = ScrapeStatsProcessor(db_connection)
    stats_results = scrape_stats_processor.refresh()
    versions_repository = VersionsRepository(db_connection)
    version_update_result = versions_repository.touch_scrape_version()
    logger.info(
        f"Persisted scrape session_date={persisted_results['session_date']} "
        f"(deleted={persisted_results['deleted_rows']}, saved={persisted_results['saved_rows']}, "
        f"purged_old={persisted_results['purged_rows']}, "
        f"retention_days={persisted_results['retention_days']}, "
        f"cutoff={persisted_results['retention_cutoff_date']})."
    )
    logger.info(
        f"Refreshed scrape_consolidated for session_date={consolidated_results['session_date']} "
        f"(deleted={consolidated_results['deleted_rows']}, saved={consolidated_results['saved_rows']})."
    )
    logger.info(
        f"Refreshed scrape_analysis (deleted={analysis_results['deleted_rows']}, "
        f"saved={analysis_results['saved_rows']})."
    )
    logger.info(
        f"Refreshed scrape_stats (deleted={stats_results['deleted_rows']}, "
        f"saved={stats_results['saved_rows']})."
    )
    logger.info(
        "Updated versions scrape_version "
        f"(updated_rows={version_update_result['updated_rows']}, "
        f"row_count={version_update_result['row_count']})."
    )
    return {
        "session_date": persisted_results["session_date"],
        "deleted_rows": persisted_results["deleted_rows"],
        "saved_rows": persisted_results["saved_rows"],
        "purged_rows": persisted_results["purged_rows"],
        "retention_days": persisted_results["retention_days"],
        "retention_cutoff_date": persisted_results["retention_cutoff_date"],
        "consolidated": consolidated_results,
        "analysis": analysis_results,
        "stats": stats_results,
        "versions": version_update_result,
    }
