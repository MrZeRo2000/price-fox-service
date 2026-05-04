from cfg import CatalogConfig
from scraper import Parser, Scraper

from .persist_latest_session import persist_latest_scrape_results


def run_pipeline(
    catalog_config: CatalogConfig, *, parse_only: bool = False, collect_only: bool = False
) -> dict:
    if collect_only:
        persisted_results = persist_latest_scrape_results(catalog_config)
        return {
            "fetch_results": [],
            "parse_results": [],
            "collect_results": persisted_results,
        }

    if parse_only:
        parser = Parser(catalog_config)
        parse_results = parser.execute()
        return {
            "fetch_results": [],
            "parse_results": parse_results,
        }

    scraper = Scraper(catalog_config)
    return scraper.execute()
