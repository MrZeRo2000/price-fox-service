from datetime import datetime

from cfg import CatalogConfig
from models import ScrapeSession

from .fetcher import Fetcher
from .parser import Parser


class Scraper:
    def __init__(self, catalog_config: CatalogConfig):
        self.catalog_config = catalog_config
        self.scrape_session = ScrapeSession(start_datetime=datetime.today())

    def execute(self):
        fetcher = Fetcher(self.catalog_config, self.scrape_session)
        fetch_results = fetcher.execute()

        parser = Parser(self.catalog_config)
        parse_results = parser.execute()

        self.scrape_session.end_datetime = datetime.today()
        return {
            "fetch_results": fetch_results,
            "parse_results": parse_results,
        }


def run_pipeline(catalog_config: CatalogConfig, *, parse_only: bool = False) -> dict:
    """Run fetch + parse (or parse only) and return their results.

    Persistence is deliberately not done here: it needs the shared Turso
    replica connection, which the caller owns. See persist_latest_scrape_results.
    """
    if parse_only:
        parser = Parser(catalog_config)
        return {
            "fetch_results": [],
            "parse_results": parser.execute(),
        }

    return Scraper(catalog_config).execute()
