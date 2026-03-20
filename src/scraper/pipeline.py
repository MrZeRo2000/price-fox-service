from datetime import datetime

from cfg import Configuration
from models import ScrapeSession

from .fetcher import Fetcher
from .parser import Parser


class Scraper:
    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self.scrape_session = ScrapeSession(start_datetime=datetime.today())

    def execute(self):
        fetcher = Fetcher(self.configuration, self.scrape_session)
        fetch_results = fetcher.execute()

        parser = Parser(self.configuration)
        parse_results = parser.execute()

        self.scrape_session.end_datetime = datetime.today()
        return {
            "fetch_results": fetch_results,
            "parse_results": parse_results,
        }
