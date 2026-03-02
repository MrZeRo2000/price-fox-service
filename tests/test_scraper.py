import pytest
import os

from cfg import Configuration
from scraper import Scraper

@pytest.fixture
def configuration() -> Configuration:
    configuration = Configuration(
        data_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/")),
        config_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json"))
    )
    return configuration


def test_scraper(configuration: Configuration):

    scraper = Scraper(configuration)
    scraper.execute()

