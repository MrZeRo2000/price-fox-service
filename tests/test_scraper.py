import pytest
import os
from pathlib import Path

from cfg import Configuration
from scraper import Scraper

@pytest.fixture
def configuration() -> Configuration:
    test_data_path = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/test")))
    test_data_path.mkdir(parents=True, exist_ok=True)
    configuration = Configuration(
        data_path=str(test_data_path),
        config_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json"))
    )
    return configuration


def test_scraper(configuration: Configuration):

    scraper = Scraper(configuration)
    scraper.execute()

