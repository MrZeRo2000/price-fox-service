import pytest
import os
from pathlib import Path

from cfg import CatalogConfig
from scraper import Scraper

@pytest.fixture
def configuration() -> CatalogConfig:
    test_data_path = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/test")))
    test_data_path.mkdir(parents=True, exist_ok=True)
    configuration = CatalogConfig(
        data_path=str(test_data_path),
        config_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json"))
    )
    return configuration


def test_scraper(configuration: CatalogConfig):

    scraper = Scraper(configuration)
    scraper.execute()

