import pytest
import os
from pathlib import Path

from cfg import CatalogConfig
from scraper import Parser

@pytest.fixture
def config() -> CatalogConfig:
    test_data_path = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/test")))
    config = CatalogConfig(
        data_path=str(test_data_path),
        config_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json"))
    )
    return config


def test_parser(config: CatalogConfig):
    parser = Parser(config)
    parser.execute()


def test_parse_watsons_palmolive_old_new_price(config: CatalogConfig):
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_watsons_palmolive_old_new" / "page.html"
    result = parser.parse_file(html_path)
    assert result["price"] == 31.99

def test_parse_page_epicentr_ariel_old_new_price(config: CatalogConfig):
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_epicentr_ariel_old_new"  / "page.html"
    result = parser.parse_file(html_path)
    assert result["price"] == 329
