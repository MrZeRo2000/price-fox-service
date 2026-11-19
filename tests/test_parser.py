import pytest
import os
from pathlib import Path

from cfg import CatalogConfig
from scraper import Parser
from scraper.parser import TextSources

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


def test_parse_out_of_stock_text_marker_fails(config: CatalogConfig):
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_oos_text" / "page.html"
    result = parser.parse_file(html_path)
    assert result["status"] == "failed"
    assert result["price"] is None
    assert result.get("out_of_stock") is True


def test_parse_out_of_stock_schema_availability_fails(config: CatalogConfig):
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_oos_schema" / "page.html"
    result = parser.parse_file(html_path)
    assert result["status"] == "failed"
    assert result["price"] is None
    assert result.get("out_of_stock") is True


def test_deep_out_of_stock_marker_does_not_trigger(config: CatalogConfig):
    # "нет в наличии" appears only in the per-store list far down the page,
    # so the product must still parse successfully with its price.
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_instock_deep_oos" / "page.html"
    result = parser.parse_file(html_path)
    assert not result.get("out_of_stock")
    assert result["price"] == 329


def test_itbox_delivery_widget_unavailable_is_not_out_of_stock(config: CatalogConfig):
    # Bulk scrape captured itbox's delivery widget still loading, showing
    # "Самовивіз: Недоступний у вашому місті". That is a delivery-option row,
    # not product availability; the product is in stock at 304 UAH.
    parser = Parser(config)
    html_path = Path(__file__).parent / "data" / "page_itbox_finish_delivery_loading" / "page.html"
    result = parser.parse_file(html_path)
    assert not result.get("out_of_stock")
    assert result["status"] == "success"
    assert result["price"] == 304


@pytest.mark.parametrize(
    "text",
    [
        # page.txt layout: label and value on one line
        "Купити\nСамовивіз: Недоступний у вашому місті\nКур'єром за адресою: Недоступний у вашому місті",
        # HTML-derived layout: label and value on separate lines
        "Купити\nСамовивіз:\nНедоступний у вашому місті\nКур’єром за адресою:\nНедоступний у вашому місті",
        # Russian
        "Купить\nСамовывоз: Недоступен в вашем городе",
    ],
)
def test_delivery_row_markers_do_not_trigger_out_of_stock(text: str):
    assert Parser._detect_out_of_stock(TextSources(html_path=None, txt_path=None, text=text)) is None


@pytest.mark.parametrize(
    "text",
    [
        "Товар\nНемає в наявності\nСамовивіз: Недоступний у вашому місті",
        # a delivery row first must not hide a real badge further down
        "Самовивіз: Недоступний у вашому місті\nТовар недоступний у вашому регіоні",
        # a non-label "Доставка ..." line above must not hide a real badge
        "Доставка та оплата\nНедоступний у вашому регіоні",
    ],
)
def test_real_out_of_stock_markers_still_trigger(text: str):
    assert Parser._detect_out_of_stock(TextSources(html_path=None, txt_path=None, text=text)) is not None
