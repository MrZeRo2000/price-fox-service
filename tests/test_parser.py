import os
import json
from datetime import datetime

from cfg import Configuration
from scraper import Fetcher, Parser
from src.models import ScrapeSession


def test_parser_parses_price_from_existing_files(tmp_path, monkeypatch):
    data_path = tmp_path / "data"
    shop_folder = data_path / "1" / "1"
    shop_folder.mkdir(parents=True, exist_ok=True)

    (shop_folder / "page.txt").write_text(
        "Пральний порошок Ariel\nЦіна: 359 грн\nВ наявності",
        encoding="utf-8",
    )
    (shop_folder / "page.html").write_text(
        "<html><body><div class='price'>359 грн</div></body></html>",
        encoding="utf-8",
    )

    # Keep the test fully local and fast: skip model loading.
    def _skip_model_init(self):
        self.generator = None
        self.generator_task = None
        self._generator_init_error = "skipped in test"

    monkeypatch.setattr(Parser, "_init_generator", _skip_model_init)

    configuration = Configuration(
        data_path=str(data_path),
        config_path=os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json")
        ),
    )

    parser = Parser(configuration)
    results = parser.execute()

    assert len(results) == 1
    assert results[0]["product_id"] == 1
    assert results[0]["shop_id"] == 1
    assert results[0]["status"] == "success"
    assert results[0]["price"] == 359

    parsed_path = shop_folder / "parsed.json"
    assert parsed_path.exists()

    parsed_json = json.loads(parsed_path.read_text(encoding="utf-8"))
    assert parsed_json["status"] == "success"
    assert parsed_json["price"] == 359


def test_parser_parses_files_created_by_fetcher(tmp_path, monkeypatch):
    data_path = tmp_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    configuration = Configuration(
        data_path=str(data_path),
        config_path=os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../config/test-product-catalog.json")
        ),
    )

    # Stub network-heavy batch scraping but preserve Fetcher file flow.
    def _fake_batch_scrape_optimized(urls, output_dir="batch_scrapes", delay_between_pages=3):
        output_root = os.path.abspath(output_dir)
        results = []
        for index, _url in enumerate(urls, start=1):
            html_path = os.path.join(output_root, f"mock_{index}.html")
            txt_path = os.path.join(output_root, f"mock_{index}.txt")
            metadata_path = os.path.join(output_root, f"mock_{index}_metadata.json")

            with open(html_path, "w", encoding="utf-8") as html_file:
                html_file.write("<html><body><div class='price'>359 грн</div></body></html>")
            with open(txt_path, "w", encoding="utf-8") as txt_file:
                txt_file.write("Ariel\nЦіна: 359 грн\nВ наявності")
            with open(metadata_path, "w", encoding="utf-8") as metadata_file:
                json.dump({"url": _url}, metadata_file)

            results.append(
                {
                    "url": _url,
                    "status": "success",
                    "html": html_path,
                    "text": txt_path,
                    "metadata": metadata_path,
                }
            )
        return results

    monkeypatch.setattr(Fetcher, "batch_scrape_optimized", staticmethod(_fake_batch_scrape_optimized))

    fetcher = Fetcher(configuration, ScrapeSession(start_datetime=datetime.today()))
    fetch_results = fetcher.execute()
    assert len(fetch_results) == 2

    def _skip_model_init(self):
        self.generator = None
        self.generator_task = None
        self._generator_init_error = "skipped in test"

    monkeypatch.setattr(Parser, "_init_generator", _skip_model_init)

    parser = Parser(configuration)
    parse_results = parser.execute()

    assert len(parse_results) == 2
    for item in parse_results:
        assert item["status"] == "success"
        assert item["price"] == 359

        parsed_path = data_path / str(item["product_id"]) / str(item["shop_id"]) / "parsed.json"
        assert parsed_path.exists()
