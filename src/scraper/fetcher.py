import shutil
from datetime import datetime
from pathlib import Path

from cfg import CatalogConfig
from models import ScrapeSession
from session.constants import DATA_SESSION_FOLDER_DATETIME_FORMAT

from logger import logger

from .fetch_strategies import PlaywrightFetchStrategy


class Fetcher:
    """Turns the product catalog into fetch jobs, runs them through the
    playwright fetch strategy, and files each artifact under
    <session>/<product_id>/<url_id>/.

    Fetching itself lives in the strategy; this class only orchestrates.
    """

    def __init__(self, catalog_config: CatalogConfig, scrape_session: ScrapeSession):
        self.catalog_config = catalog_config
        self.scrape_session = scrape_session

    def _prepare_output_path(self) -> Path:
        """
        Create and return this run's timestamped session output root.
        """
        base_data_root = Path(self.catalog_config.data_path)
        base_data_root.mkdir(parents=True, exist_ok=True)
        scrape_root = base_data_root / "scrape"
        scrape_root.mkdir(parents=True, exist_ok=True)
        session_folder_name = self.scrape_session.fetch_start_datetime.strftime(
            DATA_SESSION_FOLDER_DATETIME_FORMAT
        )
        session_data_root = scrape_root / session_folder_name
        session_data_root.mkdir(parents=True, exist_ok=True)
        return session_data_root

    @staticmethod
    def _product_url_output_dir(data_root: Path, product_id: int, url_id: int) -> Path:
        output_dir = data_root / str(product_id) / str(url_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @staticmethod
    def _place_result_into_product_url_folder(result: dict, output_dir: Path) -> dict:
        if result.get("status") != "success":
            return result

        html_target = output_dir / "page.html"
        text_target = output_dir / "page.txt"
        metadata_target = output_dir / "metadata.json"

        shutil.move(result["html"], html_target)
        shutil.move(result["text"], text_target)
        shutil.move(result["metadata"], metadata_target)

        result["html"] = str(html_target)
        result["text"] = str(text_target)
        result["metadata"] = str(metadata_target)
        return result

    def execute(self):
        self.scrape_session.fetch_start_datetime = datetime.today()
        data_root = self._prepare_output_path()
        url_by_id = {
            url.url_id: str(url.url)
            for url in self.catalog_config.product_catalog_data.urls
        }
        jobs = [
            {
                "product_id": product.id,
                "url_id": url_id,
                "url": url_by_id[url_id],
            }
            for product in self.catalog_config.product_catalog_data.products
            for url_id in product.url_ids
        ]

        if not jobs:
            self.scrape_session.fetch_end_datetime = datetime.today()
            return []

        urls = [job["url"] for job in jobs]
        logger.info(
            f"Fetching {len(urls)} URL(s) with the playwright fetch strategy"
        )
        raw_results = PlaywrightFetchStrategy().fetch_batch(
            urls=urls,
            output_dir=str(data_root),
        )

        all_results = []
        for job, result in zip(jobs, raw_results):
            output_dir = self._product_url_output_dir(
                data_root=data_root,
                product_id=job["product_id"],
                url_id=job["url_id"],
            )
            placed_result = self._place_result_into_product_url_folder(result, output_dir)
            all_results.append(
                {
                    "product_id": job["product_id"],
                    "url_id": job["url_id"],
                    "result": placed_result,
                }
            )

        # Keep data root organized strictly by product_id/url_id folders.
        for item in data_root.iterdir():
            if item.is_file():
                item.unlink()

        self.scrape_session.fetch_end_datetime = datetime.today()

        return all_results

    def execiute(self):
        """
        Backward-compatible misspelled alias.
        """
        return self.execute()
