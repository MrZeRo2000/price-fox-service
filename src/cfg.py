import os
from pathlib import Path

from app_logger import create_application_logger
from models import CatalogData
from repository import ProductCatalogRepository


class Configuration:
    """Class for configuration"""
    def __init__(self, data_path: str = None, config_path: str = None, db_path: str = None):
        data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/")) if data_path is None else data_path
        default_product_catalog_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/product-catalog.json"))
        product_catalog_path = default_product_catalog_path if config_path is None else config_path
        product_catalog_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/db/product-catalog.sqlite")) if db_path is None else db_path

        if not os.path.exists(data_path):
            raise ValueError(f"Data path {data_path} does not exist")

        self._data_path = data_path
        self._logger = create_application_logger(data_path=data_path)
        self._product_catalog_path = product_catalog_path if config_path is not None else None
        self._product_catalog_db_path = product_catalog_db_path if config_path is None else None
        self._product_catalog_data = (
            self.load_configuration_from_json(product_catalog_path)
            if config_path is not None
            else self.load_configuration_from_database(product_catalog_db_path)
        )

    @staticmethod
    def load_configuration_from_json(product_catalog_path: str) -> CatalogData:
        if not os.path.exists(product_catalog_path):
            raise ValueError(f"Product catalog path {product_catalog_path} does not exist")
        return CatalogData.model_validate_json(Path(product_catalog_path).read_text(encoding="utf-8"))

    @staticmethod
    def load_configuration_from_database(product_catalog_db_path: str) -> CatalogData:
        repository = ProductCatalogRepository(product_catalog_db_path)
        return repository.load_catalog_data()

    @property
    def data_path(self):
        return self._data_path

    @property
    def product_catalog_path(self):
        return self._product_catalog_path

    @property
    def product_catalog_db_path(self):
        return self._product_catalog_db_path

    @property
    def product_catalog_data(self):
        return self._product_catalog_data

    @property
    def logger(self):
        return self._logger