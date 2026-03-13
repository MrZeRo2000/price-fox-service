import os
from pathlib import Path

from models import CatalogData


class Configuration:
    """Class for configuration"""
    def __init__(self, data_path:str = None, config_path:str = None):
        data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/")) if data_path is None else data_path
        product_catalog_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/product-catalog.json")) if config_path is None else config_path

        if not os.path.exists(data_path):
            raise ValueError(f"Data path {data_path} does not exist")

        if not os.path.exists(product_catalog_path):
            raise ValueError(f"Product catalog path {product_catalog_path} does not exist")

        self._data_path = data_path
        self._product_catalog_path = product_catalog_path
        self._product_catalog_data = CatalogData.model_validate_json(Path(product_catalog_path).read_text(encoding='utf-8'))

        pass

    @property
    def data_path(self):
        return self._data_path

    @property
    def product_catalog_path(self):
        return self._product_catalog_path

    @property
    def product_catalog_data(self):
        return self._product_catalog_data