import sqlite3
from pathlib import Path

from models import CatalogData
from repositories import ProductCatalogRepository


def load_catalog_from_json(product_catalog_path: str) -> CatalogData:
    path = Path(product_catalog_path)
    if not path.exists():
        raise ValueError(f"Product catalog path {product_catalog_path} does not exist")
    return CatalogData.model_validate_json(path.read_text(encoding="utf-8"))


def load_catalog_from_database(product_catalog_db_path: str, connection=None) -> CatalogData:
    if connection is not None:
        return ProductCatalogRepository(connection).load_catalog_data()

    if not Path(product_catalog_db_path).exists():
        raise ValueError(f"Product catalog db path {product_catalog_db_path} does not exist")
    with sqlite3.connect(product_catalog_db_path) as fallback_connection:
        return ProductCatalogRepository(fallback_connection).load_catalog_data()
