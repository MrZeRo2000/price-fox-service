from pathlib import Path

from models import CatalogData
from repositories import ProductCatalogRepository


def load_catalog_from_json(product_catalog_path: str) -> CatalogData:
    path = Path(product_catalog_path)
    if not path.exists():
        raise ValueError(f"Product catalog path {product_catalog_path} does not exist")
    return CatalogData.model_validate_json(path.read_text(encoding="utf-8"))


def load_catalog_from_database(product_catalog_db_path: str) -> CatalogData:
    repository = ProductCatalogRepository(product_catalog_db_path)
    return repository.load_catalog_data()
