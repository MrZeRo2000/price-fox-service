import os
import sqlite3

from models import CatalogData


class ProductCatalogRepository:
    """Repository responsible for all product-catalog DB communication."""

    def __init__(self, db_path: str):
        if not os.path.exists(db_path):
            raise ValueError(f"Product catalog db path {db_path} does not exist")
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def load_catalog_data(self) -> CatalogData:
        with sqlite3.connect(self._db_path) as connection:
            shops = self._fetch_shops(connection)
            categories = self._fetch_categories(connection)
            category_ids_by_product = self._fetch_category_ids_by_product(connection)
            urls_by_product = self._fetch_urls_by_product(connection)
            products = self._fetch_products(
                connection=connection,
                category_ids_by_product=category_ids_by_product,
                urls_by_product=urls_by_product,
            )

        return CatalogData.model_validate(
            {
                "shops": shops,
                "categories": categories,
                "products": products,
            }
        )

    @staticmethod
    def _fetch_shops(connection: sqlite3.Connection) -> list[dict]:
        rows = connection.execute(
            "SELECT id, name FROM shops ORDER BY id"
        ).fetchall()
        return [{"id": row[0], "name": row[1]} for row in rows]

    @staticmethod
    def _fetch_categories(connection: sqlite3.Connection) -> list[dict]:
        rows = connection.execute(
            "SELECT id, name FROM categories ORDER BY id"
        ).fetchall()
        return [{"id": row[0], "name": row[1]} for row in rows]

    @staticmethod
    def _fetch_category_ids_by_product(connection: sqlite3.Connection) -> dict[int, list[int]]:
        rows = connection.execute(
            "SELECT product_id, category_id FROM product_categories ORDER BY product_id, category_id"
        ).fetchall()
        mapping: dict[int, list[int]] = {}
        for product_id, category_id in rows:
            mapping.setdefault(product_id, []).append(category_id)
        return mapping

    @staticmethod
    def _fetch_urls_by_product(connection: sqlite3.Connection) -> dict[int, list[dict]]:
        rows = connection.execute(
            "SELECT product_id, shop_id, url FROM product_urls ORDER BY product_id, shop_id"
        ).fetchall()
        mapping: dict[int, list[dict]] = {}
        for product_id, shop_id, url in rows:
            mapping.setdefault(product_id, []).append(
                {"shop_id": shop_id, "url": url}
            )
        return mapping

    @staticmethod
    def _fetch_products(
        connection: sqlite3.Connection,
        category_ids_by_product: dict[int, list[int]],
        urls_by_product: dict[int, list[dict]],
    ) -> list[dict]:
        rows = connection.execute(
            "SELECT id, name FROM products ORDER BY id"
        ).fetchall()
        products = []
        for product_id, product_name in rows:
            products.append(
                {
                    "id": product_id,
                    "name": product_name,
                    "category_ids": category_ids_by_product.get(product_id, []),
                    "urls": urls_by_product.get(product_id, []),
                }
            )
        return products
