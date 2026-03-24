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
            urls = self._fetch_urls(connection)
            categories = self._fetch_categories(connection)
            category_ids_by_product = self._fetch_category_ids_by_product(connection)
            url_ids_by_product = self._fetch_url_ids_by_product(connection)
            products = self._fetch_products(
                connection=connection,
                category_ids_by_product=category_ids_by_product,
                url_ids_by_product=url_ids_by_product,
            )

        return CatalogData.model_validate(
            {
                "urls": urls,
                "categories": categories,
                "products": products,
            }
        )

    @staticmethod
    def _fetch_urls(connection: sqlite3.Connection) -> list[dict]:
        rows = connection.execute(
            "SELECT id, url FROM urls ORDER BY id"
        ).fetchall()
        return [{"url_id": row[0], "url": row[1]} for row in rows]

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
    def _fetch_url_ids_by_product(connection: sqlite3.Connection) -> dict[int, list[int]]:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(product_urls)").fetchall()
        }
        if "is_active" in columns:
            rows = connection.execute(
                """
                SELECT product_id, url_id
                FROM product_urls
                WHERE is_active = 1
                ORDER BY product_id, url_id
                """
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT product_id, url_id FROM product_urls ORDER BY product_id, url_id"
            ).fetchall()
        mapping: dict[int, list[int]] = {}
        for product_id, url_id in rows:
            mapping.setdefault(product_id, []).append(url_id)
        return mapping

    @staticmethod
    def _fetch_products(
        connection: sqlite3.Connection,
        category_ids_by_product: dict[int, list[int]],
        url_ids_by_product: dict[int, list[int]],
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
                    "url_ids": url_ids_by_product.get(product_id, []),
                }
            )
        return products
