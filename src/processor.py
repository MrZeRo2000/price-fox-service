import os
import sqlite3


class ScrapeConsolidatedProcessor:
    """Build and refresh scrape_consolidated from scrape_detailed for one session."""

    TABLE_NAME = "scrape_consolidated"

    def __init__(self, db_path: str):
        if not os.path.exists(db_path):
            raise ValueError(f"SQLite database path {db_path} does not exist")
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def _create_table_if_missing(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                session_date INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                best_shop_id INTEGER NOT NULL,
                best_shop_url TEXT NOT NULL,
                best_value INTEGER,
                PRIMARY KEY (session_date, product_id)
            )
            """
        )

    def replace_for_session(self, session_date: int) -> dict:
        try:
            with sqlite3.connect(self._db_path) as connection:
                self._create_table_if_missing(connection)

                deleted_rows = connection.execute(
                    f"DELETE FROM {self.TABLE_NAME}"
                ).rowcount

                connection.execute(
                    f"""
                    INSERT INTO {self.TABLE_NAME} (
                        session_date,
                        product_id,
                        best_shop_id,
                        best_shop_url,
                        best_value
                    )
                    WITH src AS (
                        SELECT
                            session_date,
                            product_id,
                            shop_id,
                            shop_url,
                            parsed_value,
                            ROW_NUMBER() OVER (
                                PARTITION BY session_date, product_id
                                ORDER BY parsed_value
                            ) AS rn
                        FROM scrape_detailed
                        WHERE session_date = ?
                    )
                    SELECT
                        session_date,
                        product_id,
                        shop_id AS best_shop_id,
                        shop_url AS best_shop_url,
                        parsed_value AS best_value
                    FROM src
                    WHERE rn = 1
                    """,
                    (session_date,),
                )

                saved_rows = connection.execute(
                    f"SELECT COUNT(1) FROM {self.TABLE_NAME} WHERE session_date = ?",
                    (session_date,),
                ).fetchone()[0]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower() and "scrape_detailed" in str(exc).lower():
                raise RuntimeError(
                    f"Table 'scrape_detailed' does not exist in SQLite database '{self._db_path}'."
                ) from exc
            raise

        return {
            "session_date": session_date,
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": saved_rows,
        }
