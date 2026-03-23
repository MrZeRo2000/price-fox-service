import os
import sqlite3
from typing import Optional


class ScrapeDetailedRepository:
    """Repository responsible for scrape_detailed table communication."""

    TABLE_NAME = "scrape_detailed"

    def __init__(self, db_path: str):
        if not os.path.exists(db_path):
            raise ValueError(f"SQLite database path {db_path} does not exist")
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def replace_session_rows(
        self,
        session_date: int,
        rows: list[tuple[int, int, int, str, int, Optional[int]]],
    ) -> dict:
        try:
            with sqlite3.connect(self._db_path) as connection:
                deleted_rows = connection.execute(
                    f"DELETE FROM {self.TABLE_NAME} WHERE session_date = ?",
                    (session_date,),
                ).rowcount
                connection.executemany(
                    (
                        f"INSERT INTO {self.TABLE_NAME} "
                        "(session_date, product_id, url_id, url, parsed_status, parsed_value) "
                        "VALUES (?, ?, ?, ?, ?, ?)"
                    ),
                    rows,
                )
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc).lower():
                raise RuntimeError(
                    f"Table '{self.TABLE_NAME}' does not exist in SQLite database '{self._db_path}'."
                ) from exc
            raise

        return {
            "session_date": session_date,
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": len(rows),
        }
