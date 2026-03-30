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
        rows: list[tuple[int, int, int, str, int, Optional[int], Optional[str]]],
    ) -> dict:
        try:
            with sqlite3.connect(self._db_path) as connection:
                # Backfill older DBs that were created before parse_error existed.
                table_columns = {
                    row[1]
                    for row in connection.execute(f"PRAGMA table_info({self.TABLE_NAME})").fetchall()
                }
                if table_columns and "parse_error" not in table_columns:
                    connection.execute(
                        f"ALTER TABLE {self.TABLE_NAME} ADD COLUMN parse_error TEXT"
                    )

                deleted_rows = connection.execute(
                    f"DELETE FROM {self.TABLE_NAME} WHERE session_date = ?",
                    (session_date,),
                ).rowcount
                connection.executemany(
                    (
                        f"INSERT INTO {self.TABLE_NAME} "
                        "(session_date, product_id, url_id, url, parsed_status, parsed_value, parse_error) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)"
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
