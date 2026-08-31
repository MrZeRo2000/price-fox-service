from datetime import datetime, timedelta
from typing import Optional

from .sqlite_base_repository import BaseSqliteRepository


class ScrapeDetailedRepository(BaseSqliteRepository):
    """Repository responsible for scrape_detailed table communication."""

    TABLE_NAME = "scrape_detailed"
    RETENTION_DAYS = 30

    def replace_session_rows(
        self,
        session_date: int,
        rows: list[tuple[int, int, int, str, int, Optional[int], Optional[str]]],
    ) -> dict:
        connection = self._connection
        try:
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
            cutoff_date = int(
                (
                    datetime.strptime(str(session_date), "%Y%m%d")
                    - timedelta(days=self.RETENTION_DAYS - 1)
                ).strftime("%Y%m%d")
            )
            purged_rows = connection.execute(
                f"DELETE FROM {self.TABLE_NAME} WHERE session_date < ?",
                (cutoff_date,),
            ).rowcount
        except Exception as exc:
            if "no such table" in str(exc).lower():
                raise RuntimeError(
                    f"Table '{self.TABLE_NAME}' does not exist in the product catalog database."
                ) from exc
            raise

        return {
            "session_date": session_date,
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": len(rows),
            "purged_rows": purged_rows if purged_rows is not None else 0,
            "retention_days": self.RETENTION_DAYS,
            "retention_cutoff_date": cutoff_date,
        }
