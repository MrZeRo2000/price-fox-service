from .base_sqlite_repository import BaseSqliteRepository


class VersionsRepository(BaseSqliteRepository):
    """Repository responsible for updates in the versions table."""

    TABLE_NAME = "versions"

    def touch_scrape_version(self) -> dict:
        """
        Update scrape_version using SQLite unixepoch().

        The table is expected to contain exactly one row and is updated in-place.
        """
        connection = self._connection
        updated_rows = connection.execute(
            f"UPDATE {self.TABLE_NAME} SET scrape_version = unixepoch()"
        ).rowcount
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {self.TABLE_NAME}"
        ).fetchone()[0]

        if row_count != 1:
            raise RuntimeError(
                f"Table '{self.TABLE_NAME}' must contain exactly one row, found {row_count}."
            )
        if updated_rows != 1:
            raise RuntimeError(
                f"Expected to update exactly one row in '{self.TABLE_NAME}', updated {updated_rows}."
            )

        return {
            "table": self.TABLE_NAME,
            "updated_rows": updated_rows,
            "row_count": row_count,
        }
