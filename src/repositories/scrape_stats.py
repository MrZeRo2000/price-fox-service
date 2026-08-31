from .sqlite_base_repository import BaseSqliteRepository


class ScrapeStatsRepository(BaseSqliteRepository):
    """Repository responsible for scrape_stats table communication."""

    TABLE_NAME = "scrape_stats"

    def has_rows_for_session_date(self, session_date: int) -> bool:
        try:
            row = self._connection.execute(
                f"SELECT 1 FROM {self.TABLE_NAME} WHERE session_date = ? LIMIT 1",
                (session_date,),
            ).fetchone()
            return row is not None
        except Exception as exc:
            if "no such table" in str(exc).lower():
                return False
            raise
