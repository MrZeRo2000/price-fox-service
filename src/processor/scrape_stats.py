class ScrapeStatsProcessor:
    """Build and refresh scrape_stats from latest scrape_detailed session."""

    TABLE_NAME = "scrape_stats"

    def __init__(self, connection):
        self._connection = connection

    def refresh(self) -> dict:
        connection = self._connection
        try:
            deleted_rows = connection.execute(
                f"DELETE FROM {self.TABLE_NAME}"
            ).rowcount

            connection.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    session_date,
                    successful_count,
                    failed_count
                )
                WITH latest_session AS (
                    SELECT MAX(session_date) AS session_date
                    FROM scrape_detailed
                )
                SELECT
                    ls.session_date,
                    SUM(CASE WHEN sd.parsed_status = 1 THEN 1 ELSE 0 END) AS successful_count,
                    SUM(CASE WHEN sd.parsed_status = 0 THEN 1 ELSE 0 END) AS failed_count
                FROM latest_session ls
                JOIN scrape_detailed sd
                    ON sd.session_date = ls.session_date
                GROUP BY ls.session_date
                """
            )
            saved_rows = connection.execute(
                f"SELECT COUNT(1) FROM {self.TABLE_NAME}"
            ).fetchone()[0]
        except Exception as exc:
            if "no such table" in str(exc).lower() and "scrape_detailed" in str(exc).lower():
                raise RuntimeError(
                    "Table 'scrape_detailed' does not exist in the product catalog database."
                ) from exc
            raise

        return {
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": saved_rows,
        }
