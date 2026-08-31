class ScrapeConsolidatedProcessor:
    """Build and refresh scrape_consolidated from scrape_detailed for one session."""

    TABLE_NAME = "scrape_consolidated"

    def __init__(self, connection):
        self._connection = connection

    def replace_for_session(self, session_date: int) -> dict:
        connection = self._connection
        try:
            deleted_rows = connection.execute(
                f"DELETE FROM {self.TABLE_NAME} WHERE session_date = ?",
                (session_date,),
            ).rowcount

            connection.execute(
                f"""
                INSERT INTO {self.TABLE_NAME} (
                    session_date,
                    product_id,
                    best_url_id,
                    best_url,
                    best_value
                )
                WITH src AS (
                    SELECT
                        session_date,
                        product_id,
                        url_id,
                        url,
                        parsed_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY session_date, product_id
                            ORDER BY parsed_value
                        ) AS rn
                    FROM scrape_detailed
                    WHERE session_date = ? AND parsed_status = 1
                )
                SELECT
                    session_date,
                    product_id,
                    url_id AS best_url_id,
                    url AS best_url,
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
        except Exception as exc:
            if "no such table" in str(exc).lower() and "scrape_detailed" in str(exc).lower():
                raise RuntimeError(
                    "Table 'scrape_detailed' does not exist in the product catalog database."
                ) from exc
            raise

        return {
            "session_date": session_date,
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": saved_rows,
        }
