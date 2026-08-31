class ScrapeAnalysisProcessor:
    """Build and refresh scrape_analysis from scrape_consolidated."""

    TABLE_NAME = "scrape_analysis"

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
                    product_id,
                    url_id,
                    url,
                    value,
                    diff
                )
                WITH current_day AS (
                    SELECT MAX(session_date) AS session_date
                    FROM scrape_consolidated
                ),
                last_10_days AS (
                    SELECT DISTINCT sc.session_date
                    FROM scrape_consolidated sc
                    JOIN current_day cd ON 1 = 1
                    WHERE sc.session_date < cd.session_date
                    ORDER BY sc.session_date DESC
                    LIMIT 10
                ),
                hist AS (
                    SELECT
                        sc.product_id,
                        sc.best_value
                    FROM scrape_consolidated sc
                    JOIN last_10_days d ON d.session_date = sc.session_date
                    WHERE sc.best_value IS NOT NULL
                ),
                ranked AS (
                    SELECT
                        h.product_id,
                        h.best_value,
                        ROW_NUMBER() OVER (
                            PARTITION BY h.product_id
                            ORDER BY h.best_value
                        ) AS rn,
                        COUNT(*) OVER (
                            PARTITION BY h.product_id
                        ) AS cnt
                    FROM hist h
                ),
                median_per_product AS (
                    SELECT
                        r.product_id,
                        AVG(1.0 * r.best_value) AS median_best_value
                    FROM ranked r
                    WHERE r.rn IN (
                        CAST((r.cnt + 1) / 2 AS INTEGER),
                        CAST((r.cnt + 2) / 2 AS INTEGER)
                    )
                    GROUP BY r.product_id
                ),
                curr AS (
                    SELECT
                        sc.product_id,
                        sc.best_url_id AS url_id,
                        sc.best_url AS url,
                        sc.best_value AS value
                    FROM scrape_consolidated sc
                    JOIN current_day cd
                        ON sc.session_date = cd.session_date
                )
                SELECT
                    c.product_id,
                    c.url_id,
                    c.url,
                    c.value,
                    CASE
                        WHEN m.median_best_value IS NULL
                            OR m.median_best_value = 0
                            OR c.value IS NULL
                        THEN NULL
                        ELSE CAST(
                            ROUND(
                                ((c.value - m.median_best_value) * 100.0)
                                / m.median_best_value,
                                0
                            ) AS INTEGER
                        )
                    END AS diff
                FROM curr c
                LEFT JOIN median_per_product m
                    ON m.product_id = c.product_id
                """
            )
            saved_rows = connection.execute(
                f"SELECT COUNT(1) FROM {self.TABLE_NAME}"
            ).fetchone()[0]
        except Exception as exc:
            if "no such table" in str(exc).lower() and "scrape_consolidated" in str(exc).lower():
                raise RuntimeError(
                    "Table 'scrape_consolidated' does not exist in the product catalog database."
                ) from exc
            raise

        return {
            "deleted_rows": deleted_rows if deleted_rows is not None else 0,
            "saved_rows": saved_rows,
        }
