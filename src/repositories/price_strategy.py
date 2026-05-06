from urllib.parse import urlparse

from .sqlite_base_repository import BaseSqliteRepository


class PriceStrategyRepository(BaseSqliteRepository):
    """Repository for domain-to-price-strategy configuration."""

    def __init__(self, db_path: str):
        super().__init__(
            db_path,
            missing_db_message=f"Product catalog db path {db_path} does not exist",
        )

    def ensure_schema(self) -> None:
        with self._connect() as connection:
            connection.executemany(
                "INSERT OR IGNORE INTO strategies (strategy_name) VALUES (?)",
                [("gemini_url",), ("playwright",), ("jina",)],
            )
            connection.execute(
                """
                UPDATE strategy_domains
                SET strategy_id = (
                    SELECT id FROM strategies WHERE strategy_name = 'playwright'
                )
                WHERE strategy_id = (
                    SELECT id FROM strategies WHERE strategy_name = 'default'
                )
                """
            )
            connection.execute(
                "DELETE FROM strategies WHERE strategy_name = 'default'"
            )
            connection.executemany(
                "INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value) VALUES (?, ?)",
                [
                    ("default_fetch_strategy", "playwright"),
                    ("jina_rate_limit_rpm", "20"),
                    ("gemini_model", "gemini-2.0-flash"),
                    ("gemini_timeout_seconds", "45"),
                ],
            )

    def load_domain_strategy_overrides(self) -> dict[str, str]:
        self.ensure_schema()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT sd.domain, s.strategy_name
                FROM strategy_domains sd
                JOIN strategies s ON s.id = sd.strategy_id
                ORDER BY sd.id
                """
            ).fetchall()
        mapping: dict[str, str] = {}
        for domain, strategy_name in rows:
            domain_key = self._normalize_domain(domain)
            strategy = str(strategy_name or "").strip().lower()
            if not domain_key or not strategy:
                continue
            mapping[domain_key] = strategy
        return mapping

    @staticmethod
    def _normalize_domain(domain_value) -> str:
        raw = str(domain_value or "").strip().lower()
        if not raw:
            return ""
        if "://" in raw:
            host = (urlparse(raw).hostname or "").strip().lower()
            return host
        return raw

    def load_settings(self) -> dict[str, str]:
        self.ensure_schema()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT setting_key, setting_value
                FROM strategy_settings
                """
            ).fetchall()
        settings: dict[str, str] = {}
        for key, value in rows:
            settings[str(key).strip().lower()] = str(value).strip()
        return settings
